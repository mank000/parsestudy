import gzip
import json
import os
import re
import subprocess
import sys
import tempfile
import threading
import unittest
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import monotonic
from urllib.parse import parse_qs, urlsplit

import psycopg


def vehicle(car_id):
    country = ("CN", "JP", "KR")[car_id % 3]
    total = {"CN": 2_000_000, "JP": 1_500_000, "KR": 1_750_000}[country] + car_id * 500
    return {
        "id": car_id,
        "source": {"country": country},
        "identity": {"title": "Toyota Test", "manufacturer": "Toyota", "model": "Test"},
        "listing": {"sell_status": "active", "year": 2022, "mileage_km": 42_000},
        "specs": {"fuel_type": "gasoline", "engine_power_hp": 150},
        "media": {"main_photo": "https://example.test/photo.jpg"},
        "pricing": {
            "status": "complete", "listing_rub": total - 250_000, "total_rub": total,
            "steps": [{"key": "transport", "label": "Доставка", "amount_rub": 90_000}],
        },
    }


class ApiFixture(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        url = urlsplit(self.path)
        query = parse_qs(url.query)
        self.server.requests[self.path] += 1
        status, extra = 200, {}
        if url.path == "/robots.txt":
            body = self.server.robots.encode()
            status = self.server.robots_status
            if self.server.robots_fails_once:
                status = 503 if self.server.requests[self.path] == 1 else 200
            content_type = "text/plain"
            if status == 503:
                extra["Retry-After"] = self.server.robots_retry_after
        elif url.path == "/api/v2/cars":
            content_type = "application/json"
            if self.server.block:
                status, extra = 429, {"Retry-After": "120"}
                data = {"detail": "Too many requests"}
            else:
                start = 100 if query.get("cursor") else 1
                data = {
                    "data": [vehicle(i) for i in range(start, 121 if start == 100 else 101)],
                    "pagination": {
                        "has_more": start == 1 or self.server.repeat,
                        "next_cursor": self.server.page_cursor if start == 1 or self.server.repeat else None,
                    },
                }
            body = json.dumps(data).encode()
        elif re.fullmatch(r"/api/v2/cars/\d+", url.path):
            content_type = "application/json"
            body = json.dumps({"data": vehicle(int(url.path.split("/")[-1]))}).encode()
        else:
            content_type = "text/plain"
            status, body = 404, b"not found"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        for key, value in extra.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)


class CatalogIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dsn = os.environ.get("TEST_DATABASE_URL")
        if not cls.dsn:
            raise unittest.SkipTest("Укажите TEST_DATABASE_URL для локального PostgreSQL")
        cls.schema = f"test_catalog_{os.getpid()}"
        with psycopg.connect(cls.dsn, autocommit=True) as db:
            db.execute(f"CREATE SCHEMA {cls.schema}")
        cls.dsn = psycopg.conninfo.make_conninfo(cls.dsn, options=f"-c search_path={cls.schema}")

    @classmethod
    def tearDownClass(cls):
        with psycopg.connect(cls.dsn, autocommit=True) as db:
            db.execute(f"DROP SCHEMA {cls.schema} CASCADE")

    def setUp(self):
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), ApiFixture)
        self.server.robots = "User-agent: *\nAllow: /\n"
        self.server.robots_status = 200
        self.server.robots_retry_after = "180"
        self.server.robots_fails_once = False
        self.server.requests = Counter()
        self.server.block = self.server.repeat = False
        self.server.page_cursor = "fixture"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def cli(self, *args, code=0, public=False, satauto=False):
        if args[0] == "satauto-api" and not any(
            flag in args for flag in ("--available-only", "--no-available-only")
        ):
            args = (*args, "--no-available-only")
        origin = f"http://127.0.0.1:{self.server.server_port}"
        entry = ["-m", "catalog"]
        if public:
            entry = ["-c", "import sys; import catalog.__main__ as cli; "
                     "cli.PUBLIC_ORIGIN = sys.argv.pop(1); raise SystemExit(cli.main())", origin]
        if satauto:
            entry = ["-c", "import sys; import catalog.satauto as sat; sat.BASE = sys.argv.pop(1); "
                     "from catalog.__main__ import main; raise SystemExit(main())", origin]
        result = subprocess.run(
            [sys.executable, *entry, *args],
            env={**os.environ, "DATABASE_URL": self.dsn, "CATALOG_API_ORIGIN": origin},
            capture_output=True, text=True, timeout=35,
        )
        self.assertEqual(result.returncode, code, result.stdout + result.stderr)
        return result.stdout

    def test_satauto_queue_defers_bad_card_and_resumes(self):
        from catalog.store import initialize

        class HtmlFixture(ApiFixture):
            def do_GET(self):
                if self.path == "/robots.txt":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                car_id = int(self.path.rsplit("/", 2)[-2].split("-", 1)[0])
                if car_id == 101:
                    self.send_error(410)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                if car_id == 103 and self.server.block:
                    self.wfile.write(b"<p>Unexpected rendering</p>")
                else:
                    data = json.dumps(vehicle(car_id))
                    self.wfile.write(f"<script data-carhub-current-car>{data}</script>".encode())

        self.server.RequestHandlerClass = HtmlFixture
        self.server.block = True
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            with db.cursor() as cursor:
                cursor.executemany("INSERT INTO sitemap_car VALUES (%s,'JP','toyota','test','Toyota',%s)", [
                    (car_id, f"/catalog/japan/toyota/test/{car_id}-toyota/") for car_id in (101, 102, 103)
                ])
        try:
            first = json.loads(self.cli(
                "public-html", "--enrich", "--all-details", "--details", "1", satauto=True,
            ))
            self.assertEqual((first["new_details"], first["deferred_details"], first["missing_details"]), (1, 1, 1))
            queue = json.loads(self.cli("queue", satauto=True))
            self.assertEqual(queue["tasks"], {"pending": 0, "complete": 1, "missing": 1, "deferred": 1})
            self.assertEqual(queue["recent_failures"][0]["car_id"], 103)
            requests = dict(self.server.requests)
            self.cli("public-html", "--enrich", "--details", "3", satauto=True)
            self.assertEqual(dict(self.server.requests), requests)
            with psycopg.connect(self.dsn, autocommit=True) as db:
                db.execute("UPDATE satauto_detail_task SET retry_at=now() WHERE run_id=%s AND car_id=103",
                           (first["run_id"],))
            self.server.block = False
            second = json.loads(self.cli(
                "public-html", "--enrich", "--all-details", "--details", "1", satauto=True,
            ))
            self.assertEqual((second["run_id"], second["new_details"], second["session_attempts"]),
                             (first["run_id"], 1, 2))
            queue = json.loads(self.cli("queue", satauto=True))
            self.assertEqual(queue["tasks"], {"pending": 0, "complete": 2, "missing": 1, "deferred": 0})
        finally:
            with psycopg.connect(self.dsn, autocommit=True) as db:
                db.execute("DELETE FROM sitemap_car WHERE car_id IN (101,102,103)")

    def test_satauto_full_scan_crosses_batch_boundary_and_keeps_pending_tasks(self):
        from catalog.detail_queue import finish_detail, plan_full_scan
        from catalog.store import initialize

        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            with db.transaction(force_rollback=True):
                run_id = db.execute("""
                    INSERT INTO crawl_run (origin, mode, page_limit)
                    VALUES ('fixture', 'listing', 1) RETURNING id
                    """).fetchone()[0]
                for car_id in (703, 702, 701):
                    db.execute("INSERT INTO sitemap_car VALUES (%s,'JP','toyota','test','Toyota',%s)",
                               (car_id, f"/catalog/japan/toyota/test/{car_id}-toyota/"))
                origin = "https://example.test"
                first = plan_full_scan(db, run_id, 1, origin)
                self.assertEqual([car_id for _, car_id in first], [703])
                self.assertEqual(plan_full_scan(db, run_id, 1, origin), first)
                for car_id in (703, 702, 701):
                    self.assertEqual(plan_full_scan(db, run_id, 1, origin)[0][1], car_id)
                    finish_detail(db, run_id, car_id, "complete")
                self.assertEqual(plan_full_scan(db, run_id, 1, origin), [])
                self.assertEqual(db.execute("SELECT before_car_id, exhausted FROM satauto_scan WHERE run_id=%s",
                                            (run_id,)).fetchone(), (701, True))

    def test_satauto_follows_same_card_redirect_and_defers_unrelated_target(self):
        from catalog.store import initialize

        old_path = "/catalog/china/buick/envisionplus/901-buick/"
        new_path = "/catalog/china/buick/envision-plus/901-buick/"
        unrelated = "/catalog/china/buick/envisionplus/902-buick/"

        class RedirectFixture(ApiFixture):
            def do_GET(self):
                if self.path == "/robots.txt":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                if self.path in {old_path, unrelated}:
                    self.send_response(301)
                    self.send_header("Location", new_path if self.path == old_path else "/catalog/")
                    self.end_headers()
                elif self.path == new_path:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    raw = json.dumps(vehicle(901))
                    self.wfile.write(f"<script data-carhub-current-car>{raw}</script>".encode())
                else:
                    self.send_error(404)

        self.server.RequestHandlerClass = RedirectFixture
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            with db.cursor() as cursor:
                cursor.executemany("INSERT INTO sitemap_car VALUES (%s,'CN','buick','envision','Buick',%s)",
                                   [(901, old_path), (902, unrelated)])
        try:
            result = json.loads(self.cli("public-html", "--enrich", "--details", "2", satauto=True))
            self.assertEqual((result["new_details"], result["deferred_details"], result["redirects_followed"]),
                             (1, 1, 1))
            self.assertEqual(self.server.requests[new_path], 1)
            self.assertNotIn("/catalog/", self.server.requests)
            with psycopg.connect(self.dsn) as db:
                self.assertEqual(db.execute(
                    "SELECT path,status FROM satauto_detail_task WHERE run_id=%s AND car_id=901",
                    (result["run_id"],),
                ).fetchone(), (new_path, "complete"))
        finally:
            with psycopg.connect(self.dsn, autocommit=True) as db:
                db.execute("DELETE FROM sitemap_car WHERE car_id IN (901,902)")

    def test_satauto_list_saves_unique_cars_without_requesting_details(self):
        from catalog.inventory import car_html
        from catalog.store import initialize, insert_vehicles

        def article(car_id, price):
            return (
                f'<article data-car-id="{car_id}" data-availability-state="active">'
                f'<a class="satauto-car-card__link" href="/catalog/china/toyota/test/{car_id}-toyota/"></a>'
                '<span class="satauto-car-card__title">Toyota</span>'
                '<span class="satauto-car-card__meta">2024 · 10 000 км · Бензин · Передний</span>'
                f'<span class="satauto-car-card__price">{price}</span></article>'
            )

        class ListingFixture(ApiFixture):
            def do_GET(self):
                if self.path == "/robots.txt":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                if self.path == "/catalog/":
                    links = '<a data-catalog-country href="/catalog/?source_country=CN">Китай</a>'
                    cards = article(801, "2 000 000 ₽")
                    cards = cards.replace("/catalog/china/toyota/test/801-toyota/", "/catalog/japan/toyota/801-toyota/")
                    seed = {"id": 801, "identity": {"trim": "Premium"},
                            "specs": {"engine_power_hp": 170, "transmission": "Автомат"},
                            "pricing": {"listing_rub": 1200000, "total_rub": 2000000}}
                elif self.path == "/catalog/?source_country=CN":
                    links = ""
                    lot = article(802, "Цена по запросу").replace('data-car-id="802"', 'data-car-id="main:Lot802"')
                    lot = lot.replace("/catalog/china/toyota/test/802-toyota/", "/catalog/japan/lot-main-Lot802/")
                    cards = article(801, "2 000 000 ₽") + lot + article(0, "")
                    seed = {"id": "main:Lot802", "specs": {"color": "white"},
                            "identity": {"manufacturer": "Toyota", "model": "Test"}}
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                payload = json.dumps({"data": [seed], "pagination": {"has_more": True}})
                self.wfile.write((f'{links}<div id="catalog-results">{cards}</div>'
                                  f'<script type="application/json" data-catalog-seed>{payload}</script>').encode())

        self.server.RequestHandlerClass = ListingFixture
        origin = f"http://127.0.0.1:{self.server.server_port}"
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            run_id = db.execute("""
                INSERT INTO crawl_run (origin, mode, page_limit, status)
                VALUES (%s,'listing',24,'partial') RETURNING id
                """, (origin,)).fetchone()[0]
            insert_vehicles(db, run_id, [vehicle(800), {"id": 801}])
            db.execute("UPDATE vehicle SET detail=summary WHERE run_id=%s AND car_id=800", (run_id,))
        result = json.loads(self.cli("public-html", "--list-only", satauto=True))
        self.assertEqual((result["run_id"], result["new_records"], result["details"]), (run_id, 1, 1))
        self.assertEqual((result["list_pages"], result["list_records"], result["list_records_priced"]), (2, 2, 1))
        self.assertEqual(result["list_records_skipped"], 1)
        self.assertEqual(dict(self.server.requests), {"/robots.txt": 1, "/catalog/": 1, "/catalog/?source_country=CN": 1})
        with psycopg.connect(self.dsn) as db:
            self.assertEqual(db.execute("""
                SELECT v.summary#>>'{source,listing_id}', v.summary#>>'{specs,color}', v.manufacturer
                FROM vehicle v JOIN satauto_lot_id j USING (car_id)
                WHERE v.run_id=%s
                """, (run_id,)).fetchone(), ("main:Lot802", "white", "Toyota"))
            self.assertEqual(db.execute("""
                SELECT power_hp, listing_rub, total_rub, detail FROM vehicle WHERE run_id=%s AND car_id=801
                """, (run_id,)).fetchone(), (170, 1200000, 2000000, None))
        from unittest.mock import patch

        with patch("catalog.inventory.SAT", origin):
            status, html = car_html(self.dsn, run_id, 801)
        self.assertEqual(status, 200)
        self.assertIn("Комплектация · Premium", html)
        self.assertIn("Коробка передач · Автомат", html)

    def test_satauto_list_continues_from_saved_brand(self):
        class BrandFixture(ApiFixture):
            def do_GET(self):
                if self.path == "/robots.txt":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                if self.path == "/catalog/brands/":
                    content = "".join(f'<a class="satauto-brands__main" href="{href}"></a>' for href in (
                        "/catalog/?manufacturer=Toyota", "https://other.example/catalog/?manufacturer=Fake",
                        "/catalog/?manufacturer=BMW", "/catalog/?manufacturer=BMW",
                    ))
                elif self.path in {"/catalog/", "/catalog/?manufacturer=BMW", "/catalog/?manufacturer=Toyota"}:
                    car_id = {"/catalog/": 811, "/catalog/?manufacturer=BMW": 812,
                              "/catalog/?manufacturer=Toyota": 813}[self.path]
                    content = (
                        '<a href="/catalog/brands/">Марки</a>'
                        f'<div id="catalog-results"><article data-car-id="{car_id}">'
                        f'<a class="satauto-car-card__link" href="/catalog/china/test/model/{car_id}-car/"></a>'
                        '</article></div>'
                    )
                else:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.end_headers()
                self.wfile.write(content.encode())

        self.server.RequestHandlerClass = BrandFixture
        command = ("public-html", "--list-only", "--brand-pages", "1")
        first = json.loads(self.cli(*command, satauto=True))
        self.assertEqual((first["list_pages"], first["brand_pages"], first["brands_remaining"]), (2, 1, 1))
        with psycopg.connect(self.dsn, autocommit=True) as db:
            # Simulate interruption after the first page transaction committed.
            db.execute("UPDATE crawl_run SET status='running' WHERE id=%s", (first["run_id"],))
        second = json.loads(self.cli(*command, satauto=True))
        self.assertEqual(second["run_id"], first["run_id"])
        self.assertEqual((second["list_pages"], second["brand_pages"], second["brands_remaining"]), (1, 1, 0))
        self.assertEqual(dict(self.server.requests), {"/robots.txt": 2, "/catalog/": 1, "/catalog/brands/": 2,
                                                      "/catalog/?manufacturer=BMW": 1,
                                                      "/catalog/?manufacturer=Toyota": 1})
        with psycopg.connect(self.dsn) as db:
            self.assertIsNone(db.execute("SELECT cursor FROM crawl_run WHERE id=%s",
                                         (first["run_id"],)).fetchone()[0])

    def test_recent_lane_keeps_archive_cursor_and_refreshes_status_without_deletion(self):
        from catalog.store import initialize
        from psycopg.types.json import Jsonb

        class RecentFixture(ApiFixture):
            def do_GET(self):
                url = urlsplit(self.path)
                if url.path != "/api/carhub/cars":
                    return super().do_GET()
                query = parse_qs(url.query)
                if "_lane" in query or "available_only" in query:
                    self.send_error(400)
                    return
                cursor = query.get("cursor")
                car = vehicle(880002 if cursor else 880001)
                car["source"]["country"] = "CN"
                if self.server.block:
                    car["listing"]["sell_status"] = "sold"
                payload = {"data": [car], "pagination": {
                    "has_more": not bool(cursor), "next_cursor": None if cursor else "archive-next"}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

        self.server.RequestHandlerClass = RecentFixture
        first = json.loads(self.cli("satauto-api", "--country", "CN", "--pages", "1", satauto=True))
        run = first["run_id"]
        with psycopg.connect(self.dsn, autocommit=True) as db:
            car = vehicle(880001)
            car["specs"]["engine_power_hp"] = 999
            car["media"]["photos"] = ["https://example.test/detail-photo.jpg"]
            db.execute("UPDATE vehicle SET detail=%s WHERE run_id=%s AND car_id=880001", (Jsonb(car), run))
        self.server.block = True
        recent = json.loads(self.cli("satauto-api", "--country", "CN", "--lane", "recent", "--restart", "--pages", "1", satauto=True))
        self.assertEqual(recent["lane"], "recent")
        self.assertEqual(recent["new_records"], 0)
        with psycopg.connect(self.dsn, autocommit=True) as db:
            self.assertEqual(db.execute("SELECT cursor FROM satauto_api_scan WHERE run_id=%s AND filter_key='' AND country='CN'", (run,)).fetchone()[0], "archive-next")
            self.assertEqual(db.execute("SELECT availability,power FROM vehicle_inventory WHERE run_id=%s AND car_id=880001", (run,)).fetchone(), ("sold", 999))
            self.assertEqual(db.execute("SELECT detail#>>'{media,photos,0}' FROM vehicle WHERE run_id=%s AND car_id=880001", (run,)).fetchone()[0], "https://example.test/detail-photo.jpg")
            self.assertTrue(db.execute("SELECT last_seen_at>=first_seen_at FROM vehicle_observation WHERE run_id=%s AND car_id=880001", (run,)).fetchone()[0])
        finished = json.loads(self.cli("satauto-api", "--country", "CN", satauto=True))
        self.assertTrue(finished["complete"])
        self.assertEqual(finished["new_records"], 1)

    def test_satauto_api_resumes_server_cursor_without_restarting_other_countries(self):
        from catalog.store import initialize, insert_vehicles

        class SatautoApiFixture(ApiFixture):
            def do_GET(self):
                url = urlsplit(self.path)
                if url.path != "/api/carhub/cars":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                query = parse_qs(url.query)
                country = query["source_country"][0]
                cursor = query.get("cursor", [None])[0]
                if query.get("limit") != ["48"] or query.get("photo_only") != ["false"]:
                    self.send_error(400)
                    return
                if cursor and self.server.expire_cursor:
                    self.send_response(409)
                    self.end_headers()
                    return
                car_id = {"CN": 821 if not cursor else 822, "KR": 823, "JP": 824}[country]
                car = vehicle(car_id)
                car["source"]["country"] = country
                car["listing"]["year"] = int(query.get("year_min", ["2022"])[0])
                next_cursor = "saved+cursor=" if country == "CN" and cursor is None else None
                data = {"data": [car], "pagination": {"has_more": bool(next_cursor),
                                                      "next_cursor": next_cursor}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode())

        self.server.RequestHandlerClass = SatautoApiFixture
        self.server.robots = "User-agent: *\nDisallow: /api/\n"
        self.server.expire_cursor = False
        origin = f"http://127.0.0.1:{self.server.server_port}"
        brand_cursor = "/catalog/?manufacturer=BMW"
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            run_id = db.execute("""
                INSERT INTO crawl_run (origin, mode, page_limit, status, cursor)
                VALUES (%s, 'listing', 24, 'partial', %s) RETURNING id
                """, (origin, brand_cursor)).fetchone()[0]
            insert_vehicles(db, run_id, [{"id": 821, "links": {"detail": "/catalog/china/test/model/821-car/"}}])
            db.execute("UPDATE vehicle SET detail=summary WHERE run_id=%s", (run_id,))

        first = json.loads(self.cli("satauto-api", "--pages", "1", satauto=True))
        self.assertEqual((first["run_id"], first["reason"], first["session_attempts"]), (run_id, "page_budget", 1))
        self.assertEqual(first["countries"]["CN"], {"pages": 1, "done": False, "has_cursor": True})
        self.server.expire_cursor = True
        expired = json.loads(self.cli("satauto-api", "--country", "CN", satauto=True, code=1))
        self.assertEqual(expired["reason"], "cursor_expired")
        self.assertEqual(expired["countries"]["CN"], first["countries"]["CN"])
        self.server.expire_cursor = False
        finished = json.loads(self.cli("satauto-api", satauto=True))
        self.assertEqual((finished["complete"], finished["new_records"], finished["session_attempts"]), (True, 3, 3))
        self.assertEqual(dict(self.server.requests), {
            "/api/carhub/cars?limit=48&source_country=CN&photo_only=false": 1,
            "/api/carhub/cars?limit=48&source_country=CN&photo_only=false&cursor=saved%2Bcursor%3D": 2,
            "/api/carhub/cars?limit=48&source_country=KR&photo_only=false": 1,
            "/api/carhub/cars?limit=48&source_country=JP&photo_only=false": 1,
        })
        unchanged = json.loads(self.cli("satauto-api", satauto=True))
        self.assertEqual((unchanged["reason"], unchanged["session_attempts"]), ("already_complete", 0))
        restarted = json.loads(self.cli("satauto-api", "--country", "CN", "--restart", "--pages", "1", satauto=True))
        self.assertEqual(restarted["countries"]["CN"], first["countries"]["CN"])
        self.assertEqual(restarted["countries"]["KR"], finished["countries"]["KR"])
        filtered_command = ("satauto-api", "--country", "CN", "--year", "2020")
        filtered = json.loads(self.cli(*filtered_command, "--pages", "1", satauto=True))
        self.assertEqual(filtered["filters"], {"year_min": 2020, "year_max": 2020})
        self.assertEqual(filtered["countries"], {"CN": first["countries"]["CN"]})
        resumed = json.loads(self.cli(*filtered_command, satauto=True))
        self.assertTrue(resumed["complete"])
        self.assertEqual(resumed["countries"]["CN"], {"pages": 2, "done": True, "has_cursor": False})
        other_year = json.loads(self.cli("satauto-api", "--country", "CN", "--year", "2021",
                                        "--pages", "1", satauto=True))
        self.assertEqual(other_year["countries"]["CN"]["pages"], 1)
        restarted_filtered = json.loads(self.cli(*filtered_command, "--restart", "--pages", "1", satauto=True))
        self.assertEqual(restarted_filtered["countries"]["CN"]["pages"], 1)
        with psycopg.connect(self.dsn) as db:
            self.assertEqual(db.execute("SELECT cursor FROM crawl_run WHERE id=%s",
                                        (run_id,)).fetchone()[0], brand_cursor)
            self.assertEqual(db.execute("""
                SELECT power_hp, detail IS NOT NULL, summary#>>'{links,detail}'
                FROM vehicle WHERE run_id=%s AND car_id=821
                """, (run_id,)).fetchone(), (150, True, "/catalog/china/test/model/821-car/"))
            self.assertEqual(db.execute("""
                SELECT pages,cursor FROM satauto_api_scan
                WHERE run_id=%s AND country='CN' AND filter_key=''
                """, (run_id,)).fetchone(), (1, "saved+cursor="))
            self.assertEqual(db.execute("""
                SELECT count(*) FROM satauto_api_scan WHERE run_id=%s AND country='CN'
                """, (run_id,)).fetchone()[0], 3)

    def test_satauto_api_runs_countries_concurrently_and_slows_after_503(self):
        class ConcurrentFixture(ApiFixture):
            def do_GET(self):
                url = urlsplit(self.path)
                if url.path != "/api/carhub/cars":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                count = self.server.requests[self.path]
                query = parse_qs(url.query)
                country = query["source_country"][0]
                cursor = query.get("cursor", [None])[0]
                if cursor is None and count == 1:
                    # All countries must reach the server before any first response is sent.
                    self.server.barrier.wait(timeout=5)
                if country == "KR" and cursor is None and count == 1:
                    self.send_response(self.server.transient_status)
                    self.send_header("Retry-After", "0")
                    self.end_headers()
                    return
                car = vehicle({"CN": 831, "KR": 833, "JP": 835}[country] + bool(cursor))
                car["source"]["country"] = country
                next_cursor = None if cursor else country + "-next"
                data = {"data": [car], "pagination": {"has_more": bool(next_cursor),
                                                      "next_cursor": next_cursor}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode())

        self.server.RequestHandlerClass = ConcurrentFixture
        self.server.barrier = threading.Barrier(3)
        self.server.transient_status = 503
        result = json.loads(self.cli("satauto-api", satauto=True))
        self.assertTrue(result["complete"])
        self.assertEqual(result["new_records"], 6)
        self.assertEqual(result["parallel_requests"], 1)
        self.assertEqual(result["retries_503"], 1)
        self.assertEqual(result["session_attempts"], 7)
        self.assertEqual(result["http_statuses"], {"200": 6, "503": 1})
        self.assertTrue(all(state == {"pages": 2, "done": True, "has_cursor": False}
                            for state in result["countries"].values()))
        self.server.requests.clear()
        self.server.transient_status = 502
        repeated = json.loads(self.cli("satauto-api", "--restart", satauto=True))
        self.assertTrue(repeated["complete"])
        self.assertEqual(repeated["parallel_requests"], 1)
        self.assertEqual(repeated["http_statuses"], {"200": 6, "502": 1})
        self.assertEqual(repeated["session_attempts"], 7)

    def test_satauto_public_paths_replace_old_lot_links_in_existing_projection(self):
        from unittest.mock import patch

        from catalog.inventory import car_html, car_source
        from catalog.store import initialize, insert_vehicles

        lot_id = 100000000000000070
        lot = vehicle(lot_id)
        lot["source"]["country"] = "JP"
        lot["source"]["listing_id"] = "main:example"
        lot["links"] = {"detail": "https://satauto.ru/catalog/japan/lot-main-example/"}
        direct = vehicle(9020)
        direct["links"] = {"detail": "/catalog/japan/lexus/9020-lexus-nx/"}
        numeric = vehicle(9021)
        numeric["source"]["country"] = "JP"
        numeric["links"] = {"detail": "/catalog/japan/lexus/nx/9999-other/"}
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            run_id = db.execute("""
                INSERT INTO crawl_run(origin,mode,page_limit) VALUES ('https://satauto.ru','listing',48) RETURNING id
                """).fetchone()[0]
            insert_vehicles(db, run_id, [lot, direct, numeric])
            db.execute("UPDATE vehicle_inventory SET path=%s WHERE run_id=%s AND car_id=%s",
                       (lot["links"]["detail"], run_id, lot_id))
            db.execute("UPDATE vehicle_inventory SET path=NULL WHERE run_id=%s AND car_id=%s", (run_id, 9021))
            initialize(db)
            self.assertEqual(db.execute("""
                SELECT car_id,path FROM vehicle_inventory WHERE run_id=%s ORDER BY car_id
                """, (run_id,)).fetchall(), [
                    (9020, direct["links"]["detail"]),
                    (9021, "/catalog/japan/car/car/9021-car/"),
                    (lot_id, None),
                ])
        self.assertIn('href="https://satauto.ru/catalog/japan/car/car/9021-car/"',
                      car_html(self.dsn, run_id, 9021)[1])
        self.assertIn(f'href="/car/source?run={run_id}&amp;id={lot_id}"', car_html(self.dsn, run_id, lot_id)[1])
        self.assertNotIn("lot-main-example", car_html(self.dsn, run_id, lot_id)[1])
        with patch("catalog.inventory.resolve_auction_page", return_value=(302, "https://satauto.ru/example")) as resolve:
            self.assertEqual(car_source(self.dsn, run_id, lot_id), (302, "https://satauto.ru/example"))
            resolve.assert_called_once_with("main:example")

    def test_satauto_auction_uses_the_same_lot_key_in_the_archive(self):
        from unittest.mock import MagicMock, Mock, patch

        from catalog.japan_lots import resolve_auction_page

        live, archive = Mock(), Mock()
        live.status, archive.status = 404, 200
        live.getheader.return_value = archive.getheader.return_value = None
        archive.read.return_value = b'<main data-car-id="stats:lot123">'
        first, second = MagicMock(), MagicMock()
        first.getresponse.return_value.__enter__.return_value = live
        second.getresponse.return_value.__enter__.return_value = archive
        with patch("catalog.japan_lots.HTTPSConnection", side_effect=[first, second]):
            self.assertEqual(resolve_auction_page("main:lot123"),
                             (302, "https://satauto.ru/catalog/japan/lot-stats-lot123/"))
            self.assertEqual(second.request.call_args.args[:2], ("GET", "/catalog/japan/lot-stats-lot123/"))
        archive.read.return_value = b'<main data-car-id="stats:another-lot">'
        with patch("catalog.japan_lots.HTTPSConnection", side_effect=[first, second]):
            self.assertEqual(resolve_auction_page("main:lot123"), (404, None))
        live.status = 503
        with patch("catalog.japan_lots.HTTPSConnection", return_value=first) as connect:
            self.assertEqual(resolve_auction_page("main:lot123"), (503, None))
            connect.assert_called_once()

    def test_satauto_gateway_burst_sets_shared_pause(self):
        class GatewayFixture(ApiFixture):
            def do_GET(self):
                self.server.requests[self.path] += 1
                if self.server.requests[self.path] == 1:
                    self.server.barrier.wait(timeout=5)
                self.send_response(502)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(b"<title>502 Bad Gateway</title>")

        self.server.RequestHandlerClass = GatewayFixture
        self.server.barrier = threading.Barrier(3)
        result = json.loads(self.cli("satauto-api", satauto=True, code=1))
        self.assertEqual(result["reason"], "gateway_backoff")
        self.assertTrue(result["http_502"]["html_bad_gateway"])
        self.assertIsNotNone(result["retry_at"])
        attempts = sum(self.server.requests.values())
        self.cli("satauto-api", satauto=True, code=1)
        self.assertEqual(sum(self.server.requests.values()), attempts)

    def test_satauto_archive_defers_one_gateway_failure_and_resumes_with_saved_cursor(self):
        class GatewayFixture(ApiFixture):
            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query)
                self.server.requests[self.path] += 1
                country = query["source_country"][0]
                page = int(query.get("cursor", ["0"])[0])
                if country == "CN" and page == 1 and self.server.block:
                    self.send_response(502)
                    self.end_headers()
                    return
                car = vehicle({"CN": 1200, "JP": 1300, "KR": 1400}[country] + page)
                car["source"]["country"] = country
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"data": [car], "pagination": {
                    "has_more": page < 3, "next_cursor": str(page + 1)}}).encode())

        from catalog.dashboard import database_sections
        from catalog.store import initialize

        def counters():
            with psycopg.connect(self.dsn) as db:
                return db.execute("""
                    SELECT coalesce(sum(attempts),0), coalesce(sum(responses),0),
                           coalesce(sum(ok),0), coalesce(sum(bad_gateway),0)
                    FROM satauto_http_daily
                    """).fetchone()

        with psycopg.connect(self.dsn) as db:
            initialize(db)
        before = counters()
        self.server.RequestHandlerClass = GatewayFixture
        self.server.block = True
        stalled = json.loads(self.cli("satauto-api", satauto=True, code=1))
        self.assertEqual(stalled["reason"], "partition_backoff")
        self.assertEqual(stalled["deferred_scans"], 1)
        self.assertEqual(stalled["parallel_requests"], 3)
        self.assertEqual(stalled["http_statuses"], {"200": 9, "502": 3})
        self.assertTrue(stalled["countries"]["JP"]["done"])
        self.assertTrue(stalled["countries"]["KR"]["done"])
        paused = json.loads(self.cli("satauto-api", satauto=True, code=1))
        self.assertEqual(paused["session_attempts"], 0)
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute("UPDATE satauto_api_scan SET retry_at=now() WHERE run_id=%s", (stalled["run_id"],))
        self.server.block = False
        resumed = json.loads(self.cli("satauto-api", satauto=True))
        self.assertTrue(resumed["complete"])
        self.assertEqual(resumed["session_attempts"], 3)
        self.assertEqual(resumed["records"], 12)

        self.assertEqual(tuple(after - previous for after, previous in zip(counters(), before)), (15, 15, 12, 3))
        from unittest.mock import patch

        with patch("catalog.inventory.SAT", f"http://127.0.0.1:{self.server.server_port}"):
            requests, prices = database_sections(self.dsn)
        self.assertIn("HTTP 200", requests)
        self.assertIn("HTTP 502", requests)
        self.assertIn("Предложения по странам", prices)
        self.assertNotIn("прогон", prices.lower())

    def test_satauto_retry_after_blocks_new_requests_to_other_countries(self):
        class RetryAfterFixture(ApiFixture):
            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query)
                self.server.requests[self.path] += 1
                country = query["source_country"][0]
                if country == "CN" and self.server.requests[self.path] == 1:
                    self.send_response(429)
                    self.send_header("Retry-After", "2")
                    self.end_headers()
                    self.server.first_429 = monotonic()
                    self.server.rate_limit_sent.set()
                    return
                if country == "JP":
                    self.server.rate_limit_sent.wait(timeout=5)
                    threading.Event().wait(0.25)
                if country == "KR":
                    self.server.kr_delay = monotonic() - self.server.first_429
                car = vehicle({"CN": 1200, "JP": 1300, "KR": 1400}[country])
                car["source"]["country"] = country
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"data": [car], "pagination": {"has_more": False}}).encode())

        self.server.RequestHandlerClass = RetryAfterFixture
        self.server.rate_limit_sent = threading.Event()
        result = json.loads(self.cli("satauto-api", "--workers", "2", satauto=True))
        self.assertTrue(result["complete"])
        self.assertGreaterEqual(self.server.kr_delay, 1.9)
        self.assertEqual(result["session_attempts"], 4)

    def test_satauto_api_year_partitions_resume_from_country_cursor(self):
        from catalog.store import initialize, insert_vehicles

        years = {840: 2014, 841: 2010, 842: 2017, 843: 2022, 844: 2011, 845: 2019, 846: 2024}

        class PartitionFixture(ApiFixture):
            def do_GET(self):
                url = urlsplit(self.path)
                if url.path != "/api/carhub/cars":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                query = parse_qs(url.query)
                after = int(query.get("cursor", ["0"])[0])
                lower = int(query.get("year_min", ["0"])[0])
                upper = int(query.get("year_max", ["9999"])[0])
                ids = [car_id for car_id, year in years.items() if car_id > after and lower <= year <= upper]
                rows = []
                for car_id in ids[:1]:
                    car = vehicle(car_id)
                    car["source"]["country"] = "CN"
                    car["listing"]["year"] = years[car_id]
                    rows.append(car)
                more = len(ids) > 1
                payload = {"data": rows, "pagination": {
                    "has_more": more, "next_cursor": str(ids[0]) if more else None,
                }}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

        self.server.RequestHandlerClass = PartitionFixture
        origin = f"http://127.0.0.1:{self.server.server_port}"
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            run_id = db.execute("""
                INSERT INTO crawl_run(origin,mode,page_limit,status) VALUES (%s,'listing',48,'partial')
                RETURNING id
                """, (origin,)).fetchone()[0]
            car = vehicle(840)
            car["source"]["country"] = "CN"
            car["listing"]["year"] = years[840]
            insert_vehicles(db, run_id, [car])
            db.execute("""
                INSERT INTO satauto_api_scan (run_id,country,cursor,pages)
                VALUES (%s,'CN','840',5)
                """, (run_id,))
        first = json.loads(self.cli("satauto-api", "--country", "CN", "--workers", "6", "--pages", "3", satauto=True))
        self.assertEqual((first["new_records"], first["session_attempts"], first["complete"]), (3, 3, False))
        self.assertEqual(first["countries"]["CN"]["pages"], 8)
        self.assertEqual(len(first["partitions"]), 3)
        self.assertTrue(all(partition["pages"] == 1 and not partition["done"] for partition in first["partitions"]))
        resumed = json.loads(self.cli("satauto-api", "--country", "CN", "--workers", "2", satauto=True))
        self.assertEqual((resumed["new_records"], resumed["session_attempts"], resumed["complete"]), (3, 3, True))
        self.assertEqual(resumed["countries"]["CN"], {"pages": 11, "done": True, "has_cursor": False})
        self.assertTrue(all(partition["pages"] == 2 and partition["done"] for partition in resumed["partitions"]))
        with psycopg.connect(self.dsn) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM vehicle WHERE run_id=%s", (run_id,)).fetchone()[0], 7)
        self.assertEqual(sum(self.server.requests.values()), 6)
        restarted = json.loads(self.cli("satauto-api", "--country", "CN", "--workers", "6",
                                        "--restart", "--pages", "1", satauto=True))
        self.assertEqual(restarted["countries"]["CN"]["pages"], 1)
        self.assertEqual(sum(partition["pages"] for partition in restarted["partitions"]), 1)
        next_page = json.loads(self.cli("satauto-api", "--country", "CN", "--workers", "1",
                                        "--pages", "1", satauto=True))
        self.assertEqual([partition["pages"] for partition in next_page["partitions"]], [2, 0, 0])

    def test_satauto_api_available_only_survives_partition_resume(self):
        class AvailableFixture(ApiFixture):
            def do_GET(self):
                self.server.requests[self.path] += 1
                query = parse_qs(urlsplit(self.path).query)
                if query.get("available_only") != ["true"] or "cursor" in query:
                    self.send_error(400)
                    return
                country = query["source_country"][0]
                year = int(query.get("year_min", ["2010"])[0])
                car = vehicle(1100 + {"CN": 0, "KR": 10, "JP": 20}[country] + (year > 2015) + (year >= 2020))
                car["source"]["country"] = country
                car["listing"].update(year=year, sell_status="active")
                payload = {"data": [car], "pagination": {"has_more": False, "next_cursor": None}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

        self.server.RequestHandlerClass = AvailableFixture
        first = json.loads(self.cli("satauto-api", "--available-only", "--workers", "6", "--pages", "2", satauto=True))
        self.assertFalse(first["complete"])
        resumed = json.loads(self.cli("satauto-api", "--available-only", "--workers", "2", satauto=True))
        self.assertTrue(resumed["complete"])
        self.assertEqual(resumed["filters"], {"available_only": "true"})
        self.assertEqual(len(resumed["partitions"]), 9)
        self.assertEqual(sum(self.server.requests.values()), 9)

    def test_satauto_api_recovers_catalog_version_change(self):
        class VersionFixture(ApiFixture):
            def do_GET(self):
                query = parse_qs(urlsplit(self.path).query)
                self.server.requests[self.path] += 1
                cursor = query.get("cursor", [None])[0]
                status = 200
                if cursor == "expired":
                    status = 409
                    payload = {"detail": "Catalog version changed; restart from the first page"}
                else:
                    car = vehicle(851 if cursor is None else 852)
                    car["source"]["country"] = "CN"
                    payload = {"data": [car], "pagination": {
                        "has_more": cursor is None, "next_cursor": "fresh" if cursor is None else None}}
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

        from catalog.store import initialize, insert_vehicles
        self.server.RequestHandlerClass = VersionFixture
        origin = f"http://127.0.0.1:{self.server.server_port}"
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            run_id = db.execute("""
                INSERT INTO crawl_run(origin,mode,page_limit,status) VALUES (%s,'listing',48,'partial')
                RETURNING id
                """, (origin,)).fetchone()[0]
            insert_vehicles(db, run_id, [vehicle(850)])
            db.execute("INSERT INTO satauto_api_scan(run_id,country,cursor) VALUES (%s,'CN','expired')", (run_id,))
        result = json.loads(self.cli("satauto-api", "--country", "CN", satauto=True))
        self.assertTrue(result["complete"])
        self.assertEqual((result["cursor_resets"], result["session_attempts"], result["new_records"]), (1, 3, 2))
        self.assertEqual(result["http_statuses"], {"409": 1, "200": 2})
        self.assertEqual(result["records"], 3)

    def test_satauto_api_refines_only_the_invalidated_archive_range(self):
        from unittest.mock import Mock, patch

        from catalog.satauto_api import ApiScan, SatautoApiSpider, prepare_scans
        from catalog.store import initialize, insert_vehicles

        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            run_id = db.execute("""
                INSERT INTO crawl_run(origin,mode,page_limit) VALUES ('test-refine','listing',48) RETURNING id
                """).fetchone()[0]
            cars = [vehicle(car_id) for car_id in range(2000, 2006)]
            for offset, car in enumerate(cars):
                car["source"]["country"] = "CN"
                car["listing"]["year"] = 2020 + offset // 2
            insert_vehicles(db, run_id, cars)
            db.execute("INSERT INTO satauto_api_scan(run_id,country,pages) VALUES (%s,'CN',777)", (run_id,))
            db.execute("""
                INSERT INTO satauto_api_partition(run_id,country,filter_key,partition,year_min,year_max,cursor,pages)
                VALUES (%s,'CN','',0,NULL,2019,'keep',16), (%s,'CN','',1,2020,NULL,'expired',761)
                """, (run_id, run_id))
            expired = ApiScan("CN", "expired", {"year_min": 2020}, 1)
            spider = SatautoApiSpider(db, run_id, [expired], 120, workers=6)
            spider.crawler = Mock()
            db.execute("""
                INSERT INTO crawl_cursor SELECT %s,%s||n FROM generate_series(1,120) AS n
                """, (run_id, spider.cursor_prefix(expired)))
            with patch("catalog.satauto_api.TARGET_RANGE_ROWS", 2):
                replacements = spider.reset_cursor(expired)
            self.assertEqual([scan.filters for scan in replacements], [
                {"year_min": 2020, "year_max": 2020},
                {"year_min": 2021, "year_max": 2021},
                {"year_min": 2022},
            ])
            resumed = prepare_scans(db, run_id, ["CN"], {}, 6)
            self.assertEqual(resumed[0], ApiScan("CN", "keep", {"year_max": 2019}, 0))
            self.assertEqual(resumed[1:], replacements)
            self.assertEqual(db.execute("SELECT pages FROM satauto_api_scan WHERE run_id=%s",
                                        (run_id,)).fetchone()[0], 777)
            spider.crawler.stats.inc_value.assert_any_call("catalog/invalidated_checkpoints", 120)

    def test_satauto_api_retries_transport_and_rate_limit(self):
        import socket

        class FaultFixture(ApiFixture):
            def do_GET(self):
                self.server.requests[self.path] += 1
                attempt = self.server.requests[self.path]
                if attempt == 1:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                if attempt == 2:
                    self.send_response(429)
                    self.send_header("Retry-After", "0")
                    self.end_headers()
                    return
                car = vehicle(861)
                car["source"]["country"] = "CN"
                payload = {"data": [car], "pagination": {"has_more": False, "next_cursor": None}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

        self.server.RequestHandlerClass = FaultFixture
        result = json.loads(self.cli("satauto-api", "--country", "CN", satauto=True))
        self.assertTrue(result["complete"])
        self.assertEqual(result["transport_errors"], 1)
        self.assertEqual(result["http_statuses"], {"429": 1, "200": 1})
        self.assertEqual(result["session_attempts"], 3)

    def test_satauto_api_restores_parallelism_after_healthy_pages(self):
        class RecoveryFixture(ApiFixture):
            def do_GET(self):
                self.server.requests[self.path] += 1
                query = parse_qs(urlsplit(self.path).query)
                country = query["source_country"][0]
                page = int(query.get("cursor", ["0"])[0])
                if country == "KR" and page == 0 and self.server.requests[self.path] == 1:
                    self.send_response(503)
                    self.send_header("Retry-After", "0")
                    self.end_headers()
                    return
                car = vehicle(900 + {"CN": 0, "KR": 40, "JP": 80}[country] + page)
                car["source"]["country"] = country
                payload = {"data": [car], "pagination": {
                    "has_more": page < 19, "next_cursor": str(page + 1) if page < 19 else None}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(payload).encode())

        self.server.RequestHandlerClass = RecoveryFixture
        result = json.loads(self.cli("satauto-api", satauto=True))
        self.assertTrue(result["complete"])
        self.assertEqual(result["parallel_requests"], 3)
        self.assertEqual(result["http_statuses"], {"200": 60, "503": 1})

    def test_satauto_worker_backoff_honors_server_deadline(self):
        from datetime import datetime, timedelta, timezone
        from catalog.worker import retry_delay
        self.assertEqual([retry_delay(n) for n in (1, 2, 3, 7)], [15, 30, 60, 300])
        wait = retry_delay(1, datetime.now(timezone.utc) + timedelta(seconds=600))
        self.assertGreater(wait, 599)
        self.assertLessEqual(wait, 600)

    def test_satauto_year_options_reject_invalid_selections(self):
        for options in (
            ("--year", "1899"), ("--year", "2101"), ("--year-min", "2022", "--year-max", "2020"),
            ("--year", "2020", "--year-min", "2019"),
        ):
            self.cli("satauto-api", *options, satauto=True, code=2)

    def test_trim_reference_enriches_only_missing_verified_technical_fields(self):
        from catalog.inventory import car_html
        from catalog.store import initialize, insert_vehicles
        from catalog.trims import LOCAL_SOURCE

        source = {"name": "Fixture", "country": "CN"}
        identity = {"manufacturer": "Toyota", "model": "Reference", "trim": "2.0 CVT"}
        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            run = db.execute("INSERT INTO crawl_run(origin,mode,page_limit) VALUES ('https://satauto.ru','listing',3) RETURNING id").fetchone()[0]
            cars = []
            for car_id in (401, 402, 403):
                car = vehicle(car_id)
                car["source"] = {**source, "listing_id": str(car_id)}
                car["identity"] = identity
                car["specs"] = {"engine_power_hp": 150, "transmission": "CVT"}
                cars.append(car)
            cars[2]["specs"].pop("transmission")
            insert_vehicles(db, run, cars)
        result = json.loads(self.cli("trim-build", "--run", str(run)))
        self.assertEqual(result["references"], 1)
        checked = json.loads(self.cli("trim-check", "--run", str(run)))
        self.assertEqual((checked["matched"], checked["can_enrich"]), (3, 1))
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute("DELETE FROM trim_reference WHERE source=%s", (LOCAL_SOURCE,))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "specs.ndjson"
            reference = {**cars[0], "reference_source": "Учебный справочник", "reference_id": "example-1"}
            reference["specs"] = {**reference["specs"], "color": "Синий", "total_rub": 99}
            path.write_text(json.dumps(reference), encoding="utf-8")
            self.assertEqual(self.cli("trim-import", str(path)).strip(), "1")
            checked = json.loads(self.cli("trim-check", "--run", str(run)))
            self.assertEqual((checked["matched"], checked["can_enrich"]), (3, 1))
            status, page = car_html(self.dsn, run, 403)
            self.assertEqual(status, 200)
            self.assertIn("Коробка передач · CVT", page)
            self.assertIn("Учебный справочник", page)
            self.assertNotIn("Синий", page)
            other = {**reference, "reference_id": "example-2"}
            path.write_text(json.dumps(other), encoding="utf-8")
            self.cli("trim-import", str(path))
            checked = json.loads(self.cli("trim-check", "--run", str(run)))
            self.assertEqual((checked["ambiguous"], checked["can_enrich"]), (3, 0))

    def test_inventory_filters_and_price_analytics(self):
        from psycopg.rows import dict_row

        from catalog.inventory import fetch_inventory, filter_choices, filter_form, parse_filters
        from catalog.store import initialize, insert_vehicles
        from psycopg.types.json import Jsonb

        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            with db.transaction(force_rollback=True):
                run_id = db.execute("""
                    INSERT INTO crawl_run (origin, mode, page_limit, status)
                    VALUES ('https://satauto.ru', 'listing', 48, 'partial') RETURNING id
                    """).fetchone()[0]
                cars = [vehicle(10001), vehicle(10002)]
                cars[0]["specs"]["color"] = "Белый"
                cars[0]["identity"]["trim"] = "Luxury"
                cars[0]["specs"].update(engine_displacement_cc=1998, transmission="AT", body_type="Седан", drive_type="AWD")
                cars[0]["pricing"]["sanctioned"] = True
                cars[0]["vehicle_history"] = {"damage_confirmed": False}
                cars[1]["specs"]["color"] = "Чёрный"
                cars[1]["listing"]["sell_status"] = "sold"
                insert_vehicles(db, run_id, cars)
                detail = {**cars[1], "pricing": {"status": "partial", "presentation": {
                    "kind": "known_part", "amount_rub": 700000,
                }}}
                db.execute("UPDATE vehicle SET detail=%s WHERE run_id=%s AND car_id=10002",
                           (Jsonb(detail), run_id))
                db.execute("DELETE FROM vehicle_inventory WHERE run_id=%s", (run_id,))
                initialize(db)
                self.assertEqual(db.execute("SELECT count(*) FROM vehicle_inventory WHERE run_id=%s",
                                             (run_id,)).fetchone()[0], 2)
                carhub = db.execute("""
                    INSERT INTO crawl_run (origin, mode, page_limit, status)
                    VALUES ('https://carhubauto.ru', 'listing', 100, 'partial') RETURNING id
                    """).fetchone()[0]
                insert_vehicles(db, carhub, [vehicle(10001), vehicle(10004)])
                db.execute("""
                    INSERT INTO sitemap_car VALUES
                    (10001,'KR','Toyota','Test','Toyota Test','/catalog/example/10001/'),
                    (10003,'JP','Honda','Fit','Honda Fit','/catalog/example/10003/')
                    """)
                with db.cursor(row_factory=dict_row) as cursor:
                    legacy = parse_filters({"scope": ["all"], "source": ["https://carhubauto.ru"]})
                    self.assertEqual(legacy, {})
                    _, groups, records, _, _ = fetch_inventory(cursor, legacy)
                    self.assertEqual((groups[0]["count"], groups[0]["priced"], groups[0]["partial"]), (2, 1, 1))
                    self.assertEqual([car["car_id"] for car in records], [10002, 10001])
                    _, groups, records, _, _ = fetch_inventory(cursor, {
                        "color": "белый", "availability": "active", "min_price": "1000000",
                        "max_price": "10000000", "sort": "price_up",
                    })
                    self.assertEqual([car["car_id"] for car in records], [10001])
                    self.assertEqual(groups[0]["median"], records[0]["price"])
                    combined = parse_filters({key: [value] for key, value in {
                        "make": "toyota", "model": "Test", "trim": "Luxury",
                        "body": "Седан", "drive": "AWD", "transmission": "AT",
                        "min_engine": "1900", "max_engine": "2000", "min_power": "140", "max_power": "160",
                        "min_mileage": "40000", "max_mileage": "45000",
                        "sanctioned": "yes", "damage_confirmed": "no",
                    }.items()})
                    _, _, matched, _, _ = fetch_inventory(cursor, combined)
                    self.assertEqual([row["car_id"] for row in matched], [10001])
                    choices = filter_choices(cursor, run_id, combined)
                    self.assertEqual(choices["trim"], ["Luxury"])
                    self.assertIn("AT", choices["transmission"])
                    form = filter_form(combined, [{"make": "toyota"}], choices)
                    self.assertIn('name="min_engine"', form)
                    self.assertIn('value="Luxury" selected', form)
                    with self.assertRaises(ValueError):
                        parse_filters({"min_engine": ["2000"], "max_engine": ["1000"]})
                    _, _, records, _, _ = fetch_inventory(cursor, {"availability": "unknown"})
                    self.assertEqual(records, [])
                db.execute("UPDATE vehicle SET summary=%s, detail=NULL WHERE run_id=%s AND car_id=10002",
                           (Jsonb(cars[1]), run_id))
                self.assertEqual(db.execute("""
                    SELECT has_detail, color, full_price, partial_price
                    FROM vehicle_inventory WHERE run_id=%s AND car_id=10002
                    """, (run_id,)).fetchone(), (False, "Чёрный", cars[1]["pricing"]["total_rub"], None))
                db.execute("DELETE FROM vehicle WHERE run_id=%s AND car_id=10002", (run_id,))
                self.assertEqual(db.execute("""
                    SELECT count(*) FROM vehicle_inventory WHERE run_id=%s AND car_id=10002
                    """, (run_id,)).fetchone()[0], 0)

    def test_listing_resume_dedup_and_report(self):
        first = json.loads(self.cli("crawl", "--pages", "1"))
        final = json.loads(self.cli("crawl", "--resume", str(first["run_id"])))
        self.assertEqual((first["status"], first["records"], first["session_attempts"]), ("partial", 100, 2))
        self.assertEqual((final["status"], final["records"], final["session_attempts"]), ("complete", 120, 2))
        self.assertEqual(self.server.requests["/api/v2/cars?limit=100"], 1)
        self.assertEqual(self.server.requests["/api/v2/cars?limit=100&cursor=fixture"], 1)
        report = self.cli("report", "--run", str(first["run_id"]))
        self.assertIn("| Япония | 40 | 40", report)
        self.assertIn("B − A, ₽", report)

    def test_live_length_cursor_reaches_next_page(self):
        self.server.page_cursor = "A" * 2500
        metrics = json.loads(self.cli("crawl"))
        self.assertEqual((metrics["status"], metrics["records"], metrics["pages"]), ("complete", 120, 2))

    def test_full_and_no_media_requests(self):
        # The first response ends pagination with three cards in this fixture.
        class SmallFixture(ApiFixture):
            def do_GET(self):
                if urlsplit(self.path).path != "/api/v2/cars":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                data = {"data": [vehicle(i) for i in range(1, 4)], "pagination": {"has_more": False}}
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(data).encode())

        self.server.RequestHandlerClass = SmallFixture
        metrics = json.loads(self.cli("crawl", "--full"))
        self.assertEqual((metrics["status"], metrics["details"], metrics["session_attempts"]), ("complete", 3, 5))
        self.assertFalse(any("photo" in path for path in self.server.requests))
        self.assertIn("90 000", self.cli("report", "--run", str(metrics["run_id"])))

    def test_429_stops_and_persists_cooldown(self):
        self.server.block = True
        metrics = json.loads(self.cli("crawl", code=1))
        self.assertEqual((metrics["reason"], metrics["session_attempts"]), ("http_429", 2))
        self.assertIsNotNone(metrics["retry_at"])
        self.cli("crawl", "--resume", str(metrics["run_id"]), code=1)
        self.assertEqual(sum(self.server.requests.values()), 2)

    def test_robots_denial_has_no_api_request(self):
        self.server.robots = "User-agent: *\nDisallow: /api/\n"
        metrics = json.loads(self.cli("crawl", code=1))
        self.assertEqual(metrics["reason"], "robots_denied")
        self.assertEqual(dict(self.server.requests), {"/robots.txt": 1})

    def test_unavailable_robots_has_no_api_request(self):
        self.server.robots_status = 503
        metrics = json.loads(self.cli("crawl", code=1))
        self.assertEqual(metrics["reason"], "robots_http_503")
        self.assertEqual(metrics["http_statuses"], {"503": 1})
        self.assertEqual(metrics["http_errors"], 1)
        self.assertEqual(metrics["http_503"]["path"], "/robots.txt")
        self.assertEqual(metrics["http_503"]["retry_after"], "180")
        self.assertEqual(metrics["retries_503"], 0)
        self.assertIsNotNone(metrics["retry_at"])
        self.assertEqual(dict(self.server.requests), {"/robots.txt": 1})

    def test_robots_retry_checks_rules_before_api_request(self):
        self.server.robots_fails_once = True
        self.server.robots_retry_after = "1"
        self.server.robots = "User-agent: *\nDisallow: /api/\n"
        metrics = json.loads(self.cli("crawl", code=1))
        self.assertEqual(metrics["reason"], "robots_denied")
        self.assertEqual(metrics["http_statuses"], {"503": 1, "200": 1})
        self.assertEqual(metrics["recoveries_503"], 1)
        self.assertEqual(dict(self.server.requests), {"/robots.txt": 2})

    def test_503_retries_same_cursor_and_completes_five_pages(self):
        request_times = []

        class FivePageFixture(ApiFixture):
            def do_GET(self):
                url = urlsplit(self.path)
                if url.path != "/api/v2/cars":
                    return super().do_GET()
                self.server.requests[self.path] += 1
                page = int(parse_qs(url.query).get("cursor", ["1"])[0])
                if page == 3:
                    request_times.append(monotonic())
                if page == 3 and self.server.requests[self.path] == 1:
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Retry-After", "2")
                    self.end_headers()
                    self.wfile.write(b'{"detail": "Temporarily unavailable"}')
                    return
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "data": [vehicle(page)],
                    "pagination": {"has_more": page < 5, "next_cursor": str(page + 1)},
                }).encode())

        self.server.RequestHandlerClass = FivePageFixture
        metrics = json.loads(self.cli("crawl"))
        self.assertEqual((metrics["status"], metrics["records"], metrics["pages"]), ("complete", 5, 5))
        self.assertEqual(metrics["http_statuses"], {"503": 1, "200": 6})
        self.assertEqual(metrics["retries_503"], 1)
        self.assertEqual(metrics["recoveries_503"], 1)
        self.assertEqual(metrics["http_503"]["next_status"], 200)
        self.assertGreaterEqual(request_times[1] - request_times[0], 2)
        self.assertEqual(self.server.requests["/api/v2/cars?limit=100&cursor=2"], 1)

    def test_public_resume_keeps_saved_cursor_and_total_budget(self):
        self.server.repeat = True
        first = json.loads(self.cli("crawl", "--pages", "1", public=True))
        self.server.page_cursor = "another"
        second = json.loads(self.cli("crawl", "--resume", str(first["run_id"]), "--pages", "1", public=True))
        self.server.page_cursor = "last"
        third = json.loads(self.cli("crawl", "--resume", str(first["run_id"]), public=True))
        self.assertEqual([first["pages"], second["pages"], third["pages"]], [1, 2, 3])
        self.assertEqual(third["reason"], "page_budget")
        before = dict(self.server.requests)
        self.cli("crawl", "--resume", str(first["run_id"]), public=True, code=1)
        self.assertEqual(dict(self.server.requests), before)

    def test_repeat_cursor_rolls_back_page(self):
        self.server.repeat = True
        metrics = json.loads(self.cli("crawl", code=1))
        self.assertEqual((metrics["reason"], metrics["records"], metrics["pages"]), ("repeated_cursor", 100, 1))

    def test_export_and_transaction_rollback(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "catalog.ndjson"
            path.write_text("\n".join(json.dumps(vehicle(i)) for i in range(1, 10)), encoding="utf-8")
            run_id = self.cli("import-export", str(path)).strip()
            self.assertIn("90 000", self.cli("report", "--run", run_id))
            compressed = path.with_suffix(".ndjson.gz")
            compressed.write_bytes(gzip.compress(path.read_bytes()))
            zipped_id = self.cli("import-export", str(compressed)).strip()
            with psycopg.connect(self.dsn) as db:
                self.assertEqual(db.execute(
                    "SELECT count(*),count(detail) FROM vehicle WHERE run_id=%s", (zipped_id,)
                ).fetchone(), (9, 9))
                before = db.execute("SELECT count(*) FROM crawl_run").fetchone()
            path.write_text("\n".join([json.dumps(vehicle(1)), json.dumps(vehicle(1))]), encoding="utf-8")
            self.cli("import-export", str(path), code=1)
            path.write_text(json.dumps(vehicle(1)) + "\ninvalid", encoding="utf-8")
            self.cli("import-export", str(path), code=1)
            path.write_text("\n", encoding="utf-8")
            self.cli("import-export", str(path), code=1)
            with psycopg.connect(self.dsn) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM crawl_run").fetchone(), before)


if __name__ == "__main__":
    unittest.main()
