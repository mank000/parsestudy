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
            content_type = "text/plain"
            if status == 503:
                extra["Retry-After"] = "60"
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
        self.server.requests = Counter()
        self.server.block = self.server.repeat = False
        self.server.page_cursor = "fixture"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def cli(self, *args, code=0):
        origin = f"http://127.0.0.1:{self.server.server_port}"
        result = subprocess.run(
            [sys.executable, "-m", "catalog", *args],
            env={**os.environ, "DATABASE_URL": self.dsn, "CATALOG_API_ORIGIN": origin},
            capture_output=True, text=True, timeout=35,
        )
        self.assertEqual(result.returncode, code, result.stdout + result.stderr)
        return result.stdout

    def test_inventory_filters_and_price_analytics(self):
        from psycopg.rows import dict_row

        from catalog.inventory import fetch_inventory
        from catalog.store import initialize, insert_vehicles
        from psycopg.types.json import Jsonb

        with psycopg.connect(self.dsn, autocommit=True) as db:
            initialize(db)
            with db.transaction(force_rollback=True):
                run_id = db.execute("""
                    INSERT INTO crawl_run (origin, mode, page_limit, status)
                    VALUES ('export', 'import', 0, 'complete') RETURNING id
                    """).fetchone()[0]
                cars = [vehicle(10001), vehicle(10002)]
                cars[0]["specs"]["color"] = "Белый"
                cars[1]["specs"]["color"] = "Чёрный"
                cars[1]["listing"]["sell_status"] = "sold"
                insert_vehicles(db, run_id, cars)
                detail = {**cars[1], "pricing": {"status": "partial", "presentation": {
                    "kind": "known_part", "amount_rub": 700000,
                }}}
                db.execute("UPDATE vehicle SET detail=%s WHERE run_id=%s AND car_id=10002",
                           (Jsonb(detail), run_id))
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
                    _, groups, records, _, _ = fetch_inventory(cursor, {"scope": "all"})
                    self.assertEqual((groups[0]["count"], groups[0]["enriched"]), (2, 1))
                    self.assertEqual(sum(car["car_id"] == 10001 for car in records), 1)
                    _, groups, records, _, _ = fetch_inventory(cursor, {"scope": "details"})
                    self.assertEqual((groups[0]["priced"], groups[0]["partial"]), (1, 1))
                    self.assertEqual([car["car_id"] for car in records], [10002, 10001])
                    _, groups, records, _, _ = fetch_inventory(cursor, {
                        "scope": "details", "source": "https://carhubauto.ru", "price_type": "seller",
                    })
                    self.assertEqual([car["car_id"] for car in records], [10004, 10001])
                    self.assertEqual((groups[0]["full"], groups[0]["seller"]), (2, 2))
                    self.assertEqual(records[0]["price"], records[0]["seller_price"])
                    _, groups, records, _, _ = fetch_inventory(cursor, {
                        "color": "белый", "availability": "active", "min_price": "1000000",
                        "max_price": "10000000", "sort": "price_up",
                    })
                    self.assertEqual([car["car_id"] for car in records], [10001])
                    self.assertEqual(groups[0]["median"], records[0]["price"])
                    _, _, records, _, _ = fetch_inventory(cursor, {"availability": "unknown"})
                    self.assertEqual([car["car_id"] for car in records], [10003])

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
        self.assertEqual(metrics["http_503"], {
            "path": "/robots.txt", "content_type": "text/plain", "retry_after": "60",
        })
        self.assertIsNotNone(metrics["retry_at"])
        self.assertEqual(dict(self.server.requests), {"/robots.txt": 1})

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
            with psycopg.connect(self.dsn) as db:
                before = db.execute("SELECT count(*) FROM crawl_run").fetchone()
            path.write_text("\n".join([json.dumps(vehicle(1)), json.dumps(vehicle(1))]), encoding="utf-8")
            self.cli("import-export", str(path), code=1)
            with psycopg.connect(self.dsn) as db:
                self.assertEqual(db.execute("SELECT count(*) FROM crawl_run").fetchone(), before)


if __name__ == "__main__":
    unittest.main()
