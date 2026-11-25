import os
import unittest
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

import psycopg
from fastapi.testclient import TestClient

from catalog.api import app, password_hash, encode_token
from catalog.inventory import inventory_html
from catalog.store import initialize, insert_vehicles, migrate


class CatalogApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dsn = os.getenv("TEST_DATABASE_URL")
        if not dsn:
            raise unittest.SkipTest("Нужен TEST_DATABASE_URL")
        cls.schema = f"test_api_{os.getpid()}"
        with psycopg.connect(dsn, autocommit=True) as db:
            db.execute(f"CREATE SCHEMA {cls.schema}")
        cls.dsn = psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={cls.schema}")
        cls.env = patch.dict(os.environ, {"DATABASE_URL": cls.dsn, "CATALOG_JWT_SECRET": "test-secret-" * 5})
        cls.env.start()
        with psycopg.connect(cls.dsn, autocommit=True) as db:
            initialize(db)
            db.execute("INSERT INTO api_user(username,password_hash) VALUES('reader',%s)", (password_hash("test-password-123"),))
            cls.run_id = db.execute("INSERT INTO crawl_run(origin,mode,page_limit) VALUES('https://satauto.ru','listing',48) RETURNING id").fetchone()[0]
            insert_vehicles(db, cls.run_id, [{"id": i, "source": {"country": "CN"},
                "identity": {"manufacturer": "Toyota", "model": "Test"},
                "listing": {"year": 2010 + i, "city": "Шанхай", "sell_status": "active" if i % 2 else "sold"},
                "specs": {"engine_power_hp": 120 + i}} for i in range(1, 6)])
            db.execute("""
                UPDATE vehicle_observation SET
                    first_seen_at='2026-11-19 06:00:00+00',
                    last_seen_at='2026-11-19 06:00:00+00'::timestamptz + car_id * interval '1 minute'
                WHERE run_id=%s
                """, (cls.run_id,))
            db.execute("DELETE FROM vehicle_observation WHERE run_id=%s AND car_id=5", (cls.run_id,))
        cls.client = TestClient(app)

    @classmethod
    def tearDownClass(cls):
        with psycopg.connect(cls.dsn, autocommit=True) as db:
            db.execute(f"DROP SCHEMA {cls.schema} CASCADE")
        cls.env.stop()

    def setUp(self):
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute("TRUNCATE api_token_rate")

    def test_token_rate_limit_applies_before_password_check(self):
        with patch("catalog.api.TOKEN_ATTEMPTS", 2):
            for username in ("reader", "absent"):
                response = self.client.post("/token", data={"username": username, "password": "bad"})
                self.assertEqual(response.status_code, 401)
            with patch("catalog.api.verify_password", side_effect=AssertionError("hashed blocked request")):
                response = self.client.post("/token", data={"username": "reader", "password": "x"})
            self.assertEqual(response.status_code, 429)
            self.assertGreater(int(response.headers["Retry-After"]), 0)
            with psycopg.connect(self.dsn, autocommit=True) as db:
                db.execute("UPDATE api_token_rate SET reset_at=now()-interval '1 second'")
            self.headers()

    def test_legacy_cursor_migration_preserves_progress_and_is_repeatable(self):
        schema = self.schema + "_legacy"
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute(f"CREATE SCHEMA {schema}")
            try:
                db.execute(f"SET search_path TO {schema}")
                legacy = Path(__file__).resolve().parents[1] / "catalog/migrations/0001_legacy_schema.sql"
                db.execute(legacy.read_text(encoding="utf-8"))
                ids = dict(db.execute("""
                    INSERT INTO crawl_run(origin,mode,page_limit)
                    VALUES ('https://satauto.ru','listing',48), ('https://carhubauto.ru','listing',48)
                    RETURNING origin,id
                    """).fetchall())
                satauto_id, carhub_id = ids["https://satauto.ru"], ids["https://carhubauto.ru"]
                digest, scope = sha256(b"cursor").hexdigest(), sha256(b"").hexdigest()
                raw_cursor = f"satauto-api:JP:{digest}"
                db.execute("INSERT INTO satauto_api_scan(run_id,country,cursor,pages) VALUES (%s,'JP','continue',8)",
                           (satauto_id,))
                db.execute("INSERT INTO crawl_cursor VALUES (%s,%s),(%s,%s),(%s,%s)",
                           (satauto_id, raw_cursor, satauto_id, f"satauto-api-part:JP:{scope}:0:{digest}",
                            carhub_id, raw_cursor))
                migrate(db)
                migrate(db)
                self.assertEqual(db.execute("SELECT count(*) FROM catalog_schema_migration").fetchone(), (4,))
                self.assertEqual(db.execute("SELECT cursor,pages FROM satauto_api_scan").fetchone(), ("continue", 8))
                self.assertCountEqual(db.execute("""
                    SELECT run_id,value,kind,country,filter_digest,partition FROM crawl_cursor
                    """).fetchall(), [(satauto_id, digest, "satauto", "JP", scope, None),
                                       (satauto_id, digest, "satauto", "JP", scope, 0),
                                       (carhub_id, raw_cursor, "carhub", None, None, None)])
            finally:
                db.execute(f"DROP SCHEMA {schema} CASCADE")

    def headers(self):
        response = self.client.post("/token", data={"username": "reader", "password": "test-password-123"})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers["cache-control"], "no-store")
        return {"Authorization": "Bearer " + response.json()["access_token"]}

    def test_auth_and_swagger(self):
        self.assertEqual(self.client.get("/cars").status_code, 401)
        self.assertEqual(self.client.get("/docs").status_code, 200)
        spec = self.client.get("/openapi.json").json()
        self.assertIn("OAuth2PasswordBearer", spec["components"]["securitySchemes"])
        parameters = {p["name"] for p in spec["paths"]["/cars"]["get"]["parameters"]}
        self.assertTrue({"country", "year_min", "year_max", "parsed_from", "parsed_to"} <= parameters)
        self.assertNotIn("filters", parameters)
        for username in ("reader", "absent"):
            self.assertEqual(self.client.post("/token", data={"username": username, "password": "bad"}).status_code, 401)
        self.assertEqual(self.client.get("/cars", headers={"Authorization": "Bearer broken"}).status_code, 401)
        expired = encode_token({"sub": "reader", "ver": 1}, "catalog-api", -1)
        self.assertEqual(self.client.get("/cars", headers={"Authorization": "Bearer " + expired}).status_code, 401)

    def test_pagination_includes_inactive_without_duplicates(self):
        headers, cursor, ids = self.headers(), None, []
        while True:
            params = {"limit": 2}
            if cursor:
                params["cursor"] = cursor
            response = self.client.get("/cars", params=params, headers=headers)
            self.assertEqual(response.status_code, 200, response.text)
            page = response.json()
            ids.extend(car["car_id"] for car in page["items"])
            cursor = page["next_cursor"]
            if not cursor:
                break
        self.assertEqual(ids, [1, 2, 3, 4, 5])

    def test_filters_cover_scalars_and_raw_json(self):
        headers = self.headers()
        response = self.client.post("/cars/search", headers=headers, json={"include_raw": True,
            "filters": [{"field": "year", "op": "gte", "value": 2012},
                        {"field": "summary.listing.city", "value": "Шанхай"},
                        {"field": "availability", "value": "sold"}]})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual([c["car_id"] for c in response.json()["items"]], [2, 4])
        self.assertIn("summary", response.json()["items"][0])
        response = self.client.post("/cars/search", headers=headers, json={"filters": [
            {"field": "summary", "op": "contains", "value": {"source": {"country": "CN"}}},
            {"field": "car_id", "op": "in", "value": [1, 4]}]})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual([c["car_id"] for c in response.json()["items"]], [1, 4])
        self.assertEqual(self.client.get("/cars/2", headers=headers).json()["availability"], "sold")
        self.assertEqual(self.client.get("/cars/9000", headers=headers).status_code, 404)

    def test_invalid_filters_and_cursor_scope(self):
        headers = self.headers()
        for field, op, value in [("year; DROP TABLE vehicle", "eq", 1), ("year", "gte", "abc"),
                                 ("has_detail", "eq", "false"), ("country", "in", []),
                                 ("summary.listing.city');--", "eq", "x"), ("year", "isnull", "yes")]:
            response = self.client.post("/cars/search", headers=headers, json={"filters": [{"field": field, "op": op, "value": value}]})
            self.assertEqual(response.status_code, 422, response.text)
        cursor = self.client.get("/cars?limit=1", headers=headers).json()["next_cursor"]
        self.assertEqual(self.client.get("/cars", params={"cursor": cursor, "country": "CN"}, headers=headers).status_code, 422)
        self.assertEqual(self.client.get("/cars", params={"cursor": cursor + "x"}, headers=headers).status_code, 422)
        self.assertEqual(self.client.get("/cars?limit=1001", headers=headers).status_code, 422)

    def test_query_filters_date_bounds_and_cursor(self):
        headers = self.headers()
        params = [("country", "KR"), ("country", "CN"), ("year_min", 2012),
                  ("power_min", 121.5), ("summary.listing.city", "Шанхай"),
                  ("parsed_from", "2026-11-19T09:02:00+03:00"),
                  ("parsed_to", "2026-11-19T06:04:00Z"), ("limit", 1)]
        response = self.client.get("/cars", params=params, headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        page = response.json()
        self.assertEqual([(c["car_id"], c["parsed_at"]) for c in page["items"]], [(2, "2026-11-19T06:02:00Z")])
        cursor = ("cursor", page["next_cursor"])
        response = self.client.get("/cars", params=[*reversed(params), cursor], headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual([c["car_id"] for c in response.json()["items"]], [3])
        self.assertFalse(response.json()["has_more"])
        response = self.client.get("/cars", params=[*params[:-2], ("parsed_to", "2026-11-19T06:05:00Z"), cursor], headers=headers)
        self.assertEqual(response.status_code, 422, response.text)
        for invalid in ({"parsed_from": "2026-11-19T09:02:00"},
                        {"parsed_from": "2026-11-19T06:04:00Z", "parsed_to": "2026-11-19T06:02:00Z"},
                        {"year_min": 2024, "year_max": 2020}, {"filters": "[]"}):
            self.assertEqual(self.client.get("/cars", params=invalid, headers=headers).status_code, 422)
        self.assertIsNone(self.client.get("/cars/5", headers=headers).json()["parsed_at"])
        response = self.client.get("/cars?availability=sold&include_raw=true", headers=headers)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual([c["summary"]["id"] for c in response.json()["items"]], [2, 4])

    def test_dashboard_filters_by_moscow_time(self):
        status, html = inventory_html(self.dsn, {"parsed_from": ["2026-11-19T09:02"],
                                                "parsed_to": ["2026-11-19T09:04"]})
        self.assertEqual(status, 200, html)
        self.assertIn("19.11.2026 09:02:00", html)
        self.assertIn("19.11.2026 09:03:00", html)
        self.assertNotIn("19.11.2026 09:04:00", html)

    def test_dashboard_loads_listings_independently_of_summary_and_options(self):
        with patch("catalog.inventory.inventory_statistics", side_effect=AssertionError), \
                patch("catalog.inventory.filter_choices", side_effect=AssertionError), \
                patch("catalog.inventory.PAGE_SIZE", 2):
            status, html = inventory_html(self.dsn, {"page": ["2"]})
            self.assertEqual(status, 200)
            self.assertIn('data-catalog-summary', html)
            self.assertIn('id=3', html)
            self.assertIn('id=2', html)
            self.assertIn('Далее →', html)
            status, last = inventory_html(self.dsn, {"page": ["99"]})
            self.assertEqual(status, 200)
            self.assertIn('id=1', last)
            self.assertNotIn('Далее →', last)
        status, summary = inventory_html(self.dsn, {}, section="summary")
        self.assertEqual(status, 200)
        self.assertIn("Карточек в выборке", summary)
        status, options = inventory_html(self.dsn, {"make": ["toyota"]}, section="options")
        self.assertEqual(status, 200)
        self.assertIn('value="Test"', options)

    def test_unchanged_listing_updates_last_observation(self):
        with psycopg.connect(self.dsn, autocommit=True) as db, db.transaction(force_rollback=True):
            car = {"id": 42, "source": {"country": "CN"}}
            insert_vehicles(db, self.run_id, [car], batch=True)
            db.execute("""
                UPDATE vehicle_observation SET last_seen_at='2000-01-01 00:00:00+00',
                    first_seen_at='2000-01-01 00:00:00+00' WHERE run_id=%s AND car_id=42
                """, (self.run_id,))
            self.assertEqual(insert_vehicles(db, self.run_id, [car], refresh_summary=True, batch=True), 0)
            self.assertTrue(db.execute("SELECT last_seen_at>first_seen_at FROM vehicle_observation "
                                       "WHERE run_id=%s AND car_id=42", (self.run_id,)).fetchone()[0])

    def test_revocation_and_missing_secret(self):
        headers = self.headers()
        with psycopg.connect(self.dsn, autocommit=True) as db:
            db.execute("UPDATE api_user SET token_version=token_version+1 WHERE username='reader'")
        self.assertEqual(self.client.get("/cars", headers=headers).status_code, 401)
        with patch.dict(os.environ, {"CATALOG_JWT_SECRET": ""}):
            self.assertEqual(self.client.post("/token", data={"username": "reader", "password": "test-password-123"}).status_code, 503)


if __name__ == "__main__":
    unittest.main()
