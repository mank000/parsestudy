"""Time a deterministic Scrapy -> PostgreSQL run against loopback HTTP."""
import json
import os
import subprocess
import sys
import threading
from collections import Counter
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import psycopg

from test_integration import ApiFixture, vehicle


class BenchmarkFixture(ApiFixture):
    def do_GET(self):
        url = urlsplit(self.path)
        if url.path != "/api/v2/cars":
            return super().do_GET()
        query = parse_qs(url.query)
        first = int(query.get("cursor", ["1"])[0])
        limit = int(query["limit"][0])
        stop = min(first + limit, 1001)
        cars = []
        for car_id in range(first, stop):
            car = vehicle(car_id)
            car["pricing"].pop("steps")
            cars.append(car)
        body = json.dumps({
            "data": cars,
            "pagination": {"has_more": stop < 1001, "next_cursor": str(stop) if stop < 1001 else None},
        }).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(body)


def main():
    dsn = os.environ["TEST_DATABASE_URL"]
    schema = f"benchmark_catalog_{os.getpid()}"
    with psycopg.connect(dsn, autocommit=True) as db, ThreadingHTTPServer(("127.0.0.1", 0), BenchmarkFixture) as server:
        db.execute(f"CREATE SCHEMA {schema}")
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        server.robots = "User-agent: *\nAllow: /\n"
        server.robots_status = 200
        server.requests = Counter()
        thread.start()
        try:
            result = subprocess.run(
                [sys.executable, "-m", "catalog", "crawl"],
                env={
                    **os.environ,
                    "DATABASE_URL": psycopg.conninfo.make_conninfo(dsn, options=f"-c search_path={schema}"),
                    "CATALOG_API_ORIGIN": f"http://127.0.0.1:{server.server_port}",
                },
                check=True, capture_output=True, text=True, timeout=90,
            )
            metrics = json.loads(result.stdout)
            assert metrics["status"] == "complete" and metrics["records"] == 1000 and metrics["session_attempts"] == 11
            metrics["source"] = "synthetic localhost fixture"
            print(json.dumps(metrics, ensure_ascii=False, indent=2))
        finally:
            server.shutdown()
            thread.join()
            db.execute(f"DROP SCHEMA {schema} CASCADE")


if __name__ == "__main__":
    main()
