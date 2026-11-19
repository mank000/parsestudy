"""Run resumable API sessions with backoff and graceful signal forwarding."""

import os
import signal
import subprocess
import sys
import threading
from time import monotonic
from datetime import datetime, timezone

import psycopg

from .satauto import BASE


def retry_delay(failures, retry_at=None):
    delay = min(300, 15 * 2 ** min(failures - 1, 5))
    if retry_at is not None:
        delay = max(delay, (retry_at - datetime.now(timezone.utc)).total_seconds())
    return max(0, delay)


def source_retry_at():
    dsn = os.getenv("DATABASE_URL", "postgresql://catalog:catalog-local@127.0.0.1:55432/catalog")
    try:
        with psycopg.connect(dsn, connect_timeout=3) as db:
            db.execute("SET LOCAL statement_timeout='3s'")
            return db.execute("SELECT max(retry_at) FROM crawl_run WHERE origin=%s", (BASE,)).fetchone()[0]
    except psycopg.Error:
        return None


def main():
    stopped = threading.Event()
    child = None

    def stop(signum, frame):
        stopped.set()
        if child is not None and child.poll() is None:
            try:
                child.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    failures = 0
    options = sys.argv[1:]
    while not stopped.is_set():
        started = monotonic()
        child = subprocess.Popen([sys.executable, "-u", "-m", "catalog", "satauto-api", *options])
        while child.poll() is None and not stopped.wait(0.5):
            pass
        if stopped.is_set():
            try:
                child.wait(timeout=25)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
            return 0
        code = child.wait()
        options = [value for value in options if value != "--restart"]
        if code == 0:
            return 0
        if code == 2:
            print("Сбор остановлен: требуется проверка данных или параметров. Подробности выше.", flush=True)
            return 0
        failures = 1 if monotonic() - started >= 300 else failures + 1
        wait = retry_delay(failures, source_retry_at())
        print(f"Сбор прерван, код {code}. Продолжение с сохранённой позиции через {wait:.0f} с.", flush=True)
        stopped.wait(wait)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
