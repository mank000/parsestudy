"""Interleave a bounded recent window and resumable archive chunks.

One supervisor owns the work: additional identical instances cannot duplicate it.
No absence-based deletions; only explicit source statuses change availability.
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

from .satauto import BASE
from .store import initialize
from .worker import retry_delay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recent-pages", type=int, default=12)
    parser.add_argument("--recent-interval", type=int, default=600)
    parser.add_argument("--archive-pages", type=int, default=120)
    parser.add_argument("--archive-interval", type=int, default=86400)
    parser.add_argument("--workers", type=int, choices=range(1, 7), default=os.getenv("SATAUTO_WORKERS") or 6)
    parser.add_argument("--country", choices=["CN", "KR", "JP"], default=os.getenv("SATAUTO_COUNTRY") or None)
    parser.add_argument("--year", type=int, default=os.getenv("SATAUTO_YEAR") or None)
    parser.add_argument("--year-min", type=int, default=os.getenv("SATAUTO_YEAR_MIN") or None)
    parser.add_argument("--year-max", type=int, default=os.getenv("SATAUTO_YEAR_MAX") or None)
    parser.add_argument("--once", action="store_true", help="Одна порция для проверки")
    args = parser.parse_args()
    if min(args.recent_pages, args.recent_interval, args.archive_pages, args.archive_interval) < 1:
        parser.error("Лимиты и интервалы должны быть положительными")
    if args.year is not None and (args.year_min is not None or args.year_max is not None):
        parser.error("--year используется отдельно от --year-min и --year-max")
    if args.year_min is not None and args.year_max is not None and args.year_min > args.year_max:
        parser.error("Нижний год больше верхнего")
    selection = {name: getattr(args, name) for name in ("country", "year", "year_min", "year_max")
                 if getattr(args, name) is not None}
    profile = json.dumps(selection, sort_keys=True) if selection else "all"
    stop = threading.Event()
    child = None

    def shutdown(signum, frame):
        stop.set()
        if child is not None and child.poll() is None:
            try:
                child.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    default_dsn = "postgresql://catalog:catalog-local@127.0.0.1:55432/catalog"
    with psycopg.connect(os.getenv("DATABASE_URL", default_dsn), autocommit=True) as db:
        if not db.execute("SELECT pg_try_advisory_lock(274812)").fetchone()[0]:
            print("Синхронизация уже запущена", flush=True)
            return 2
        initialize(db)
        db.execute("INSERT INTO catalog_sync_state(profile) VALUES(%s) ON CONFLICT DO NOTHING", (profile,))
        failures = {"archive": 0, "recent": 0}
        archive_retry_at = None
        recent_in_progress = False
        while not stop.is_set():
            recent, completed = db.execute("SELECT recent_at,full_completed_at FROM catalog_sync_state WHERE profile=%s", (profile,)).fetchone()
            now = datetime.now(timezone.utc)
            lane = "recent" if recent is None or (now - recent).total_seconds() >= args.recent_interval else "archive"
            restart = (not recent_in_progress if lane == "recent" else
                       completed is not None and (now - completed).total_seconds() >= args.archive_interval)
            if lane == "archive" and completed is not None and not restart:
                if args.once:
                    return 0
                stop.wait(min(30, args.recent_interval))
                continue
            if lane == "archive" and archive_retry_at is not None and archive_retry_at > now:
                next_recent = recent + timedelta(seconds=args.recent_interval)
                stop.wait(max(0, (min(archive_retry_at, next_recent) - now).total_seconds()))
                continue
            pages = args.recent_pages if lane == "recent" else args.archive_pages
            with tempfile.TemporaryDirectory(prefix="catalog-sync-") as folder:
                output = Path(folder) / "metrics.json"
                command = [sys.executable, "-u", "-m", "catalog", "satauto-api", "--no-available-only",
                           "--schema-ready", "--lane", lane, "--pages", str(pages),
                           "--workers", str(args.workers), "--output", str(output)]
                for name, value in selection.items():
                    command.extend(["--" + name.replace("_", "-"), str(value)])
                if restart:
                    command.append("--restart")
                print(f"Синхронизация: {lane}, до {pages} страниц", flush=True)
                child = subprocess.Popen(command)
                while child.poll() is None:
                    if stop.wait(0.5):
                        try:
                            child.wait(timeout=25)
                        except subprocess.TimeoutExpired:
                            child.kill()
                            child.wait()
                        return 0
                metrics = json.loads(output.read_text()) if output.exists() else {}
            code = child.returncode
            if metrics and restart:
                if lane == "recent":
                    recent_in_progress = True
                else:
                    db.execute("UPDATE catalog_sync_state SET full_completed_at=NULL WHERE profile=%s", (profile,))
            if code == 2:
                print("Синхронизация остановлена: источник или параметры требуют проверки", flush=True)
                return 0
            if code:
                failures[lane] += 1
                if lane == "archive" and metrics.get("reason") == "partition_backoff":
                    retry = datetime.fromisoformat(metrics["partition_retry_at"])
                    archive_retry_at = datetime.now(timezone.utc) + timedelta(seconds=retry_delay(failures[lane], retry))
                else:
                    retry = db.execute("SELECT max(retry_at) FROM crawl_run WHERE origin=%s", (BASE,)).fetchone()[0]
                    stop.wait(retry_delay(failures[lane], retry))
                continue
            failures[lane] = 0
            if lane == "recent":
                recent_in_progress = False
                db.execute("UPDATE catalog_sync_state SET recent_at=clock_timestamp() WHERE profile=%s", (profile,))
            elif metrics.get("complete"):
                db.execute("UPDATE catalog_sync_state SET full_completed_at=clock_timestamp() WHERE profile=%s", (profile,))
            if args.once:
                return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
