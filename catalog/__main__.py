import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit

import psycopg
from psycopg.rows import dict_row
from scrapy.crawler import CrawlerProcess

from .report import render_report
from .satauto import run_public_html
from .sitemap import run_sitemap
from .spider import PUBLIC_ORIGIN, CatalogSpider, session_metrics
from .store import import_export, initialize


PUBLIC_PAGE_BUDGET = 3


def arguments():
    parser = argparse.ArgumentParser(prog="python -m catalog")
    commands = parser.add_subparsers(dest="action", required=True)
    crawl = commands.add_parser("crawl")
    crawl.add_argument("--country", choices=["JP", "KR", "CN"])
    crawl.add_argument("--limit", type=int, default=100)
    crawl.add_argument("--pages", type=int)
    crawl.add_argument("--full", action="store_true")
    crawl.add_argument("--resume", type=int)
    crawl.add_argument("--output", type=Path)
    feed = commands.add_parser("import-export")
    feed.add_argument("path", type=Path)
    report = commands.add_parser("report")
    report.add_argument("--run", type=int, required=True)
    report.add_argument("--output", type=Path)
    audit = commands.add_parser("sitemap")
    audit.add_argument("--shards", type=int, default=1)
    audit.add_argument("--all", action="store_true")
    audit.add_argument("--store", action="store_true")
    audit.add_argument("--output", type=Path)
    public = commands.add_parser("public-html")
    public.add_argument("--root-only", action="store_true")
    public.add_argument("--enrich", action="store_true", help="Загрузить недостающие карточки из sitemap")
    public.add_argument("--make", help="Марка в sitemap, например bmw или mercedes-benz")
    public.add_argument("--details", type=int, choices=range(0, 97), metavar="0–96", default=24)
    public.add_argument("--output", type=Path)
    commands.add_parser("init")
    args = parser.parse_args()
    if args.action == "public-html" and args.make and not args.enrich:
        parser.error("--make используется с --enrich")
    if args.action == "sitemap" and args.store and not args.all:
        parser.error("--store требует --all для записи полной карты")
    if args.action == "sitemap" and args.shards < 1:
        parser.error("Число файлов должно быть больше нуля")
    if args.action == "crawl":
        if not 1 <= args.limit <= 100 or args.pages is not None and args.pages < 1:
            parser.error("Размер страницы 1–100, число страниц больше нуля")
        if args.resume and (args.country or args.full or args.limit != 100):
            parser.error("При продолжении параметры источника берутся из сохранённого прогона")
    return args


def get_run(db, run_id):
    with db.cursor(row_factory=dict_row) as cursor:
        cursor.execute("SELECT * FROM crawl_run WHERE id=%s", (run_id,))
        run = cursor.fetchone()
    if run is None:
        raise ValueError(f"Прогон {run_id} отсутствует")
    return run


def run_crawl(db, args):
    origin = os.getenv("CATALOG_API_ORIGIN", PUBLIC_ORIGIN).rstrip("/")
    public = origin == PUBLIC_ORIGIN
    url = urlsplit(origin)
    if (not url.hostname or url.username or url.password or url.path or url.query or url.fragment
            or not (url.scheme == "https" or (url.scheme == "http" and url.hostname == "127.0.0.1"))):
        raise ValueError("Нужен HTTPS-адрес согласованного API или локальный HTTP для теста")
    if public and (args.full or args.pages is not None and args.pages > PUBLIC_PAGE_BUDGET):
        raise ValueError("Публичный режим поддерживает пробу до 3 страниц. Для полной выгрузки используйте согласованный источник или импорт.")
    budget = (args.pages or 1) if public else args.pages
    token = os.getenv("CATALOG_API_TOKEN") if not public else None
    if args.resume:
        run_id = args.resume
    else:
        run_id = db.execute(
            """INSERT INTO crawl_run (origin, source_country, mode, page_limit)
               VALUES (%s,%s,%s,%s) RETURNING id""",
            (origin, args.country, "full" if args.full else "listing", args.limit),
        ).fetchone()[0]

    lock_key = f"catalog-crawl:{origin}"
    locked = db.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (lock_key,)).fetchone()[0]
    if not locked:
        if not args.resume:
            db.execute("DELETE FROM crawl_run WHERE id=%s", (run_id,))
        raise ValueError("Для этого источника уже запущен сбор")
    run = get_run(db, run_id)
    if run["origin"] != origin or run["status"] not in {"running", "partial", "failed"}:
        raise ValueError("Продолжить можно незавершённый прогон на том же источнике")
    if public and args.resume:
        remaining = PUBLIC_PAGE_BUDGET - run["pages"]
        if remaining <= 0 or run["mode"] != "listing":
            raise ValueError("Лимит публичной пробы исчерпан. Для полной выгрузки используйте согласованный источник или импорт.")
        budget = min(remaining, args.pages or remaining)
    cooldown = db.execute(
        "SELECT max(retry_at) FROM crawl_run WHERE origin=%s", (origin,),
    ).fetchone()[0]
    if cooldown and cooldown > datetime.now(timezone.utc):
        if not args.resume:
            db.execute("DELETE FROM crawl_run WHERE id=%s", (run_id,))
        raise ValueError(f"Источник просит повторить после {cooldown.isoformat()}")

    db.execute("UPDATE crawl_run SET status='running', finished_at=NULL WHERE id=%s", (run_id,))
    process = CrawlerProcess({"LOG_LEVEL": "WARNING", "LOGSTATS_INTERVAL": 0})
    crawler = process.create_crawler(CatalogSpider)
    started = perf_counter()
    process.crawl(crawler, db, run, budget, token)
    process.start()
    seconds = perf_counter() - started
    stats = crawler.stats.get_stats()
    saved = get_run(db, run_id)

    total, details, missing = db.execute(
        """SELECT count(*),count(detail),count(*) FILTER (WHERE detail_missing)
           FROM vehicle WHERE run_id=%s""", (run_id,),
    ).fetchone()
    reason = stats.get("catalog/diagnostic") or stats.get("finish_reason") or "startup_error"
    complete = (
        reason == "finished" and saved["listing_done"] and not stats.get("log_count/ERROR")
        and (saved["mode"] != "full" or total == details)
    )
    status = "complete" if complete else ("partial" if saved["pages"] else "failed")
    attempted = stats.get("catalog/attempted", 0)
    downloaded = stats.get("catalog/downloaded", 0)
    transfer = stats.get("catalog/bytes", 0)
    db.execute(
        """UPDATE crawl_run SET status=%s, diagnostic=%s, retry_at=%s,
           attempted_requests=attempted_requests+%s, downloaded_responses=downloaded_responses+%s,
           response_bytes=response_bytes+%s, seconds=seconds+%s, finished_at=clock_timestamp()
           WHERE id=%s""",
        (status, reason, stats.get("catalog/retry_at"), attempted, downloaded, transfer, seconds, run_id),
    )
    metrics = {
        "run_id": run_id, "status": status, "reason": reason, "records": total,
        "details": details, "detail_missing": missing, "pages": saved["pages"],
        **session_metrics(stats, seconds),
    }
    output = json.dumps(metrics, ensure_ascii=False, indent=2, default=str)
    print(output)
    if args.output:
        args.output.write_text(output + "\n", encoding="utf-8")
    if complete or reason == "page_budget":
        return 0
    return 1


def main():
    args = arguments()
    if args.action == "sitemap" and not args.store:
        return run_sitemap(args.output, None if args.all else args.shards)
    dsn = os.getenv("DATABASE_URL", "postgresql://catalog:catalog-local@127.0.0.1:55432/catalog")
    try:
        with psycopg.connect(dsn, autocommit=True) as db:
            initialize(db)
            if args.action == "sitemap":
                with db.transaction():
                    return run_sitemap(args.output, None, db=db)
            if args.action == "public-html":
                return run_public_html(db, args.details, args.root_only, args.enrich, args.make, args.output)
            if args.action == "crawl":
                return run_crawl(db, args)
            if args.action == "import-export":
                with db.transaction():
                    print(import_export(db, args.path))
            if args.action == "report":
                run = get_run(db, args.run)
                report = render_report(db, run)
                if args.output:
                    args.output.write_text(report, encoding="utf-8")
                else:
                    print(report, end="")
    except (ValueError, OSError, psycopg.Error) as exc:
        logging.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
