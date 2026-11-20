import asyncio
import json
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from time import perf_counter
from urllib.parse import urlencode

from scrapy.crawler import CrawlerProcess
from scrapy.downloadermiddlewares.retry import get_retry_request
from scrapy.exceptions import IgnoreRequest

from .satauto import BASE, finish_satauto_session, latest_satauto_run, lock_satauto, satauto_counts
from .sitemap import COUNTRY_CODES
from .spider import MAX_INLINE_WAIT, AccessGuard, CatalogSpider, response_retry_at, session_metrics
from .store import insert_vehicles


PAGE_SIZE = 48
COUNTRIES = tuple(COUNTRY_CODES.values())
GATEWAY_ERRORS = {502, 504}
PARTITION_RETRY_SECONDS = 30
GATEWAY_PAUSE = "gateway_backoff"
MIN_REFINEMENT_PAGES = 120
TARGET_RANGE_ROWS = PAGE_SIZE * 300


@dataclass
class ApiScan:
    country: str
    cursor: str | None
    filters: dict
    partition: int | None = None
    cursor_resets: int = 0


def year_ranges(filters):
    ranges = []
    lower, upper = filters.get("year_min"), filters.get("year_max")
    for start, end in ((None, 2015), (2016, 2019), (2020, None)):
        first = max((value for value in (lower, start) if value is not None), default=None)
        last = min((value for value in (upper, end) if value is not None), default=None)
        if first is None or last is None or first <= last:
            bounds = {key: value for key, value in filters.items() if key not in {"year_min", "year_max"}}
            bounds.update({key: value for key, value in (("year_min", first), ("year_max", last))
                           if value is not None})
            ranges.append(bounds)
    return ranges


def selection_key(filters):
    return json.dumps(filters, sort_keys=True, separators=(",", ":")) if filters else ""


class SatautoApiAccessGuard(AccessGuard):
    async def process_request(self, request):
        spider = self.crawler.spider
        if spider.gateway_pause:
            raise IgnoreRequest()
        while spider.source_retry_at is not None:
            wait = (spider.source_retry_at - datetime.now(timezone.utc)).total_seconds()
            if wait <= 0:
                break
            if wait > MAX_INLINE_WAIT:
                raise IgnoreRequest()
            await asyncio.sleep(wait)

    async def process_response(self, request, response):
        spider = self.crawler.spider
        retries = request.meta.get("retry_times", 0)
        when = None
        if response.status == 200:
            spider.last_success = perf_counter()
        if response.status in GATEWAY_ERRORS:
            key = f"catalog/http_{response.status}"
            if self.crawler.stats.get_value(key) is None:
                sample = self.diagnostic_503(request, response)
                sample.update(server=response.headers.get("Server", b"").decode("latin-1")[:100],
                              latency=round(request.meta.get("download_latency", 0), 3),
                              country=request.cb_kwargs["scan"].country,
                              has_cursor=bool(request.cb_kwargs["scan"].cursor),
                              html_bad_gateway=b"bad gateway" in response.body[:4096].lower())
                self.crawler.stats.set_value(key, sample)
        if response.status >= 400:
            when = response_retry_at(response)
            if when is not None:
                spider.pause_source(response.status, when)
            elif response.status in GATEWAY_ERRORS and spider.is_archive:
                spider.note_gateway_failure(request.cb_kwargs["scan"], retries)
                if spider.gateway_pause:
                    return response
        if response.status in {408, 429, 500, 502, 504}:
            if response.status not in GATEWAY_ERRORS or not spider.is_archive:
                spider.slow_down()
            if response.status in GATEWAY_ERRORS and spider.is_archive and when is None and retries >= 2:
                return response
            wait = max(0, (when - datetime.now(timezone.utc)).total_seconds()) if when else 1 + retries
            if wait <= MAX_INLINE_WAIT:
                retry = get_retry_request(request, spider=spider,
                                          reason=f"http_{response.status}", max_retry_times=2)
                if retry:
                    spider.logger.warning("HTTP %s · повтор через %.1f с", response.status, wait)
                    await asyncio.sleep(wait)
                    return retry
        return await super().process_response(request, response)

    async def process_exception(self, request, exception):
        if isinstance(exception, IgnoreRequest):
            return None
        super().process_exception(request, exception)
        self.crawler.spider.slow_down()
        retry = get_retry_request(request, spider=self.crawler.spider,
                                  reason=type(exception).__name__, max_retry_times=2)
        if retry:
            await asyncio.sleep(1 + request.meta.get("retry_times", 0))
            return retry
        return None


class SatautoApiSpider(CatalogSpider):
    name = "satauto_api"
    api_path = "/api/carhub/cars"
    custom_settings = {
        **CatalogSpider.custom_settings,
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0,
        "CONCURRENT_REQUESTS": 6,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 6,
        "DOWNLOADER_MIDDLEWARES": {
            "catalog.spider.AccessGuard": None,
            "catalog.satauto_api.SatautoApiAccessGuard": 110,
        },
    }

    def __init__(self, db, run_id, scans, page_budget, filters=None, workers=3, state_key=None,
                 lane="archive", recovering=False, sync_profile=None):
        super().__init__(db, {"id": run_id, "origin": BASE, "page_limit": PAGE_SIZE}, page_budget)
        self.scans = deque(scans)
        self.filters = filters or {}
        self.filter_key = state_key if state_key is not None else selection_key(self.filters)
        self.is_archive = lane == "archive"
        self.workers = workers
        self.maximum_parallelism = min(workers, len(scans))
        self.parallelism = 1 if recovering else self.maximum_parallelism
        self.healthy_pages = 0
        self.in_flight = 0
        self.source_retry_at = None
        self.gateway_failures = {}
        self.last_success = perf_counter()
        self.gateway_pause = False
        self.sync_profile = sync_profile

    def slow_down(self):
        self.parallelism = 1
        self.healthy_pages = 0
        self.crawler.stats.set_value("catalog/api_parallelism", 1)

    def pause_source(self, status, when):
        self.slow_down()
        if self.source_retry_at is None or when > self.source_retry_at:
            self.source_retry_at = when
            self.db.execute("UPDATE crawl_run SET retry_at=%s WHERE id=%s", (when, self.run["id"]))
            if (when - datetime.now(timezone.utc)).total_seconds() > MAX_INLINE_WAIT:
                self.crawler.stats.set_value("catalog/diagnostic", f"http_{status}")
                self.crawler.stats.set_value("catalog/retry_at", when)

    def note_gateway_failure(self, scan, retries):
        now = perf_counter()
        self.gateway_failures[(scan.country, scan.partition)] = now
        self.gateway_failures = {key: at for key, at in self.gateway_failures.items() if now - at < 5}
        if len(self.gateway_failures) >= 3:
            self.slow_down()
            if not self.gateway_pause and retries >= 2 and now - self.last_success >= 3:
                self.gateway_pause = True
                when = datetime.now(timezone.utc) + timedelta(seconds=PARTITION_RETRY_SECONDS)
                self.pause_source(502, when)
                self.crawler.stats.set_value("catalog/diagnostic", GATEWAY_PAUSE)
                self.crawler.stats.set_value("catalog/retry_at", self.source_retry_at)
                self.logger.warning("Ошибки шлюза в нескольких диапазонах · пауза до %s", when.isoformat())

    def next_requests(self):
        while self.scans and self.in_flight < self.parallelism:
            if self.page_budget is not None and self.pages_this_session + self.in_flight >= self.page_budget:
                return
            scan = self.scans.popleft()
            query = {"limit": PAGE_SIZE, "source_country": scan.country, "photo_only": "false", **scan.filters}
            if scan.cursor:
                query["cursor"] = scan.cursor
            suffix = "?" + urlencode(query)
            if len(BASE) + len(self.api_path) + len(suffix) > self.settings.getint("URLLENGTH_LIMIT"):
                self.stop("cursor_url_too_long")
            self.in_flight += 1
            # A server-side restart may legitimately reuse a URL. Cursor loops are checked in the DB.
            yield self.request(suffix, self.parse_page, cb_kwargs={"scan": scan}, dont_filter=True)

    async def start(self):
        self.crawler.stats.set_value("catalog/api_parallelism", self.parallelism)
        for request in self.next_requests():
            yield request

    def payload(self, response):
        if response.status == 409 and "cursor=" in response.url:
            self.stop("cursor_expired")
        payload = super().payload(response)
        if payload.get("partial"):
            self.stop("partial_api_page")
        return payload

    def add_detail_links(self, cars):
        ids = [car["id"] for car in cars]
        paths = dict(self.db.execute("""
            SELECT ids.id, coalesce(v.summary#>>'{links,detail}', s.path)
            FROM unnest(%s::bigint[]) AS ids(id)
            LEFT JOIN vehicle v ON v.run_id=%s AND v.car_id=ids.id
            LEFT JOIN sitemap_car s ON s.car_id=ids.id
            """, (ids, self.run["id"])).fetchall())
        for car in cars:
            if path := paths.get(car["id"]):
                car["links"] = {**(car.get("links") or {}), "detail": path}

    def parse_page(self, response, scan):
        started = perf_counter()
        if self.gateway_pause and response.status in GATEWAY_ERRORS:
            self.stop(GATEWAY_PAUSE, self.source_retry_at)
        if (self.is_archive and response.status in GATEWAY_ERRORS
                and not response.headers.get("Retry-After")
                and (self.source_retry_at is None or self.source_retry_at <= datetime.now(timezone.utc))):
            self.defer_scan(scan, f"HTTP {response.status}")
            yield from self.next_requests()
            return
        if response.status == 409 and scan.cursor:
            try:
                detail = response.json().get("detail", "")
            except (ValueError, AttributeError):
                detail = ""
            if "catalog version changed" in str(detail).lower() and not response.headers.get("Retry-After"):
                if scan.cursor_resets >= 2 and self.is_archive:
                    self.defer_scan(scan, "повторная смена версии каталога")
                    yield from self.next_requests()
                    return
                if scan.cursor_resets >= 2:
                    self.stop("cursor_expired")
                replacements = self.reset_cursor(scan)
                self.in_flight -= 1
                self.scans.extend(replacements)
                self.maximum_parallelism = min(self.workers, max(
                    self.maximum_parallelism, len(self.scans) + self.in_flight))
                yield from self.next_requests()
                return
        cars, cursor = self.listing_page(response, allow_empty=True)
        country = scan.country
        run_id = self.run["id"]
        try:
            for car in cars:
                if type(car.get("id")) is not int or not 0 < car["id"] < 2**63:
                    self.stop("invalid_vehicle")
                if (car.get("source") or {}).get("country") != country:
                    self.stop("unexpected_source_country")
                year = (car.get("listing") or {}).get("year")
                if ("year_min" in scan.filters or "year_max" in scan.filters) and (
                    type(year) is not int or
                    year < scan.filters.get("year_min", year) or year > scan.filters.get("year_max", year)
                ):
                    self.stop("unexpected_listing_year")
            with self.db.transaction():
                if cursor:
                    digest = sha256(cursor.encode()).hexdigest()
                    self.remember_cursor(self.cursor_prefix(scan) + digest)
                if cars:
                    self.add_detail_links(cars)
                with self.db.pipeline():
                    if cars:
                        insert_vehicles(self.db, run_id, cars, refresh_summary=True, batch=True)
                    self.save_position(scan, cursor)
                    self.db.execute("UPDATE crawl_run SET pages=pages+1, status='partial' WHERE id=%s", (run_id,))
                    if self.sync_profile is not None:
                        self.db.execute("UPDATE catalog_sync_state SET pages_saved=pages_saved+1 WHERE profile=%s",
                                        (self.sync_profile,))
        except (ValueError, TypeError, AttributeError):
            self.stop("invalid_vehicle")
        self.pages_this_session += 1
        self.healthy_pages += 1
        if self.healthy_pages >= 20 and self.parallelism < self.maximum_parallelism:
            self.parallelism += 1
            self.healthy_pages = 0
            self.crawler.stats.set_value("catalog/api_parallelism", self.parallelism)
            self.logger.warning("Источник отвечает успешно · параллельных запросов: %s", self.parallelism)
        self.crawler.stats.inc_value("catalog/api_database_seconds", perf_counter() - started)
        self.in_flight -= 1
        self.crawler.stats.inc_value("catalog/api_items", len(cars))
        if self.pages_this_session == 1 or self.pages_this_session % 100 == 0:
            self.logger.warning("SATAuto API · прогон %s · %s · страниц за запуск: %s",
                                run_id, country, self.pages_this_session)
        if cursor:
            scan.cursor = cursor
            if self.is_archive:
                # Finish active chains before opening more cursors that can expire together.
                self.scans.appendleft(scan)
            else:
                self.scans.append(scan)
        if self.scans and self.in_flight == 0 and self.page_budget is not None and self.pages_this_session >= self.page_budget:
            self.stop("page_budget")
        yield from self.next_requests()

    def defer_scan(self, scan, reason):
        when = datetime.now(timezone.utc) + timedelta(seconds=PARTITION_RETRY_SECONDS)
        args = (when, self.run["id"], scan.country, self.filter_key)
        if scan.partition is None:
            self.db.execute("""
                UPDATE satauto_api_scan SET retry_at=%s
                WHERE run_id=%s AND country=%s AND filter_key=%s
                """, args)
        else:
            self.db.execute("""
                UPDATE satauto_api_partition SET retry_at=%s
                WHERE run_id=%s AND country=%s AND filter_key=%s AND partition=%s
                """, (*args, scan.partition))
        self.in_flight -= 1
        self.crawler.stats.inc_value("catalog/deferred_scans")
        self.logger.warning("%s · %s · %s · продолжу диапазон после %s",
                            reason, scan.country, scan.filters, when.isoformat())

    def cursor_prefix(self, scan):
        if scan.partition is not None:
            parent = sha256(self.filter_key.encode()).hexdigest()
            return f"satauto-api-part:{scan.country}:{parent}:{scan.partition}:"
        prefix = f"satauto-api:{scan.country}:"
        return prefix + sha256(self.filter_key.encode()).hexdigest() + ":" if self.filter_key else prefix

    def reset_cursor(self, scan):
        prefix = self.cursor_prefix(scan)
        args = (self.run["id"], scan.country, self.filter_key)
        scans = [scan]
        with self.db.transaction():
            checkpoints = self.db.execute("""
                DELETE FROM crawl_cursor WHERE run_id=%s AND starts_with(value,%s)
                """, (self.run["id"], prefix)).rowcount
            if scan.partition is None:
                self.db.execute("""
                    UPDATE satauto_api_scan SET cursor=NULL,done=false
                    WHERE run_id=%s AND country=%s AND filter_key=%s
                    """, args)
            else:
                self.db.execute("""
                    UPDATE satauto_api_partition SET cursor=NULL,done=false
                    WHERE run_id=%s AND country=%s AND filter_key=%s AND partition=%s
                    """, (*args, scan.partition))
                if self.is_archive and self.workers > 3 and checkpoints >= MIN_REFINEMENT_PAGES:
                    scans = self.refine_scan(scan, args)
        scan.cursor = None
        scan.cursor_resets += 1
        self.crawler.stats.inc_value("catalog/cursor_resets")
        self.crawler.stats.inc_value("catalog/invalidated_checkpoints", checkpoints)
        self.crawler.stats.inc_value("catalog/created_partitions", len(scans) - 1)
        self.logger.warning("Версия каталога изменилась · %s · %s · утрачено курсоров: %s · диапазонов: %s",
                            scan.country, scan.filters, checkpoints, len(scans))
        return scans

    def refine_scan(self, scan, args):
        lower, upper = scan.filters.get("year_min"), scan.filters.get("year_max")
        years = self.db.execute("""
            SELECT year,count(*) FROM vehicle_inventory
            WHERE run_id=%s AND country=%s AND year BETWEEN %s AND %s GROUP BY year ORDER BY year
            """, (*args[:2], lower if lower is not None else 1900, upper if upper is not None else 2100)).fetchall()
        target = max(TARGET_RANGE_ROWS, (sum(count for _, count in years) + 7) // 8)
        ranges = []
        start, rows = lower, 0
        for year, count in years[:-1]:
            rows += count
            if rows >= target:
                ranges.append((start, year))
                start, rows = year + 1, 0
        if not ranges:
            return [scan]
        ranges.append((start, upper))
        next_partition = self.db.execute("""
            SELECT max(partition)+1 FROM satauto_api_partition
            WHERE run_id=%s AND country=%s AND filter_key=%s
            """, args).fetchone()[0]
        self.db.execute("""
            UPDATE satauto_api_partition SET year_max=%s
            WHERE run_id=%s AND country=%s AND filter_key=%s AND partition=%s
            """, (ranges[0][1], *args, scan.partition))
        with self.db.cursor() as cursor:
            cursor.executemany("""
                INSERT INTO satauto_api_partition(run_id,country,filter_key,partition,year_min,year_max)
                VALUES (%s,%s,%s,%s,%s,%s)
                """, [(*args, number, start, end)
                      for number, (start, end) in enumerate(ranges[1:], start=next_partition)])
        filters = {name: value for name, value in scan.filters.items() if name not in {"year_min", "year_max"}}
        replacements = []
        for offset, (start, end) in enumerate(ranges):
            bounds = {key: value for key, value in (("year_min", start), ("year_max", end)) if value is not None}
            number = scan.partition if offset == 0 else next_partition + offset - 1
            replacements.append(ApiScan(scan.country, None, {**filters, **bounds}, number))
        return replacements

    def save_position(self, scan, cursor):
        args = (self.run["id"], scan.country, self.filter_key)
        if scan.partition is None:
            self.db.execute("""
                UPDATE satauto_api_scan SET cursor=%s, pages=pages+1, done=%s, retry_at=NULL
                WHERE run_id=%s AND country=%s AND filter_key=%s
                """, (cursor, cursor is None, *args))
            return
        self.db.execute("""
            UPDATE satauto_api_partition SET cursor=%s, pages=pages+1, done=%s, retry_at=NULL
            WHERE run_id=%s AND country=%s AND filter_key=%s AND partition=%s
            """, (cursor, cursor is None, *args, scan.partition))
        if cursor:
            self.db.execute("""
                UPDATE satauto_api_scan SET pages=pages+1
                WHERE run_id=%s AND country=%s AND filter_key=%s
                """, args)
            return
        self.db.execute("""
            UPDATE satauto_api_scan s SET pages=pages+1, done=finished.done,
                cursor=CASE WHEN finished.done THEN NULL ELSE s.cursor END
            FROM (
                SELECT NOT EXISTS (
                    SELECT 1 FROM satauto_api_partition p
                    WHERE p.run_id=%s AND p.country=%s AND p.filter_key=%s AND NOT p.done
                ) AS done
            ) finished
            WHERE s.run_id=%s AND s.country=%s AND s.filter_key=%s
            """, (*args, *args))


def prepare_scans(db, run_id, countries, filters, workers, state_key=None):
    key = state_key if state_key is not None else selection_key(filters)
    parents = db.execute("""
        SELECT country,cursor,retry_at>clock_timestamp() FROM satauto_api_scan
        WHERE run_id=%s AND country=ANY(%s) AND filter_key=%s AND NOT done ORDER BY country
        """, (run_id, countries, key)).fetchall()
    scans = []
    for country, cursor, delayed in parents:
        partitions = db.execute("""
            SELECT partition,year_min,year_max,cursor,done,retry_at>clock_timestamp() FROM satauto_api_partition
            WHERE run_id=%s AND country=%s AND filter_key=%s ORDER BY partition
            """, (run_id, country, key)).fetchall()
        ranges = year_ranges(filters)
        if not partitions and not delayed and workers > 3 and len(ranges) > 1:
            for number, bounds in enumerate(ranges):
                db.execute("""
                    INSERT INTO satauto_api_partition
                        (run_id,country,filter_key,partition,year_min,year_max,cursor)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """, (run_id, country, key, number, bounds.get("year_min"), bounds.get("year_max"), cursor))
                partitions.append((number, bounds.get("year_min"), bounds.get("year_max"), cursor, False, False))
        if partitions:
            for number, lower, upper, position, done, delayed_partition in partitions:
                if not done and not delayed_partition:
                    bounds = {key: value for key, value in (("year_min", lower), ("year_max", upper))
                              if value is not None}
                    bounds.update({key: value for key, value in filters.items()
                                   if key not in {"year_min", "year_max"}})
                    scans.append(ApiScan(country, position, bounds, number))
        elif not delayed:
            scans.append(ApiScan(country, cursor, filters))
    return sorted(scans, key=lambda scan: scan.cursor is None)


def next_scan_retry(db, run_id, countries, key):
    return db.execute("""
        SELECT min(retry_at) FROM (
            SELECT retry_at FROM satauto_api_partition
            WHERE run_id=%s AND country=ANY(%s) AND filter_key=%s AND NOT done
            UNION ALL
            SELECT s.retry_at FROM satauto_api_scan s
            WHERE s.run_id=%s AND s.country=ANY(%s) AND s.filter_key=%s AND NOT s.done
              AND NOT EXISTS (SELECT 1 FROM satauto_api_partition p
                              WHERE (p.run_id,p.country,p.filter_key)=(s.run_id,s.country,s.filter_key))
        ) outstanding
        """, (run_id, countries, key, run_id, countries, key)).fetchone()[0]


def scan_state(db, run_id, filters=None):
    return {
        country: {"pages": pages, "done": done, "has_cursor": has_cursor}
        for country, pages, done, has_cursor in db.execute("""
            SELECT country, pages, done, cursor IS NOT NULL
            FROM satauto_api_scan WHERE run_id=%s AND filter_key=%s ORDER BY country
            """, (run_id, selection_key(filters)))
    }


def filtered_scan_state(db, run_id):
    return [
        {"country": country, "filters": json.loads(key), "pages": pages, "done": done, "has_cursor": has_cursor}
        for country, key, pages, done, has_cursor in db.execute("""
            SELECT country, filter_key, pages, done, cursor IS NOT NULL
            FROM satauto_api_scan WHERE run_id=%s AND filter_key<>'' ORDER BY country, filter_key
            """, (run_id,))
    ]


def partition_state(db, run_id):
    return [
        {"country": country, "selection": json.loads(key) if key else {},
         "year_min": lower, "year_max": upper, "pages": pages, "done": done}
        for country, key, lower, upper, pages, done in db.execute("""
            SELECT country, filter_key, year_min, year_max, pages, done
            FROM satauto_api_partition WHERE run_id=%s ORDER BY country, filter_key, partition
            """, (run_id,))
    ]


def run_satauto_api(db, country, page_budget, restart, output, filters=None, workers=3, lane="archive", *,
                   sync_profile=None):
    lock_satauto(db)
    filters = filters or {}
    state_filters = {**filters, "_lane": "recent"} if lane == "recent" else filters
    key = selection_key(state_filters)
    if lane == "recent":
        workers = min(workers, 3)
    countries = [country] if country else list(COUNTRIES)
    with db.transaction():
        run = latest_satauto_run(db)
        run_id = run[0] if run else db.execute("""
            INSERT INTO crawl_run (origin, mode, page_limit)
            VALUES (%s,'listing',%s) RETURNING id
            """, (BASE, PAGE_SIZE)).fetchone()[0]
        if restart:
            db.execute("""
                UPDATE satauto_api_scan SET cursor=NULL, pages=0, done=false, retry_at=NULL
                WHERE run_id=%s AND country=ANY(%s) AND filter_key=%s
                """, (run_id, countries, key))
            db.execute("""
                UPDATE satauto_api_partition SET cursor=NULL, pages=0, done=false, retry_at=NULL
                WHERE run_id=%s AND country=ANY(%s) AND filter_key=%s
                """, (run_id, countries, key))
            selection = sha256(key.encode()).hexdigest() if key else None
            db.execute("""
                DELETE FROM crawl_cursor WHERE run_id=%s AND split_part(value, ':', 1)='satauto-api'
                AND split_part(value, ':', 2)=ANY(%s)
                AND (CASE WHEN %s::text IS NULL THEN array_length(string_to_array(value, ':'), 1)=3
                          ELSE split_part(value, ':', 3)=%s END)
                """, (run_id, countries, selection, selection))
            db.execute("""
                DELETE FROM crawl_cursor WHERE run_id=%s AND split_part(value, ':', 1)='satauto-api-part'
                AND split_part(value, ':', 2)=ANY(%s) AND split_part(value, ':', 3)=%s
                """, (run_id, countries, sha256(key.encode()).hexdigest()))
        db.execute("""
            INSERT INTO satauto_api_scan (run_id, country, filter_key) SELECT %s, unnest(%s::text[]), %s
            ON CONFLICT DO NOTHING
            """, (run_id, countries, key))
        scans = prepare_scans(db, run_id, countries, filters, workers, key)
    before, _ = satauto_counts(db, run_id)
    stats, elapsed, records, reason = {}, 0, before, "already_complete"
    if scans:
        recovering = db.execute("SELECT diagnostic=%s FROM crawl_run WHERE id=%s",
                                (GATEWAY_PAUSE, run_id)).fetchone()[0]
        db.execute("""
            UPDATE crawl_run SET diagnostic='satauto_api_running', finished_at=NULL, retry_at=NULL WHERE id=%s
            """, (run_id,))
        process = CrawlerProcess({"LOG_LEVEL": "WARNING", "LOGSTATS_INTERVAL": 0})
        crawler = process.create_crawler(SatautoApiSpider)
        started = perf_counter()
        process.crawl(crawler, db, run_id, scans, page_budget, filters, workers, key, lane, recovering, sync_profile)
        process.start()
        elapsed = perf_counter() - started
        stats = crawler.stats.get_stats()
        reason = stats.get("catalog/diagnostic") or stats.get("finish_reason") or "startup_error"
        records, _ = satauto_counts(db, run_id)
        if reason == "finished" and stats.get("log_count/ERROR"):
            reason = "api_error"
        unfinished = bool(db.execute("""
            SELECT 1 FROM satauto_api_scan
            WHERE run_id=%s AND country=ANY(%s) AND filter_key=%s AND NOT done LIMIT 1
            """, (run_id, countries, key)).fetchone())
        if reason == "finished" and unfinished:
            reason = "partition_backoff" if next_scan_retry(db, run_id, countries, key) else "incomplete_scan"
        diagnostic = "satauto_api_complete" if reason == "finished" else reason
        finish_satauto_session(db, run_id, stats, elapsed, records, diagnostic)
    states = scan_state(db, run_id, state_filters)
    complete = all(states[country]["done"] for country in countries)
    partition_retry_at = None if complete else next_scan_retry(db, run_id, countries, key)
    if reason == "already_complete" and partition_retry_at is not None:
        reason = "partition_backoff"
    metrics = {
        "run_id": run_id, "reason": reason, "complete": complete, "page_size": PAGE_SIZE,
        "filters": filters, "lane": lane,
        "records": records, "new_records": records - before,
        "session_items": stats.get("catalog/api_items", 0), "countries": states,
        "parallel_requests": stats.get("catalog/api_parallelism", 0),
        "database_seconds": round(stats.get("catalog/api_database_seconds", 0), 3),
        "cursor_resets": stats.get("catalog/cursor_resets", 0),
        "invalidated_checkpoints": stats.get("catalog/invalidated_checkpoints", 0),
        "created_partitions": stats.get("catalog/created_partitions", 0),
        "deferred_scans": stats.get("catalog/deferred_scans", 0),
        "partition_retry_at": partition_retry_at,
        "http_502": stats.get("catalog/http_502"),
        "http_504": stats.get("catalog/http_504"),
        "partitions": partition_state(db, run_id),
        **session_metrics(stats, elapsed),
    }
    serialized = json.dumps(metrics, ensure_ascii=False, indent=2, default=str) + "\n"
    print(serialized, end="")
    if output:
        output.write_text(serialized, encoding="utf-8")
    if complete or reason in {"page_budget", "shutdown"}:
        return 0
    permanent = {
        "http_400", "http_401", "http_403", "http_404", "http_405", "http_422",
        "invalid_json", "invalid_payload", "invalid_page", "invalid_pagination",
        "invalid_vehicle", "unexpected_source_country", "unexpected_listing_year",
        "unexpected_content_type", "repeated_cursor", "cursor_url_too_long",
    }
    return 2 if reason in permanent else 1
