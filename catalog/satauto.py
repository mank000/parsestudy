import json
import re
import shutil
import sys
from collections import deque
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit

import scrapy
from psycopg.types.json import Jsonb
from scrapy.crawler import CrawlerProcess

from .detail_queue import enqueue_details, finish_detail, plan_details, plan_full_scan, start_detail
from .sitemap import BASE, CAR, COUNTRY_CODES
from .spider import CatalogSpider, response_retry_at, session_metrics
from .store import fields, insert_vehicles


CARD_REDIRECTS = {301, 302, 303, 307, 308}
JAPAN_LOT = re.compile(r"/catalog/japan/lot-(main|stats)-([a-zA-Z0-9_-]+)/")
JAPAN_SHORT_CAR = re.compile(r"/catalog/japan/([^/]+)/([0-9]+)-[^/]+/")
BRAND_DIRECTORY = "/catalog/brands/"
DEFAULT_BRAND_PAGES = 8


def brand_page(href):
    url = urlsplit(href)
    query = parse_qsl(url.query, max_num_fields=1)
    if (not url.scheme and not url.netloc and not url.fragment and url.path == "/catalog/"
            and len(query) == 1 and query[0][0] == "manufacturer" and 0 < len(query[0][1]) <= 120):
        return url.path + "?" + urlencode(query)


def catalog_seed(response):
    try:
        seed = json.loads(response.css("script[data-catalog-seed]::text").get() or "null")
    except ValueError:
        return {}
    return seed if isinstance(seed, dict) and isinstance(seed.get("data"), list) else {}


def enrich_listing(car, seed):
    listing = {**seed, "id": car["id"]}
    for section in ("source", "identity", "listing", "specs", "pricing", "media"):
        values = seed.get(section)
        listing[section] = {
            **car[section],
            **({key: value for key, value in values.items() if value is not None and value != ""}
               if isinstance(values, dict) else {}),
        }
    if car["id"] is None:
        listing["source"]["listing_id"] = car["source"]["listing_id"]
    if not isinstance(listing["pricing"].get("presentation"), dict):
        listing["pricing"]["presentation"] = car["pricing"]["presentation"]
    listing["links"] = car["links"]
    return listing


def text(node, selector):
    return node.css(selector).xpath("normalize-space(.)").get() or ""


def integer(text_value):
    return int("".join(re.findall(r"[0-9]", text_value))) if re.search(r"[0-9]", text_value) else None


def card_from_html(response, article):
    path = article.css("a.satauto-car-card__link::attr(href)").get() or ""
    match = CAR.fullmatch(path)
    source_id = None
    if match:
        country, make, model, car_id, _ = match.groups()
    elif short := JAPAN_SHORT_CAR.fullmatch(path):
        country, make, model, car_id = "japan", short[1], None, short[2]
    elif (lot := JAPAN_LOT.fullmatch(path)) and article.attrib.get("data-car-id") == f"{lot[1]}:{lot[2]}":
        country, make, model, car_id = "japan", None, None, None
        source_id = f"{lot[1]}:{lot[2]}"
    else:
        raise ValueError("Unexpected car URL")
    if car_id is not None:
        car_id = int(car_id)
        if car_id <= 0 or car_id > 2**63 - 1:
            raise ValueError("Invalid listing ID")
    meta = [part.strip() for part in text(article, ".satauto-car-card__meta").split("·")]
    price = text(article, ".satauto-car-card__price")
    photo = article.css(".satauto-car-card__photo img::attr(src)").get()
    gallery = article.css("[data-card-photos]::attr(data-card-photos)").get()
    fuel = next((part for part in meta if not re.search(r"[0-9]", part)
                 and part not in {"Передний", "Задний", "Полный"} and part), None)
    return {
        "id": car_id,
        "source": {"country": COUNTRY_CODES.get(country, country),
                   **({"listing_id": source_id} if source_id else {})},
        "identity": {"title": text(article, ".satauto-car-card__title"), "manufacturer": make, "model": model},
        "listing": {"year": next((int(p) for p in meta if re.fullmatch(r"(?:19|20)[0-9]{2}", p)), None),
                    "mileage_km": next((integer(p) for p in meta if p.endswith("км")), None),
                    "sell_status": article.attrib.get("data-availability-state") or None},
        "specs": {"fuel_type": fuel, "drive_type": next((p for p in meta if p in {"Передний", "Задний", "Полный"}), None)},
        "pricing": {"presentation": {"amount_rub": integer(price) if "₽" in price else None,
                                     "label": price, "kind": "on_request" if "запрос" in price.lower() else None}},
        "links": {"detail": response.urljoin(path)} if not source_id else {},
        "media": {"main_photo": response.urljoin(photo) if photo else None,
                  "listing_photos": [{"url": response.urljoin(url)} for url in (json.loads(gallery) if gallery else [])]},
    }


class SatautoSpider(CatalogSpider):
    name = "satauto_public_html"

    def __init__(self, db, run_id, detail_limit, root_only, targets=None, full_scan=False,
                 brand_pages=0, brand_after=None):
        scrapy.Spider.__init__(self)
        self.db = db
        self.run_id = run_id
        self.detail_limit = detail_limit
        self.root_only = root_only
        self.listings = deque([BRAND_DIRECTORY if brand_after else "/catalog/"] if targets is None else [])
        self.brand_pages = brand_pages
        self.brand_after = brand_after
        self.brands_remaining = 0
        self.details = dict(targets or ())
        self.pages = 0
        self.full_scan = full_scan
        self.list_ids = set()
        self.list_priced_ids = set()

    async def start(self):
        if request := self.next_request():
            yield request

    def next_request(self):
        if self.listings:
            path = self.listings.popleft()
            if path == BRAND_DIRECTORY:
                return scrapy.Request(BASE + path, self.parse_brand_directory, errback=self.request_failed)
            return scrapy.Request(BASE + path, self.parse_list, errback=self.request_failed,
                                  cb_kwargs={"path": path})
        if self.full_scan and not self.details:
            if shutil.disk_usage("/").free < 2 * 1024**3:
                self.stop("low_disk_space")
            batch = plan_full_scan(self.db, self.run_id, self.detail_limit, BASE)
            if any(not CAR.fullmatch(url.removeprefix(BASE)) for url, _ in batch):
                self.stop("invalid_sitemap_card_url")
            self.details.update(reversed(batch))
            if batch:
                self.logger.warning("Очередь прогона %s · %s карточек · %s",
                                    self.run_id, len(batch), batch[0][1])
        if self.details:
            url, car_id = self.details.popitem()
            start_detail(self.db, self.run_id, car_id)
            return scrapy.Request(url, self.parse_car, errback=self.detail_failed, cb_kwargs={"car_id": car_id})
        return None

    def detail_failed(self, failure):
        finish_detail(self.db, self.run_id, failure.request.cb_kwargs["car_id"], "pending", failure.type.__name__)
        self.request_failed(failure)

    def validate(self, response):
        if response.status != 200:
            self.stop(f"http_{response.status}", response_retry_at(response))

    def defer_card(self, car_id, reason):
        finish_detail(self.db, self.run_id, car_id, "deferred", reason)
        self.crawler.stats.inc_value("catalog/detail_deferred")
        self.logger.warning("Карточка %s отложена · %s", car_id, reason)

    def follow_card_redirect(self, response, car_id):
        location = response.headers.get("Location")
        previous = (*response.meta.get("card_redirect_chain", ()), response.url)
        if not location:
            return None
        try:
            target = urljoin(response.url, location.decode("latin-1").strip())
            url = urlsplit(target)
        except ValueError:
            return None
        match = CAR.fullmatch(url.path)
        if (f"{url.scheme}://{url.netloc}" != BASE or url.query or url.fragment
                or not match or int(match.group(4)) != car_id
                or target in previous or len(previous) > 3):
            return None
        request = scrapy.Request(target, self.parse_car, errback=self.detail_failed,
                                 cb_kwargs={"car_id": car_id}, meta={"card_redirect_chain": previous})
        if request.url in previous:
            return None
        enqueue_details(self.db, self.run_id, [(request.url, car_id)])
        self.crawler.stats.inc_value("catalog/redirects_followed")
        return request

    def parse_brand_directory(self, response):
        self.validate(response)
        paths = set()
        for href in response.css("a.satauto-brands__main::attr(href)").getall():
            try:
                path = brand_page(href)
            except ValueError:
                continue
            if path:
                paths.add(path)
        if not paths:
            self.stop("no_catalog_brands")
        remaining = sorted(path for path in paths if not self.brand_after or path > self.brand_after)
        self.brands_remaining = len(remaining)
        self.crawler.stats.set_value("catalog/brands_remaining", self.brands_remaining)
        self.listings.extend(remaining[:self.brand_pages])
        if not remaining:
            self.db.execute("UPDATE crawl_run SET cursor=NULL WHERE id=%s", (self.run_id,))
        if request := self.next_request():
            yield request

    def parse_list(self, response, path):
        self.validate(response)
        brand = path.startswith("/catalog/?manufacturer=")
        seed = catalog_seed(response)
        if seed.get("partial"):
            self.stop("partial_catalog_page")
        pagination = seed.get("pagination")
        has_more = pagination.get("has_more") if isinstance(pagination, dict) else None
        if path == "/catalog/" and not self.root_only:
            allowed = {"/catalog/?source_country=" + c for c in COUNTRY_CODES.values()}
            self.listings.extend(dict.fromkeys(
                href for href in response.css("a[data-catalog-country]::attr(href)").getall() if href in allowed
            ))
            if self.brand_pages and response.css(f'a[href="{BRAND_DIRECTORY}"]'):
                self.listings.append(BRAND_DIRECTORY)
        seed_by_id = {str(row["id"]): row for row in seed.get("data", [])
                      if isinstance(row, dict) and type(row.get("id")) in (int, str)}
        cars, enriched = {}, {}
        for article in response.css("#catalog-results article[data-car-id]"):
            try:
                car = card_from_html(response, article)
            except (ValueError, TypeError):
                self.crawler.stats.inc_value("catalog/list_records_skipped")
                continue
            key = str(car["id"] or car["source"]["listing_id"])
            if key in seed_by_id:
                enriched[key] = enrich_listing(car, seed_by_id[key])
            else:
                cars[key] = car
        if not (cars or enriched) and not (path != "/catalog/" and seed.get("data") == [] and has_more is False):
            self.stop("no_html_cards")
        cars = list(cars.values())
        enriched = list(enriched.values())
        records = cars + enriched
        with self.db.transaction():
            for car in records:
                if car["id"] is None:
                    car["id"] = self.db.execute("""
                        INSERT INTO satauto_lot_id (source_id) VALUES (%s)
                        ON CONFLICT (source_id) DO UPDATE SET source_id=excluded.source_id
                        RETURNING car_id
                        """, (car["source"]["listing_id"],)).fetchone()[0]
            insert_vehicles(self.db, self.run_id, cars)
            insert_vehicles(self.db, self.run_id, enriched, refresh_summary=True)
            self.db.execute("UPDATE crawl_run SET pages=pages+1 WHERE id=%s", (self.run_id,))
            if brand:
                self.db.execute("UPDATE crawl_run SET cursor=%s WHERE id=%s",
                                (path if self.brands_remaining > 1 else None, self.run_id))
            if self.detail_limit:
                self.queue_listing_details(records)
        if brand:
            self.brands_remaining -= 1
            self.crawler.stats.inc_value("catalog/brand_pages")
            self.crawler.stats.set_value("catalog/brands_remaining", self.brands_remaining)
        if has_more:
            self.crawler.stats.inc_value("catalog/list_pages_with_more")
        self.list_ids.update(car["id"] for car in records)
        self.list_priced_ids.update(
            car["id"] for car in records if (car["pricing"].get("presentation") or {}).get("amount_rub") is not None
        )
        self.pages += 1
        self.crawler.stats.set_value("catalog/list_pages", self.pages)
        self.crawler.stats.set_value("catalog/list_records", len(self.list_ids))
        self.crawler.stats.set_value("catalog/list_records_priced", len(self.list_priced_ids))
        request = self.next_request()
        if request:
            yield request

    def queue_listing_details(self, cars):
        ready = {row[0] for row in self.db.execute("""
            SELECT v.car_id FROM vehicle v
            LEFT JOIN satauto_detail_task t USING (run_id, car_id)
            WHERE v.run_id=%s AND v.car_id=ANY(%s)
              AND v.detail IS NULL AND NOT v.detail_missing
              AND (t.status IS NULL OR t.status IN ('pending', 'deferred'))
              AND (t.retry_at IS NULL OR t.retry_at <= clock_timestamp())
            """, (self.run_id, [car["id"] for car in cars]))}
        for car in cars:
            if len(self.details) >= self.detail_limit:
                break
            detail_url = car["links"].get("detail")
            if car["id"] in ready and detail_url and CAR.fullmatch(urlsplit(detail_url).path):
                self.details[detail_url] = car["id"]
        enqueue_details(self.db, self.run_id, self.details.items())

    def parse_car(self, response, car_id):
        if response.status in CARD_REDIRECTS:
            request = self.follow_card_redirect(response, car_id)
            if request:
                yield request
                return
            self.defer_card(car_id, f"http_{response.status}_invalid_card_redirect")
        elif response.status in {404, 410}:
            with self.db.transaction():
                insert_vehicles(self.db, self.run_id, [{"id": car_id, "index_only": True}])
                self.db.execute("UPDATE vehicle SET detail_missing=true WHERE run_id=%s AND car_id=%s",
                                (self.run_id, car_id))
                finish_detail(self.db, self.run_id, car_id, "missing", f"http_{response.status}")
            self.crawler.stats.inc_value("catalog/detail_missing")
        else:
            if response.status != 200:
                finish_detail(self.db, self.run_id, car_id, "pending", f"http_{response.status}")
                self.validate(response)
            raw = response.css("script[data-carhub-current-car]::text").get()
            try:
                car = json.loads(raw or "")
                if str(car.get("id")) != str(car_id):
                    raise ValueError
                car["id"] = car_id
                car["links"] = {**(car.get("links") or {}), "detail": response.url}
                specs = car.setdefault("specs", {})
                if not specs.get("color"):
                    colors = response.xpath(
                        '//dt[normalize-space(.)="Цвет" or normalize-space(.)="Цвет кузова"]'
                        '/following-sibling::dd[1]'
                    )
                    if colors:
                        specs["color"] = colors[0].xpath("normalize-space(.)").get()
                fields(car)
            except (ValueError, AttributeError, TypeError):
                self.defer_card(car_id, "invalid_html_detail")
            else:
                with self.db.transaction():
                    insert_vehicles(self.db, self.run_id, [car])
                    self.db.execute("UPDATE vehicle SET detail=%s WHERE run_id=%s AND car_id=%s",
                                    (Jsonb(car), self.run_id, car_id))
                    finish_detail(self.db, self.run_id, car_id, "complete")
        request = self.next_request()
        if request:
            yield request


def lock_satauto(db):
    lock = db.execute("SELECT pg_try_advisory_lock(274811)").fetchone()[0]
    if not lock:
        raise ValueError("Сбор SATAuto уже запущен")
    cooldown = db.execute("SELECT max(retry_at) FROM crawl_run WHERE origin=%s", (BASE,)).fetchone()[0]
    if cooldown and cooldown > datetime.now(timezone.utc):
        raise ValueError(f"SATAuto просит повторить после {cooldown.isoformat()}")


def latest_satauto_run(db):
    return db.execute("""
        SELECT id, cursor FROM crawl_run r WHERE origin=%s AND mode='listing'
          AND (status='partial' OR (
              status IN ('running', 'failed') AND (
                  r.pages>0
                  OR EXISTS (SELECT 1 FROM satauto_detail_task t WHERE t.run_id=r.id)
                  OR EXISTS (SELECT 1 FROM satauto_api_scan s WHERE s.run_id=r.id)
              )
          ))
        ORDER BY id DESC LIMIT 1
        """, (BASE,)).fetchone()


def satauto_counts(db, run_id):
    return db.execute("""
        SELECT count(*), count(*) FILTER (WHERE has_detail)
        FROM vehicle_inventory WHERE run_id=%s
        """, (run_id,)).fetchone()


def finish_satauto_session(db, run_id, stats, elapsed, records, diagnostic):
    attempts = stats.get("catalog/attempted", 0)
    responses = stats.get("catalog/downloaded", 0)
    db.execute("""
        WITH saved AS (
            UPDATE crawl_run SET status=%s, diagnostic=%s, finished_at=clock_timestamp(),
               attempted_requests=attempted_requests+%s, downloaded_responses=downloaded_responses+%s,
               response_bytes=response_bytes+%s, seconds=seconds+%s, retry_at=greatest(retry_at,%s)
            WHERE id=%s RETURNING origin, finished_at
        )
        INSERT INTO satauto_http_daily AS daily (day, attempts, responses, ok, bad_gateway)
        SELECT (finished_at AT TIME ZONE 'UTC')::date, %s, %s, %s, %s
        FROM saved WHERE origin=%s AND %s>0
        ON CONFLICT (day) DO UPDATE SET
            attempts=daily.attempts+excluded.attempts, responses=daily.responses+excluded.responses,
            ok=daily.ok+excluded.ok, bad_gateway=daily.bad_gateway+excluded.bad_gateway
        """, ("partial" if records else "failed", diagnostic, attempts, responses,
              stats.get("catalog/bytes", 0), elapsed, stats.get("catalog/retry_at"), run_id,
              attempts, responses, stats.get("catalog/status/200", 0),
              stats.get("catalog/status/502", 0), BASE, attempts))


def run_public_html(db, detail_limit, root_only, enrich=False, make=None, output=None, *,
                    full_scan=False, list_only=False, brand_pages=0):
    lock_satauto(db)
    run, targets = None, None
    if list_only:
        detail_limit = 0
    if full_scan:
        if not db.execute("SELECT pg_try_advisory_lock(274810)").fetchone()[0]:
            raise ValueError("Индекс SATAuto обновляется. Запустите сбор после обновления.")
        if not db.execute("SELECT EXISTS (SELECT 1 FROM sitemap_car)").fetchone()[0]:
            raise ValueError("Загрузите индекс SATAuto командой ./parse index")
    if enrich or list_only:
        run = latest_satauto_run(db)
    if enrich:
        targets = [] if full_scan else plan_details(db, run[0] if run else None, make, detail_limit)
        if targets:
            targets = [(BASE + path, car_id) for path, car_id in reversed(targets) if CAR.fullmatch(path)]
            if not targets:
                raise ValueError("Не найдены подходящие адреса карточек")
        elif full_scan:
            targets = []
        elif make or db.execute("SELECT EXISTS (SELECT 1 FROM sitemap_car)").fetchone()[0]:
            print("Нет карточек, готовых к загрузке. Состояние очереди показывает ./parse status.")
            return 0
        else:
            print("Индекс пока пуст. Начинаю с публичной витрины SATAuto.", file=sys.stderr, flush=True)
            root_only, targets = True, None
    run_id = run[0] if run else db.execute("""
        INSERT INTO crawl_run (origin,mode,page_limit)
        VALUES (%s,'listing',24) RETURNING id
        """, (BASE,)).fetchone()[0]
    if targets:
        enqueue_details(db, run_id, targets)
    before_records, before_details = satauto_counts(db, run_id)
    process = CrawlerProcess({"LOG_LEVEL": "WARNING", "LOGSTATS_INTERVAL": 0})
    crawler = process.create_crawler(SatautoSpider)
    started = perf_counter()
    brand_pages = brand_pages if list_only and not root_only else 0
    brand_after = run[1] if run and brand_pages else None
    process.crawl(crawler, db, run_id, detail_limit, root_only, targets, full_scan, brand_pages, brand_after)
    process.start()
    stats = crawler.stats.get_stats()
    elapsed = perf_counter() - started
    reason = stats.get("catalog/diagnostic") or stats.get("finish_reason")
    records, details = satauto_counts(db, run_id)
    diagnostic = "public_html_sample" if reason == "finished" else reason
    finish_satauto_session(db, run_id, stats, elapsed, records, diagnostic)
    metrics = {"run_id": run_id, "records": records, "details": details,
               "new_records": records - before_records, "new_details": details - before_details,
               "list_pages": stats.get("catalog/list_pages", 0),
               "list_records": stats.get("catalog/list_records", 0),
               "list_records_priced": stats.get("catalog/list_records_priced", 0),
               "list_records_skipped": stats.get("catalog/list_records_skipped", 0),
               "brand_pages": stats.get("catalog/brand_pages", 0),
               "brands_remaining": stats.get("catalog/brands_remaining"),
               "list_pages_with_more": stats.get("catalog/list_pages_with_more", 0),
               "deferred_details": stats.get("catalog/detail_deferred", 0),
               "missing_details": stats.get("catalog/detail_missing", 0),
               "redirects_followed": stats.get("catalog/redirects_followed", 0),
               "reason": reason, **session_metrics(stats, elapsed)}
    payload = json.dumps(metrics, ensure_ascii=False, indent=2, default=str) + "\n"
    print(payload, end="")
    if output:
        output.write_text(payload, encoding="utf-8")
    return 0 if records and reason == "finished" and not stats.get("log_count/ERROR") else 1
