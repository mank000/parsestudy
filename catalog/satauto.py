import json
import re
import shutil
import sys
from collections import deque
from datetime import datetime, timezone
from time import perf_counter
from urllib.parse import urljoin, urlsplit

import scrapy
from psycopg.types.json import Jsonb
from scrapy.crawler import CrawlerProcess

from .detail_queue import enqueue_details, finish_detail, plan_details, plan_full_scan, start_detail
from .sitemap import BASE, CAR, COUNTRY_CODES
from .spider import CatalogSpider, response_retry_at, session_metrics
from .store import fields, insert_vehicles


CARD_REDIRECTS = {301, 302, 303, 307, 308}
JAPAN_LOT = re.compile(r"/catalog/japan/lot-main-([a-zA-Z0-9_-]+)/")


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
        car_id = int(car_id)
        if car_id <= 0 or car_id > 2**63 - 1:
            raise ValueError("Invalid listing ID")
    elif (lot := JAPAN_LOT.fullmatch(path)) and article.attrib.get("data-car-id") == f"main:{lot[1]}":
        country, make, model, car_id = "japan", None, None, None
        source_id = f"main:{lot[1]}"
    else:
        raise ValueError("Unexpected car URL")
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
        "links": {"detail": response.urljoin(path)},
        "media": {"main_photo": response.urljoin(photo) if photo else None,
                  "listing_photos": [{"url": response.urljoin(url)} for url in (json.loads(gallery) if gallery else [])]},
    }


class SatautoSpider(CatalogSpider):
    name = "satauto_public_html"

    def __init__(self, db, run_id, detail_limit, root_only, targets=None, full_scan=False):
        scrapy.Spider.__init__(self)
        self.db = db
        self.run_id = run_id
        self.detail_limit = detail_limit
        self.root_only = root_only
        self.listings = deque(["/catalog/"] if targets is None else [])
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
            return scrapy.Request(BASE + self.listings.popleft(), self.parse_list, errback=self.request_failed)
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

    def parse_list(self, response):
        self.validate(response)
        if self.pages == 0 and not self.root_only:
            allowed = {"/catalog/?source_country=" + c for c in COUNTRY_CODES.values()}
            self.listings.extend(dict.fromkeys(
                href for href in response.css("a[data-catalog-country]::attr(href)").getall() if href in allowed
            ))
        cars = []
        for article in response.css("#catalog-results article[data-car-id]"):
            try:
                cars.append(card_from_html(response, article))
            except (ValueError, TypeError):
                self.crawler.stats.inc_value("catalog/list_records_skipped")
        if not cars:
            self.stop("no_html_cards")
        with self.db.transaction():
            for car in cars:
                if car["id"] is None:
                    car["id"] = self.db.execute("""
                        INSERT INTO satauto_lot_id (source_id) VALUES (%s)
                        ON CONFLICT (source_id) DO UPDATE SET source_id=excluded.source_id
                        RETURNING car_id
                        """, (car["source"]["listing_id"],)).fetchone()[0]
            insert_vehicles(self.db, self.run_id, cars)
            self.db.execute("UPDATE crawl_run SET pages=pages+1 WHERE id=%s", (self.run_id,))
            if self.detail_limit:
                self.queue_listing_details(cars)
        self.list_ids.update(car["id"] for car in cars)
        self.list_priced_ids.update(
            car["id"] for car in cars if car["pricing"]["presentation"]["amount_rub"] is not None
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
            if car["id"] in ready and CAR.fullmatch(urlsplit(car["links"]["detail"]).path):
                self.details[car["links"]["detail"]] = car["id"]
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


def run_public_html(db, detail_limit, root_only, enrich=False, make=None, output=None, *,
                    full_scan=False, list_only=False):
    lock = db.execute("SELECT pg_try_advisory_lock(274811)").fetchone()[0]
    if not lock:
        raise ValueError("Сбор публичных страниц уже запущен")
    cooldown = db.execute("SELECT max(retry_at) FROM crawl_run WHERE origin=%s", (BASE,)).fetchone()[0]
    if cooldown and cooldown > datetime.now(timezone.utc):
        raise ValueError(f"SATAuto просит повторить после {cooldown.isoformat()}")
    run, targets = None, None
    if list_only:
        detail_limit = 0
    if full_scan:
        if not db.execute("SELECT pg_try_advisory_lock(274810)").fetchone()[0]:
            raise ValueError("Индекс SATAuto обновляется. Запустите сбор после обновления.")
        if not db.execute("SELECT EXISTS (SELECT 1 FROM sitemap_car)").fetchone()[0]:
            raise ValueError("Загрузите индекс SATAuto командой ./parse index")
    if enrich or list_only:
        run = db.execute("""
            SELECT id FROM crawl_run r WHERE origin=%s AND mode='listing'
              AND (status='partial' OR (
                  status IN ('running', 'failed') AND EXISTS (
                      SELECT 1 FROM satauto_detail_task t WHERE t.run_id=r.id
                  )
              ))
            ORDER BY id DESC LIMIT 1
            """, (BASE,)).fetchone()
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
    counts_sql = """
        SELECT count(*) FILTER (WHERE NOT coalesce(summary ? 'index_only', false)), count(detail)
        FROM vehicle WHERE run_id=%s
    """
    before_records, before_details = db.execute(counts_sql, (run_id,)).fetchone()
    process = CrawlerProcess({"LOG_LEVEL": "WARNING", "LOGSTATS_INTERVAL": 0})
    crawler = process.create_crawler(SatautoSpider)
    started = perf_counter()
    process.crawl(crawler, db, run_id, detail_limit, root_only, targets, full_scan)
    process.start()
    stats = crawler.stats.get_stats()
    elapsed = perf_counter() - started
    reason = stats.get("catalog/diagnostic") or stats.get("finish_reason")
    records, details = db.execute(counts_sql, (run_id,)).fetchone()
    attempts = stats.get("catalog/attempted", 0)
    db.execute("""
        UPDATE crawl_run SET status=%s, diagnostic=%s, finished_at=clock_timestamp(),
           attempted_requests=attempted_requests+%s, downloaded_responses=downloaded_responses+%s,
           response_bytes=response_bytes+%s, seconds=seconds+%s, retry_at=%s
        WHERE id=%s
        """, ("partial" if records else "failed", "public_html_sample" if reason == "finished" else reason,
              attempts, stats.get("catalog/downloaded", 0), stats.get("catalog/bytes", 0), elapsed,
              stats.get("catalog/retry_at"), run_id))
    metrics = {"run_id": run_id, "records": records, "details": details,
               "new_records": records - before_records, "new_details": details - before_details,
               "list_pages": stats.get("catalog/list_pages", 0),
               "list_records": stats.get("catalog/list_records", 0),
               "list_records_priced": stats.get("catalog/list_records_priced", 0),
               "list_records_skipped": stats.get("catalog/list_records_skipped", 0),
               "deferred_details": stats.get("catalog/detail_deferred", 0),
               "missing_details": stats.get("catalog/detail_missing", 0),
               "redirects_followed": stats.get("catalog/redirects_followed", 0),
               "reason": reason, **session_metrics(stats, elapsed)}
    payload = json.dumps(metrics, ensure_ascii=False, indent=2, default=str) + "\n"
    print(payload, end="")
    if output:
        output.write_text(payload, encoding="utf-8")
    return 0 if records and reason == "finished" and not stats.get("log_count/ERROR") else 1
