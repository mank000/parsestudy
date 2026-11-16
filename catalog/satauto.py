import json
import re
from collections import deque
from datetime import datetime, timezone
from time import perf_counter

import scrapy
from psycopg.types.json import Jsonb
from scrapy.crawler import CrawlerProcess

from .sitemap import BASE, CAR, COUNTRY_CODES
from .spider import CatalogSpider, retry_time, session_metrics
from .store import insert_vehicles


def text(node, selector):
    return node.css(selector).xpath("normalize-space(.)").get() or ""


def integer(text_value):
    return int("".join(re.findall(r"[0-9]", text_value))) if re.search(r"[0-9]", text_value) else None


def card_from_html(response, article):
    path = article.css("a.satauto-car-card__link::attr(href)").get() or ""
    match = CAR.fullmatch(path)
    if not match:
        raise ValueError("Unexpected car URL")
    country, make, model, car_id, _ = match.groups()
    meta = [part.strip() for part in text(article, ".satauto-car-card__meta").split("·")]
    price = text(article, ".satauto-car-card__price")
    photo = article.css(".satauto-car-card__photo img::attr(src)").get()
    gallery = article.css("[data-card-photos]::attr(data-card-photos)").get()
    fuel = next((part for part in meta if not re.search(r"[0-9]", part)
                 and part not in {"Передний", "Задний", "Полный"} and part), None)
    return {
        "id": int(car_id),
        "source": {"country": COUNTRY_CODES.get(country, country)},
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

    def __init__(self, db, run_id, detail_limit, root_only, targets=None):
        scrapy.Spider.__init__(self)
        self.db = db
        self.run_id = run_id
        self.detail_limit = detail_limit
        self.root_only = root_only
        self.listings = deque(["/catalog/"] if targets is None else [])
        self.details = dict(targets or ())
        self.pages = 0

    async def start(self):
        yield self.next_request()

    def next_request(self):
        if self.listings:
            return scrapy.Request(BASE + self.listings.popleft(), self.parse_list, errback=self.request_failed)
        if self.details:
            url, car_id = self.details.popitem()
            return scrapy.Request(url, self.parse_car, errback=self.request_failed, cb_kwargs={"car_id": car_id})
        return None

    def validate(self, response):
        if response.status != 200:
            self.stop(f"http_{response.status}", retry_time(response.headers.get("Retry-After")))

    def parse_list(self, response):
        self.validate(response)
        if self.pages == 0 and not self.root_only:
            allowed = {"/catalog/?source_country=" + c for c in COUNTRY_CODES.values()}
            self.listings.extend(dict.fromkeys(
                href for href in response.css("a[data-catalog-country]::attr(href)").getall() if href in allowed
            ))
        try:
            cars = [card_from_html(response, article)
                    for article in response.css("#catalog-results article[data-car-id]")]
        except (ValueError, TypeError):
            self.stop("invalid_html_card")
        if not cars:
            self.stop("no_html_cards")
        with self.db.transaction():
            insert_vehicles(self.db, self.run_id, cars)
            self.db.execute("UPDATE crawl_run SET pages=pages+1 WHERE id=%s", (self.run_id,))
        self.pages += 1
        for car in cars:
            if len(self.details) >= self.detail_limit:
                break
            self.details[car["links"]["detail"]] = car["id"]
        request = self.next_request()
        if request:
            yield request

    def parse_car(self, response, car_id):
        if response.status in {404, 410}:
            with self.db.transaction():
                insert_vehicles(self.db, self.run_id, [{"id": car_id, "index_only": True}])
                self.db.execute("UPDATE vehicle SET detail_missing=true WHERE run_id=%s AND car_id=%s",
                                (self.run_id, car_id))
        else:
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
            except (ValueError, AttributeError, TypeError):
                self.stop("invalid_html_detail")
            with self.db.transaction():
                insert_vehicles(self.db, self.run_id, [car])
                self.db.execute("UPDATE vehicle SET detail=%s WHERE run_id=%s AND car_id=%s",
                                (Jsonb(car), self.run_id, car_id))
        request = self.next_request()
        if request:
            yield request


def run_public_html(db, detail_limit, root_only, enrich=False, make=None, output=None):
    lock = db.execute("SELECT pg_try_advisory_lock(274811)").fetchone()[0]
    if not lock:
        raise ValueError("Сбор публичных страниц уже запущен")
    cooldown = db.execute("SELECT max(retry_at) FROM crawl_run WHERE origin=%s", (BASE,)).fetchone()[0]
    if cooldown and cooldown > datetime.now(timezone.utc):
        raise ValueError(f"SATAuto просит повторить после {cooldown.isoformat()}")
    if enrich:
        run = db.execute("""
            SELECT id FROM crawl_run WHERE origin=%s AND mode='listing' AND status='partial'
            ORDER BY id DESC LIMIT 1
            """, (BASE,)).fetchone()
        targets = db.execute("""
            SELECT path,s.car_id FROM sitemap_car s
            WHERE (%s::text IS NULL OR make=%s)
              AND NOT EXISTS (SELECT 1 FROM vehicle v WHERE v.run_id=%s AND v.car_id=s.car_id
                              AND (v.detail IS NOT NULL OR v.detail_missing))
            ORDER BY s.car_id DESC LIMIT %s
            """, (make, make, run[0] if run else None, detail_limit)).fetchall()
        if not targets:
            print("Нет необработанных карточек по выбранной марке.")
            return 0
        targets = [(BASE + path, car_id) for path, car_id in reversed(targets) if CAR.fullmatch(path)]
        if not targets:
            raise ValueError("Не найдены подходящие адреса карточек")
    else:
        run, targets = None, None
    run_id = run[0] if run else db.execute("""
        INSERT INTO crawl_run (origin,mode,page_limit)
        VALUES ('https://satauto.ru','listing',24) RETURNING id
        """).fetchone()[0]
    counts_sql = """
        SELECT count(*) FILTER (WHERE NOT coalesce(summary ? 'index_only', false)), count(detail)
        FROM vehicle WHERE run_id=%s
    """
    before_records, before_details = db.execute(counts_sql, (run_id,)).fetchone()
    process = CrawlerProcess({"LOG_LEVEL": "WARNING", "LOGSTATS_INTERVAL": 0})
    crawler = process.create_crawler(SatautoSpider)
    started = perf_counter()
    process.crawl(crawler, db, run_id, detail_limit, root_only, targets)
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
               "reason": reason, **session_metrics(stats, elapsed)}
    payload = json.dumps(metrics, ensure_ascii=False, indent=2, default=str) + "\n"
    print(payload, end="")
    if output:
        output.write_text(payload, encoding="utf-8")
    return 0 if records and reason == "finished" and not stats.get("log_count/ERROR") else 1
