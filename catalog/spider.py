import json
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import urlencode, urlsplit

import scrapy
from psycopg.types.json import Jsonb
from scrapy.exceptions import CloseSpider, IgnoreRequest
from scrapy.http import TextResponse

from .store import insert_vehicles


PUBLIC_ORIGIN = "https://carhubauto.ru"


def session_metrics(stats, seconds):
    return {
        "session_attempts": stats.get("catalog/attempted", 0),
        "session_responses": stats.get("catalog/downloaded", 0),
        "session_bytes": stats.get("catalog/bytes", 0),
        "session_seconds": round(seconds, 3),
        "http_statuses": {
            key.rsplit("/", 1)[-1]: count
            for key, count in stats.items() if key.startswith("catalog/status/")
        },
        "http_errors": stats.get("catalog/http_errors", 0),
        "transport_errors": stats.get("catalog/transport_errors", 0),
        "logged_errors": stats.get("log_count/ERROR", 0),
        "retry_at": stats.get("catalog/retry_at"),
        "http_503": stats.get("catalog/http_503"),
    }


def retry_time(header):
    if not header:
        return None
    value = header.decode("ascii", errors="replace").strip()
    try:
        return datetime.now(timezone.utc) + timedelta(seconds=max(0, int(value)))
    except OverflowError:
        return None
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
            return when if when.tzinfo else when.replace(tzinfo=timezone.utc)
        except (TypeError, ValueError, OverflowError):
            return None


class CatalogSpider(scrapy.Spider):
    name = "carhub_catalog"
    custom_settings = {
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": "CatalogStudy/1.0",
        "CONCURRENT_REQUESTS": 1,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "DOWNLOAD_DELAY": 1.1,
        "DOWNLOAD_DELAY_JITTER": 0,
        "DOWNLOAD_TIMEOUT": 20,
        "DOWNLOAD_MAXSIZE": 10 * 1024 * 1024,
        "URLLENGTH_LIMIT": 16_384,
        "RETRY_ENABLED": False,
        "REDIRECT_ENABLED": False,
        "METAREFRESH_ENABLED": False,
        "COOKIES_ENABLED": False,
        "HTTPPROXY_ENABLED": False,
        "TELNETCONSOLE_ENABLED": False,
        "HTTPERROR_ALLOW_ALL": True,
        "DOWNLOADER_MIDDLEWARES": {
            "catalog.spider.AccessGuard": 110,
        },
    }

    def __init__(self, db, run, page_budget, token=None, **kwargs):
        super().__init__(**kwargs)
        self.db = db
        self.run = run
        self.page_budget = page_budget
        self.pages_this_session = 0
        self.headers = {"Accept": "application/json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def request(self, suffix, callback, **kwargs):
        return scrapy.Request(
            self.run["origin"] + "/api/v2/cars" + suffix,
            callback=callback, errback=self.request_failed, headers=self.headers, **kwargs,
        )

    async def start(self):
        if self.run["listing_done"]:
            request = self.next_detail()
        else:
            request = self.page_request(self.run["cursor"])
        if request:
            yield request

    def page_request(self, cursor):
        query = {"limit": self.run["page_limit"]}
        if self.run["source_country"]:
            query["source_country"] = self.run["source_country"]
        if cursor:
            query["cursor"] = cursor
        suffix = "?" + urlencode(query)
        if len(self.run["origin"]) + len("/api/v2/cars") + len(suffix) > self.settings.getint("URLLENGTH_LIMIT"):
            self.stop("cursor_url_too_long")
        return self.request(suffix, self.parse_page)

    def stop(self, reason, retry_at=None):
        stats = self.crawler.stats
        stats.set_value("catalog/diagnostic", reason)
        stats.set_value("catalog/retry_at", retry_at)
        raise CloseSpider(reason)

    def request_failed(self, failure):
        if failure.check(IgnoreRequest):
            reason = self.crawler.stats.get_value("catalog/diagnostic", "robots_denied")
            self.stop(reason, self.crawler.stats.get_value("catalog/retry_at"))
        self.stop(failure.type.__name__)

    def payload(self, response):
        if response.status != 200:
            when = retry_time(response.headers.get("Retry-After"))
            if response.status == 429 and when is None:
                when = datetime.now(timezone.utc) + timedelta(minutes=1)
            self.stop(f"http_{response.status}", when)
        content_type = response.headers.get("Content-Type", b"").lower()
        if b"application/json" not in content_type:
            self.stop("unexpected_content_type")
        try:
            result = json.loads(response.body)
        except (ValueError, UnicodeError):
            self.stop("invalid_json")
        if not isinstance(result, dict):
            self.stop("invalid_payload")
        return result

    def parse_page(self, response):
        payload = self.payload(response)
        cars = payload.get("data")
        pagination = payload.get("pagination")
        if not isinstance(cars, list) or not isinstance(pagination, dict):
            self.stop("invalid_page")
        more = pagination.get("has_more")
        cursor = pagination.get("next_cursor") if more else None
        if (type(more) is not bool or len(cars) > self.run["page_limit"]
                or (more and (not cars or not isinstance(cursor, str) or not cursor))):
            self.stop("invalid_pagination")
        try:
            with self.db.transaction():
                if more:
                    added = self.db.execute(
                        "INSERT INTO crawl_cursor VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (self.run["id"], cursor),
                    ).rowcount
                    if not added:
                        self.stop("repeated_cursor")
                insert_vehicles(self.db, self.run["id"], cars)
                self.db.execute(
                    """UPDATE crawl_run SET pages=pages+1, cursor=%s, listing_done=%s
                       WHERE id=%s""", (cursor, not more, self.run["id"]),
                )
        except (ValueError, TypeError, AttributeError):
            self.stop("invalid_vehicle")
        self.pages_this_session += 1
        if not more:
            request = self.next_detail()
            if request:
                yield request
        elif self.page_budget is not None and self.pages_this_session >= self.page_budget:
            self.stop("page_budget")
        else:
            yield self.page_request(cursor)

    def next_detail(self):
        if self.run["mode"] != "full":
            return None
        car = self.db.execute(
            """SELECT car_id FROM vehicle WHERE run_id=%s AND detail IS NULL
               AND NOT detail_missing ORDER BY car_id LIMIT 1""",
            (self.run["id"],),
        ).fetchone()
        if car:
            return self.request(f"/{car[0]}", self.parse_detail, cb_kwargs={"car_id": car[0]})
        return None

    def parse_detail(self, response, car_id):
        if response.status in {404, 410}:
            self.db.execute(
                "UPDATE vehicle SET detail_missing=true WHERE run_id=%s AND car_id=%s",
                (self.run["id"], car_id),
            )
        else:
            payload = self.payload(response)
            car = payload.get("data")
            if not isinstance(car, dict) or car.get("id") != car_id:
                self.stop("invalid_detail")
            self.db.execute(
                "UPDATE vehicle SET detail=%s WHERE run_id=%s AND car_id=%s",
                (Jsonb(payload), self.run["id"], car_id),
            )
        request = self.next_detail()
        if request:
            yield request


class AccessGuard:
    """Fail closed when robots.txt cannot be read and count network attempts."""

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls()
        instance.crawler = crawler
        crawler.signals.connect(instance.scheduled_network_request, signal=scrapy.signals.request_reached_downloader)
        crawler.signals.connect(instance.downloaded_response, signal=scrapy.signals.response_downloaded)
        return instance

    def scheduled_network_request(self, request, spider=None):
        self.crawler.stats.inc_value("catalog/attempted")

    def downloaded_response(self, response, request, spider=None):
        self.crawler.stats.inc_value("catalog/downloaded")
        self.crawler.stats.inc_value("catalog/bytes", len(response.body))
        self.crawler.stats.inc_value(f"catalog/status/{response.status}")
        if response.status >= 400:
            self.crawler.stats.inc_value("catalog/http_errors")

    def process_response(self, request, response):
        if response.status == 503:
            url = urlsplit(request.url)
            content_type = response.headers.get("Content-Type", b"").decode("ascii", errors="replace")
            retry_after = response.headers.get("Retry-After")
            diagnostic = {
                "path": url.path,
                "content_type": content_type,
                "retry_after": retry_after.decode("ascii", errors="replace") if retry_after else None,
            }
            if url.netloc in {"carhubauto.ru", "satauto.ru"} and "application/json" in content_type.lower():
                try:
                    payload = json.loads(response.body)
                except (ValueError, UnicodeError):
                    payload = {}
                if isinstance(payload, dict):
                    diagnostic["message"] = {
                        key: payload[key][:240] for key in ("error", "detail", "message")
                        if isinstance(payload.get(key), str)
                    }
            self.crawler.stats.set_value("catalog/http_503", diagnostic)
        if request.url.endswith("/robots.txt") and response.status not in {200, 404, 410}:
            self.crawler.stats.set_value("catalog/diagnostic", f"robots_http_{response.status}")
            self.crawler.stats.set_value("catalog/retry_at", retry_time(response.headers.get("Retry-After")))
            return TextResponse(request.url, body=b"User-agent: *\nDisallow: /\n", encoding="utf-8")
        return response

    def process_exception(self, request, exception):
        self.crawler.stats.inc_value("catalog/transport_errors")
        if request.url.endswith("/robots.txt"):
            self.crawler.stats.set_value("catalog/diagnostic", "robots_unavailable")
            return TextResponse(request.url, body=b"User-agent: *\nDisallow: /\n", encoding="utf-8")
        return None
