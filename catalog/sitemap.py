import json
import re
from collections import Counter
from time import perf_counter
from urllib.parse import unquote, urlsplit
from xml.etree.ElementTree import ParseError

import scrapy
from defusedxml.ElementTree import fromstring
from defusedxml.common import DefusedXmlException
from scrapy.crawler import CrawlerProcess

from .spider import CatalogSpider, retry_time


BASE = "https://satauto.ru"
NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
SHARD = re.compile(r"/sitemaps/[a-zA-Z0-9-]+/cars-[0-9]+\.xml")
CAR = re.compile(r"/catalog/([^/]+)/([^/]+)/([^/]+)/([0-9]+)-([^/]+)/")
COUNTRY_CODES = {"china": "CN", "korea": "KR", "japan": "JP"}


class SitemapAudit(CatalogSpider):
    name = "satauto_sitemap"

    def __init__(self, output, shards, origin, db=None):
        scrapy.Spider.__init__(self)
        self.output = output
        self.shards = shards
        self.origin = origin
        self.db = db

    async def start(self):
        yield scrapy.Request(self.origin + "/sitemap-cars.xml", self.parse_index, errback=self.request_failed)

    def xml(self, response, root_tag):
        if response.status != 200:
            self.stop(f"http_{response.status}", retry_time(response.headers.get("Retry-After")))
        try:
            root = fromstring(response.body)
        except (ParseError, DefusedXmlException):
            self.stop("invalid_xml")
        if root.tag != NS + root_tag:
            self.stop("invalid_sitemap_root")
        return root

    def parse_index(self, response):
        urls = []
        for element in self.xml(response, "sitemapindex"):
            url = element.findtext(NS + "loc") or ""
            parts = urlsplit(url)
            if f"{parts.scheme}://{parts.netloc}" != self.origin or not SHARD.fullmatch(parts.path) or parts.query or parts.fragment:
                self.stop("unexpected_sitemap_url")
            urls.append(url)
        if not urls or len(set(urls)) != len(urls):
            self.stop("invalid_sitemap_index")
        self.output["shards_advertised"] = len(urls)
        self.remaining = iter(urls[:self.shards])
        yield self.next_shard()

    def next_shard(self):
        url = next(self.remaining, None)
        if url:
            return scrapy.Request(url, self.parse_shard, errback=self.request_failed)
        return None

    def parse_shard(self, response):
        country_ids = self.output["country_ids"]
        root = self.xml(response, "urlset")
        records = []
        for element in root:
            url = element.findtext(NS + "loc") or ""
            parts = urlsplit(url)
            match = CAR.fullmatch(parts.path)
            if f"{parts.scheme}://{parts.netloc}" != self.origin or not match or parts.query or parts.fragment:
                self.output["unrecognized_urls"] += 1
                continue
            country, make, model, car_id, title = match.groups()
            if self.db is not None:
                records.append((int(car_id), COUNTRY_CODES.get(country, country), unquote(make),
                                unquote(model), unquote(title).replace("-", " "), parts.path))
            ids = country_ids.setdefault(country, set())
            if int(car_id) in ids:
                self.output["duplicate_country_ids"] += 1
            ids.add(int(car_id))
        if self.db is not None:
            with self.db.cursor().copy("COPY sitemap_stage FROM STDIN") as copy:
                for record in records:
                    copy.write_row(record)
        self.output["url_entries"] += len(root)
        self.output["shards_downloaded"] += 1
        request = self.next_shard()
        if request:
            yield request


def run_sitemap(path, shards, origin=BASE, db=None):
    if db is not None:
        db.execute("SELECT pg_advisory_xact_lock(274810)")
        db.execute("CREATE TEMP TABLE sitemap_stage (LIKE sitemap_car) ON COMMIT DROP")
    output = {
        "index": origin + "/sitemap-cars.xml",
        "shards_advertised": 0, "shards_downloaded": 0,
        "url_entries": 0, "unrecognized_urls": 0, "duplicate_country_ids": 0,
        "country_ids": {},
    }
    process = CrawlerProcess({"LOG_LEVEL": "WARNING", "LOGSTATS_INTERVAL": 0})
    crawler = process.create_crawler(SitemapAudit)
    start = perf_counter()
    process.crawl(crawler, output, shards, origin, db)
    process.start()
    stats = crawler.stats.get_stats()
    country_ids = output.pop("country_ids")
    output["country_counts"] = dict(Counter({key: len(values) for key, values in country_ids.items()}).most_common())
    output["unique_ids"] = len(set().union(*country_ids.values()))
    output["reason"] = stats.get("catalog/diagnostic") or stats.get("finish_reason")
    output["complete"] = (
        output["reason"] == "finished" and output["shards_downloaded"] > 0
        and output["shards_downloaded"] == output["shards_advertised"]
        and not output["unrecognized_urls"] and not stats.get("log_count/ERROR")
    )
    if db is not None and output["complete"]:
        db.execute("DELETE FROM sitemap_car")
        output["stored"] = db.execute("INSERT INTO sitemap_car SELECT * FROM sitemap_stage ON CONFLICT DO NOTHING").rowcount
    output["attempts"] = stats.get("catalog/attempted", 0)
    output["responses"] = stats.get("catalog/downloaded", 0)
    output["response_body_bytes"] = stats.get("catalog/bytes", 0)
    output["seconds"] = round(perf_counter() - start, 3)
    output["retry_at"] = stats.get("catalog/retry_at")
    payload = json.dumps(output, ensure_ascii=False, indent=2, default=str)
    print(payload)
    if path:
        path.write_text(payload + "\n", encoding="utf-8")
    if db is not None and not output["complete"]:
        return 1
    return 0 if output["reason"] == "finished" and not stats.get("log_count/ERROR") else 1
