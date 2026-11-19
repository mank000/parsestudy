import re
from http.client import HTTPException, HTTPSConnection


AUCTION_ID = re.compile(r"(main|stats):([A-Za-z0-9_-]{1,128})")
SATAUTO_HOST = "satauto.ru"


def resolve_auction_page(source_id):
    match = AUCTION_ID.fullmatch(source_id or "")
    if match is None:
        return 404, None

    feed, lot_key = match.groups()
    alternate_feed = "stats" if feed == "main" else "main"
    for dataset in (feed, alternate_feed):
        path = f"/catalog/japan/lot-{dataset}-{lot_key}/"
        connection = HTTPSConnection(SATAUTO_HOST, timeout=6)
        try:
            connection.request("GET", path, headers={"Accept": "text/html", "User-Agent": "CatalogStudy/1.0"})
            with connection.getresponse() as response:
                if response.getheader("Retry-After"):
                    return 503, None
                if response.status in (404, 410):
                    continue
                if response.status != 200:
                    return 503, None
                # The lot page identifies itself with the feed and key used by SATAuto's frontend.
                marker = f'data-car-id="{dataset}:{lot_key}"'.encode("ascii")
                if marker in response.read(512 * 1024):
                    return 302, f"https://{SATAUTO_HOST}{path}"
        except (HTTPException, OSError):
            return 503, None
        finally:
            connection.close()
    return 404, None
