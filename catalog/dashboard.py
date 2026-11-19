"""Local, read-only dashboard for SATAuto listings and collection metrics."""
import argparse
import os
import re
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import psycopg
from psycopg.rows import dict_row

from .inventory import COUNTRIES, amount, car_html, car_source, current_satauto_catalog, inventory_html


ASSETS = Path(__file__).with_name("dashboard_assets")
DEFAULT_DSN = "postgresql://catalog:catalog-local@127.0.0.1:55432/catalog"


def number(value, decimals=0):
    return f"{value:,.{decimals}f}".replace(",", " ").replace(".", ",")


def http_statistics(db):
    if not db.execute("SELECT to_regclass('satauto_http_daily')").fetchone()["to_regclass"]:
        return '<p class="hint">Счётчики HTTP пока недоступны.</p>'
    totals = db.execute("""
        SELECT min(day) AS first, max(day) AS last, sum(attempts) AS attempts,
               sum(responses) AS responses, sum(ok) AS ok, sum(bad_gateway) AS bad_gateway
        FROM satauto_http_daily
        """).fetchone()
    if not totals["attempts"]:
        return '<p class="hint">Пока нет сохранённых данных о запросах к SATAuto.</p>'
    first = totals["first"].strftime("%d.%m.%Y")
    last = totals["last"].strftime("%d.%m.%Y")
    dates = first if first == last else f"{first} — {last}"
    attempts = totals["attempts"]
    metrics = [
        ("HTTP-запросов", attempts, "Все учтённые попытки, включая повторные запросы"),
        ("HTTP 200 · Успешные ответы", totals["ok"], f'{number(100 * totals["ok"] / attempts, 1)}% запросов'),
        ("HTTP 502 · Ошибка шлюза", totals["bad_gateway"],
         f'{number(100 * totals["bad_gateway"] / attempts, 1)}% запросов'),
    ]
    cards = "".join(
        f'<div class="metric"><span>{label}</span><strong>{number(value)}</strong><small>{note}</small></div>'
        for label, value, note in metrics
    )
    other = totals["responses"] - totals["ok"] - totals["bad_gateway"]
    unanswered = attempts - totals["responses"]
    return f"""
      <p class="hint">Период учтённых запросов · {dates} (UTC)</p>
      <div class="metrics">{cards}</div>
      <dl class="http-details">
        <div><dt>Ответы с другими кодами</dt><dd>{number(other)}</dd></div>
        <div><dt>Запросы без HTTP-ответа</dt><dd>{number(unanswered)}</dd></div>
      </dl>
    """


def market_statistics(db, run_id):
    countries = db.execute("""
        SELECT country, grouping(country) AS overall, count(*) AS listings,
               count(*) FILTER (WHERE availability='active') AS active,
               count(*) FILTER (WHERE availability='sold') AS sold,
               count(*) FILTER (WHERE availability IN ('inactive', 'possibly_sold')) AS inactive,
               count(*) FILTER (
                   WHERE coalesce(availability, 'unknown') NOT IN ('active', 'sold', 'inactive', 'possibly_sold')
               ) AS unknown,
               count(seller_price) FILTER (WHERE availability='active' AND seller_price>0) AS seller_count,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY seller_price)
                   FILTER (WHERE availability='active' AND seller_price>0) AS seller_median,
               count(full_price) FILTER (WHERE availability='active' AND full_price>0) AS full_count,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY full_price)
                   FILTER (WHERE availability='active' AND full_price>0) AS full_median,
               count(*) FILTER (WHERE has_detail) AS detailed, count(photo) AS photos, count(mileage) AS odometer
        FROM vehicle_inventory WHERE run_id=%s
        GROUP BY GROUPING SETS ((), (country))
        ORDER BY overall, listings DESC, country
        """, (run_id,)).fetchall()
    if not countries[-1]["listings"]:
        return '<p class="hint">Сохранённых объявлений пока нет.</p>'
    headings = (
        "Страна", "Объявления", "В продаже", "Продано", "Снято или уточняется", "Статус неизвестен",
        "С ценой продавца", "Медиана цены продавца, ₽", "С полным расчётом", "Медиана полного расчёта, ₽",
    )
    coverage_headings = ("Страна", "С подробной карточкой", "С главным фото", "С пробегом")
    offers = ("listings", "active", "sold", "inactive", "unknown", "seller_count", "seller_median",
              "full_count", "full_median")
    offer_rows, coverage_rows = [], []
    for row in countries:
        country = "Всего" if row["overall"] else COUNTRIES.get(row["country"], row["country"] or "Не указана")
        offer_rows.append([country, *(amount(row[key]) for key in offers)])
        coverage_rows.append([country, *(amount(row[key]) for key in ("detailed", "photos", "odometer"))])
    return f"""
      <h3>Предложения по странам</h3>
      <p class="hint">Медианы рассчитаны по объявлениям в продаже с положительной ценой для каждого вида расчёта.</p>
      {statistics_table(headings, offer_rows)}
      <h3>Полнота карточек</h3>
      {statistics_table(coverage_headings, coverage_rows)}
    """


def statistics_table(headings, rows):
    header = "".join(f'<th scope="col">{escape(heading)}</th>' for heading in headings)
    body = "".join(
        f'<tr><th scope="row">{escape(label)}</th>' +
        "".join(f"<td>{escape(cell)}</td>" for cell in values) + "</tr>"
        for label, *values in rows
    )
    return f'<div class="table-wrap"><table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table></div>'


def database_sections(dsn):
    unavailable = '<p class="hint">Данные временно недоступны.</p>'
    try:
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as db:
            db.read_only = True
            db.execute("SET LOCAL statement_timeout = '20s'")
            requests = http_statistics(db)
            try:
                current = current_satauto_catalog(db)
                prices = (market_statistics(db, current["id"]) if current
                          else '<p class="hint">Сохранённых объявлений пока нет.</p>')
            except psycopg.errors.UndefinedTable:
                db.rollback()
                return requests, '<p class="hint">Сохранённых объявлений пока нет.</p>'
            except psycopg.Error:
                db.rollback()
                return requests, unavailable
    except psycopg.Error:
        return unavailable, unavailable
    return requests, prices


class DashboardHandler(BaseHTTPRequestHandler):
    def send_content(self, status, content_type, body):
        content = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type + "; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def do_GET(self):
        url = urlsplit(self.path)
        if url.path == "/style.css":
            return self.send_content(200, "text/css", (ASSETS / "style.css").read_text(encoding="utf-8"))
        if url.path == "/filters.js":
            return self.send_content(200, "text/javascript", (ASSETS / "filters.js").read_text(encoding="utf-8"))
        params = parse_qs(url.query, keep_blank_values=True)
        if url.path in ("/inventory/options", "/inventory/summary"):
            status, content = inventory_html(self.server.dsn, params, section=url.path.rsplit("/", 1)[1])
            return self.send_content(status, "text/html", content)
        if url.path in ("/car", "/car/source") or url.path == "/":
            if url.path in ("/car", "/car/source"):
                run, car = (parse_id(params.get(key, [""])[0]) for key in ("run", "id"))
                if run is None or car is None:
                    return self.send_content(400, "text/plain", "Некорректная ссылка на карточку")
                if url.path == "/car/source":
                    status, target = car_source(self.server.dsn, run, car)
                    if target:
                        self.send_response(status)
                        self.send_header("Location", target)
                        self.send_header("Cache-Control", "no-store")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    message = ("SATAuto временно недоступен. Попробуйте позже." if status == 503
                               else "Карточка этого лота сейчас отсутствует на SATAuto.")
                    content = f'<p>{message}</p><p><a href="/car?run={run}&amp;id={car}">Вернуться в карточку</a></p>'
                else:
                    status, content = car_html(self.server.dsn, run, car)
            else:
                status, content = inventory_html(self.server.dsn, params)
            template = (ASSETS / "inventory.html").read_text(encoding="utf-8")
            return self.send_content(status, "text/html", template.format(content=content))
        if url.path != "/analytics":
            return self.send_content(404, "text/plain", "Страница не найдена")
        requests, prices = database_sections(self.server.dsn)
        html = (ASSETS / "index.html").read_text(encoding="utf-8")
        html = html.format(requests=requests, prices=prices)
        return self.send_content(200, "text/html", html)


def parse_id(value):
    if re.fullmatch(r"[1-9][0-9]{0,18}", value) and int(value) <= 2**63 - 1:
        return int(value)
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=8765, type=int)
    args = parser.parse_args()
    with ThreadingHTTPServer((args.host, args.port), DashboardHandler) as server:
        server.dsn = os.getenv("DATABASE_URL", DEFAULT_DSN)
        print(f"http://{args.host}:{args.port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
