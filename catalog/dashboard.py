"""Local, read-only dashboard for stored catalog runs."""
import argparse
import json
import os
import re
from datetime import datetime
from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zoneinfo import ZoneInfo
from urllib.parse import parse_qs, urlsplit

import psycopg
from psycopg.rows import dict_row, tuple_row

from .inventory import car_html, inventory_html
from .report import render_report


ASSETS = Path(__file__).with_name("dashboard_assets")
REPORTS = Path(__file__).resolve().parents[1] / "reports"
DEFAULT_DSN = "postgresql://catalog:catalog-local@127.0.0.1:55432/catalog"
COUNTRIES = {"china": "Китай", "korea": "Южная Корея", "japan": "Япония"}
STATUS = {"complete": "Завершён", "partial": "Частичный", "failed": "Ошибка", "running": "Идёт"}
CELL = re.compile(r"(?<!\\)\|")


def number(value, decimals=0):
    return f"{value:,.{decimals}f}".replace(",", " ").replace(".", ",")


def read_metrics(name):
    return json.loads((REPORTS / name).read_text(encoding="utf-8"))


def update_date():
    title = (REPORTS / "RESULTS.md").read_text(encoding="utf-8")
    return next((line.removeprefix("Замеры выполнены ").split(".")[0]
                 for line in title.splitlines() if line.startswith("Замеры выполнены ")), "")


def overview():
    latest = REPORTS / "catalog-sitemap.json"
    audit = read_metrics("catalog-sitemap.json" if latest.is_file() else "live-satauto-sitemap.json")
    if latest.is_file() and audit.get("complete"):
        captured = datetime.fromtimestamp(latest.stat().st_mtime, ZoneInfo("Asia/Barnaul")).strftime("%d.%m.%Y")
    else:
        audit = read_metrics("live-satauto-sitemap.json")
        captured = update_date()
    probe = read_metrics("live-carhub-demo.json" if (REPORTS / "live-carhub-demo.json").is_file()
                         else "live-carhub-2026-11-17.json")
    benchmark = read_metrics("benchmark.json")
    scope = "Полная карта сайта" if audit["complete"] else "Часть карты сайта"
    stats = [
        ("Карточек в карте сайта", number(audit["unique_ids"]), scope),
        ("Время обхода", f'{number(audit["seconds"], 1)} с', "Scrapy · живой замер"),
        ("HTTP-запросов", number(audit["attempts"]), f'{audit["shards_downloaded"]} XML-файлов, индекс и robots.txt'),
    ]
    cards = "".join(f'<div class="metric"><span>{label}</span><strong>{value}</strong><small>{note}</small></div>'
                    for label, value, note in stats)
    bars = []
    counts = audit["country_counts"]
    for rank, (country, count) in enumerate(sorted(counts.items(), key=lambda row: -row[1]), 1):
        label = escape(COUNTRIES.get(country, country))
        share = count / audit["unique_ids"] * 100 if audit["unique_ids"] else 0
        width = count / max(counts.values()) * 100 if max(counts.values()) else 0
        bars.append(f"""
          <div class="bar-row">
            <div class="bar-label"><span>{label}</span><strong>{number(count)}</strong></div>
            <div class="track"><div class="bar rank-{rank}" style="width:{width:.2f}%"></div></div>
            <span class="share">{number(share, 2)}%</span>
          </div>""")
    return f"""
    <p class="eyebrow">Снимок от {escape(captured)}</p>
    <h1>Аналитика каталога</h1>
    <p class="intro">Охват SATAuto по странам, цены и результаты сборщика.</p>
    <div class="metrics">{cards}</div>
    <div class="overview-grid">
      <section class="panel"><h2>Карточки по странам</h2>
        <p class="hint">Карта сайта учитывает опубликованные страницы. Доступность автомобилей проверяется по объявлениям.</p>
        <div class="chart">{"".join(bars)}</div>
      </section>
      <section class="panel"><h2>Проверка сборщика</h2>
        <dl class="checks">
          <div><dt>CarHub · живой прогон</dt><dd>{number(probe["records"])} записей · {number(probe["session_attempts"])} запроса · {number(probe["session_seconds"], 3)} с</dd></div>
          <div><dt>Локальный тест · синтетические данные</dt><dd>{number(benchmark["records"])} записей за {number(benchmark["session_seconds"], 3)} с</dd></div>
          <div><dt>Запросов в локальном тесте</dt><dd>{number(benchmark["session_attempts"])}</dd></div>
        </dl>
      </section>
    </div>"""


def report_html(markdown):
    """Render the headings, paragraphs and tables emitted by render_report."""
    output = []
    lines = iter(markdown.splitlines())
    for line in lines:
        if line.startswith("# "):
            continue
        if line.startswith("## "):
            output.append(f"<h3>{escape(line[3:])}</h3>")
        elif line.startswith("|"):
            output.append('<div class="table-wrap"><table>')
            row = line
            header = True
            while row.startswith("|"):
                cells = [cell.strip().replace(r"\|", "|") for cell in CELL.split(row)[1:-1]]
                if header:
                    output.append("<thead><tr>" + "".join(
                        f'<th scope="col">{escape(cell)}</th>' for cell in cells
                    ) + "</tr></thead><tbody>")
                    next(lines, "")  # Markdown header separator
                    header = False
                else:
                    output.append("<tr>" + "".join(f"<td>{escape(cell)}</td>" for cell in cells) + "</tr>")
                row = next(lines, "")
            output.append("</tbody></table></div>")
        elif line:
            output.append(f"<p>{escape(line.removeprefix('**').removesuffix('**'))}</p>")
    return "".join(output)


def database_sections(dsn, selected):
    try:
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as db:
            db.read_only = True
            db.execute("SET LOCAL statement_timeout = '15s'")
            runs = db.execute("""
                SELECT id, origin, mode, status, attempted_requests, seconds, diagnostic,
                       EXISTS (SELECT 1 FROM vehicle WHERE run_id=crawl_run.id) AS has_vehicles
                FROM crawl_run ORDER BY id DESC LIMIT 20
                """).fetchall()
            if selected is None:
                current = db.execute("""
                    SELECT crawl_run.* FROM crawl_run
                    WHERE status IN ('complete', 'partial')
                      AND EXISTS (SELECT 1 FROM vehicle WHERE run_id=crawl_run.id)
                    ORDER BY (status='complete') DESC, id DESC LIMIT 1
                    """).fetchone()
            else:
                current = db.execute("SELECT * FROM crawl_run WHERE id=%s", (selected,)).fetchone()
            # render_report uses tuple rows for its reusable reporting queries.
            db.row_factory = tuple_row
            report = report_html(render_report(db, current)) if current else ""
    except psycopg.errors.UndefinedTable:
        return '<p class="hint">В PostgreSQL пока нет прогонов. Выполните импорт или запустите сбор.</p>', ""
    except psycopg.Error:
        return '<p class="hint">PostgreSQL временно недоступен. Снимок карты сайта остаётся доступен.</p>', ""
    if not report:
        message = "Такой прогон отсутствует." if selected is not None else "В PostgreSQL пока нет объявлений с ценами. Здесь появятся медианы и сравнение моделей после успешного сбора или импорта."
        report = f'<p class="hint">{message}</p>'
    rows = []
    for run in runs:
        run_id = run["id"]
        origin = "Импорт файла" if run["mode"] == "import" else run["origin"]
        detail = f' · {escape(run["diagnostic"])}' if run["diagnostic"] else ""
        data = "Есть" if run["has_vehicles"] else "Нет"
        rows.append(f"""<tr><td><a href="/analytics?run={run_id}#prices">#{run_id}</a></td>
          <td>{escape(origin)}</td><td>{escape(STATUS.get(run["status"], run["status"]))}{detail}</td>
          <td>{data}</td><td>{number(run["attempted_requests"])}</td><td>{number(run["seconds"], 3)} с</td></tr>""")
    if current:
        report = f'<p class="selection">Прогон #{current["id"]} · <a href="/analytics#prices">Последний с данными</a></p>' + report
    history = (
        '<div class="table-wrap"><table><thead><tr>'
        '<th>Прогон</th><th>Источник</th><th>Статус</th><th>Объявления</th><th>HTTP</th><th>Время</th>'
        '</tr></thead><tbody>' + "".join(rows) + '</tbody></table></div>'
    ) if rows else '<p class="hint">История прогонов пуста.</p>'
    return report, history


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
        params = parse_qs(url.query, keep_blank_values=True)
        if url.path == "/car" or (url.path == "/" and "run" not in params):
            if url.path == "/car":
                run, car = (parse_id(params.get(key, [""])[0]) for key in ("run", "id"))
                if run is None or car is None:
                    return self.send_content(400, "text/plain", "Некорректный ID карточки или прогона")
                status, content = car_html(self.server.dsn, run, car)
            else:
                status, content = inventory_html(self.server.dsn, params)
            template = (ASSETS / "inventory.html").read_text(encoding="utf-8")
            return self.send_content(status, "text/html", template.format(content=content))
        if url.path not in ("/", "/analytics"):
            return self.send_content(404, "text/plain", "Страница не найдена")
        selected = params.get("run", [None])[0]
        if selected is not None and parse_id(selected) is None:
            return self.send_content(400, "text/plain", "Некорректный номер прогона")
        prices, history = database_sections(self.server.dsn, int(selected) if selected else None)
        html = (ASSETS / "index.html").read_text(encoding="utf-8")
        html = html.format(overview=overview(), prices=prices, history=history)
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
