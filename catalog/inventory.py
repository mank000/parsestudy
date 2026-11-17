from html import escape
from math import ceil
import re
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from .trims import SPEC_LABELS, trim_for_card


COUNTRIES = {"CN": "Китай", "KR": "Южная Корея", "JP": "Япония"}
AVAILABILITY = {"active": "В продаже", "sold": "Продано", "inactive": "Снято",
                "possibly_sold": "Уточняется", "unknown": "Не указано"}
ORDERS = {"recent": ("Сначала старшие ID", "car_id DESC"),
          "price_up": ("Цена по возрастанию", "price ASC NULLS LAST, car_id DESC"),
          "price_down": ("Цена по убыванию", "price DESC NULLS LAST, car_id DESC"),
          "year": ("Сначала новые", "year DESC NULLS LAST, car_id DESC"),
          "mileage": ("Сначала малый пробег", "mileage ASC NULLS LAST, car_id DESC")}
NUMERIC = {"min_price": ("price >=", "Цена от, ₽"), "max_price": ("price <=", "Цена до, ₽"),
           "min_year": ("year >=", "Год от"), "max_year": ("year <=", "Год до"),
           "max_mileage": ("mileage <=", "Пробег до, км")}
PRICES = {
    "published": ("Опубликованная стоимость", "coalesce(full_price, estimated_price, displayed_price)"),
    "seller": ("Цена продавца в ₽", "seller_price"),
    "estimated": ("Предварительный расчёт", "estimated_price"),
    "full": ("Полный расчёт", "full_price"),
}
SELECTS = {"country": ("Страна", COUNTRIES), "availability": ("Наличие", AVAILABILITY),
           "scope": ("Состав", {"all": "Индекс SATAuto", "details": "С данными объявления"}),
           "price_type": ("Какая цена", {key: label for key, (label, _) in PRICES.items()}),
           "sort": ("Сортировка", {key: label for key, (label, _) in ORDERS.items()})}
PAGE_SIZE = 40
SAT = "https://satauto.ru"
SOURCE_NAMES = {SAT: "SATAuto", "https://carhubauto.ru": "CarHub", "export": "Выгрузка SATAuto"}
METADATA = """
    SELECT v.*, coalesce(v.detail->'data', v.detail, v.summary) AS data
    FROM vehicle v WHERE v.run_id=%s AND NOT coalesce(v.summary ? 'index_only', false)
"""
VEHICLES = """
    SELECT car_id, country,
           trim(BOTH '-' FROM regexp_replace(lower(coalesce(data#>>'{identity,manufacturer}',manufacturer)),
                '[^[:alnum:]]+', '-', 'g')) AS make,
           coalesce(data#>>'{identity,model}', model) AS model,
           coalesce(data#>>'{identity,title}', summary#>>'{identity,title}') AS title,
           coalesce(data#>>'{links,detail}', data#>>'{links,public_url}', data#>>'{links,carhub}',
                    summary#>>'{links,detail}', summary#>>'{links,carhub}') AS path,
           coalesce(CASE WHEN data#>>'{listing,year}' ~ '^[0-9]{4}$' THEN (data#>>'{listing,year}')::int END,
                    year) AS year,
           coalesce(CASE WHEN data#>>'{listing,mileage_km}' ~ '^[0-9]{1,9}$'
                         THEN (data#>>'{listing,mileage_km}')::int END, mileage_km) AS mileage,
           coalesce(data#>>'{listing,sell_status}', sell_status) AS availability,
           nullif(btrim(coalesce(nullif(data#>>'{specs,color}', ''), data#>>'{specs,exterior_color}',
                                 data#>>'{listing,color}', data->>'color', summary#>>'{specs,color}')), '') AS color,
           coalesce(data#>>'{specs,fuel_type}', fuel) AS fuel,
           coalesce(data#>>'{media,main_photo}', summary#>>'{media,main_photo}') AS photo,
           true AS loaded,
           (year IS NOT NULL OR mileage_km IS NOT NULL OR fuel IS NOT NULL OR power_hp IS NOT NULL
             OR data#>>'{listing,year}' IS NOT NULL OR data#>>'{listing,mileage_km}' IS NOT NULL
             OR data#>>'{specs,fuel_type}' IS NOT NULL OR data#>>'{specs,drive_type}' IS NOT NULL
             OR data#>>'{specs,engine_power_hp}' IS NOT NULL) AS has_data,
           detail IS NOT NULL AS has_detail,
           coalesce(data#>>'{listing,currency}', currency) AS currency,
           coalesce(listing_price::text, data#>>'{listing,price}') AS original_price,
           seller_price, full_price, estimated_price, partial_price, displayed_price, price_kind
    FROM (
        SELECT *,
               coalesce(data#>>'{pricing,presentation,kind}', data#>>'{pricing,status}',
                        summary#>>'{pricing,presentation,kind}', status_price,
                        CASE WHEN strpos(lower(summary#>>'{pricing,presentation,label}'), 'запрос') > 0
                             THEN 'on_request' END) AS price_kind,
               CASE WHEN (data#>>'{pricing,presentation,amount_rub}') ~ '^[0-9]{1,15}$'
                    THEN (data#>>'{pricing,presentation,amount_rub}')::numeric END AS presented_amount,
               CASE WHEN (summary#>>'{pricing,presentation,amount_rub}') ~ '^[0-9]{1,15}$'
                    THEN (summary#>>'{pricing,presentation,amount_rub}')::numeric END AS summary_amount
        FROM metadata
    ) listing
    CROSS JOIN LATERAL (
        SELECT coalesce(
                   CASE WHEN (data#>>'{pricing,listing_rub}') ~ '^[0-9]{1,15}([.][0-9]{1,4})?$'
                        THEN (data#>>'{pricing,listing_rub}')::numeric END,
                   listing_rub) AS seller_price,
               CASE WHEN price_kind IN ('final', 'complete') OR price_kind IS NULL
                    THEN coalesce(CASE WHEN price_kind IN ('final', 'complete') THEN presented_amount END,
                                  total_rub) END AS full_price,
               CASE WHEN price_kind='estimated' OR price_kind IS NULL
                    THEN coalesce(CASE WHEN price_kind='estimated' THEN presented_amount END,
                                  estimated_rub) END AS estimated_price,
               CASE WHEN price_kind IN ('known_part', 'partial')
                    THEN coalesce(presented_amount, known_rub) END AS partial_price,
               CASE WHEN price_kind IS NULL
                    THEN coalesce(presented_amount, summary_amount) END AS displayed_price
    ) prices
"""
INDEXED = """
    SELECT s.car_id, s.country, s.make, s.model, s.title, s.path,
           NULL::int AS year, NULL::int AS mileage, NULL::text AS availability,
           NULL::text AS color, NULL::text AS fuel, NULL::text AS photo,
           false AS loaded, false AS has_data, false AS has_detail, NULL::text AS currency, NULL::text AS original_price,
           NULL::numeric AS seller_price, NULL::numeric AS full_price, NULL::numeric AS estimated_price,
           NULL::numeric AS partial_price, NULL::numeric AS displayed_price, NULL::text AS price_kind
    FROM sitemap_car s WHERE NOT EXISTS (SELECT 1 FROM detailed d WHERE d.car_id=s.car_id)
"""


def amount(value, currency=False):
    if value is None:
        return "—"
    return f"{value:,.0f}".replace(",", " ") + (" ₽" if currency else "")


def safe_url(value, origin=SAT):
    if not value:
        return ""
    if value.startswith("/") and not value.startswith("//"):
        return (SAT if origin == "export" else origin) + value
    try:
        url = urlsplit(value)
    except ValueError:
        return ""
    return value if url.scheme in {"https", "http"} and url.hostname else ""


BRAND_LABELS = {"bmw": "BMW", "byd": "BYD", "gac": "GAC", "mg": "MG",
                "mercedes-benz": "Mercedes-Benz", "mini": "MINI", "kg-mobility": "KG Mobility"}


def brand_label(slug):
    return BRAND_LABELS.get(slug, slug.replace("-", " ").title())


def parse_filters(params):
    filters = {}
    for key in ({"q", "make", "color", "fuel", "page", "source"} | NUMERIC.keys() | SELECTS.keys()):
        value = params.get(key, [""])[0].strip()
        if not value:
            continue
        if key in SELECTS and value not in SELECTS[key][1]:
            raise ValueError("Выберите допустимое значение фильтра.")
        if key in NUMERIC or key == "page":
            if not value.isascii() or not value.isdecimal() or len(value) > 10 or int(value) > 2_147_483_647:
                raise ValueError("Укажите целое неотрицательное число в числовых фильтрах.")
        if len(value) > 120:
            raise ValueError("Текст фильтра должен быть короче 121 символа.")
        if key == "make":
            value = re.sub(r"[^\w]+", "-", value.lower().replace("_", "-")).strip("-")
            if not value:
                raise ValueError("Укажите марку автомобиля.")
        filters[key] = value
    for lower, upper in (("min_price", "max_price"), ("min_year", "max_year")):
        if lower in filters and upper in filters and int(filters[lower]) > int(filters[upper]):
            raise ValueError("Нижняя граница диапазона выше верхней.")
    return filters


def query_parts(filters, run_id):
    clauses, args = [], [run_id]
    if "country" in filters:
        clauses.append("country=%s")
        args.append(filters["country"])
    if "make" in filters:
        clauses.append("make=%s")
        args.append(filters["make"])
    q = filters.get("q", "")
    if q.isascii() and q.isdecimal() and len(q) <= 18:
        clauses.append("car_id=%s")
        args.append(int(q))
    elif q:
        for word in q.split():
            clauses.append("strpos(lower(concat_ws(' ', title, replace(make,'-',' '), replace(model,'-',' '), car_id)), lower(%s))>0")
            args.append(word)
    for field in ("color", "fuel"):
        if field in filters:
            clauses.append(f"strpos(lower({field}), lower(%s))>0")
            args.append(filters[field])
    if "availability" in filters:
        clauses.append("coalesce(availability, 'unknown')=%s")
        args.append(filters["availability"])
    for key, (comparison, _) in NUMERIC.items():
        if key in filters:
            clauses.append(comparison + " %s")
            args.append(int(filters[key]))
    index_scope = filters.get("scope") != "details"
    needs_known = bool(filters.keys() & ({"color", "fuel"} | NUMERIC.keys()))
    needs_known |= filters.get("availability", "unknown") != "unknown"
    price = PRICES[filters.get("price_type", "published")][1]
    prefix = f"WITH metadata AS ({METADATA}), detailed AS ({VEHICLES}), inventory AS (SELECT * FROM detailed"
    if index_scope:
        prefix += " WHERE EXISTS (SELECT 1 FROM sitemap_car s WHERE s.car_id=detailed.car_id)"
        if not needs_known:
            prefix += " UNION ALL " + INDEXED
    prefix += f"), fleet AS (SELECT *, {price} AS price FROM inventory) "
    return prefix, " WHERE " + " AND ".join(clauses) if clauses else "", args


def available_sources(db):
    return db.execute("""
        SELECT DISTINCT ON (origin) id, origin, status, started_at, finished_at
        FROM crawl_run WHERE status IN ('complete','partial')
          AND EXISTS (SELECT 1 FROM vehicle WHERE run_id=crawl_run.id
                      AND NOT coalesce(summary ? 'index_only', false))
        ORDER BY origin, (status='complete') DESC, id DESC
        """).fetchall()


def fetch_inventory(db, filters, sources=None):
    if sources is None:
        sources = available_sources(db)
    preferred = ("export", SAT, "https://carhubauto.ru")
    by_origin = {source["origin"]: source for source in sources}
    if filters.get("scope") != "details":
        run = by_origin.get("export") or by_origin.get(SAT)
    elif filters.get("source"):
        run = by_origin.get(filters["source"])
    else:
        run = next((by_origin[key] for key in preferred if key in by_origin), sources[0] if sources else None)
    prefix, where, args = query_parts(filters, run["id"] if run else None)
    index_scope = filters.get("scope") != "details"
    sets = "(),(country)" if index_scope else "(),(country),(make)"
    grouping = "NULL::text AS make, grouping(country)*2+1" if index_scope else "make, grouping(country, make)"
    stats = db.execute(prefix + f"""
        SELECT country, {grouping} AS level, count(*) AS count,
               count(*) FILTER (WHERE availability='active') AS active,
               count(*) FILTER (WHERE availability IS NOT NULL AND availability<>'unknown') AS status_known,
               count(price) AS priced, count(color) AS colored,
               count(*) FILTER (WHERE has_data) AS enriched,
               count(*) FILTER (WHERE has_detail) AS detailed,
               count(seller_price) AS seller, count(full_price) AS full,
               count(estimated_price) AS estimated, count(partial_price) AS partial,
               count(*) FILTER (WHERE price<1500000) AS economy,
               count(*) FILTER (WHERE price>=1500000 AND price<3000000) AS midrange,
               count(*) FILTER (WHERE price>=3000000 AND price<5000000) AS upper_midrange,
               count(*) FILTER (WHERE price>=5000000) AS premium,
               min(price) AS low, max(price) AS high,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median
        FROM fleet
        """ + where + f" GROUP BY GROUPING SETS ({sets}) ORDER BY level DESC, count DESC, country, make", args).fetchall()
    pages = max(1, ceil(stats[0]["count"] / PAGE_SIZE))
    page = min(max(1, int(filters.get("page", "1"))), pages)
    order = ORDERS[filters.get("sort", "recent")][1]
    cars = db.execute(prefix + "SELECT * FROM fleet" + where + f" ORDER BY {order} LIMIT %s OFFSET %s",
                      [*args, PAGE_SIZE, (page - 1) * PAGE_SIZE]).fetchall()
    return run, stats, cars, page, pages


def url_for(filters, **changes):
    return "/?" + escape(urlencode({**filters, **changes}))


def inventory_html(dsn, params):
    try:
        filters = parse_filters(params)
    except ValueError as exc:
        return 400, f'<h1>Автомобили</h1><p>{escape(str(exc))}</p><a href="/">Сбросить фильтры</a>'
    try:
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as db:
            db.read_only = True
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            db.execute("SET LOCAL statement_timeout = '20s'")
            filters.setdefault("scope", "details")
            sources = available_sources(db)
            run, stats, cars, page, pages = fetch_inventory(db, filters, sources)
            if run and filters["scope"] == "details":
                filters["source"] = run["origin"]
            if filters["scope"] == "all":
                brands = db.execute("""
                    SELECT make, count(*) AS n FROM sitemap_car
                    GROUP BY make ORDER BY n DESC, make
                    """).fetchall()
            else:
                brands = db.execute(f"""
                    WITH metadata AS ({METADATA})
                    SELECT make, count(*) AS n FROM ({VEHICLES}) cars
                    WHERE make<>'' GROUP BY make ORDER BY n DESC, make
                    """, (run["id"] if run else None,)).fetchall()
    except psycopg.errors.UndefinedTable:
        return 200, '<h1>Автомобили</h1><p>Каталог пуст. Запустите инициализацию и загрузку данных.</p>'
    except psycopg.Error:
        return 503, '<h1>Автомобили</h1><p>Каталог в PostgreSQL временно недоступен.</p>'
    source = ""
    if run:
        stamp = run["finished_at"] or run["started_at"]
        date = stamp.astimezone(ZoneInfo("Asia/Barnaul")).strftime("%d.%m.%Y %H:%M")
        label = escape(SOURCE_NAMES.get(run["origin"], run["origin"]))
        scope = "выборка" if run["status"] == "partial" else "снимок"
        source = f' {label} · {scope} от {date} (Барнаул) · <a href="/analytics?run={run["id"]}#prices">прогон #{run["id"]}</a>.'
    pagination = [f'<span>Страница {amount(page)} из {amount(pages)}</span>']
    for target, label in ((page - 1, "← Назад"), (page + 1, "Далее →")):
        if 1 <= target <= pages:
            pagination.append(f'<a href="{url_for(filters, page=target)}">{label}</a>')
    tabs = []
    if not sources:
        tabs.append('<a href="/" aria-current="page">С данными объявления</a>')
    for item in sorted(sources, key=lambda item: (item["origin"] != SAT, item["origin"])):
        origin = item["origin"]
        current = filters["scope"] == "details" and run and run["origin"] == origin
        href = url_for(filters, scope="details", source=origin, page=1)
        tabs.append(f'<a href="{href}"{" aria-current=page" if current else ""}>'
                    f'{escape(SOURCE_NAMES.get(origin, origin))}</a>')
    current = filters["scope"] == "all"
    tabs.append(f'<a href="/?scope=all"{" aria-current=page" if current else ""}>Индекс SATAuto</a>')
    explanation = ("Адреса из сохранённой карты сайта. Загруженные данные показаны для совпавших ID."
                   if current else "Объявления из выбранного источника. Показатели относятся к текущей выборке.")
    return 200, f"""
      <h1>Автомобили</h1>
      <nav class="inventory-tabs" aria-label="Источники каталога">{"".join(tabs)}</nav>
      <p class="intro">{explanation}{source}</p>
      {filter_form(filters, brands)}
      {statistics(stats, filters.get("price_type", "published"))}
      {vehicle_table(cars, run)}
      <nav class="pager" aria-label="Страницы каталога">{"".join(pagination)}</nav>
    """


def filter_form(filters, brands):
    chosen_brand = filters.get("make")
    values = {row["make"] for row in brands}
    if chosen_brand:
        values.add(chosen_brand)
    options = ['<option value="">Все марки</option>']
    for value in sorted(values, key=lambda brand: brand_label(brand).casefold()):
        selected = " selected" if chosen_brand == value else ""
        options.append(f'<option value="{escape(value)}"{selected}>{escape(brand_label(value))}</option>')
    fields = [
        f'<label>Марка<select name="make">{"".join(options)}</select></label>',
        f'<label>Марка, модель или ID<input name="q" value="{escape(filters.get("q", ""))}" maxlength="120"></label>',
    ]
    advanced = []
    for name, label in (("color", "Цвет кузова"), ("fuel", "Топливо")):
        value = escape(filters.get(name, ""))
        advanced.append(f'<label>{label}<input name="{name}" value="{value}" maxlength="120"></label>')
    for name, (_, label) in NUMERIC.items():
        value = escape(filters.get(name, ""))
        target = fields if name in {"min_price", "max_price"} else advanced
        target.append(f'<label>{label}<input type="number" min="0" name="{name}" value="{value}"></label>')
    for name, (label, values) in SELECTS.items():
        if name == "scope":
            continue
        options = ['<option value="">Любое</option>'] if name in {"country", "availability"} else []
        for value, title in values.items():
            chosen = " selected" if filters.get(name) == value else ""
            options.append(f'<option value="{value}"{chosen}>{title}</option>')
        fields.append(f'<label>{label}<select name="{name}">{"".join(options)}</select></label>')
    scope = filters["scope"]
    quick = "".join(
        f'<a href="{url_for(filters, make=row["make"], page=1)}">{escape(brand_label(row["make"]))}</a>'
        for row in brands[:6]
    )
    source = filters.get("source", "") if scope == "details" else ""
    reset = url_for({"scope": scope, "source": source})
    expanded = " open" if filters.keys() & {"color", "fuel", "min_year", "max_year", "max_mileage"} else ""
    return f"""
      <form class="panel filter-form" action="/" method="get">
        <input type="hidden" name="scope" value="{scope}">
        <input type="hidden" name="source" value="{escape(source)}">
        <div class="filters">{"".join(fields)}</div>
        <details{expanded}><summary>Год, пробег, топливо и цвет</summary>
          <div class="filters">{"".join(advanced)}</div>
        </details>
        <div class="form-actions"><button>Применить</button><a href="{reset}">Сбросить</a></div>
        <nav class="quick-brands" aria-label="Популярные марки">{quick}</nav>
      </form>"""


def price_groups(rows, label, names):
    headings = (label, "Карточек", "В продаже", "С ценой", "Минимум, ₽", "Медиана, ₽", "Максимум, ₽")
    body = []
    for name, row in zip(names, rows):
        values = [name, *(amount(row[key]) for key in ("count", "active", "priced", "low", "median", "high"))]
        body.append("<tr>" + "".join(f"<td>{escape(value)}</td>" for value in values) + "</tr>")
    return ('<div class="table-wrap"><table><thead><tr>' +
            "".join(f'<th scope="col">{heading}</th>' for heading in headings) +
            '</tr></thead><tbody>' + "".join(body) + '</tbody></table></div>')


def statistics(stats, price_type):
    total = stats[0]
    values = (
        ("Карточек в выборке", amount(total["count"])),
        ("С характеристиками", amount(total["enriched"])),
        ("В продаже", amount(total["active"])),
        ("С выбранной ценой", amount(total["priced"])),
        ("Медианная цена", amount(total["median"], True)),
        ("С указанным цветом", amount(total["colored"])),
    )
    cards = "".join(f'<div class="metric"><span>{key}</span><strong>{val}</strong></div>' for key, val in values)
    countries = [row for row in stats[1:] if row["level"] == 1]
    makes = [row for row in stats[1:] if row["level"] == 2]
    country_table = price_groups(
        countries, "Страна", [COUNTRIES.get(row["country"], row["country"] or "Не указана") for row in countries]
    ) if countries else ""
    make_table = price_groups(
        makes, "Марка", [brand_label(row["make"]) if row["make"] else "Не указана" for row in makes]
    ) if makes else ""
    bands = (("До 1,5 млн ₽", "economy"), ("1,5–3 млн ₽", "midrange"),
             ("3–5 млн ₽", "upper_midrange"), ("От 5 млн ₽", "premium"))
    buckets = " · ".join(f"{label} — {amount(total[key])}" for label, key in bands)
    breakdown = (("Цена продавца в ₽", "seller"), ("полный расчёт", "full"),
                 ("предварительный", "estimated"), ("частичный", "partial"))
    coverage = " · ".join(f"{label} — {amount(total[key])}" for label, key in breakdown)
    chosen_price = PRICES[price_type][0]
    country_html = f"<details><summary>Цены по странам</summary>{country_table}</details>" if countries else ""
    makes_html = f"<details><summary>Цены по маркам</summary>{make_table}</details>" if makes else ""
    return f"""
      <section class="inventory-stats"><h2>Аналитика выборки</h2><div class="metrics">{cards}</div>
      <p class="hint">Подробных карточек — {amount(total["detailed"])} · статус известен у {amount(total["status_known"])}.</p>
      <p class="hint">{coverage}.</p>
      <p class="hint"><strong>{chosen_price}</strong> · {buckets}</p>
      {country_html}{makes_html}</section>"""


def price_display(car):
    for key, label in (("full_price", "Полный расчёт"), ("estimated_price", "Предварительный"),
                       ("partial_price", "Частичный расчёт"), ("displayed_price", "На витрине")):
        if car[key] is not None:
            return f"{amount(car[key], True)}<small>{label}</small>"
    return "По запросу" if car["price_kind"] == "on_request" else "Не указано"


def seller_display(car):
    if car["seller_price"] is not None:
        return amount(car["seller_price"], True)
    if car["original_price"] and car["currency"]:
        return escape(f'{car["original_price"]} {car["currency"]}')
    return "По запросу" if car["price_kind"] == "on_request" else "Не указано"


def vehicle_table(cars, run):
    if not cars:
        return '<p class="panel">По этим фильтрам карточки не найдены. Измените диапазон или сбросьте фильтры.</p>'
    rows = []
    for car in cars:
        title = escape(car["title"] or f'Автомобиль #{car["car_id"]}')
        origin = run["origin"] if run else SAT
        country = escape(COUNTRIES.get(car["country"], car["country"] or "—"))
        if not car["loaded"]:
            path = safe_url(car["path"], SAT)
            name = f'<a href="{escape(path)}">{title}</a>' if path else title
            rows.append(f'<tr><th scope="row">{name}<small>#{car["car_id"]} · SATAuto</small></th>'
                        f'<td>{country}</td><td colspan="8" class="unloaded">Адрес из карты сайта</td></tr>')
            continue
        path = f'/car?run={run["id"]}&amp;id={car["car_id"]}'
        source = escape(SOURCE_NAMES.get(origin, origin))
        availability = AVAILABILITY.get(car["availability"], car["availability"] or "Не указано")
        columns = [car["year"] or "—", amount(car["mileage"]), car["fuel"] or "—",
                   car["color"] or "Не указано", availability]
        photo = safe_url(car["photo"], origin)
        photo_link = f'<a href="{escape(photo)}">Фото</a>' if photo else "—"
        rows.append(f'<tr><th scope="row"><a href="{path}">{title}</a>'
                    f'<small>#{car["car_id"]} · {source}</small></th><td>{country}</td>' +
                    "".join(f"<td>{escape(str(value))}</td>" for value in columns) +
                    f'<td>{seller_display(car)}</td><td>{price_display(car)}</td><td>{photo_link}</td></tr>')
    return f"""
      <div class="table-wrap inventory-table"><table><thead><tr><th>Автомобиль</th><th>Страна</th>
      <th>Год</th><th>Пробег, км</th><th>Топливо</th><th>Цвет</th><th>Наличие</th>
      <th>Цена продавца</th><th>Расчёт / цена на витрине</th><th>Фото</th>
      </tr></thead><tbody>{"".join(rows)}</tbody></table></div>"""


def car_html(dsn, run_id, car_id):
    reference = None
    try:
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as db:
            db.read_only = True
            run = db.execute("SELECT id, origin FROM crawl_run WHERE id=%s", (run_id,)).fetchone()
            metadata = METADATA + " AND v.car_id=%s"
            car = db.execute(f"""
                WITH metadata AS ({metadata})
                SELECT cars.*, metadata.data->'pricing' AS pricing, metadata.data AS raw
                FROM ({VEHICLES}) cars JOIN metadata USING (car_id)
                """, (run_id, car_id)).fetchone()
            if car and car["raw"]:
                reference = trim_for_card(db, car["raw"])
    except psycopg.errors.UndefinedTable:
        car = None
    except psycopg.Error:
        return 503, '<h1>Каталог временно недоступен</h1>'
    if car is None or run is None:
        return 404, '<h1>Карточка не найдена</h1><p><a href="/">Вернуться в каталог</a></p>'
    origin = run["origin"]
    title = escape(car["title"] or f"Автомобиль #{car_id}")
    source = escape(SOURCE_NAMES.get(origin, origin))
    path = safe_url(car["path"], origin)
    external = f'<p><a href="{escape(path)}">Открыть объявление на {source}</a></p>' if path else ""
    seller = car["original_price"]
    original = (f'<small>В валюте объявления · {escape(seller)} {escape(car["currency"])}</small>'
                if seller and car["currency"] and car["seller_price"] is not None else "")
    pricing = car["pricing"] or {}
    notice = (pricing.get("presentation") or {}).get("notice") or pricing.get("message")
    details = f'<p class="hint">{escape(notice)}</p>' if notice else ""
    steps = [
        f'<li>{escape(str(step["label"]))} · {amount(step["amount_rub"], True)}</li>'
        for step in pricing.get("steps") or []
        if isinstance(step, dict) and step.get("label") and type(step.get("amount_rub")) in (int, float)
    ]
    breakdown = f'<details><summary>Статьи расчёта</summary><ul>{"".join(steps)}</ul></details>' if steps else ""
    reference_html = ""
    if reference and reference["specs"]:
        rows = "".join(f'<li>{SPEC_LABELS[key]} · {escape(str(value))}</li>'
                       for key, value in reference["specs"].items())
        label = escape(reference["source"])
        href = safe_url(reference["url"])
        if type(reference["run_id"]) is int:
            label += f' · прогон №{reference["run_id"]}'
            href = f'/analytics?run={reference["run_id"]}#prices'
        provenance = f'<a href="{escape(href)}">{label}</a>' if href else label
        reference_html = f'<h3>Из справочника комплектаций</h3><ul>{rows}</ul><p>{provenance}</p>'
    return 200, f"""
      <p><a href="{url_for({"source": origin})}">← Каталог {source}</a></p>
      <h1>{title}</h1><p class="intro">#{car_id} · {source} · прогон #{run_id}</p>
      <section class="panel">
        <h2>Характеристики и цена</h2>
        {vehicle_table([car], run)}
        {original}{details}{breakdown}{reference_html}{external}
      </section>
    """
