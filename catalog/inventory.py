from datetime import datetime
from html import escape
from math import ceil
import re
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row

from .japan_lots import AUCTION_ID, resolve_auction_page
from .trims import SPEC_LABELS, technical_specs, trim_for_card


COUNTRIES = {"CN": "Китай", "KR": "Южная Корея", "JP": "Япония"}
AVAILABILITY = {"active": "В продаже", "sold": "Продано", "inactive": "Снято",
                "possibly_sold": "Уточняется", "unknown": "Не указано"}
ORDERS = {"recent": ("Сначала новые объявления", "car_id DESC"),
          "price_up": ("Цена по возрастанию", "price ASC NULLS LAST, car_id DESC"),
          "price_down": ("Цена по убыванию", "price DESC NULLS LAST, car_id DESC"),
          "year": ("Сначала новее по году", "year DESC NULLS LAST, car_id DESC"),
          "mileage": ("Сначала малый пробег", "mileage ASC NULLS LAST, car_id DESC")}
NUMERIC = {"min_price": ("price >=", "Цена от, ₽"), "max_price": ("price <=", "Цена до, ₽"),
           "min_year": ("year >=", "Год от"), "max_year": ("year <=", "Год до"),
           "min_mileage": ("mileage >=", "Пробег от, км"), "max_mileage": ("mileage <=", "Пробег до, км"),
           "min_engine": ("engine_cc >=", "Объём от, см³"), "max_engine": ("engine_cc <=", "Объём до, см³"),
           "min_power": ("power >=", "Мощность от, л.с."), "max_power": ("power <=", "Мощность до, л.с.")}
TEXT_FILTERS = {"model": "Модель", "trim": "Комплектация", "body": "Кузов",
                "drive": "Привод", "transmission": "Коробка передач"}
BOOL_FILTERS = {"sanctioned": "Санкционные авто", "damage_confirmed": "История ДТП"}
PRICES = {
    "published": ("Опубликованная стоимость", "coalesce(full_price, estimated_price, displayed_price)"),
    "seller": ("Цена продавца в ₽", "seller_price"),
    "estimated": ("Предварительный расчёт", "estimated_price"),
    "full": ("Полный расчёт", "full_price"),
}
SELECTS = {"country": ("Страна", COUNTRIES), "availability": ("Наличие", AVAILABILITY),
           "price_type": ("Какая цена", {key: label for key, (label, _) in PRICES.items()}),
           "sort": ("Сортировка", {key: label for key, (label, _) in ORDERS.items()})}
SELECTS.update({
    "sanctioned": ("Санкционные авто", {"yes": "С отметкой", "no": "Без отметки", "unknown": "Не указано"}),
    "damage_confirmed": ("История ДТП", {"yes": "Есть подтверждённые повреждения",
                                       "no": "Подтверждение отсутствует", "unknown": "Не указано"}),
})
PAGE_SIZE = 40
SAT = "https://satauto.ru"
MOSCOW = ZoneInfo("Europe/Moscow")
PARSED_DATES = {"parsed_from": (">=", "Парсинг от, МСК"), "parsed_to": ("<", "Парсинг до, МСК")}
INVENTORY_FIELDS = """
    car_id, country, make, model, title, path, year, mileage, availability, color, fuel,
    photo, loaded, has_data, has_detail, currency, original_price, seller_price,
    full_price, estimated_price, partial_price, displayed_price, price_kind
    , trim, transmission, drive, body, engine_cc, power, sanctioned, damage_confirmed
"""
EXTRA_FIELDS = """
    coalesce(data#>>'{identity,trim}', summary#>>'{identity,trim}') AS trim,
    coalesce(data#>>'{specs,transmission}', summary#>>'{specs,transmission}') AS transmission,
    coalesce(data#>>'{specs,drive_type}', summary#>>'{specs,drive_type}') AS drive,
    coalesce(data#>>'{specs,body_type}', summary#>>'{specs,body_type}') AS body,
    CASE WHEN coalesce(data#>>'{specs,engine_displacement_cc}', summary#>>'{specs,engine_displacement_cc}')
                  ~ '^[0-9]{1,6}([.][0-9]{1,3})?$'
         THEN coalesce(data#>>'{specs,engine_displacement_cc}', summary#>>'{specs,engine_displacement_cc}')::numeric END AS engine_cc,
    CASE WHEN coalesce(data#>>'{specs,engine_power_hp}', summary#>>'{specs,engine_power_hp}')
                  ~ '^[0-9]{1,6}([.][0-9]{1,3})?$'
         THEN coalesce(data#>>'{specs,engine_power_hp}', summary#>>'{specs,engine_power_hp}')::numeric END AS power,
    CASE lower(coalesce(data#>>'{pricing,sanctioned}', summary#>>'{pricing,sanctioned}',
                        data#>>'{specs,sanction}', summary#>>'{specs,sanction}'))
         WHEN 'true' THEN true WHEN '1' THEN true WHEN 'false' THEN false WHEN '0' THEN false END AS sanctioned,
    CASE coalesce(data#>>'{vehicle_history,damage_confirmed}', summary#>>'{vehicle_history,damage_confirmed}')
         WHEN 'true' THEN true WHEN 'false' THEN false END AS damage_confirmed,
    true AS filter_ready
"""
METADATA = """
    SELECT v.*, r.origin, coalesce(v.detail->'data', v.detail, v.summary) AS data
    FROM vehicle v JOIN crawl_run r ON r.id=v.run_id
    WHERE v.run_id=%s AND NOT coalesce(v.summary ? 'index_only', false)
"""
SATAUTO_PATH = """
    satauto_public_path(car_id, country, ARRAY[
        data#>>'{links,detail}', data#>>'{links,public_url}', summary#>>'{links,detail}',
        summary#>>'{links,public_url}'
    ])
"""
VEHICLES = """
    SELECT car_id, country,
           trim(BOTH '-' FROM regexp_replace(lower(coalesce(data#>>'{identity,manufacturer}',manufacturer)),
                '[^[:alnum:]]+', '-', 'g')) AS make,
           coalesce(data#>>'{identity,model}', model) AS model,
           coalesce(data#>>'{identity,title}', summary#>>'{identity,title}') AS title,
           CASE WHEN origin='https://satauto.ru' THEN """ + SATAUTO_PATH + """
                ELSE coalesce(data#>>'{links,detail}', data#>>'{links,public_url}', data#>>'{links,carhub}',
                              summary#>>'{links,detail}', summary#>>'{links,carhub}') END AS path,
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
           seller_price, full_price, estimated_price, partial_price, displayed_price, price_kind,
           """ + EXTRA_FIELDS + """
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
def amount(value, currency=False):
    if value is None:
        return "—"
    return f"{value:,.0f}".replace(",", " ") + (" ₽" if currency else "")


def safe_url(value):
    if not value:
        return ""
    if value.startswith("/") and not value.startswith("//"):
        return SAT + value
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
    for key in ({"q", "make", "color", "fuel", "page"} | NUMERIC.keys() | SELECTS.keys()
                | TEXT_FILTERS.keys() | PARSED_DATES.keys()):
        value = params.get(key, [""])[0].strip()
        if not value:
            continue
        if key in SELECTS and value not in SELECTS[key][1]:
            raise ValueError("Выберите допустимое значение фильтра.")
        if key in NUMERIC or key == "page":
            if not value.isascii() or not value.isdecimal() or len(value) > 10 or int(value) > 2_147_483_647:
                raise ValueError("Укажите целое неотрицательное число в числовых фильтрах.")
        limit = 500 if key == "trim" else 120
        if len(value) > limit:
            raise ValueError(f"Текст фильтра должен быть не длиннее {limit} символов.")
        if key == "make":
            value = re.sub(r"[^\w]+", "-", value.lower().replace("_", "-")).strip("-")
            if not value:
                raise ValueError("Укажите марку автомобиля.")
        if key in PARSED_DATES:
            try:
                value = datetime.strptime(value, "%Y-%m-%dT%H:%M").isoformat(timespec="minutes")
            except ValueError:
                raise ValueError("Укажите дату и время парсинга в формате ГГГГ-ММ-ДДTЧЧ:ММ.") from None
        filters[key] = value
    for lower, upper in (("min_price", "max_price"), ("min_year", "max_year"),
                         ("min_mileage", "max_mileage"), ("min_engine", "max_engine"), ("min_power", "max_power")):
        if lower in filters and upper in filters and int(filters[lower]) > int(filters[upper]):
            raise ValueError("Нижняя граница диапазона выше верхней.")
    if {"parsed_from", "parsed_to"} <= filters.keys() and filters["parsed_from"] >= filters["parsed_to"]:
        raise ValueError("Начало периода парсинга должно быть раньше конца.")
    return filters


def query_parts(filters, run_id):
    clauses, args = [], [run_id]
    if "country" in filters:
        clauses.append("country=%s")
        args.append(filters["country"])
    if "make" in filters:
        clauses.append("make=%s")
        args.append(filters["make"])
    for field in TEXT_FILTERS:
        if field in filters:
            clauses.append(f"{field}=%s")
            args.append(filters[field])
    for field in BOOL_FILTERS:
        if field in filters:
            clauses.append(f"{field} IS " + {"yes": "TRUE", "no": "FALSE", "unknown": "NULL"}[filters[field]])
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
    date_join = ""
    date_column = ""
    if filters.keys() & PARSED_DATES.keys():
        date_join = "JOIN vehicle_observation o USING (run_id, car_id)"
        date_column = ", o.last_seen_at AS parsed_at"
        for key, (comparison, _) in PARSED_DATES.items():
            if key in filters:
                clauses.append(f"parsed_at {comparison} %s")
                args.append(datetime.fromisoformat(filters[key]).replace(tzinfo=MOSCOW))
    price = PRICES[filters.get("price_type", "published")][1]
    prefix = f"""WITH fleet AS (
        SELECT run_id, {INVENTORY_FIELDS}, {price} AS price{date_column}
        FROM vehicle_inventory {date_join} WHERE run_id=%s
    ) """
    return prefix, " WHERE " + " AND ".join(clauses) if clauses else "", args


def current_satauto_catalog(db):
    return db.execute("""
        SELECT id, finished_at FROM crawl_run WHERE origin=%s
          AND EXISTS (SELECT 1 FROM vehicle_inventory WHERE run_id=crawl_run.id)
        ORDER BY id DESC LIMIT 1
        """, (SAT,)).fetchone()


def inventory_statistics(db, query):
    prefix, where, args = query
    return db.execute(prefix + """
        SELECT country, make, grouping(country, make) AS level, count(*) AS count,
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
        """ + where + " GROUP BY GROUPING SETS ((),(country),(make)) ORDER BY level DESC, count DESC, country, make", args).fetchall()


def inventory_page(db, filters, query, page, limit=PAGE_SIZE):
    prefix, where, args = query
    order = ORDERS[filters.get("sort", "recent")][1]
    page_query = prefix + ", page AS (SELECT * FROM fleet" + where + f" ORDER BY {order} LIMIT %s OFFSET %s)"
    if filters.keys() & PARSED_DATES.keys():
        page_query += " SELECT page.* FROM page"
    else:
        page_query += """
            SELECT page.*, o.last_seen_at AS parsed_at
            FROM page LEFT JOIN vehicle_observation o USING (run_id, car_id)
            """
    return db.execute(page_query + f" ORDER BY {order}", [*args, limit, (page - 1) * PAGE_SIZE]).fetchall()


def fetch_inventory(db, filters):
    run = current_satauto_catalog(db)
    query = query_parts(filters, run["id"] if run else None)
    stats = inventory_statistics(db, query)
    pages = max(1, ceil(stats[0]["count"] / PAGE_SIZE))
    page = min(max(1, int(filters.get("page", "1"))), pages)
    cars = inventory_page(db, filters, query, page)
    return run, stats, cars, page, pages


def url_for(filters, **changes):
    return "/?" + escape(urlencode({**filters, **changes}))


def filter_choices(db, run_id, filters):
    clauses, args = ["run_id=%s"], [run_id]
    for field in ("country", "make"):
        if filters.get(field):
            clauses.append(f"{field}=%s")
            args.append(filters[field])
    where = " WHERE " + " AND ".join(clauses)
    names = ("fuel", "body", "drive", "transmission", "color")
    if filters.get("make"):
        names += ("model",)
    columns = ", ".join(names)
    groups = ", ".join(f"({name})" for name in names)
    choices = {name: [] for name in names}
    for row in db.execute(
        f"SELECT {columns} FROM vehicle_inventory{where} GROUP BY GROUPING SETS ({groups})", args
    ):
        for name in names:
            if row[name]:
                choices[name].append(row[name])
    for values in choices.values():
        values.sort()
    if filters.get("model"):
        choices["trim"] = [row["trim"] for row in db.execute(
            "SELECT DISTINCT trim FROM vehicle_inventory" + where +
            " AND model=%s AND trim IS NOT NULL AND trim<>'' ORDER BY trim", [*args, filters["model"]])]
    return choices


def inventory_html(dsn, params, section=None):
    try:
        filters = parse_filters(params)
    except ValueError as exc:
        return 400, f'<h1>Автомобили</h1><p>{escape(str(exc))}</p><a href="/">Сбросить фильтры</a>'
    try:
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as db:
            db.read_only = True
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            db.execute("SET LOCAL statement_timeout = '20s'")
            run = current_satauto_catalog(db)
            run_id = run["id"] if run else None
            if section == "options":
                brands = db.execute("""
                    SELECT make FROM vehicle_inventory WHERE run_id=%s AND make<>''
                    GROUP BY make ORDER BY count(*) DESC, make
                    """, (run_id,)).fetchall()
                return 200, filter_form(filters, brands, filter_choices(db, run_id, filters))
            query = query_parts(filters, run_id)
            if section == "summary":
                db.execute("SET LOCAL work_mem = '32MB'")
                stats = inventory_statistics(db, query)
                return 200, statistics(stats, filters.get("price_type", "published"))
            page = max(1, int(filters.get("page", "1")))
            cars = inventory_page(db, filters, query, page, PAGE_SIZE + 1)
            if page > 1 and not cars:
                prefix, where, args = query
                total = db.execute(prefix + "SELECT count(*) AS total FROM fleet" + where, args).fetchone()["total"]
                page = max(1, ceil(total / PAGE_SIZE))
                cars = inventory_page(db, filters, query, page, PAGE_SIZE + 1)
    except psycopg.errors.UndefinedTable:
        return 200, '<h1>Автомобили</h1><p>Каталог пуст. Запустите инициализацию и загрузку данных.</p>'
    except psycopg.Error:
        return 503, '<h1>Автомобили</h1><p>Каталог в PostgreSQL временно недоступен.</p>'
    intro = "Объявления SATAuto"
    if run and run["finished_at"]:
        updated = run["finished_at"].astimezone(MOSCOW).strftime("%d.%m.%Y %H:%M")
        intro += f" · данные собраны к {updated} (МСК)"
    pagination = [f'<span>Страница {amount(page)}<span data-page-total></span></span>']
    if page > 1:
        pagination.append(f'<a href="{url_for(filters, page=page - 1)}">← Назад</a>')
    if len(cars) > PAGE_SIZE:
        pagination.append(f'<a href="{url_for(filters, page=page + 1)}">Далее →</a>')
    return 200, f"""
      <h1>Автомобили</h1>
      <p class="intro">{intro}</p>
      {filter_form(filters, [])}
      <section class="inventory-stats" data-catalog-summary aria-live="polite">
        <h2>Аналитика выборки</h2><p class="hint">Загрузка…</p>
      </section>
      {vehicle_table(cars[:PAGE_SIZE], run_id)}
      <nav class="pager" aria-label="Страницы каталога">{"".join(pagination)}</nav>
    """


def select_control(name, label, values, filters, empty="Любое", disabled=False):
    values = dict(values)
    chosen = filters.get(name, "")
    if chosen and chosen not in values:
        values[chosen] = chosen
    options = [f'<option value="">{escape(empty)}</option>'] if empty is not None else []
    for value, title in values.items():
        selected = " selected" if chosen == value else ""
        options.append(f'<option value="{escape(value)}"{selected}>{escape(title)}</option>')
    return (f'<label class="filter-field">{label}<select name="{name}"'
            f'{" disabled" if disabled else ""}>{"".join(options)}</select></label>')


def filter_form(filters, brands, choices=None):
    choices = choices or {}
    brand_values = {row["make"]: brand_label(row["make"]) for row in
                    sorted(brands, key=lambda row: brand_label(row["make"]).casefold())}
    fields = [
        select_control("make", "Марка", brand_values, filters, "Все марки"),
        f'<label>Марка, модель или ID<input name="q" value="{escape(filters.get("q", ""))}" maxlength="120"></label>',
        select_control("model", "Модель", {value: value for value in choices.get("model") or []},
                       filters, "Любая модель" if filters.get("make") else "Выберите марку",
                       disabled=not filters.get("make")),
        select_control("trim", "Комплектация", {value: value for value in choices.get("trim") or []},
                       filters, "Любая комплектация" if filters.get("model") else "Выберите модель",
                       disabled=not filters.get("model")),
    ]
    advanced = []
    for name, (_, label) in NUMERIC.items():
        value = escape(filters.get(name, ""))
        target = fields if name in {"min_price", "max_price"} else advanced
        target.append(f'<label>{label}<input type="number" min="0" name="{name}" value="{value}"></label>')
    for name in ("country", "availability", "price_type", "sort"):
        label, values = SELECTS[name]
        fields.append(select_control(name, label, values, filters,
                                     "Любое" if name in {"country", "availability"} else None))
    for name, (_, label) in PARSED_DATES.items():
        value = escape(filters.get(name, ""))
        fields.append(f'<label>{label}<input type="datetime-local" name="{name}" value="{value}"></label>')
    for name, label in (("fuel", "Топливо"), ("color", "Цвет"), ("body", "Кузов"),
                        ("drive", "Привод"), ("transmission", "Коробка передач")):
        advanced.append(select_control(name, label, {value: value for value in choices.get(name) or []}, filters))
    for name in BOOL_FILTERS:
        label, values = SELECTS[name]
        advanced.append(select_control(name, label, values, filters, "Любые"))
    quick = "".join(
        f'<a href="{url_for(filters, make=row["make"], model="", trim="", page=1)}">{escape(brand_label(row["make"]))}</a>'
        for row in brands[:6]
    )
    expanded = " open" if filters.keys() & (NUMERIC.keys() | TEXT_FILTERS.keys() | BOOL_FILTERS.keys() | {"color", "fuel"}) else ""
    return f"""
      <form class="panel filter-form" action="/" method="get" data-inventory-filters>
        <div class="filters">{"".join(fields)}</div>
        <details{expanded}><summary>Год, пробег и характеристики</summary>
          <div class="filters">{"".join(advanced)}</div>
        </details>
        <div class="form-actions"><button>Применить</button><a href="/">Сбросить</a></div>
        <nav class="quick-brands" aria-label="Популярные марки">{quick}</nav>
      </form>
      <script src="/filters.js" defer></script>"""


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
    pages = amount(max(1, ceil(total["count"] / PAGE_SIZE)))
    return f"""
      <section class="inventory-stats" data-total-pages="{pages}"><h2>Аналитика выборки</h2><div class="metrics">{cards}</div>
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


def vehicle_table(cars, run_id):
    if not cars:
        return '<p class="panel">По этим фильтрам карточки не найдены. Измените диапазон или сбросьте фильтры.</p>'
    rows = []
    for car in cars:
        title = escape(car["title"] or f'Автомобиль #{car["car_id"]}')
        country = escape(COUNTRIES.get(car["country"], car["country"] or "—"))
        path = f'/car?run={run_id}&amp;id={car["car_id"]}'
        availability = AVAILABILITY.get(car["availability"], car["availability"] or "Не указано")
        columns = [car["year"] or "—", amount(car["mileage"]), car["fuel"] or "—",
                   car["color"] or "Не указано", availability]
        photo = safe_url(car["photo"])
        photo_link = f'<a href="{escape(photo)}">Фото</a>' if photo else "—"
        parsed_at = car["parsed_at"]
        parsing_time = parsed_at.astimezone(MOSCOW).strftime("%d.%m.%Y %H:%M:%S") if parsed_at else "—"
        rows.append(f'<tr><th scope="row"><a href="{path}">{title}</a>'
                    f'<small>#{car["car_id"]}</small></th><td>{country}</td>' +
                    "".join(f"<td>{escape(str(value))}</td>" for value in columns) +
                    f'<td>{seller_display(car)}</td><td>{price_display(car)}</td>'
                    f'<td>{parsing_time}</td><td>{photo_link}</td></tr>')
    return f"""
      <div class="table-wrap inventory-table"><table><thead><tr><th>Автомобиль</th><th>Страна</th>
      <th>Год</th><th>Пробег, км</th><th>Топливо</th><th>Цвет</th><th>Наличие</th>
      <th>Цена продавца</th><th>Расчёт / цена на витрине</th><th>Парсинг, МСК</th><th>Фото</th>
      </tr></thead><tbody>{"".join(rows)}</tbody></table></div>"""


def car_source(dsn, run_id, car_id):
    try:
        with psycopg.connect(dsn, connect_timeout=3) as db:
            lot = db.execute("""
                SELECT v.summary#>>'{source,listing_id}' FROM vehicle v JOIN crawl_run r ON r.id=v.run_id
                WHERE v.run_id=%s AND v.car_id=%s AND v.country='JP' AND r.origin=%s
                """, (run_id, car_id, SAT)).fetchone()
    except psycopg.Error:
        return 503, None
    return resolve_auction_page(lot[0]) if lot else (404, None)


def car_html(dsn, run_id, car_id):
    reference = None
    try:
        with psycopg.connect(dsn, connect_timeout=3, row_factory=dict_row) as db:
            db.read_only = True
            metadata = METADATA + " AND v.car_id=%s AND r.origin=%s"
            car = db.execute(f"""
                WITH metadata AS ({metadata})
                SELECT cars.*, metadata.data->'pricing' AS pricing, metadata.data AS raw,
                       metadata.summary#>>'{{source,listing_id}}' AS listing_id, o.last_seen_at AS parsed_at
                FROM ({VEHICLES}) cars JOIN metadata USING (car_id)
                LEFT JOIN vehicle_observation o ON (o.run_id, o.car_id)=(metadata.run_id, metadata.car_id)
                """, (run_id, car_id, SAT)).fetchone()
            if car and car["raw"]:
                reference = trim_for_card(db, car["raw"])
    except psycopg.errors.UndefinedTable:
        car = None
    except psycopg.Error:
        return 503, '<h1>Каталог временно недоступен</h1>'
    if car is None:
        return 404, '<h1>Карточка не найдена</h1><p><a href="/">Вернуться в каталог</a></p>'
    title = escape(car["title"] or f"Автомобиль #{car_id}")
    path = safe_url(car["path"])
    external = f'<p><a href="{escape(path)}">Открыть объявление на SATAuto</a></p>' if path else ""
    if car["country"] == "JP" and AUCTION_ID.fullmatch(car["listing_id"] or ""):
        external = f'<p><a href="/car/source?run={run_id}&amp;id={car_id}">Открыть лот на SATAuto</a></p>'
    elif not path:
        external = '<p class="hint">Публичный адрес объявления SATAuto недоступен.</p>'
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
    raw = car["raw"] or {}
    trim = (raw.get("identity") or {}).get("trim")
    specs = technical_specs(raw.get("specs"))
    specs.pop("fuel_type", None)
    specifications = [("Комплектация", trim)] if trim else []
    specifications.extend((SPEC_LABELS[key], value) for key, value in specs.items())
    spec_rows = "".join(f'<li>{label} · {escape(str(value))}</li>' for label, value in specifications)
    spec_list = f'<ul>{spec_rows}</ul>' if spec_rows else ""
    reference_html = ""
    if reference and reference["specs"]:
        rows = "".join(f'<li>{SPEC_LABELS[key]} · {escape(str(value))}</li>'
                       for key, value in reference["specs"].items())
        label = escape(reference["source"])
        href = safe_url(reference["url"])
        provenance = f'<a href="{escape(href)}">{label}</a>' if href else label
        reference_html = f'<h3>Из справочника комплектаций</h3><ul>{rows}</ul><p>{provenance}</p>'
    return 200, f"""
      <p><a href="/">← Каталог SATAuto</a></p>
      <h1>{title}</h1><p class="intro">#{car_id} · SATAuto</p>
      <section class="panel">
        <h2>Характеристики и цена</h2>
        {vehicle_table([car], run_id)}
        {spec_list}{original}{details}{breakdown}{reference_html}{external}
      </section>
    """
