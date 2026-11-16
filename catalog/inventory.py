from html import escape
from math import ceil
import re
from urllib.parse import urlencode, urlsplit

import psycopg
from psycopg.rows import dict_row


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
SELECTS = {"country": ("Страна", COUNTRIES), "availability": ("Наличие", AVAILABILITY),
           "scope": ("Состав", {"all": "Вся карта сайта", "details": "С данными объявления"}),
           "sort": ("Сортировка", {key: label for key, (label, _) in ORDERS.items()})}
PAGE_SIZE = 40
METADATA = """
    SELECT v.*, coalesce(v.detail->'data', v.detail, v.summary) AS data
    FROM vehicle v WHERE v.run_id=%s
"""
VEHICLES = """
    SELECT car_id, country, make, model, title, path, year, mileage, availability, color,
           fuel, price, price_label, photo, has_data, local_listing_rub, partial_price
    FROM (
        SELECT car_id, country,
               trim(BOTH '-' FROM regexp_replace(lower(manufacturer), '[^[:alnum:]]+', '-', 'g')) AS make, model,
               coalesce(summary#>>'{identity,title}', data#>>'{identity,title}') AS title,
               coalesce(summary#>>'{links,detail}', data#>>'{links,detail}', data#>>'{links,public_url}') AS path,
               year, mileage_km AS mileage, coalesce(data#>>'{listing,sell_status}', sell_status) AS availability,
               nullif(btrim(coalesce(data#>>'{specs,color}', data#>>'{listing,color}', data->>'color',
                                    data#>>'{specs,exterior_color}', summary#>>'{specs,color}')), '') AS color,
               coalesce(data#>>'{specs,fuel_type}', fuel) AS fuel,
               coalesce(total_rub, estimated_rub,
                   CASE WHEN (data#>>'{pricing,presentation,amount_rub}') ~ '^[0-9]{1,15}$'
                        AND coalesce(data#>>'{pricing,presentation,kind}', '') NOT IN ('known_part', 'none', 'on_request')
                        THEN (data#>>'{pricing,presentation,amount_rub}')::numeric END,
                   CASE WHEN (summary#>>'{pricing,presentation,amount_rub}') ~ '^[0-9]{1,15}$'
                        AND coalesce(data#>>'{pricing,presentation,kind}', '') NOT IN ('known_part', 'none', 'on_request')
                        AND coalesce(summary#>>'{pricing,presentation,kind}', '') NOT IN ('known_part', 'none', 'on_request')
                        THEN (summary#>>'{pricing,presentation,amount_rub}')::numeric END) AS price,
               coalesce(data#>>'{pricing,presentation,label}', summary#>>'{pricing,presentation,label}') AS price_label,
               coalesce(data#>>'{media,main_photo}', summary#>>'{media,main_photo}') AS photo,
               true AS has_data,
               coalesce(listing_rub,
                   CASE WHEN (data#>>'{pricing,listing_rub}') ~ '^[0-9]{1,15}$'
                        THEN (data#>>'{pricing,listing_rub}')::numeric END) AS local_listing_rub,
               CASE WHEN data#>>'{pricing,presentation,kind}'='known_part'
                         AND (data#>>'{pricing,presentation,amount_rub}') ~ '^[0-9]{1,15}$'
                    THEN (data#>>'{pricing,presentation,amount_rub}')::numeric END AS partial_price
        FROM metadata
    ) detail
"""
INDEXED = """
    SELECT s.car_id, s.country, s.make, s.model, s.title, s.path,
           NULL::int AS year, NULL::int AS mileage, NULL::text AS availability, NULL::text AS color,
           NULL::text AS fuel, NULL::numeric AS price, NULL::text AS price_label, NULL::text AS photo,
           false AS has_data, NULL::numeric AS local_listing_rub, NULL::numeric AS partial_price
    FROM sitemap_car s WHERE NOT EXISTS (SELECT 1 FROM detailed d WHERE d.car_id=s.car_id)
"""


def amount(value, currency=False):
    if value is None:
        return "—"
    return f"{value:,.0f}".replace(",", " ") + (" ₽" if currency else "")


def safe_url(value):
    if not value:
        return ""
    if value.startswith("/") and not value.startswith("//"):
        return "https://satauto.ru" + value
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
    for key in ({"q", "make", "color", "fuel", "page"} | NUMERIC.keys() | SELECTS.keys()):
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
    needs_known = bool(filters.keys() & ({"color", "fuel"} | NUMERIC.keys()))
    needs_known |= filters.get("availability", "unknown") != "unknown"
    include_index = filters.get("scope") != "details" and not needs_known
    prefix = f"WITH metadata AS ({METADATA}), detailed AS ({VEHICLES}), fleet AS (SELECT * FROM detailed"
    if include_index:
        prefix += " UNION ALL " + INDEXED
    prefix += ") "
    return prefix, " WHERE " + " AND ".join(clauses) if clauses else "", args


def fetch_inventory(db, filters):
    run = db.execute("""
        SELECT id, origin, status
        FROM crawl_run WHERE status IN ('complete','partial')
          AND EXISTS (SELECT 1 FROM vehicle WHERE run_id=crawl_run.id)
        ORDER BY (origin IN ('https://satauto.ru','export')) DESC,
                 (status='complete') DESC, id DESC LIMIT 1
        """).fetchone()
    prefix, where, args = query_parts(filters, run["id"] if run else None)
    stats = db.execute(prefix + """
        SELECT country, grouping(country) AS overall, count(*) AS count,
               count(*) FILTER (WHERE availability='active') AS active,
               count(*) FILTER (WHERE availability IS NOT NULL AND availability<>'unknown') AS status_known,
               count(price) AS priced,
               count(color) AS colored, count(*) FILTER (WHERE has_data) AS enriched,
               count(*) FILTER (WHERE price<1500000) AS economy,
               count(*) FILTER (WHERE price>=1500000 AND price<3000000) AS midrange,
               count(*) FILTER (WHERE price>=3000000 AND price<5000000) AS upper_midrange,
               count(*) FILTER (WHERE price>=5000000) AS premium,
               min(price) AS low, max(price) AS high,
               count(price-local_listing_rub) AS paired,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price-local_listing_rub) AS median_gap,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY price) AS median
        FROM fleet
        """ + where + " GROUP BY GROUPING SETS ((),(country)) ORDER BY overall DESC, count DESC", args).fetchall()
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
            run, stats, cars, page, pages = fetch_inventory(db, filters)
            brands = db.execute("""
                SELECT make, sum(n) AS n FROM (
                    SELECT make, count(*) AS n FROM sitemap_car GROUP BY make
                    UNION ALL
                    SELECT trim(BOTH '-' FROM regexp_replace(lower(manufacturer), '[^[:alnum:]]+', '-', 'g')), 0
                    FROM vehicle WHERE run_id=%s AND manufacturer IS NOT NULL GROUP BY manufacturer
                ) b WHERE make<>'' GROUP BY make ORDER BY sum(n) DESC, make
                """, (run["id"] if run else None,)).fetchall()
    except psycopg.errors.UndefinedTable:
        return 200, '<h1>Автомобили</h1><p>Каталог пуст. Запустите инициализацию и загрузку данных.</p>'
    except psycopg.Error:
        return 503, '<h1>Автомобили</h1><p>Каталог в PostgreSQL временно недоступен.</p>'
    source = ""
    if run:
        label = "выгрузка" if run["origin"] == "export" else escape(run["origin"])
        scope = "частичный снимок" if run["status"] == "partial" else "снимок"
        source = f' Дополнительные данные — {scope}, {label}, прогон #{run["id"]}.'
    pagination = [f'<span>Страница {amount(page)} из {amount(pages)}</span>']
    for target, label in ((page - 1, "← Назад"), (page + 1, "Далее →")):
        if 1 <= target <= pages:
            pagination.append(f'<a href="{url_for(filters, page=target)}">{label}</a>')
    tabs = "".join(
        f'<a href="{url_for(filters, scope=key, page=1)}"'
        f'{" aria-current=page" if filters["scope"] == key else ""}>{label}</a>'
        for key, label in (("details", "С данными объявления"), ("all", "Все карточки"))
    )
    explanation = ("Из карты сайта доступны название, страна и ссылка. Характеристики появляются после загрузки страницы объявления."
                   if filters["scope"] == "all" else
                   "Здесь показаны карточки, для которых получены сведения с витрины. Цена, пробег и цвет зависят от объявления.")
    return 200, f"""
      <h1>Автомобили</h1>
      <nav class="inventory-tabs" aria-label="Состав списка">{tabs}</nav>
      <p class="intro">{explanation}{source}</p>
      {filter_form(filters, brands)}
      {statistics(stats)}
      {vehicle_table(cars)}
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
    fields = [f'<label>Марка<select name="make">{"".join(options)}</select></label>']
    for name, label in (("q", "Марка, модель или ID"), ("color", "Цвет кузова"), ("fuel", "Топливо")):
        value = escape(filters.get(name, ""))
        fields.append(f'<label>{label}<input name="{name}" value="{value}" maxlength="120"></label>')
    for name, (_, label) in NUMERIC.items():
        value = escape(filters.get(name, ""))
        fields.append(f'<label>{label}<input type="number" min="0" name="{name}" value="{value}"></label>')
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
    return f"""
      <form class="panel filter-form" action="/" method="get">
        <input type="hidden" name="scope" value="{scope}">
        <div class="filters">{"".join(fields)}</div>
        <div class="form-actions"><button>Применить</button><a href="/?scope={scope}">Сбросить</a></div>
        <nav class="quick-brands" aria-label="Популярные марки">{quick}</nav>
      </form>"""


def statistics(stats):
    total = stats[0]
    values = (("Найдено карточек", amount(total["count"])), ("В продаже", amount(total["active"])),
              ("Медианная цена", amount(total["median"], True)), ("С указанным цветом", amount(total["colored"])))
    cards = "".join(f'<div class="metric"><span>{key}</span><strong>{val}</strong></div>' for key, val in values)
    countries = []
    for row in stats[1:]:
        values = [COUNTRIES.get(row["country"], row["country"] or "Не указана"), amount(row["count"]),
                  amount(row["active"]), amount(row["priced"]), amount(row["low"], True),
                  amount(row["median"], True), amount(row["high"], True),
                  f'{amount(row["median_gap"], True)} ({amount(row["paired"])})' if row["paired"] else "—"]
        countries.append("<tr>" + "".join(f"<td>{escape(value)}</td>" for value in values) + "</tr>")
    bands = (("До 1,5 млн ₽", "economy"), ("1,5–3 млн ₽", "midrange"),
             ("3–5 млн ₽", "upper_midrange"), ("От 5 млн ₽", "premium"))
    buckets = " · ".join(f"{label} — {amount(total[key])}" for label, key in bands)
    return f"""
      <section class="inventory-stats"><h2>Аналитика выборки</h2><div class="metrics">{cards}</div>
      <p class="hint">Данные объявления — {amount(total["enriched"])} из {amount(total["count"])}.
      Известен статус у {amount(total["status_known"])}, цена есть у {amount(total["priced"])}.
      Медиана по опубликованным рублёвым ценам, включая предварительные.</p>
      <p class="hint"><strong>По бюджету</strong> · {buckets}</p>
      <p class="hint">Разница — медиана между ценой на витрине и пересчитанной ценой продавца по одним и тем же авто. В скобках число пар.</p>
      <div class="table-wrap"><table><thead><tr><th>Страна</th><th>Карточек</th><th>В продаже</th>
      <th>С ценой</th><th>Минимум</th><th>Медиана</th><th>Максимум</th><th>Разница с ценой продавца</th></tr></thead>
      <tbody>{"".join(countries)}</tbody></table></div></section>"""


def vehicle_table(cars):
    if not cars:
        return '<p class="panel">По этим фильтрам карточки не найдены. Измените диапазон или сбросьте фильтры.</p>'
    rows = []
    for car in cars:
        title = escape(car["title"] or f'Автомобиль #{car["car_id"]}')
        path = safe_url(car["path"])
        name = f'<a href="{escape(path)}">{title}</a>' if path else title
        if not car["has_data"]:
            country = escape(COUNTRIES.get(car["country"], car["country"] or "—"))
            rows.append(f'<tr><th scope="row">{name}<small>#{car["car_id"]}</small></th>'
                        f'<td>{country}</td><td colspan="7" class="unloaded">Есть в карте сайта · характеристики ещё не загружены</td></tr>')
            continue
        availability = AVAILABILITY.get(car["availability"], car["availability"] or "Не указано")
        price = amount(car["price"], True) if car["price"] else (
            "По запросу" if "запрос" in (car["price_label"] or "").lower() else "—")
        if car["price"] is None and car["partial_price"] is not None:
            price = f'{amount(car["partial_price"], True)} (частичный расчёт)'
        columns = [COUNTRIES.get(car["country"], car["country"] or "—"), car["year"] or "—",
                   amount(car["mileage"]), car["fuel"] or "—", car["color"] or "—", availability, price]
        photo = safe_url(car["photo"])
        photo_link = f'<a href="{escape(photo)}">Фото</a>' if photo else "—"
        rows.append(f'<tr><th scope="row">{name}<small>#{car["car_id"]}</small></th>' +
                    "".join(f"<td>{escape(str(value))}</td>" for value in columns) + f'<td>{photo_link}</td></tr>')
    return f"""
      <div class="table-wrap inventory-table"><table><thead><tr><th>Автомобиль</th><th>Страна</th>
      <th>Год</th><th>Пробег, км</th><th>Топливо</th><th>Цвет</th><th>Наличие</th><th>Цена</th><th>Фото</th>
      </tr></thead><tbody>{"".join(rows)}</tbody></table></div>"""
