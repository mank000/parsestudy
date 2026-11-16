from decimal import Decimal


COUNTRIES = {"CN": "Китай", "JP": "Япония", "KR": "Южная Корея"}


def display(value):
    if isinstance(value, (Decimal, float)):
        return f"{value:,.1f}".replace(",", " ").removesuffix(".0")
    if isinstance(value, int):
        return f"{value:,}".replace(",", " ")
    if value is None:
        return "—"
    return str(value).replace("|", "\\|").replace("\n", " ")


def table(headers, rows):
    def line(values):
        return "| " + " | ".join(map(display, values)) + " |"
    return [line(headers), line(["---"] * len(headers)), *(line(row) for row in rows), ""]


def render_report(db, run):
    run_id = run["id"]
    result = [
        f"# Каталог автомобилей · прогон {run_id}", "",
        f"Источник: {run['origin']}. Режим: {run['mode']}. Статус: {run['status']}.",
        f"Страниц сохранено: {run['pages']}. Попыток HTTP: {run['attempted_requests']}. "
        f"Получено ответов: {run['downloaded_responses']}. Время: {run['seconds']:.3f} с.",
        f"Причина завершения: {run['diagnostic'] or '—'}.", "",
    ]
    if run["status"] == "partial":
        result.extend(["**Доступна только сохранённая часть выборки.**", ""])
    if run["origin"] == "https://carhubauto.ru":
        result.extend(["Публичная выдача CarHub. Покрытие и цены витрины SATAuto могут отличаться.", ""])
    countries = db.execute(
        """
        SELECT country, count(*), count(*) FILTER (WHERE sell_status='active'),
            count(*) FILTER (WHERE sell_status='sold'),
            count(*) FILTER (WHERE sell_status IN ('possibly_sold','inactive')),
            count(*) FILTER (WHERE sell_status IS NULL OR sell_status NOT IN ('active','sold','possibly_sold','inactive')),
            count(*) FILTER (WHERE sell_status='active' AND listing_rub>0),
            percentile_cont(0.5) WITHIN GROUP (ORDER BY listing_rub)
                FILTER (WHERE sell_status='active' AND listing_rub>0),
            count(*) FILTER (WHERE sell_status='active' AND status_price='complete' AND total_rub>0),
            percentile_cont(0.5) WITHIN GROUP (ORDER BY total_rub)
                FILTER (WHERE sell_status='active' AND status_price='complete' AND total_rub>0)
        FROM vehicle WHERE run_id=%s GROUP BY country ORDER BY count(*) DESC, country
        """, (run_id,),
    ).fetchall()
    if not countries:
        return "\n".join([*result, "Объявлений в базе за этот прогон нет. Сравнение цен и количества недоступно.", ""])
    result += ["## Предложения по странам", ""]
    result += table(
        ["Страна", "Всего", "В продаже", "Продано", "Снято или уточняется", "Статус неизвестен",
         "Есть цена в ₽", "Медиана объявления, ₽", "Есть полный расчёт", "Медиана полного расчёта, ₽"],
        [(COUNTRIES.get(row[0], row[0]), *row[1:]) for row in countries],
    )
    result += [
        "Рублёвые цены рассчитаны по объявлениям со статусом «в продаже» и положительной суммой. "
        "Цена объявления и полный расчёт до Владивостока показаны отдельно. "
        "Число объявлений зависит от покрытия площадок и наличия записей с подтверждённым статусом.", "",
    ]
    result += ["## Самые частые марки и модели по странам", ""]
    models = db.execute(
        """
        WITH groups AS (
            SELECT country, manufacturer, model, count(*) AS n,
                count(*) FILTER (WHERE listing_rub>0) AS priced,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY listing_rub)
                    FILTER (WHERE listing_rub>0) AS median
            FROM vehicle WHERE run_id=%s AND sell_status='active'
                AND manufacturer IS NOT NULL AND model IS NOT NULL
            GROUP BY country, manufacturer, model
        ), ranked AS (
            SELECT *, row_number() OVER (PARTITION BY country ORDER BY n DESC, manufacturer, model) AS r
            FROM groups
        )
        SELECT country, manufacturer, model, n, priced, median FROM ranked WHERE r<=5 ORDER BY country, r
        """, (run_id,),
    ).fetchall()
    result += (table(["Страна", "Марка", "Модель", "В продаже", "Есть цена", "Медиана объявления, ₽"], models)
               if models else ["Нет активных объявлений с заполненными маркой и моделью.", ""])
    result += ["## Сопоставимые предложения по модели, году и топливу", ""]
    matched = db.execute(
        """
        WITH segment AS (
            SELECT manufacturer, model, year, fuel, country, count(*) AS n,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY total_rub) AS price,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY mileage_km) AS mileage,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY power_hp) AS power
            FROM vehicle
            WHERE run_id=%s AND sell_status='active' AND status_price='complete' AND total_rub>0
                AND manufacturer IS NOT NULL AND model IS NOT NULL AND year IS NOT NULL
                AND fuel IS NOT NULL AND country IS NOT NULL
            GROUP BY manufacturer, model, year, fuel, country HAVING count(*)>=3
        )
        SELECT a.manufacturer, a.model, a.year, a.fuel,
            a.country, a.n, a.price, a.mileage, a.power,
            b.country, b.n, b.price, b.mileage, b.power, b.price-a.price
        FROM segment a JOIN segment b USING (manufacturer, model, year, fuel)
        WHERE a.country < b.country
        ORDER BY least(a.n,b.n) DESC, abs(b.price-a.price) DESC LIMIT 15
        """, (run_id,),
    ).fetchall()
    if matched:
        result += table(
            ["Марка", "Модель", "Год", "Топливо", "A", "n A", "₽ A", "км A", "л.с. A",
             "B", "n B", "₽ B", "км B", "л.с. B", "B − A, ₽"], matched,
        )
    else:
        result += ["Нет групп с тремя или более полными расчётами с каждой стороны.", ""]
    result += [
        "Пробег и мощность помогают проверить различия состава внутри одной модели. "
        "Комплектация, состояние, курс и условия ввоза могут также влиять на цену. "
        "Разницу между странами нельзя причинно объяснить одной этой группировкой.", "",
        "## Статьи расчёта до Владивостока по полным карточкам", "",
    ]
    components = db.execute(
        """
        WITH detail_prices AS (
            SELECT country, coalesce(detail->'data'->'pricing', detail->'pricing') AS price
            FROM vehicle WHERE run_id=%s AND sell_status='active' AND detail IS NOT NULL
        )
        SELECT country, step->>'key', max(step->>'label'), count(*),
            percentile_cont(0.5) WITHIN GROUP (ORDER BY (step->>'amount_rub')::numeric)
        FROM detail_prices p
        CROSS JOIN LATERAL jsonb_array_elements(
            CASE WHEN jsonb_typeof(p.price->'steps')='array' THEN p.price->'steps' ELSE '[]'::jsonb END
        ) s(step)
        WHERE p.price->>'status'='complete' AND jsonb_typeof(step->'amount_rub')='number'
        GROUP BY country, step->>'key' ORDER BY country, count(*) DESC, step->>'key'
        """, (run_id,),
    ).fetchall()
    result += (table(["Страна", "Код статьи", "Статья", "Записей", "Медиана, ₽"], components)
               if components else ["Для этого прогона нет разбивки по статьям.", ""])
    result += ["## Полнота данных и статус расчёта", ""]
    result += table(
        ["Страна", "Статус расчёта", "Объявлений", "Есть детализация", "Есть главное фото или галерея", "Есть пробег"],
        db.execute(
            """
            SELECT country, status_price, count(*), count(detail),
                count(*) FILTER (
                    WHERE nullif(summary#>>'{media,main_photo}', '') IS NOT NULL
                    OR jsonb_path_exists(coalesce(detail->'data',detail), '$.media.listing_photos[*].url')
                    OR jsonb_path_exists(coalesce(detail->'data',detail), '$.media.configuration_photos[*].url')
                ), count(mileage_km)
            FROM vehicle WHERE run_id=%s GROUP BY country, status_price
            ORDER BY country, count(*) DESC, status_price
            """, (run_id,),
        ).fetchall(),
    )
    return "\n".join(result)
