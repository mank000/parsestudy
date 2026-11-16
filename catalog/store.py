import json
from pathlib import Path

from psycopg.types.json import Jsonb


def fields(car):
    if not isinstance(car, dict) or type(car.get("id")) is not int or car["id"] < 1:
        raise ValueError("Карточка без валидного id")
    source = car.get("source") or {}
    listing = car.get("listing") or {}
    identity = car.get("identity") or {}
    specs = car.get("specs") or {}
    price = car.get("pricing") or {}
    return (
        car["id"], source.get("country"), identity.get("manufacturer"),
        identity.get("model"), listing.get("year"), listing.get("mileage_km"),
        specs.get("fuel_type") or specs.get("powertrain_type"), specs.get("engine_power_hp"),
        listing.get("sell_status"), listing.get("price"), listing.get("currency"),
        price.get("listing_rub"), price.get("status"), price.get("total_rub"),
        price.get("estimated_total_rub"), price.get("known_total_rub"),
    )


def initialize(db):
    db.execute(Path(__file__).with_name("schema.sql").read_text(encoding="utf-8"))


def insert_vehicles(db, run_id, cars):
    rows = [(run_id, *fields(car), Jsonb(car)) for car in cars]
    with db.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO vehicle (
                run_id, car_id, country, manufacturer, model, year, mileage_km,
                fuel, power_hp, sell_status, listing_price, currency, listing_rub,
                status_price, total_rub, estimated_rub, known_rub, summary
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (run_id, car_id) DO NOTHING
            """, rows,
        )
        return cursor.rowcount


def import_export(db, path):
    """Import one complete NDJSON export of full CarHub-shaped records atomically."""
    run_id = db.execute(
        "INSERT INTO crawl_run (origin, mode, page_limit) VALUES ('export', 'import', 0) RETURNING id"
    ).fetchone()[0]
    with path.open(encoding="utf-8") as stream:
        chunk = []
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                car = json.loads(line)
                fields(car)
            except (ValueError, TypeError, AttributeError) as exc:
                raise ValueError(f"Некорректная строка NDJSON {number}") from exc
            chunk.append(car)
            if len(chunk) == 1000:
                save_export_batch(db, run_id, chunk)
                chunk.clear()
        save_export_batch(db, run_id, chunk)
    if not db.execute("SELECT 1 FROM vehicle WHERE run_id=%s LIMIT 1", (run_id,)).fetchone():
        raise ValueError("NDJSON-выгрузка пуста")
    db.execute(
        """UPDATE crawl_run SET status='complete', listing_done=true,
           finished_at=clock_timestamp(),
           seconds=extract(epoch FROM clock_timestamp()-started_at) WHERE id=%s""", (run_id,),
    )
    return run_id


def save_export_batch(db, run_id, cars):
    if not cars:
        return
    if insert_vehicles(db, run_id, cars) != len(cars):
        raise ValueError("Повторяющийся id в выгрузке")
    db.execute(
        "UPDATE vehicle SET detail=summary WHERE run_id=%s AND car_id=ANY(%s)",
        (run_id, [car["id"] for car in cars]),
    )
