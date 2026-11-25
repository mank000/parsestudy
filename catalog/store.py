import gzip
import json
from pathlib import Path

from psycopg.errors import UniqueViolation
from psycopg.rows import scalar_row
from psycopg.types.json import Jsonb


VEHICLE_COLUMNS = """
    run_id, car_id, country, manufacturer, model, year, mileage_km,
    fuel, power_hp, sell_status, listing_price, currency, listing_rub,
    status_price, total_rub, estimated_rub, known_rub, summary
"""


class InvalidVehicle(ValueError):
    pass


def validate_vehicle(car):
    if not isinstance(car, dict) or type(car.get("id")) is not int or not 0 < car["id"] < 2**63:
        raise InvalidVehicle("Ожидалась карточка с положительным целочисленным id")
    for name in ("source", "listing", "identity", "specs", "pricing", "links"):
        section = car.get(name)
        if section is not None and not isinstance(section, dict):
            raise InvalidVehicle(f"Карточка {car['id']}: поле {name} должно быть объектом")


def fields(car):
    validate_vehicle(car)
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


def migrate(db):
    migrations = sorted(Path(__file__).with_name("migrations").glob("[0-9][0-9][0-9][0-9]_*.sql"))
    with db.transaction():
        db.execute("SELECT pg_advisory_xact_lock(274813)")
        db.execute("""
            CREATE TABLE IF NOT EXISTS catalog_schema_migration (
                version integer PRIMARY KEY,
                name text NOT NULL,
                applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
            )
            """)
        with db.cursor(row_factory=scalar_row) as cursor:
            applied = set(cursor.execute("SELECT version FROM catalog_schema_migration").fetchall())
        for migration in migrations:
            version = int(migration.name.split("_", 1)[0])
            if version not in applied:
                db.execute(migration.read_text(encoding="utf-8"))
                db.execute("INSERT INTO catalog_schema_migration(version,name) VALUES (%s,%s)",
                           (version, migration.name))


def initialize(db):
    from .inventory_cache import initialize_inventory

    migrate(db)
    initialize_inventory(db)


def insert_vehicles(db, run_id, cars, *, refresh_summary=False, batch=False):
    cars = list(cars)
    rows = [(run_id, *fields(car), Jsonb(car)) for car in cars]
    if not rows:
        return 0
    observed = sorted({car["id"] for car in cars if "index_only" not in car})
    conflict = "DO NOTHING"
    if refresh_summary:
        columns = [column.strip() for column in VEHICLE_COLUMNS.split(",")[2:]]
        values = ", ".join(f"excluded.{column}" for column in columns)
        # Detail can enrich specs, but must not mask a newer source price/status.
        volatile = """coalesce((SELECT jsonb_object_agg(key,
                         CASE WHEN key IN ('listing','media') THEN
                              coalesce(vehicle.detail #> ARRAY['data',key], vehicle.detail->key, '{}'::jsonb) || value
                              ELSE value END) FROM jsonb_each(excluded.summary)
                         WHERE key IN ('listing','pricing','media','availability_state')), '{}'::jsonb)"""
        detail = f"""CASE WHEN vehicle.detail IS NULL THEN NULL
                    WHEN vehicle.detail ? 'data' THEN jsonb_set(vehicle.detail, '{{data}}',
                         (vehicle.detail->'data') || {volatile})
                    ELSE vehicle.detail || {volatile} END"""
        conflict = f"""DO UPDATE SET ({", ".join(columns)}) = ({values}), detail={detail}
                       WHERE vehicle.summary IS DISTINCT FROM excluded.summary"""
    with db.transaction(), db.cursor() as cursor:
        if batch:
            rows = list({row[1]: row for row in rows}.values())
            placeholders = "(" + ", ".join(["%s"] * len(rows[0])) + ")"
            cursor.execute(
                f"""INSERT INTO vehicle ({VEHICLE_COLUMNS})
                    VALUES {", ".join([placeholders] * len(rows))}
                    ON CONFLICT (run_id, car_id) {conflict}""",
                [value for row in rows for value in row],
            )
        else:
            cursor.executemany(
                f"""
                INSERT INTO vehicle ({VEHICLE_COLUMNS})
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (run_id, car_id) {conflict}
                """, rows,
            )
        changed = cursor.rowcount
        if observed:
            cursor.execute("""
                INSERT INTO vehicle_observation(run_id, car_id, first_seen_at, last_seen_at)
                SELECT %s, car_id, statement_timestamp(), statement_timestamp()
                FROM unnest(%s::bigint[]) AS observed(car_id)
                ON CONFLICT(run_id, car_id) DO UPDATE SET
                    last_seen_at=greatest(vehicle_observation.last_seen_at, excluded.last_seen_at)
                """, (run_id, observed))
        return changed


def import_export(db, path):
    """Import one complete NDJSON export of full CarHub-shaped records atomically."""
    read_export = gzip.open if path.suffix.lower() == ".gz" else open
    try:
        with db.transaction(), read_export(path, "rt", encoding="utf-8") as stream:
            run_id = db.execute(
                "INSERT INTO crawl_run (origin, mode, page_limit) VALUES ('export', 'import', 0) RETURNING id"
            ).fetchone()[0]
            with db.cursor() as cursor:
                with cursor.copy(f"COPY vehicle ({VEHICLE_COLUMNS}, detail) FROM STDIN") as copy:
                    for row in export_rows(stream, run_id):
                        copy.write_row(row)
                if cursor.rowcount == 0:
                    raise ValueError("NDJSON-выгрузка пуста")
            db.execute(
                """UPDATE crawl_run SET status='complete', listing_done=true,
                   finished_at=clock_timestamp(),
                   seconds=extract(epoch FROM clock_timestamp()-started_at) WHERE id=%s""", (run_id,),
            )
    except UniqueViolation as exc:
        raise ValueError("Повторяющийся id в выгрузке") from exc
    except EOFError as exc:
        raise ValueError("Файл выгрузки оборван") from exc
    return run_id


def export_rows(stream, run_id):
    for number, line in enumerate(stream, 1):
        line = line.strip()
        if not line:
            continue
        try:
            values = fields(json.loads(line))
        except (json.JSONDecodeError, InvalidVehicle) as exc:
            raise ValueError(f"Некорректная строка NDJSON {number}") from exc
        yield run_id, *values, line, line
