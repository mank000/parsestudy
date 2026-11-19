from psycopg.rows import tuple_row

from .inventory import METADATA, SATAUTO_PATH, VEHICLES


def initialize_inventory(db):
    """Keep the searchable projection current for API, imports and detail updates."""
    metadata = METADATA.replace("v.run_id=%s AND ", "")
    vehicles = VEHICLES.replace("SELECT car_id, country,", "SELECT run_id, car_id, country,", 1)
    db.execute(f"""
        CREATE OR REPLACE VIEW vehicle_inventory_source AS
        WITH metadata AS ({metadata}) {vehicles}
        """)
    columns = [column.name for column in db.execute("SELECT * FROM vehicle_inventory LIMIT 0").description]
    values = columns[2:]
    names = ", ".join(columns)
    updates = ", ".join(f"{name}=excluded.{name}" for name in values)
    db.execute(f"""
        CREATE OR REPLACE FUNCTION refresh_vehicle_inventory() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            IF coalesce(NEW.summary ? 'index_only', false) THEN
                DELETE FROM vehicle_inventory WHERE run_id=NEW.run_id AND car_id=NEW.car_id;
            ELSE
                INSERT INTO vehicle_inventory ({names})
                SELECT {names} FROM vehicle_inventory_source
                WHERE run_id=NEW.run_id AND car_id=NEW.car_id
                ON CONFLICT (run_id, car_id) DO UPDATE SET {updates};
            END IF;
            RETURN NEW;
        END $$;
        """)
    db.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid='vehicle'::regclass AND tgname='vehicle_inventory_refresh'
            ) THEN
                CREATE TRIGGER vehicle_inventory_refresh AFTER INSERT OR UPDATE ON vehicle
                FOR EACH ROW EXECUTE FUNCTION refresh_vehicle_inventory();
            END IF;
        END $$;
        """)
    db.execute(f"""
        WITH pending AS MATERIALIZED (
            SELECT i.run_id, i.car_id, i.country
            FROM vehicle_inventory i JOIN crawl_run r ON r.id=i.run_id
            WHERE r.origin='https://satauto.ru'
              AND (i.path ~ '/lot-(main|stats)-'
                   OR (i.path IS NULL AND i.car_id<100000000000000000 AND i.country IN ('CN','KR','JP')))
        ), repaired AS MATERIALIZED (
            SELECT p.run_id, p.car_id, {SATAUTO_PATH} AS path
            FROM pending p CROSS JOIN LATERAL (
                SELECT v.summary, coalesce(v.detail->'data', v.detail, v.summary) AS data
                FROM vehicle v WHERE (v.run_id,v.car_id)=(p.run_id,p.car_id)
                OFFSET 0
            ) listing
        )
        UPDATE vehicle_inventory cached SET path=repaired.path
        FROM repaired WHERE (cached.run_id,cached.car_id)=(repaired.run_id,repaired.car_id)
          AND cached.path IS DISTINCT FROM repaired.path
        """)
    # Find missing IDs before evaluating JSON fields from the source view.
    after = (0, 0)
    with db.cursor(row_factory=tuple_row) as cursor:
        while missing := cursor.execute("""
            SELECT v.run_id, v.car_id FROM vehicle v
            WHERE (v.run_id, v.car_id)>(%s,%s) AND NOT EXISTS (
                SELECT 1 FROM vehicle_inventory cached
                WHERE cached.run_id=v.run_id AND cached.car_id=v.car_id
            )
            ORDER BY v.run_id, v.car_id LIMIT 5000
            """, after).fetchall():
            db.execute(f"""
                INSERT INTO vehicle_inventory ({names})
                SELECT {", ".join("source." + name for name in columns)}
                FROM unnest(%s::bigint[], %s::bigint[]) AS missing(run_id, car_id)
                JOIN vehicle_inventory_source source USING (run_id, car_id)
                ON CONFLICT (run_id, car_id) DO NOTHING
                """, ([row[0] for row in missing], [row[1] for row in missing]))
            after = missing[-1]
        after = (0, 0)
        fields = ("trim", "transmission", "drive", "body", "engine_cc", "power", "sanctioned", "damage_confirmed", "filter_ready")
        while pending := cursor.execute("""
            SELECT run_id,car_id FROM vehicle_inventory
            WHERE NOT filter_ready AND (run_id,car_id)>(%s,%s)
            ORDER BY run_id,car_id LIMIT 5000
            """, after).fetchall():
            last = pending[-1]
            db.execute(f"""
                UPDATE vehicle_inventory cached
                SET {", ".join(field + "=source." + field for field in fields)}
                FROM vehicle_inventory_source source
                WHERE (source.run_id,source.car_id)>(%s,%s) AND (source.run_id,source.car_id)<=(%s,%s)
                  AND (cached.run_id,cached.car_id)=(source.run_id,source.car_id)
                  AND NOT cached.filter_ready
                """, (*after, *last))
            after = last
