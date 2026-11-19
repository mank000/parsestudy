"""Seed the isolated Compose demo database with synthetic listings."""
import os

import psycopg

from .inventory import SAT
from .store import insert_vehicles


SCENARIOS = (
    ("JP", "Toyota", "Corolla", 2021, "active", 1_580_000, 1_960_000, "Белый"),
    ("JP", "Toyota", "RAV4", 2023, "sold", 2_250_000, 2_940_000, "Синий"),
    ("KR", "Hyundai", "Tucson", 2022, "active", 2_090_000, 2_710_000, "Серый"),
    ("KR", "Kia", "Sportage", 2020, "inactive", 1_720_000, None, "Чёрный"),
    ("CN", "Geely", "Monjaro", 2024, "active", 2_720_000, 3_350_000, "Серый"),
    ("CN", "BYD", "Song Plus", 2023, "active", 2_490_000, None, "Белый"),
)
DEMO_TAG = "synthetic_demo"


def demo_listings():
    for number, (country, make, model, year, status, seller_rub, total_rub, color) in enumerate(SCENARIOS, 1):
        yield {
            "id": 100_000_000_000_000_000 + number,
            "source": {"country": country},
            "identity": {"manufacturer": make, "model": model, "title": f"{make} {model} · пример"},
            "listing": {"year": year, "mileage_km": 85_000 - 8_000 * number, "sell_status": status},
            "specs": {"color": color, "fuel_type": "Гибрид" if make == "BYD" else "Бензин"},
            "pricing": {"listing_rub": seller_rub, "total_rub": total_rub},
        }


def main():
    with psycopg.connect(os.environ["DATABASE_URL"]) as db:
        db.execute("LOCK TABLE crawl_run IN SHARE ROW EXCLUSIVE MODE")
        runs = db.execute("SELECT diagnostic FROM crawl_run").fetchall()
        if runs == [(DEMO_TAG,)]:
            return
        if runs:
            raise SystemExit("Для демонстрации нужна отдельная база без других прогонов. Используйте ./parse demo.")
        run_id = db.execute(
            """INSERT INTO crawl_run (origin, mode, page_limit, diagnostic, status, listing_done, finished_at)
               VALUES (%s, 'import', 0, %s, 'complete', true, clock_timestamp()) RETURNING id""",
            (SAT, DEMO_TAG),
        ).fetchone()[0]
        insert_vehicles(db, run_id, demo_listings())


if __name__ == "__main__":
    main()
