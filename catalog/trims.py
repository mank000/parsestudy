import json
import math
import unicodedata
from collections import Counter, defaultdict
from urllib.parse import urlsplit

from psycopg.rows import tuple_row
from psycopg.types.json import Jsonb


SPEC_LABELS = {
    "engine_displacement_cc": "Объём двигателя, см³",
    "engine_power_hp": "Мощность, л.с.",
    "fuel_type": "Топливо",
    "drive_type": "Привод",
    "transmission": "Коробка передач",
    "body_type": "Кузов",
}
NUMERIC_EVIDENCE = {"engine_displacement_cc", "engine_power_hp"}
LOCAL_SOURCE = "Сохранённые объявления"
REFERENCE_QUERY = """
    SELECT source, reference_id, specs, source_url, evidence
    FROM trim_reference WHERE (country, make_key, model_key, model_year, trim_key) = (%s, %s, %s, %s, %s)
"""
UPSERT_REFERENCE = """
    INSERT INTO trim_reference (source, reference_id, country, make_key, model_key, model_year,
                                trim_key, specs, source_url, evidence)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (source, reference_id) DO UPDATE SET
        country=excluded.country, make_key=excluded.make_key, model_key=excluded.model_key,
        model_year=excluded.model_year, trim_key=excluded.trim_key, specs=excluded.specs,
        source_url=excluded.source_url, evidence=excluded.evidence
"""


def normalized(value):
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split()) if isinstance(value, str) else ""


def identity_key(car):
    source = car.get("source") or {}
    identity = car.get("identity") or {}
    year = (car.get("listing") or {}).get("year")
    names = [normalized(identity.get(field)) for field in ("manufacturer", "model", "trim")]
    if source.get("country") == "CN" and all(names) and type(year) is int and 1900 <= year <= 2100:
        return ("CN", names[0], names[1], year, names[2])
    return None


def technical_specs(data):
    if not isinstance(data, dict):
        return {}
    specs = {}
    for key in SPEC_LABELS:
        value = data.get(key)
        if key in NUMERIC_EVIDENCE:
            if type(value) in (int, float) and 0 < value < 100_000 and math.isfinite(value):
                specs[key] = value
        elif normalized(value) not in {"", "—", "не указано"}:
            specs[key] = value
    return specs


def reference_url(value):
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError("Ссылка должна быть строкой")
    url = urlsplit(value)
    if url.scheme not in {"http", "https"} or not url.hostname or url.username or url.password:
        raise ValueError("Укажите абсолютную ссылку http или https")
    return value


def import_trims(db, path):
    seen = set()
    with db.transaction(), path.open(encoding="utf-8") as stream:
        for number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                car = json.loads(line)
                key = identity_key(car)
                source = car["reference_source"]
                reference_id = car["reference_id"]
                specs = technical_specs(car.get("specs"))
                url = reference_url(car.get("source_url"))
                if (not key or not NUMERIC_EVIDENCE.intersection(specs)
                        or not normalized(source) or source == LOCAL_SOURCE or not normalized(reference_id)
                        or (source, reference_id) in seen):
                    raise ValueError
            except (ValueError, AttributeError, KeyError, TypeError) as exc:
                raise ValueError(f"Некорректная запись справочника в строке {number}") from exc
            seen.add((source, reference_id))
            db.execute(UPSERT_REFERENCE, (source, reference_id, *key, Jsonb(specs), url, Jsonb([])))
        if not seen:
            raise ValueError("Файл справочника пуст")
    return len(seen)


def run_cards(db, run_id):
    with db.cursor(name="trim_observations") as cursor:
        cursor.execute("""
            SELECT coalesce(detail->'data', detail, summary)
            FROM vehicle WHERE run_id=%s AND country='CN'
            """, (run_id,))
        for row in cursor:
            yield row[0]


def build_trims(db, run_id):
    groups = defaultdict(lambda: defaultdict(dict))
    with db.transaction():
        for car in run_cards(db, run_id):
            key = identity_key(car)
            specs = technical_specs(car.get("specs"))
            source = car.get("source") or {}
            lot = (source.get("name"), source.get("listing_id"))
            if key and all(lot):
                lot = tuple(map(str, lot))
                for field, value in specs.items():
                    token = spec_key(field, value)
                    _, evidence = groups[key][field].setdefault(token, (value, {}))
                    if len(evidence) < 2:
                        evidence[lot] = car.get("id")
        records = []
        for key, fields in groups.items():
            if any(len(values) != 1 for values in fields.values()):
                continue
            confirmed = {field: next(iter(values.values())) for field, values in fields.items()}
            confirmed = {field: pair for field, pair in confirmed.items() if len(pair[1]) == 2}
            if NUMERIC_EVIDENCE.intersection(confirmed):
                values = {field: value for field, (value, _) in confirmed.items()}
                evidence = {
                    field: [{"source": source, "listing_id": lot, "car_id": car_id}
                            for (source, lot), car_id in observations.items()]
                    for field, (_, observations) in confirmed.items()
                }
                records.append((LOCAL_SOURCE, json.dumps(key, ensure_ascii=False), *key,
                                Jsonb(values), None, Jsonb({"run_id": run_id, "fields": evidence})))
        db.execute("DELETE FROM trim_reference WHERE source=%s", (LOCAL_SOURCE,))
        with db.cursor() as cursor:
            cursor.executemany(UPSERT_REFERENCE, records)
    return {"run_id": run_id, "identity_groups": len(groups), "references": len(records)}


def spec_key(key, value):
    return value if key in NUMERIC_EVIDENCE else normalized(value)


def resolve_trim(specs, candidates):
    compatible = []
    for candidate in candidates:
        source, reference_id, reference, url, evidence = candidate
        common = specs.keys() & reference.keys()
        if not any(specs[key] == reference[key] for key in common & NUMERIC_EVIDENCE):
            continue
        if any(spec_key(key, specs[key]) != spec_key(key, reference[key]) for key in common):
            continue
        compatible.append({
            "source": source, "reference_id": reference_id, "url": url,
            "run_id": evidence.get("run_id") if isinstance(evidence, dict) else None,
            "specs": {key: value for key, value in reference.items() if key not in specs},
        })
    if len(compatible) == 1:
        return "matched", compatible[0]
    return ("ambiguous" if compatible else "unverified"), None


def trim_for_card(db, car):
    key = identity_key(car)
    if key:
        with db.cursor(row_factory=tuple_row) as cursor:
            if cursor.execute("SELECT to_regclass('trim_reference') IS NOT NULL").fetchone()[0]:
                candidates = cursor.execute(REFERENCE_QUERY, key).fetchall()
                return resolve_trim(technical_specs(car.get("specs")), candidates)[1]
    return None


def audit_trims(db, run_id):
    references = defaultdict(list)
    counts = Counter()
    with db.transaction():
        for row in db.execute("""
            SELECT country, make_key, model_key, model_year, trim_key, source, reference_id,
                   specs, source_url, evidence
            FROM trim_reference
            """):
            references[row[:5]].append(row[5:])
        for car in run_cards(db, run_id):
            key = identity_key(car)
            if key is None:
                counts["incomplete_identity"] += 1
                continue
            result, match = resolve_trim(technical_specs(car.get("specs")), references.get(key, ()))
            counts[result] += 1
            if match and match["specs"]:
                counts["can_enrich"] += 1
    return {"run_id": run_id, **{
        key: counts[key] for key in ("matched", "can_enrich", "ambiguous", "unverified", "incomplete_identity")
    }}
