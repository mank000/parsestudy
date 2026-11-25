"""Probe catalog freshness from another host using only the standard library."""
import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode
from urllib.request import urlopen


def age_seconds(timestamp, now):
    return max(0, (now - datetime.fromisoformat(timestamp)).total_seconds()) if timestamp else None


def evaluate(snapshot, country, selection, partition, heartbeat_limit, freshness_limit):
    now = datetime.fromisoformat(snapshot["checked_at"])
    matched = next((
        row for row in snapshot["rows"]
        if row["country"] == country and row["selection"] == selection and row["partition"] == partition
    ), None)
    last_success = matched["last_success_at"] if matched else None
    heartbeat_age = age_seconds(snapshot["heartbeat_at"], now)
    fresh_age = age_seconds(last_success, now)
    if heartbeat_age is None or heartbeat_age > heartbeat_limit:
        state = "heartbeat_missing"
    elif fresh_age is None or fresh_age > freshness_limit:
        state = "selection_stale"
    else:
        state = "ok"
    result = {"state": state, "last_success_at": last_success, "heartbeat_age_seconds": heartbeat_age,
              "freshness_age_seconds": fresh_age}
    if matched:
        result["year_range"] = {name: matched[name] for name in ("year_min", "year_max") if matched.get(name) is not None}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="Адрес панели с другого хоста")
    parser.add_argument("--country", required=True, choices=["CN", "JP", "KR"])
    parser.add_argument("--profile-country", choices=["CN", "JP", "KR"])
    parser.add_argument("--year", type=int)
    parser.add_argument("--year-min", type=int)
    parser.add_argument("--year-max", type=int)
    parser.add_argument("--lane", choices=["recent", "archive"], default="recent")
    parser.add_argument("--partition", type=int)
    parser.add_argument("--heartbeat-seconds", type=int, default=75)
    parser.add_argument("--freshness-seconds", type=int, default=1800)
    parser.add_argument("--state-file", type=Path, help="Последняя отметка успеха при недоступности хоста")
    args = parser.parse_args()
    if min(args.heartbeat_seconds, args.freshness_seconds) < 1:
        parser.error("Пороги должны быть положительными")
    if args.partition is not None and (args.partition < 0 or args.lane != "archive"):
        parser.error("Номер диапазона задаётся для архива")
    bounds = {key: getattr(args, key) for key in ("year_min", "year_max") if getattr(args, key) is not None}
    if args.year is not None and bounds:
        parser.error("--year используется отдельно от --year-min и --year-max")
    if args.year_min is not None and args.year_max is not None and args.year_min > args.year_max:
        parser.error("Нижний год больше верхнего")
    profile = dict(bounds)
    if args.year is not None:
        profile = {"year": args.year}
        bounds = {"year_min": args.year, "year_max": args.year}
    if args.profile_country is not None:
        profile["country"] = args.profile_country
    selection = {**bounds, "_lane": "recent"} if args.lane == "recent" else bounds
    url = args.base_url.rstrip("/") + "/sync/status?" + urlencode(
        {"profile": json.dumps(profile, sort_keys=True) if profile else "all"})
    try:
        with urlopen(url, timeout=6) as response:
            snapshot = json.load(response)
        outcome = evaluate(snapshot, args.country, selection, args.partition,
                           args.heartbeat_seconds, args.freshness_seconds)
    except (OSError, ValueError, KeyError, TypeError):
        outcome = {"state": "status_unavailable", "last_success_at": None}
    probe = [url, args.country, selection, args.partition]
    if args.state_file is not None:
        if outcome["last_success_at"]:
            with NamedTemporaryFile(mode="w", encoding="utf-8", dir=args.state_file.parent, delete=False) as stream:
                tmp = Path(stream.name)
                json.dump({"probe": probe, "last_success_at": outcome["last_success_at"],
                           "year_range": outcome.get("year_range", bounds)}, stream)
            try:
                tmp.replace(args.state_file)
            finally:
                tmp.unlink(missing_ok=True)
        elif outcome["state"] == "status_unavailable":
            try:
                previous = json.loads(args.state_file.read_text(encoding="utf-8"))
                if previous["probe"] == probe:
                    outcome["last_success_at"] = previous["last_success_at"]
                    outcome["year_range"] = previous["year_range"]
            except (OSError, ValueError, KeyError, TypeError):
                pass
    print(json.dumps({"country": args.country, "year_range": bounds, "partition": args.partition,
                      "lane": args.lane, "observed_at": datetime.now(timezone.utc).isoformat(),
                      "status_url": url, **outcome}, ensure_ascii=False))
    return 0 if outcome["state"] == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
