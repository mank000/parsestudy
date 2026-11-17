from urllib.parse import urlsplit


def plan_details(db, run_id, make, limit):
    queued = db.execute("""
        SELECT t.path, t.car_id FROM satauto_detail_task t
        LEFT JOIN sitemap_car s ON s.car_id=t.car_id
        WHERE t.run_id=%s AND t.status IN ('pending', 'deferred')
          AND (t.retry_at IS NULL OR t.retry_at <= clock_timestamp())
          AND (%s::text IS NULL OR coalesce(s.make, split_part(t.path, '/', 4))=%s)
          AND NOT EXISTS (
              SELECT 1 FROM vehicle v WHERE v.run_id=t.run_id AND v.car_id=t.car_id
              AND (v.detail IS NOT NULL OR v.detail_missing)
          )
        ORDER BY (t.status='pending') DESC, t.car_id DESC LIMIT %s
        """, (run_id, make, make, limit)).fetchall()
    if len(queued) == limit:
        return queued
    new = db.execute("""
        SELECT path, s.car_id FROM sitemap_car s
        WHERE (%s::text IS NULL OR s.make=%s)
          AND NOT EXISTS (
              SELECT 1 FROM vehicle v WHERE v.run_id=%s AND v.car_id=s.car_id
              AND (v.detail IS NOT NULL OR v.detail_missing)
          )
          AND NOT EXISTS (
              SELECT 1 FROM satauto_detail_task t WHERE t.run_id=%s AND t.car_id=s.car_id
          )
        ORDER BY s.car_id DESC LIMIT %s
        """, (make, make, run_id, run_id, limit - len(queued))).fetchall()
    return queued + new


def enqueue_details(db, run_id, targets):
    with db.cursor() as cursor:
        cursor.executemany("""
            INSERT INTO satauto_detail_task (run_id, car_id, path)
            VALUES (%s, %s, %s)
            ON CONFLICT (run_id, car_id) DO UPDATE SET path=excluded.path
            WHERE satauto_detail_task.path<>excluded.path
            """, [(run_id, car_id, urlsplit(url).path) for url, car_id in targets])


def start_detail(db, run_id, car_id):
    db.execute("""
        UPDATE satauto_detail_task SET status='pending', attempts=attempts+1,
            retry_at=NULL, updated_at=clock_timestamp()
        WHERE run_id=%s AND car_id=%s
        """, (run_id, car_id))


def finish_detail(db, run_id, car_id, status, error=None):
    db.execute("""
        UPDATE satauto_detail_task SET status=%s, last_error=%s,
            retry_at=CASE WHEN %s='deferred' THEN clock_timestamp() + interval '15 minutes' END,
            updated_at=clock_timestamp()
        WHERE run_id=%s AND car_id=%s
        """, (status, error, status, run_id, car_id))


def queue_status(db, origin, run_id=None):
    run = db.execute("""
        SELECT id FROM crawl_run WHERE origin=%s AND (%s::bigint IS NULL OR id=%s)
        ORDER BY id DESC LIMIT 1
        """, (origin, run_id, run_id)).fetchone()
    if run is None and run_id is not None:
        raise ValueError(f"Прогон SATAuto {run_id} не найден")
    run_id = run[0] if run else None
    counts = dict(db.execute("""
        SELECT status, count(*) FROM satauto_detail_task
        WHERE run_id=%s GROUP BY status
        """, (run_id,)).fetchall())
    failures = db.execute("""
        SELECT car_id, attempts, last_error, retry_at
        FROM satauto_detail_task WHERE run_id=%s AND status IN ('pending', 'deferred')
          AND last_error IS NOT NULL
        ORDER BY updated_at DESC, car_id LIMIT 10
        """, (run_id,)).fetchall()
    return {
        "run_id": run_id,
        "tasks": {status: counts.get(status, 0) for status in ("pending", "complete", "missing", "deferred")},
        "recent_failures": [
            {"car_id": car_id, "attempts": attempts, "error": error, "retry_at": retry_at}
            for car_id, attempts, error, retry_at in failures
        ],
    }
