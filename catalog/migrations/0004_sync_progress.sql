CREATE TABLE catalog_sync_progress (
    profile text NOT NULL REFERENCES catalog_sync_state(profile) ON DELETE CASCADE,
    country text NOT NULL,
    filter_key text NOT NULL,
    partition integer,
    year_min integer,
    year_max integer,
    last_success_at timestamptz NOT NULL,
    UNIQUE NULLS NOT DISTINCT (profile,country,filter_key,partition)
);

ALTER TABLE catalog_sync_state ADD COLUMN heartbeat_at timestamptz;
