CREATE TABLE IF NOT EXISTS crawl_run (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    origin text NOT NULL,
    source_country text,
    mode text NOT NULL CHECK (mode IN ('listing', 'full', 'import')),
    page_limit int NOT NULL,
    pages int NOT NULL DEFAULT 0,
    attempted_requests int NOT NULL DEFAULT 0,
    downloaded_responses int NOT NULL DEFAULT 0,
    response_bytes bigint NOT NULL DEFAULT 0,
    seconds double precision NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'running',
    cursor text,
    listing_done boolean NOT NULL DEFAULT false,
    diagnostic text,
    retry_at timestamptz,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS vehicle (
    run_id bigint NOT NULL REFERENCES crawl_run (id) ON DELETE CASCADE,
    car_id bigint NOT NULL,
    country text,
    manufacturer text,
    model text,
    year int,
    mileage_km int,
    fuel text,
    power_hp numeric,
    sell_status text,
    listing_price numeric,
    currency text,
    listing_rub numeric,
    status_price text,
    total_rub numeric,
    estimated_rub numeric,
    known_rub numeric,
    summary jsonb NOT NULL,
    detail jsonb,
    detail_missing boolean NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, car_id)
);

CREATE TABLE IF NOT EXISTS crawl_cursor (
    run_id bigint NOT NULL REFERENCES crawl_run (id) ON DELETE CASCADE,
    value text NOT NULL,
    PRIMARY KEY (run_id, value)
);

-- Observation starts when this migration is installed, not at historical publication time.
CREATE TABLE IF NOT EXISTS vehicle_observation (
    run_id bigint NOT NULL,
    car_id bigint NOT NULL,
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(run_id,car_id),
    FOREIGN KEY(run_id,car_id) REFERENCES vehicle(run_id,car_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS catalog_sync_state (
    profile text PRIMARY KEY,
    recent_at timestamptz,
    full_completed_at timestamptz
);
ALTER TABLE catalog_sync_state ADD COLUMN IF NOT EXISTS pages_saved bigint NOT NULL DEFAULT 0;
ALTER TABLE catalog_sync_state ADD COLUMN IF NOT EXISTS alert_kind text;
ALTER TABLE catalog_sync_state ADD COLUMN IF NOT EXISTS alert_pages bigint;

CREATE INDEX IF NOT EXISTS vehicle_pending_detail_idx ON vehicle (run_id, car_id)
    WHERE detail IS NULL AND NOT detail_missing;

CREATE TABLE IF NOT EXISTS vehicle_inventory (
    run_id bigint NOT NULL,
    car_id bigint NOT NULL,
    country text,
    make text,
    model text,
    title text,
    path text,
    year int,
    mileage int,
    availability text,
    color text,
    fuel text,
    photo text,
    loaded boolean,
    has_data boolean,
    has_detail boolean,
    currency text,
    original_price text,
    seller_price numeric,
    full_price numeric,
    estimated_price numeric,
    partial_price numeric,
    displayed_price numeric,
    price_kind text,
    PRIMARY KEY (run_id, car_id),
    FOREIGN KEY (run_id, car_id) REFERENCES vehicle (run_id, car_id) ON DELETE CASCADE
);

ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS trim text;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS transmission text;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS drive text;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS body text;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS engine_cc numeric;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS power numeric;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS sanctioned boolean;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS damage_confirmed boolean;
ALTER TABLE vehicle_inventory ADD COLUMN IF NOT EXISTS filter_ready boolean NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS vehicle_observation_parsed_idx
    ON vehicle_observation (run_id, last_seen_at, car_id);
CREATE INDEX IF NOT EXISTS vehicle_inventory_country_idx
    ON vehicle_inventory (run_id, country, car_id);
CREATE INDEX IF NOT EXISTS vehicle_inventory_make_model_idx
    ON vehicle_inventory (run_id, make, model, car_id);
CREATE INDEX IF NOT EXISTS vehicle_inventory_year_idx
    ON vehicle_inventory (run_id, year DESC NULLS LAST, car_id DESC);

CREATE TABLE IF NOT EXISTS sitemap_car (
    car_id bigint PRIMARY KEY,
    country text NOT NULL,
    make text NOT NULL,
    model text NOT NULL,
    title text NOT NULL,
    path text NOT NULL
);

CREATE TABLE IF NOT EXISTS satauto_detail_task (
    run_id bigint NOT NULL REFERENCES crawl_run (id) ON DELETE CASCADE,
    car_id bigint NOT NULL,
    path text NOT NULL,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'deferred', 'complete', 'missing')),
    attempts integer NOT NULL DEFAULT 0,
    last_error text,
    retry_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (run_id, car_id)
);

CREATE TABLE IF NOT EXISTS satauto_scan (
    run_id bigint PRIMARY KEY REFERENCES crawl_run (id) ON DELETE CASCADE,
    before_car_id bigint,
    exhausted boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS satauto_http_daily (
    day date PRIMARY KEY,
    attempts bigint NOT NULL,
    responses bigint NOT NULL,
    ok bigint NOT NULL,
    bad_gateway bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS satauto_api_scan (
    run_id bigint NOT NULL REFERENCES crawl_run (id) ON DELETE CASCADE,
    country text NOT NULL CHECK (country IN ('CN', 'KR', 'JP')),
    filter_key text NOT NULL DEFAULT '',
    pages integer NOT NULL DEFAULT 0,
    cursor text,
    done boolean NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, country, filter_key)
);

ALTER TABLE satauto_api_scan ADD COLUMN IF NOT EXISTS filter_key text NOT NULL DEFAULT '';
ALTER TABLE satauto_api_scan ADD COLUMN IF NOT EXISTS retry_at timestamptz;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='satauto_api_scan'::regclass AND contype='p' AND cardinality(conkey)=2
    ) THEN
        ALTER TABLE satauto_api_scan DROP CONSTRAINT satauto_api_scan_pkey;
        ALTER TABLE satauto_api_scan ADD PRIMARY KEY (run_id, country, filter_key);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS satauto_api_partition (
    run_id bigint NOT NULL,
    country text NOT NULL,
    filter_key text NOT NULL,
    partition int NOT NULL,
    year_min int,
    year_max int,
    cursor text,
    pages int NOT NULL DEFAULT 0,
    done boolean NOT NULL DEFAULT false,
    PRIMARY KEY (run_id, country, filter_key, partition),
    FOREIGN KEY (run_id, country, filter_key)
        REFERENCES satauto_api_scan (run_id, country, filter_key) ON DELETE CASCADE
);

ALTER TABLE satauto_api_partition ADD COLUMN IF NOT EXISTS retry_at timestamptz;

CREATE TABLE IF NOT EXISTS satauto_lot_id (
    source_id text PRIMARY KEY,
    car_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 100000000000000000) UNIQUE
);

CREATE OR REPLACE FUNCTION satauto_public_path(listing_id bigint, country_code text, candidates text[])
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT coalesce(
        (SELECT candidate FROM unnest(candidates) WITH ORDINALITY AS paths(candidate, preference)
         WHERE candidate ~ ('^(https://satauto\.ru)?/catalog/(china|korea|japan)/([^/?#]+/){1,2}'
                             || listing_id::text || '-[^/?#]+/$')
         ORDER BY preference LIMIT 1),
        CASE WHEN listing_id BETWEEN 1 AND 99999999999999999 THEN
            '/catalog/' || CASE country_code WHEN 'CN' THEN 'china' WHEN 'KR' THEN 'korea' WHEN 'JP' THEN 'japan' END
            || '/car/car/' || listing_id::text || '-car/'
        END
    )
$$;

CREATE INDEX IF NOT EXISTS satauto_detail_ready_idx
    ON satauto_detail_task (run_id, car_id DESC) WHERE status IN ('pending', 'deferred');

CREATE TABLE IF NOT EXISTS trim_reference (
    source text NOT NULL,
    reference_id text NOT NULL,
    country text NOT NULL CHECK (country = 'CN'),
    make_key text NOT NULL,
    model_key text NOT NULL,
    model_year integer NOT NULL,
    trim_key text NOT NULL,
    specs jsonb NOT NULL,
    source_url text,
    evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    PRIMARY KEY (source, reference_id)
);

CREATE INDEX IF NOT EXISTS trim_reference_identity_idx
    ON trim_reference (country, make_key, model_key, model_year, trim_key);
