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

CREATE INDEX IF NOT EXISTS vehicle_pending_detail_idx ON vehicle (run_id, car_id)
    WHERE detail IS NULL AND NOT detail_missing;

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

CREATE TABLE IF NOT EXISTS satauto_lot_id (
    source_id text PRIMARY KEY,
    car_id bigint GENERATED ALWAYS AS IDENTITY (START WITH 100000000000000000) UNIQUE
);

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
