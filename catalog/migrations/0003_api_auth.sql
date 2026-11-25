CREATE TABLE IF NOT EXISTS api_user (
    username text PRIMARY KEY,
    password_hash text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    token_version int NOT NULL DEFAULT 1
);

CREATE TABLE api_token_rate (
    client text PRIMARY KEY,
    reset_at timestamptz NOT NULL,
    attempts int NOT NULL
);
CREATE INDEX api_token_rate_reset_idx ON api_token_rate (reset_at);
