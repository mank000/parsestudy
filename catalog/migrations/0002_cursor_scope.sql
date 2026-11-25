ALTER TABLE crawl_cursor
    ADD COLUMN kind text NOT NULL DEFAULT 'carhub',
    ADD COLUMN country text,
    ADD COLUMN filter_digest text,
    ADD COLUMN partition integer;

-- New scopes can contain the same digest. Remove the old uniqueness rule before backfill.
ALTER TABLE crawl_cursor DROP CONSTRAINT crawl_cursor_pkey;

-- Decode only the legacy SATAuto representations. CarHub cursor values are opaque.
WITH legacy AS (
    SELECT c.run_id, c.value, regexp_split_to_array(c.value, ':') AS parts
    FROM crawl_cursor c JOIN crawl_run r ON r.id=c.run_id
    WHERE r.origin='https://satauto.ru' AND (
        c.value ~ '^satauto-api:(CN|JP|KR):([0-9a-f]{64}:)?[0-9a-f]{64}$'
        OR c.value ~ '^satauto-api-part:(CN|JP|KR):[0-9a-f]{64}:[0-9]{1,9}:[0-9a-f]{64}$'
    )
)
UPDATE crawl_cursor c SET
    kind='satauto',
    country=l.parts[2],
    filter_digest=CASE WHEN cardinality(l.parts)=3 THEN encode(sha256(''::bytea),'hex') ELSE l.parts[3] END,
    partition=CASE WHEN l.parts[1]='satauto-api-part' THEN l.parts[4]::integer END,
    value=l.parts[cardinality(l.parts)]
FROM legacy l WHERE (c.run_id,c.value)=(l.run_id,l.value);

ALTER TABLE crawl_cursor
    ADD CONSTRAINT crawl_cursor_scope_check CHECK (
        (kind='carhub' AND country IS NULL AND filter_digest IS NULL AND partition IS NULL)
        OR (kind='satauto' AND country IS NOT NULL AND country IN ('CN','JP','KR')
            AND filter_digest IS NOT NULL AND (partition IS NULL OR partition>=0))
    ),
    ADD CONSTRAINT crawl_cursor_scope_key UNIQUE NULLS NOT DISTINCT
        (run_id,kind,country,filter_digest,partition,value);
