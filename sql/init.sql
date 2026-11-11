CREATE TABLE scores (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id text NOT NULL UNIQUE,
    score double precision NOT NULL CHECK (score BETWEEN 0 AND 1),
    fraud_flag integer NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version text NOT NULL,
    amount double precision NOT NULL CHECK (amount >= 0),
    us_state text NOT NULL,
    merch text NOT NULL,
    cat_id text NOT NULL,
    transaction_time timestamptz NOT NULL,
    scored_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX scores_scored_at_idx ON scores (scored_at);
CREATE INDEX scores_state_merch_time_idx ON scores (us_state, merch, scored_at);
CREATE USER grafana WITH PASSWORD 'grafana';
GRANT CONNECT ON DATABASE fraud TO grafana;
GRANT USAGE ON SCHEMA public TO grafana;
GRANT SELECT ON scores TO grafana;
