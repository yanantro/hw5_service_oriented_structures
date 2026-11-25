CREATE TABLE IF NOT EXISTS metrics (
 date date NOT NULL,
 metric text NOT NULL,
 dimension text NOT NULL DEFAULT '',
 value double precision,
 calculated_at timestamptz NOT NULL,
 PRIMARY KEY (date, metric, dimension)
);
CREATE TABLE IF NOT EXISTS export_jobs (
 date date PRIMARY KEY,
 exported_at timestamptz
);
