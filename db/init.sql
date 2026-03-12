-- =============================================================================
-- IoT ETL Pipeline - Database Schema
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. raw_events: landing zone for all incoming MQTT messages
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_events (
    id           SERIAL PRIMARY KEY,
    sensor_id    VARCHAR(64),
    sensor_type  VARCHAR(32),
    payload      JSONB         NOT NULL,
    received_at  TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    processed    BOOLEAN       NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_raw_events_processed    ON raw_events (processed);
CREATE INDEX IF NOT EXISTS idx_raw_events_sensor_id    ON raw_events (sensor_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_received_at  ON raw_events (received_at);

-- ---------------------------------------------------------------------------
-- 2. clean_events: validated and normalised events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS clean_events (
    id             SERIAL PRIMARY KEY,
    raw_event_id   INT           REFERENCES raw_events (id) ON DELETE SET NULL,
    sensor_id      VARCHAR(64)   NOT NULL,
    sensor_type    VARCHAR(32)   NOT NULL,
    value          FLOAT         NOT NULL,
    unit           VARCHAR(16),
    event_ts       TIMESTAMPTZ   NOT NULL,
    ingested_at    TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clean_events_sensor_id  ON clean_events (sensor_id);
CREATE INDEX IF NOT EXISTS idx_clean_events_event_ts   ON clean_events (event_ts);

-- ---------------------------------------------------------------------------
-- 3. sensor_metadata: reference data for enrichment and anomaly detection
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_metadata (
    sensor_id                VARCHAR(64)  PRIMARY KEY,
    sensor_type              VARCHAR(32)  NOT NULL,
    location                 VARCHAR(128),
    zone                     VARCHAR(64),
    min_expected             FLOAT,
    max_expected             FLOAT,
    baseline_events_per_min  FLOAT
);

-- ---------------------------------------------------------------------------
-- 4. sensor_readings: final analytical / reporting table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sensor_readings (
    id                SERIAL PRIMARY KEY,
    sensor_id         VARCHAR(64)   NOT NULL,
    sensor_type       VARCHAR(32)   NOT NULL,
    value             FLOAT         NOT NULL,
    unit              VARCHAR(16),
    event_ts          TIMESTAMPTZ   NOT NULL,
    hour_of_day       INT           NOT NULL,
    day_of_week       INT           NOT NULL,   -- 0 = Monday
    zone              VARCHAR(64),
    location          VARCHAR(128),
    is_business_hours BOOLEAN       NOT NULL DEFAULT FALSE,
    ingested_at       TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sensor_readings_sensor_id  ON sensor_readings (sensor_id);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_event_ts   ON sensor_readings (event_ts);
CREATE INDEX IF NOT EXISTS idx_sensor_readings_zone       ON sensor_readings (zone);

-- ---------------------------------------------------------------------------
-- 5. anomalies: detected anomalous events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomalies (
    id            SERIAL PRIMARY KEY,
    sensor_id     VARCHAR(64)   NOT NULL,
    sensor_type   VARCHAR(32)   NOT NULL,
    event_ts      TIMESTAMPTZ   NOT NULL,
    value         FLOAT,
    anomaly_type  VARCHAR(64)   NOT NULL,
    score         FLOAT,
    reason        TEXT,
    detected_at   TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_anomalies_sensor_id   ON anomalies (sensor_id);
CREATE INDEX IF NOT EXISTS idx_anomalies_event_ts    ON anomalies (event_ts);
CREATE INDEX IF NOT EXISTS idx_anomalies_type        ON anomalies (anomaly_type);

-- ---------------------------------------------------------------------------
-- 6. dead_letter_events: events that failed processing
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dead_letter_events (
    id            SERIAL PRIMARY KEY,
    raw_event_id  INT,
    reason        TEXT          NOT NULL,
    payload       JSONB,
    failed_at     TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_failed_at ON dead_letter_events (failed_at);

-- =============================================================================
-- Seed data: sensor_metadata
-- =============================================================================
INSERT INTO sensor_metadata (sensor_id, sensor_type, location, zone, min_expected, max_expected, baseline_events_per_min)
VALUES
    ('temp-01',   'temperature', 'Server Room A',      'zone-a', -50.0, 100.0, 0.5),
    ('temp-02',   'temperature', 'Office Floor 2',     'zone-b', -50.0, 100.0, 0.5),
    ('motion-01', 'motion',      'Main Entrance',      'zone-a',   0.0,   1.0, 2.0),
    ('motion-02', 'motion',      'Parking Lot',        'zone-c',   0.0,   1.0, 1.0),
    ('door-01',   'door',        'Front Door',         'zone-a',   0.0,   1.0, 0.3),
    ('door-02',   'door',        'Back Exit',          'zone-b',   0.0,   1.0, 0.2)
ON CONFLICT (sensor_id) DO NOTHING;
