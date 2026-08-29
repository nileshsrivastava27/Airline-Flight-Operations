-- ============================================================
-- Bronze Streaming Tables
-- Real-time flight events and weather updates
-- ============================================================

-- Flight events from Kafka / Auto Loader
CREATE TABLE IF NOT EXISTS flight_delay.bronze.flight_events_raw (
    event_id                STRING          COMMENT 'Unique event identifier',
    flight_id               STRING          COMMENT 'Flight instance identifier',
    flight_number           STRING          COMMENT 'Airline flight number (e.g., DL302)',
    airline                 STRING          COMMENT 'IATA airline code',
    event_type              STRING          COMMENT 'Event type: FLIGHT_SCHEDULED, BOARDING_STARTED, BOARDING_COMPLETED, DEPARTURE, TAKEOFF, LANDING, ARRIVAL, DELAY, CANCELLATION, GATE_CHANGE',
    airport_code            STRING          COMMENT 'Airport where event occurred',
    gate                    STRING          COMMENT 'Gate assignment',
    origin                  STRING          COMMENT 'Origin airport IATA code',
    destination             STRING          COMMENT 'Destination airport IATA code',
    aircraft_type           STRING          COMMENT 'Aircraft type code',
    tail_number             STRING          COMMENT 'Aircraft tail number',
    event_time              TIMESTAMP       COMMENT 'Time the event occurred',
    scheduled_departure     TIMESTAMP       COMMENT 'Scheduled departure time',
    scheduled_arrival       TIMESTAMP       COMMENT 'Scheduled arrival time',
    status                  STRING          COMMENT 'Flight status at event time',
    delay_minutes           INT             COMMENT 'Cumulative delay in minutes',
    reason                  STRING          COMMENT 'Reason for delay or cancellation',
    metadata                STRING          COMMENT 'Additional event metadata as JSON',
    -- Ingestion metadata
    source_system           STRING          DEFAULT 'streaming_flight_events',
    ingestion_timestamp     TIMESTAMP       COMMENT 'When the record was ingested into Bronze',
    batch_id                STRING          COMMENT 'Micro-batch or file batch identifier',
    kafka_topic             STRING          COMMENT 'Source Kafka topic (if applicable)',
    kafka_partition         INT             COMMENT 'Source Kafka partition (if applicable)',
    kafka_offset            BIGINT          COMMENT 'Source Kafka offset (if applicable)',
    event_date              DATE            COMMENT 'Event date for partitioning'
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Bronze layer: raw flight lifecycle events from streaming sources'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


-- Weather updates from Kafka / Auto Loader
CREATE TABLE IF NOT EXISTS flight_delay.bronze.weather_updates_raw (
    observation_id          STRING          COMMENT 'Unique observation identifier',
    station_id              STRING          COMMENT 'Weather station / airport IATA code',
    observation_time        TIMESTAMP       COMMENT 'Time of weather observation',
    observation_date        DATE            COMMENT 'Date of observation for partitioning',
    temp_c                  DOUBLE          COMMENT 'Temperature in Celsius',
    dewpoint_c              DOUBLE          COMMENT 'Dewpoint temperature in Celsius',
    wind_dir_degrees        INT             COMMENT 'Wind direction in degrees',
    wind_speed_kt           INT             COMMENT 'Wind speed in knots',
    wind_gust_kt            INT             COMMENT 'Wind gust speed in knots (nullable)',
    visibility_statute_mi   DOUBLE          COMMENT 'Visibility in statute miles',
    altim_in_hg             DOUBLE          COMMENT 'Altimeter setting in inches of mercury',
    flight_category         STRING          COMMENT 'Flight category: VFR, MVFR, IFR, LIFR',
    sky_cover               STRING          COMMENT 'Sky cover: CLR, FEW, SCT, BKN, OVC',
    precip_in               DOUBLE          COMMENT 'Precipitation in inches',
    weather_profile         STRING          COMMENT 'Weather profile used by simulator',
    -- Ingestion metadata
    source_system           STRING          DEFAULT 'streaming_weather',
    ingestion_timestamp     TIMESTAMP       COMMENT 'When the record was ingested into Bronze',
    batch_id                STRING          COMMENT 'Micro-batch or file batch identifier',
    kafka_topic             STRING          COMMENT 'Source Kafka topic (if applicable)',
    kafka_partition         INT             COMMENT 'Source Kafka partition (if applicable)',
    kafka_offset            BIGINT          COMMENT 'Source Kafka offset (if applicable)'
)
USING DELTA
PARTITIONED BY (observation_date)
COMMENT 'Bronze layer: raw weather observations from streaming sources'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
