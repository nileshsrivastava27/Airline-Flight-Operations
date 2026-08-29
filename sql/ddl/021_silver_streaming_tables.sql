-- ============================================================
-- Silver Streaming Tables
-- Cleaned and deduplicated streaming events
-- ============================================================

CREATE TABLE IF NOT EXISTS flight_delay.silver.flight_events_clean (
    flight_event_key        BIGINT          GENERATED ALWAYS AS IDENTITY,
    event_id                STRING          NOT NULL    COMMENT 'Unique event identifier',
    flight_id               STRING          NOT NULL    COMMENT 'Flight instance identifier',
    flight_number           STRING          NOT NULL    COMMENT 'Airline flight number',
    airline                 STRING          NOT NULL    COMMENT 'IATA airline code',
    event_type              STRING          NOT NULL    COMMENT 'Validated event type',
    airport_code            STRING          NOT NULL    COMMENT 'Airport IATA code where event occurred',
    gate                    STRING          COMMENT 'Gate assignment',
    origin                  STRING          NOT NULL    COMMENT 'Origin airport IATA code',
    destination             STRING          NOT NULL    COMMENT 'Destination airport IATA code',
    aircraft_type           STRING          COMMENT 'Aircraft type code',
    tail_number             STRING          COMMENT 'Aircraft tail number',
    event_time              TIMESTAMP       NOT NULL    COMMENT 'Time the event occurred',
    scheduled_departure     TIMESTAMP       COMMENT 'Scheduled departure time',
    scheduled_arrival       TIMESTAMP       COMMENT 'Scheduled arrival time',
    status                  STRING          COMMENT 'Flight status',
    delay_minutes           INT             DEFAULT 0   COMMENT 'Cumulative delay minutes',
    reason                  STRING          COMMENT 'Delay or cancellation reason',
    is_delay_event          BOOLEAN         COMMENT 'True if event_type is DELAY',
    is_cancellation_event   BOOLEAN         COMMENT 'True if event_type is CANCELLATION',
    event_date              DATE            NOT NULL    COMMENT 'Event date for partitioning',
    processing_timestamp    TIMESTAMP       COMMENT 'When the record was processed into Silver'
)
USING DELTA
PARTITIONED BY (event_date)
COMMENT 'Silver layer: cleaned and validated flight lifecycle events'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


CREATE TABLE IF NOT EXISTS flight_delay.silver.weather_updates_clean (
    weather_update_key      BIGINT          GENERATED ALWAYS AS IDENTITY,
    observation_id          STRING          NOT NULL    COMMENT 'Unique observation identifier',
    station_id              STRING          NOT NULL    COMMENT 'Weather station / airport IATA code',
    observation_time        TIMESTAMP       NOT NULL    COMMENT 'Observation time',
    observation_date        DATE            NOT NULL    COMMENT 'Observation date for partitioning',
    temp_c                  DOUBLE          COMMENT 'Temperature in Celsius',
    dewpoint_c              DOUBLE          COMMENT 'Dewpoint temperature in Celsius',
    wind_dir_degrees        INT             COMMENT 'Wind direction in degrees',
    wind_speed_kt           INT             COMMENT 'Wind speed in knots',
    wind_gust_kt            INT             COMMENT 'Wind gust speed in knots',
    visibility_statute_mi   DOUBLE          COMMENT 'Visibility in statute miles',
    altim_in_hg             DOUBLE          COMMENT 'Altimeter setting in inches of mercury',
    flight_category         STRING          COMMENT 'Flight category: VFR, MVFR, IFR, LIFR',
    sky_cover               STRING          COMMENT 'Sky cover: CLR, FEW, SCT, BKN, OVC',
    precip_in               DOUBLE          DEFAULT 0.0 COMMENT 'Precipitation in inches',
    wind_chill_c            DOUBLE          COMMENT 'Calculated wind chill in Celsius',
    is_gusty                BOOLEAN         COMMENT 'True if wind gusts exceed 25 knots',
    is_low_visibility       BOOLEAN         COMMENT 'True if visibility below 3 statute miles',
    weather_severity        INT             COMMENT 'Severity score: VFR=0, MVFR=1, IFR=2, LIFR=3',
    processing_timestamp    TIMESTAMP       COMMENT 'When the record was processed into Silver'
)
USING DELTA
PARTITIONED BY (observation_date)
COMMENT 'Silver layer: cleaned and enriched weather observations from streaming'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
