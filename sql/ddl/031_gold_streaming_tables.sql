-- ============================================================
-- Gold Streaming Tables
-- Real-time operational analytics
-- ============================================================

-- Real-time flight status: one row per active flight, continuously updated
CREATE TABLE IF NOT EXISTS flight_delay.gold.realtime_flight_status (
    flight_id               STRING          NOT NULL    COMMENT 'Flight instance identifier',
    flight_number           STRING          NOT NULL    COMMENT 'Airline flight number',
    airline                 STRING          NOT NULL    COMMENT 'IATA airline code',
    origin                  STRING          NOT NULL    COMMENT 'Origin airport IATA code',
    destination             STRING          NOT NULL    COMMENT 'Destination airport IATA code',
    aircraft_type           STRING          COMMENT 'Aircraft type code',
    tail_number             STRING          COMMENT 'Aircraft tail number',
    current_status          STRING          NOT NULL    COMMENT 'Latest flight status',
    current_gate            STRING          COMMENT 'Current gate assignment',
    scheduled_departure     TIMESTAMP       COMMENT 'Scheduled departure time',
    scheduled_arrival       TIMESTAMP       COMMENT 'Scheduled arrival time',
    latest_event_time       TIMESTAMP       NOT NULL    COMMENT 'Time of most recent event',
    delay_minutes           INT             DEFAULT 0   COMMENT 'Current cumulative delay',
    delay_reason            STRING          COMMENT 'Most recent delay reason',
    total_events            INT             DEFAULT 1   COMMENT 'Number of events received',
    is_active               BOOLEAN         DEFAULT TRUE COMMENT 'False once flight has arrived or been cancelled',
    flight_date             DATE            NOT NULL    COMMENT 'Flight date for partitioning',
    last_updated            TIMESTAMP       NOT NULL    COMMENT 'When this row was last updated'
)
USING DELTA
PARTITIONED BY (flight_date)
COMMENT 'Gold layer: real-time flight status board — latest state per flight'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


-- Real-time airport operations: aggregated view per airport
CREATE TABLE IF NOT EXISTS flight_delay.gold.realtime_airport_operations (
    airport_code            STRING          NOT NULL    COMMENT 'Airport IATA code',
    snapshot_time           TIMESTAMP       NOT NULL    COMMENT 'When this snapshot was computed',
    total_departures        INT             DEFAULT 0   COMMENT 'Total departures in window',
    total_arrivals          INT             DEFAULT 0   COMMENT 'Total arrivals in window',
    active_flights          INT             DEFAULT 0   COMMENT 'Flights currently en route',
    delayed_flights         INT             DEFAULT 0   COMMENT 'Flights currently delayed',
    cancelled_flights       INT             DEFAULT 0   COMMENT 'Flights cancelled in window',
    avg_delay_minutes       DOUBLE          DEFAULT 0.0 COMMENT 'Average delay of delayed flights',
    max_delay_minutes       INT             DEFAULT 0   COMMENT 'Maximum delay in window',
    current_weather         STRING          COMMENT 'Latest flight category at airport',
    congestion_level        STRING          COMMENT 'LOW / MODERATE / HIGH / CRITICAL',
    flight_date             DATE            NOT NULL    COMMENT 'Date for partitioning',
    last_updated            TIMESTAMP       NOT NULL    COMMENT 'When this row was last updated'
)
USING DELTA
PARTITIONED BY (flight_date)
COMMENT 'Gold layer: real-time airport operational dashboard metrics'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
