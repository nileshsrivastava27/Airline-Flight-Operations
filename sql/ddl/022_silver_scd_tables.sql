-- ============================================================
-- Silver SCD Type 2 Tables
-- Slowly Changing Dimensions with full history
-- ============================================================

-- Aircraft reference with SCD Type 2 history
CREATE TABLE IF NOT EXISTS airline_ops.silver.aircraft_reference_scd2 (
    aircraft_scd_key        BIGINT          GENERATED ALWAYS AS IDENTITY,
    aircraft_code           STRING          NOT NULL    COMMENT 'Aircraft type code (business key)',
    aircraft_name           STRING          COMMENT 'Aircraft name/model',
    iata_code               STRING          COMMENT 'IATA aircraft type code',
    icao_code               STRING          COMMENT 'ICAO aircraft type code',
    manufacturer            STRING          COMMENT 'Aircraft manufacturer',
    capacity                INT             COMMENT 'Passenger capacity',
    range_nm                INT             COMMENT 'Range in nautical miles',
    status                  STRING          COMMENT 'Aircraft status: ACTIVE, MAINTENANCE, GROUNDED, RETIRED',
    -- SCD Type 2 tracking columns
    effective_from          TIMESTAMP       NOT NULL    COMMENT 'When this version became effective',
    effective_to            TIMESTAMP       COMMENT 'When this version was superseded (NULL = current)',
    is_current              BOOLEAN         NOT NULL    COMMENT 'True if this is the current active version',
    record_version          INT             NOT NULL    COMMENT 'Version number starting from 1',
    record_hash             STRING          COMMENT 'Hash of tracked columns for change detection',
    -- Audit columns
    created_timestamp       TIMESTAMP       NOT NULL    COMMENT 'When this record was created',
    updated_timestamp       TIMESTAMP       NOT NULL    COMMENT 'When this record was last updated'
)
USING DELTA
COMMENT 'Silver layer: aircraft reference with SCD Type 2 historical tracking'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);


-- Airports with SCD Type 2 history
CREATE TABLE IF NOT EXISTS airline_ops.silver.airports_scd2 (
    airport_scd_key         BIGINT          GENERATED ALWAYS AS IDENTITY,
    airport_code            STRING          NOT NULL    COMMENT 'Airport IATA code (business key)',
    airport_name            STRING          COMMENT 'Airport name',
    city                    STRING          COMMENT 'City',
    country                 STRING          COMMENT 'Country code',
    timezone                STRING          COMMENT 'IANA timezone',
    terminal_count          INT             COMMENT 'Number of terminals',
    status                  STRING          COMMENT 'Operational status',
    -- SCD Type 2 tracking columns
    effective_from          TIMESTAMP       NOT NULL    COMMENT 'When this version became effective',
    effective_to            TIMESTAMP       COMMENT 'When this version was superseded (NULL = current)',
    is_current              BOOLEAN         NOT NULL    COMMENT 'True if this is the current active version',
    record_version          INT             NOT NULL    COMMENT 'Version number starting from 1',
    record_hash             STRING          COMMENT 'Hash of tracked columns for change detection',
    -- Audit columns
    created_timestamp       TIMESTAMP       NOT NULL    COMMENT 'When this record was created',
    updated_timestamp       TIMESTAMP       NOT NULL    COMMENT 'When this record was last updated'
)
USING DELTA
COMMENT 'Silver layer: airports with SCD Type 2 historical tracking'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
