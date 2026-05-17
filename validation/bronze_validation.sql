-- Bronze layer validation

-- Core row counts
SELECT 'flight_operations_raw' AS table_name, COUNT(*) AS row_count
FROM airline_ops.bronze.flight_operations_raw
UNION ALL
SELECT 'airports_raw' AS table_name, COUNT(*) AS row_count
FROM airline_ops.bronze.airports_raw
UNION ALL
SELECT 'aircraft_reference_raw' AS table_name, COUNT(*) AS row_count
FROM airline_ops.bronze.aircraft_reference_raw
UNION ALL
SELECT 'weather_metar_raw' AS table_name, COUNT(*) AS row_count
FROM airline_ops.bronze.weather_metar_raw;

-- Expected synthetic source volumes
SELECT
    COUNT(*) AS flight_count,
    CASE WHEN COUNT(*) = 720 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.bronze.flight_operations_raw;

SELECT
    COUNT(*) AS airport_count,
    CASE WHEN COUNT(*) = 8 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.bronze.airports_raw;

SELECT
    COUNT(*) AS aircraft_count,
    CASE WHEN COUNT(*) = 4 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.bronze.aircraft_reference_raw;

-- Weather count may vary if the generator changes, so keep this informational
SELECT COUNT(*) AS weather_count
FROM airline_ops.bronze.weather_metar_raw;

-- Null checks on key identifiers
SELECT COUNT(*) AS missing_flight_number_count
FROM airline_ops.bronze.flight_operations_raw
WHERE flight_number IS NULL OR TRIM(flight_number) = '';

SELECT COUNT(*) AS missing_origin_airport_count
FROM airline_ops.bronze.flight_operations_raw
WHERE origin_airport IS NULL OR TRIM(origin_airport) = '';

SELECT COUNT(*) AS missing_dest_airport_count
FROM airline_ops.bronze.flight_operations_raw
WHERE dest_airport IS NULL OR TRIM(dest_airport) = '';

-- Duplicate check on likely business key
SELECT
    flight_date,
    reporting_airline,
    flight_number,
    origin_airport,
    dest_airport,
    COUNT(*) AS duplicate_count
FROM airline_ops.bronze.flight_operations_raw
GROUP BY flight_date, reporting_airline, flight_number, origin_airport, dest_airport
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;
