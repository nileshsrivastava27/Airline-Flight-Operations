-- Silver layer validation

-- Core row counts
SELECT 'flight_operations_clean' AS table_name, COUNT(*) AS row_count
FROM airline_ops.silver.flight_operations_clean
UNION ALL
SELECT 'flight_operations_quarantine' AS table_name, COUNT(*) AS row_count
FROM airline_ops.silver.flight_operations_quarantine
UNION ALL
SELECT 'airports_clean' AS table_name, COUNT(*) AS row_count
FROM airline_ops.silver.airports_clean
UNION ALL
SELECT 'aircraft_reference_clean' AS table_name, COUNT(*) AS row_count
FROM airline_ops.silver.aircraft_reference_clean
UNION ALL
SELECT 'route_reference' AS table_name, COUNT(*) AS row_count
FROM airline_ops.silver.route_reference
UNION ALL
SELECT 'weather_observations_clean' AS table_name, COUNT(*) AS row_count
FROM airline_ops.silver.weather_observations_clean;

-- Expected clean/quarantine split for current synthetic data
SELECT
    COUNT(*) AS clean_count,
    CASE WHEN COUNT(*) = 720 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.silver.flight_operations_clean;

SELECT
    COUNT(*) AS quarantine_count,
    CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.silver.flight_operations_quarantine;

SELECT
    COUNT(*) AS route_count,
    CASE WHEN COUNT(*) = 12 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.silver.route_reference;

-- Reconciliation: clean + quarantine should equal deduplicated source expectation
SELECT
    (SELECT COUNT(*) FROM airline_ops.silver.flight_operations_clean) AS clean_count,
    (SELECT COUNT(*) FROM airline_ops.silver.flight_operations_quarantine) AS quarantine_count,
    (
        (SELECT COUNT(*) FROM airline_ops.silver.flight_operations_clean) +
        (SELECT COUNT(*) FROM airline_ops.silver.flight_operations_quarantine)
    ) AS total_processed_count;

-- Timestamp sanity
SELECT COUNT(*) AS invalid_scheduled_sequence_count
FROM airline_ops.silver.flight_operations_clean
WHERE scheduled_arrival_ts < scheduled_departure_ts;

SELECT COUNT(*) AS invalid_actual_sequence_count
FROM airline_ops.silver.flight_operations_clean
WHERE actual_arrival_ts < actual_departure_ts;

-- Route code consistency
SELECT COUNT(*) AS invalid_route_code_count
FROM airline_ops.silver.flight_operations_clean
WHERE route_code <> CONCAT(origin_airport, '-', dest_airport);

-- Airport coverage check
SELECT COUNT(*) AS unmatched_origin_airports
FROM airline_ops.silver.flight_operations_clean f
LEFT JOIN airline_ops.silver.airports_clean a
    ON f.origin_airport = a.airport_iata_code
WHERE a.airport_iata_code IS NULL;

SELECT COUNT(*) AS unmatched_dest_airports
FROM airline_ops.silver.flight_operations_clean f
LEFT JOIN airline_ops.silver.airports_clean a
    ON f.dest_airport = a.airport_iata_code
WHERE a.airport_iata_code IS NULL;
