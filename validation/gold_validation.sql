-- Gold layer validation

-- Core row counts
SELECT 'on_time_performance_daily' AS table_name, COUNT(*) AS row_count
FROM airline_ops.gold.on_time_performance_daily
UNION ALL
SELECT 'route_delay_summary' AS table_name, COUNT(*) AS row_count
FROM airline_ops.gold.route_delay_summary
UNION ALL
SELECT 'airport_delay_summary' AS table_name, COUNT(*) AS row_count
FROM airline_ops.gold.airport_delay_summary
UNION ALL
SELECT 'cancellation_summary' AS table_name, COUNT(*) AS row_count
FROM airline_ops.gold.cancellation_summary
UNION ALL
SELECT 'aircraft_performance_summary' AS table_name, COUNT(*) AS row_count
FROM airline_ops.gold.aircraft_performance_summary;

-- Expected synthetic output shape
SELECT
    COUNT(*) AS otp_daily_count,
    CASE WHEN COUNT(*) = 240 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.gold.on_time_performance_daily;

SELECT
    COUNT(*) AS route_summary_count,
    CASE WHEN COUNT(*) = 720 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.gold.route_delay_summary;

SELECT
    COUNT(*) AS airport_summary_count,
    CASE WHEN COUNT(*) = 1320 THEN 'PASS' ELSE 'CHECK' END AS status
FROM airline_ops.gold.airport_delay_summary;

-- On-time percentage should be within 0-100
SELECT COUNT(*) AS invalid_on_time_percentage_count
FROM airline_ops.gold.on_time_performance_daily
WHERE on_time_arrival_percentage < 0 OR on_time_arrival_percentage > 100;

-- Cancellation consistency
SELECT
    (SELECT COALESCE(SUM(cancellation_count), 0) FROM airline_ops.gold.cancellation_summary) AS gold_cancellations,
    (
        SELECT COUNT(*)
        FROM airline_ops.silver.flight_operations_clean
        WHERE cancelled_flag = TRUE
    ) AS silver_cancelled_flights;

-- Route summary consistency
SELECT
    (SELECT COUNT(*) FROM airline_ops.gold.route_delay_summary) AS gold_route_rows,
    (
        SELECT COUNT(*)
        FROM (
            SELECT flight_date, route_code, origin_airport, dest_airport, reporting_airline
            FROM airline_ops.silver.flight_operations_clean
            GROUP BY flight_date, route_code, origin_airport, dest_airport, reporting_airline
        ) t
    ) AS expected_route_rows;

-- Airport summary consistency
SELECT
    (SELECT COUNT(*) FROM airline_ops.gold.airport_delay_summary) AS gold_airport_rows,
    (
        SELECT COUNT(*)
        FROM (
            SELECT flight_date, origin_airport AS airport_code, 'DEPARTURE' AS airport_role, reporting_airline
            FROM airline_ops.silver.flight_operations_clean
            GROUP BY flight_date, origin_airport, reporting_airline

            UNION ALL

            SELECT flight_date, dest_airport AS airport_code, 'ARRIVAL' AS airport_role, reporting_airline
            FROM airline_ops.silver.flight_operations_clean
            GROUP BY flight_date, dest_airport, reporting_airline
        ) t
    ) AS expected_airport_rows;

-- OTP sample view
SELECT *
FROM airline_ops.gold.on_time_performance_daily
ORDER BY flight_date, reporting_airline
LIMIT 20;
