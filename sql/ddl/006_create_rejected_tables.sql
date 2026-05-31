-- 006_create_rejected_tables.sql
-- Rejected (rescued) record tables for the Bronze layer.
--
-- Rows whose values do not fit the expected schema are captured via the
-- Databricks `rescuedDataColumn` during ingestion and landed here instead of
-- being silently dropped or failing the load. Run this after the core Bronze
-- DDL and before running Bronze ingestion.

CREATE SCHEMA IF NOT EXISTS airline_ops.bronze;

-- Common shape for every rejected-records table:
--   source_table         : the Bronze target the row was meant for
--   source_system        : logical source system label
--   source_file_name     : originating file name
--   rescued_data         : JSON of the values that did not match the schema
--   raw_payload          : JSON snapshot of the full incoming row
--   batch_id             : ingestion batch identifier
--   rejection_timestamp  : when the row was rejected

CREATE TABLE IF NOT EXISTS airline_ops.bronze.flight_operations_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS airline_ops.bronze.airports_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS airline_ops.bronze.aircraft_reference_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS airline_ops.bronze.weather_metar_rejected (
    source_table        STRING,
    source_system       STRING,
    source_file_name    STRING,
    rescued_data        STRING,
    raw_payload         STRING,
    batch_id            STRING,
    rejection_timestamp TIMESTAMP
) USING DELTA;
