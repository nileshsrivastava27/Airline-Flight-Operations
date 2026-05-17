CREATE SCHEMA IF NOT EXISTS airline_ops.audit;

CREATE TABLE IF NOT EXISTS airline_ops.audit.pipeline_run_log (
    event_timestamp TIMESTAMP,
    pipeline_name STRING,
    stage_name STRING,
    target_table STRING,
    run_id STRING,
    batch_id STRING,
    status STRING,
    rows_written BIGINT,
    rows_removed BIGINT,
    restored_version BIGINT,
    error_message STRING,
    details STRING
)
USING DELTA;
