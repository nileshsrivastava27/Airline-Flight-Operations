-- ============================================================
-- Data Contract Violation Tables
-- Tracks schema and quality rule violations
-- ============================================================

CREATE TABLE IF NOT EXISTS airline_ops.audit.contract_violations (
    violation_id            STRING          NOT NULL    COMMENT 'Unique violation identifier',
    contract_name           STRING          NOT NULL    COMMENT 'Name of the data contract',
    contract_version        STRING          NOT NULL    COMMENT 'Version of the contract',
    source_system           STRING          COMMENT 'Source system that produced the data',
    violation_type          STRING          NOT NULL    COMMENT 'MISSING_FIELD, TYPE_MISMATCH, VALUE_OUT_OF_RANGE, INVALID_VALUE, QUALITY_RULE, SCHEMA_DRIFT',
    severity                STRING          NOT NULL    COMMENT 'error or warning',
    field_name              STRING          COMMENT 'Field that violated the contract',
    rule_name               STRING          COMMENT 'Quality rule that was violated',
    violation_message       STRING          NOT NULL    COMMENT 'Human-readable description',
    record_count            INT             COMMENT 'Number of records affected',
    sample_values           STRING          COMMENT 'Sample of violating values (JSON)',
    batch_id                STRING          COMMENT 'Batch or micro-batch identifier',
    pipeline_name           STRING          COMMENT 'Pipeline that detected the violation',
    detected_timestamp      TIMESTAMP       NOT NULL    COMMENT 'When the violation was detected',
    resolved                BOOLEAN         DEFAULT FALSE COMMENT 'Whether the violation has been addressed'
)
USING DELTA
COMMENT 'Audit layer: data contract violation log'
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact'   = 'true'
);
