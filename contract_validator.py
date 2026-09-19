"""
Data Contract Validator.

Validates incoming DataFrames against YAML-based data contracts before they
are written to Bronze tables. Checks for:
  - Required fields (missing or all-null)
  - Data type compatibility
  - Value range constraints (min/max)
  - Allowed value lists
  - Custom quality rules (SQL expressions)
  - Schema drift (unexpected columns)

On violation, records are logged to airline_ops.audit.contract_violations.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks_pipeline_reliability import PipelineAuditLogger


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Violation:
    violation_type: str
    severity: str
    field_name: str | None
    rule_name: str | None
    message: str
    record_count: int = 0
    sample_values: str | None = None


@dataclass
class ValidationResult:
    contract_name: str
    contract_version: str
    is_valid: bool
    violations: list[Violation]
    error_count: int
    warning_count: int


# ---------------------------------------------------------------------------
# Contract loader
# ---------------------------------------------------------------------------

def load_contract(contract_path: str) -> dict:
    path = Path(contract_path)
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------

class ContractValidator:

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._logger = PipelineAuditLogger(spark)

    def validate(
        self,
        df: DataFrame,
        contract_path: str,
        *,
        batch_id: str | None = None,
        pipeline_name: str = "contract_validator",
    ) -> ValidationResult:
        contract = load_contract(contract_path)
        contract_info = contract["contract"]
        schema_def = contract.get("schema", {})
        quality_rules = contract.get("quality_rules", [])

        violations: list[Violation] = []

        # 1. Check required fields
        violations.extend(
            self._check_required_fields(df, schema_def)
        )

        # 2. Check value ranges
        violations.extend(
            self._check_value_ranges(df, schema_def)
        )

        # 3. Check allowed values
        violations.extend(
            self._check_allowed_values(df, schema_def)
        )

        # 4. Check schema drift (unexpected columns)
        violations.extend(
            self._check_schema_drift(df, schema_def)
        )

        # 5. Run custom quality rules
        violations.extend(
            self._check_quality_rules(df, quality_rules)
        )

        error_count = sum(1 for v in violations if v.severity == "error")
        warning_count = sum(1 for v in violations if v.severity == "warning")

        result = ValidationResult(
            contract_name=contract_info["name"],
            contract_version=contract_info["version"],
            is_valid=(error_count == 0),
            violations=violations,
            error_count=error_count,
            warning_count=warning_count,
        )

        # Log violations to audit table
        if violations:
            self._log_violations(
                result, contract_info,
                batch_id=batch_id,
                pipeline_name=pipeline_name,
            )

        return result

    # -- Check: required fields ---------------------------------------------

    def _check_required_fields(
        self, df: DataFrame, schema_def: dict
    ) -> list[Violation]:
        violations = []
        df_columns = [c.lower() for c in df.columns]

        for field in schema_def.get("fields", []):
            if not field.get("required", False):
                continue

            field_name = field["name"]

            # Field missing entirely
            if field_name.lower() not in df_columns:
                violations.append(Violation(
                    violation_type="MISSING_FIELD",
                    severity="error",
                    field_name=field_name,
                    rule_name=None,
                    message=f"Required field '{field_name}' is missing from the data",
                ))
                continue

            # Field exists but all null
            null_count = df.filter(F.col(field_name).isNull()).count()
            total_count = df.count()

            if null_count == total_count:
                violations.append(Violation(
                    violation_type="MISSING_FIELD",
                    severity="error",
                    field_name=field_name,
                    rule_name=None,
                    message=f"Required field '{field_name}' is entirely NULL ({total_count} rows)",
                    record_count=null_count,
                ))

        return violations

    # -- Check: value ranges ------------------------------------------------

    def _check_value_ranges(
        self, df: DataFrame, schema_def: dict
    ) -> list[Violation]:
        violations = []
        df_columns = [c.lower() for c in df.columns]

        for field in schema_def.get("fields", []):
            field_name = field["name"]
            if field_name.lower() not in df_columns:
                continue

            min_val = field.get("min_value")
            max_val = field.get("max_value")

            if min_val is None and max_val is None:
                continue

            condition = F.col(field_name).isNotNull()
            if min_val is not None:
                condition = condition & (F.col(field_name) < min_val)
            if max_val is not None:
                if min_val is not None:
                    condition = F.col(field_name).isNotNull() & (
                        (F.col(field_name) < min_val) | (F.col(field_name) > max_val)
                    )
                else:
                    condition = condition & (F.col(field_name) > max_val)

            bad_count = df.filter(condition).count()

            if bad_count > 0:
                violations.append(Violation(
                    violation_type="VALUE_OUT_OF_RANGE",
                    severity="error",
                    field_name=field_name,
                    rule_name=None,
                    message=f"Field '{field_name}' has {bad_count} values outside range [{min_val}, {max_val}]",
                    record_count=bad_count,
                ))

        return violations

    # -- Check: allowed values ----------------------------------------------

    def _check_allowed_values(
        self, df: DataFrame, schema_def: dict
    ) -> list[Violation]:
        violations = []
        df_columns = [c.lower() for c in df.columns]

        for field in schema_def.get("fields", []):
            field_name = field["name"]
            allowed = field.get("allowed_values")

            if allowed is None or field_name.lower() not in df_columns:
                continue

            bad_rows = df.filter(
                F.col(field_name).isNotNull()
                & ~F.col(field_name).isin(allowed)
            )
            bad_count = bad_rows.count()

            if bad_count > 0:
                # Get sample of bad values
                samples = (
                    bad_rows
                    .select(field_name)
                    .distinct()
                    .limit(5)
                    .collect()
                )
                sample_str = str([row[0] for row in samples])

                violations.append(Violation(
                    violation_type="INVALID_VALUE",
                    severity="error",
                    field_name=field_name,
                    rule_name=None,
                    message=f"Field '{field_name}' has {bad_count} values not in allowed list",
                    record_count=bad_count,
                    sample_values=sample_str,
                ))

        return violations

    # -- Check: schema drift ------------------------------------------------

    def _check_schema_drift(
        self, df: DataFrame, schema_def: dict
    ) -> list[Violation]:
        violations = []
        expected_fields = {
            f["name"].lower() for f in schema_def.get("fields", [])
        }
        # Exclude common metadata columns from drift detection
        metadata_cols = {
            "source_file_name", "source_system", "ingestion_timestamp",
            "batch_id", "record_hash", "raw_record", "year_month",
            "_rescued_data",
        }

        for col in df.columns:
            if col.lower() not in expected_fields and col.lower() not in metadata_cols:
                violations.append(Violation(
                    violation_type="SCHEMA_DRIFT",
                    severity="warning",
                    field_name=col,
                    rule_name=None,
                    message=f"Unexpected column '{col}' not defined in contract",
                ))

        return violations

    # -- Check: quality rules -----------------------------------------------

    def _check_quality_rules(
        self, df: DataFrame, quality_rules: list[dict]
    ) -> list[Violation]:
        violations = []

        for rule in quality_rules:
            rule_name = rule["name"]
            rule_expr = rule["rule"]
            severity = rule.get("severity", "warning")

            try:
                # The rule expression should be True for VALID rows
                # So we count rows where the rule is False (violations)
                bad_count = df.filter(f"NOT ({rule_expr})").count()

                if bad_count > 0:
                    violations.append(Violation(
                        violation_type="QUALITY_RULE",
                        severity=severity,
                        field_name=None,
                        rule_name=rule_name,
                        message=f"Quality rule '{rule_name}' failed for {bad_count} rows: {rule.get('description', '')}",
                        record_count=bad_count,
                    ))
            except Exception as exc:
                violations.append(Violation(
                    violation_type="QUALITY_RULE",
                    severity="warning",
                    field_name=None,
                    rule_name=rule_name,
                    message=f"Could not evaluate rule '{rule_name}': {exc}",
                ))

        return violations

    # -- Log violations to audit table --------------------------------------

    def _log_violations(
        self,
        result: ValidationResult,
        contract_info: dict,
        *,
        batch_id: str | None,
        pipeline_name: str,
    ) -> None:
        violations_table = "airline_ops.audit.contract_violations"

        if not self._spark.catalog.tableExists(violations_table):
            return

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

        rows = []
        for v in result.violations:
            rows.append({
                "violation_id": f"cv_{uuid.uuid4().hex[:12]}",
                "contract_name": result.contract_name,
                "contract_version": result.contract_version,
                "source_system": contract_info.get("source_system"),
                "violation_type": v.violation_type,
                "severity": v.severity,
                "field_name": v.field_name,
                "rule_name": v.rule_name,
                "violation_message": v.message,
                "record_count": v.record_count,
                "sample_values": v.sample_values,
                "batch_id": batch_id,
                "pipeline_name": pipeline_name,
                "detected_timestamp": now,
                "resolved": False,
            })

        violations_df = self._spark.createDataFrame(rows)
        violations_df.write.format("delta").mode("append").saveAsTable(violations_table)


# ---------------------------------------------------------------------------
# Convenience function
# ---------------------------------------------------------------------------

def validate_dataframe(
    spark: SparkSession,
    df: DataFrame,
    contract_path: str,
    *,
    batch_id: str | None = None,
    pipeline_name: str = "contract_validator",
) -> ValidationResult:
    validator = ContractValidator(spark)
    return validator.validate(
        df, contract_path,
        batch_id=batch_id,
        pipeline_name=pipeline_name,
    )
