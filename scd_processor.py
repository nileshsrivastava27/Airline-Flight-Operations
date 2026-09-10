"""
SCD Type 2 Processor.

Implements Slowly Changing Dimension Type 2 processing for airline reference
data (aircraft and airports). When a record changes, the current version is
closed (effective_to set, is_current=False) and a new version is inserted.

Uses Delta Lake MERGE with hash-based change detection to determine whether
an incoming record represents a genuine change or a duplicate.

Integrates with PipelineAuditLogger for audit trail.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks_pipeline_reliability import (
    PipelineAuditLogger,
    get_current_table_version,
    restore_table_version,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class SCDTableSpec:
    name: str
    source_table: str
    target_table: str
    business_key: str
    tracked_columns: list[str]
    all_columns: list[str]


@dataclass
class SCDProcessorConfig:
    pipeline_name: str = "scd_type2_processor"
    pipeline_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))


AIRCRAFT_SCD_SPEC = SCDTableSpec(
    name="aircraft_reference",
    source_table="airline_ops.bronze.aircraft_reference_raw",
    target_table="airline_ops.silver.aircraft_reference_scd2",
    business_key="aircraft_code",
    tracked_columns=[
        "aircraft_name", "iata_code", "icao_code",
        "manufacturer", "capacity", "range_nm", "status",
    ],
    all_columns=[
        "aircraft_code", "aircraft_name", "iata_code", "icao_code",
        "manufacturer", "capacity", "range_nm", "status",
    ],
)

AIRPORTS_SCD_SPEC = SCDTableSpec(
    name="airports",
    source_table="airline_ops.bronze.airports_raw",
    target_table="airline_ops.silver.airports_scd2",
    business_key="airport_code",
    tracked_columns=[
        "airport_name", "city", "country", "timezone",
        "terminal_count", "status",
    ],
    all_columns=[
        "airport_code", "airport_name", "city", "country",
        "timezone", "terminal_count", "status",
    ],
)


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

class SCDProcessorJob:

    def __init__(self, spark: SparkSession, config: SCDProcessorConfig | None = None):
        self._spark = spark
        self._config = config or SCDProcessorConfig()
        self._logger = PipelineAuditLogger(spark)

    def run_all(self) -> list[dict]:
        results = []
        for spec in [AIRCRAFT_SCD_SPEC, AIRPORTS_SCD_SPEC]:
            result = self.process_scd(spec)
            results.append(result)
        return results

    def process_scd(self, spec: SCDTableSpec) -> dict:
        run_id = self._config.pipeline_run_id
        batch_id = f"scd2_{spec.name}_{run_id[:8]}"

        self._logger.log_event(
            pipeline_name=self._config.pipeline_name,
            stage_name=f"scd2_{spec.name}",
            target_table=spec.target_table,
            run_id=run_id,
            batch_id=batch_id,
            status="STARTED",
        )

        previous_version = get_current_table_version(self._spark, spec.target_table)

        try:
            source_df = self._read_source(spec)
            if source_df.isEmpty():
                self._logger.log_event(
                    pipeline_name=self._config.pipeline_name,
                    stage_name=f"scd2_{spec.name}",
                    target_table=spec.target_table,
                    run_id=run_id,
                    batch_id=batch_id,
                    status="SUCCESS",
                    rows_written=0,
                    details={"message": "No source records"},
                )
                return {"table": spec.name, "status": "SUCCESS", "rows": 0}

            incoming = self._prepare_incoming(source_df, spec)

            if not self._spark.catalog.tableExists(spec.target_table):
                return self._initial_load(incoming, spec, batch_id)

            stats = self._merge_scd2(incoming, spec, batch_id)

            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"scd2_{spec.name}",
                target_table=spec.target_table,
                run_id=run_id,
                batch_id=batch_id,
                status="SUCCESS",
                rows_written=stats.get("new_versions", 0),
                details=stats,
            )

            return {"table": spec.name, "status": "SUCCESS", **stats}

        except Exception as exc:
            if previous_version is not None:
                restore_table_version(self._spark, spec.target_table, previous_version)
            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"scd2_{spec.name}",
                target_table=spec.target_table,
                run_id=run_id,
                batch_id=batch_id,
                status="FAILED",
                error_message=str(exc),
                restored_version=previous_version,
            )
            return {"table": spec.name, "status": "FAILED", "error": str(exc)}

    def _read_source(self, spec: SCDTableSpec) -> DataFrame:
        return self._spark.table(spec.source_table)

    def _compute_hash(self, df: DataFrame, columns: list[str]) -> DataFrame:
        concat_expr = F.concat_ws(
            "||",
            *[F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in columns]
        )
        return df.withColumn("record_hash", F.sha2(concat_expr, 256))

    def _prepare_incoming(self, df: DataFrame, spec: SCDTableSpec) -> DataFrame:
        selected = df.select(*[
            F.col(c) for c in spec.all_columns if c in df.columns
        ])

        from pyspark.sql.window import Window
        dedup_window = Window.partitionBy(spec.business_key).orderBy(
            F.coalesce(F.col("ingestion_timestamp"), F.current_timestamp()).desc()
        ) if "ingestion_timestamp" in df.columns else Window.partitionBy(
            spec.business_key
        ).orderBy(F.lit(1))

        deduped = (
            selected
            .withColumn("_rn", F.row_number().over(dedup_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )

        return self._compute_hash(deduped, spec.tracked_columns)

    def _initial_load(self, incoming: DataFrame, spec: SCDTableSpec, batch_id: str) -> dict:
        initial = (
            incoming
            .withColumn("effective_from", F.current_timestamp())
            .withColumn("effective_to", F.lit(None).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
            .withColumn("record_version", F.lit(1))
            .withColumn("created_timestamp", F.current_timestamp())
            .withColumn("updated_timestamp", F.current_timestamp())
        )

        row_count = initial.count()

        (
            initial
            .write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(spec.target_table)
        )

        self._logger.log_event(
            pipeline_name=self._config.pipeline_name,
            stage_name=f"scd2_{spec.name}_initial",
            target_table=spec.target_table,
            run_id=self._config.pipeline_run_id,
            batch_id=batch_id,
            status="SUCCESS",
            rows_written=row_count,
            details={"type": "initial_load"},
        )

        return {"table": spec.name, "status": "SUCCESS", "initial_load": row_count}

    def _merge_scd2(self, incoming: DataFrame, spec: SCDTableSpec, batch_id: str) -> dict:
        target = DeltaTable.forName(self._spark, spec.target_table)

        current_records = (
            self._spark.table(spec.target_table)
            .filter(F.col("is_current") == True)
        )

        changed = (
            incoming.alias("inc")
            .join(
                current_records.alias("cur"),
                F.col(f"inc.{spec.business_key}") == F.col(f"cur.{spec.business_key}"),
                "inner",
            )
            .filter(F.col("inc.record_hash") != F.col("cur.record_hash"))
            .select(
                *[F.col(f"inc.{c}") for c in spec.all_columns],
                F.col("inc.record_hash"),
                F.col("cur.record_version").alias("prev_version"),
            )
        )

        new_records = (
            incoming.alias("inc")
            .join(
                current_records.alias("cur"),
                F.col(f"inc.{spec.business_key}") == F.col(f"cur.{spec.business_key}"),
                "left_anti",
            )
        )

        changed_count = changed.count()
        new_count = new_records.count()

        if changed_count > 0:
            close_keys = changed.select(
                F.col(spec.business_key),
                F.current_timestamp().alias("_close_ts"),
            )

            (
                target.alias("target")
                .merge(
                    close_keys.alias("closing"),
                    f"target.{spec.business_key} = closing.{spec.business_key} "
                    f"AND target.is_current = true"
                )
                .whenMatchedUpdate(set={
                    "effective_to": "closing._close_ts",
                    "is_current": F.lit(False),
                    "updated_timestamp": F.current_timestamp(),
                })
                .execute()
            )

            new_versions = (
                changed
                .withColumn("effective_from", F.current_timestamp())
                .withColumn("effective_to", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .withColumn("record_version", F.col("prev_version") + 1)
                .withColumn("created_timestamp", F.current_timestamp())
                .withColumn("updated_timestamp", F.current_timestamp())
                .drop("prev_version")
            )

            (
                new_versions
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(spec.target_table)
            )

        if new_count > 0:
            first_versions = (
                new_records
                .withColumn("effective_from", F.current_timestamp())
                .withColumn("effective_to", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .withColumn("record_version", F.lit(1))
                .withColumn("created_timestamp", F.current_timestamp())
                .withColumn("updated_timestamp", F.current_timestamp())
            )

            (
                first_versions
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(spec.target_table)
            )

        return {
            "changed_records": changed_count,
            "new_records": new_count,
            "new_versions": changed_count + new_count,
            "unchanged": incoming.count() - changed_count - new_count,
        }


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_job(spark: SparkSession, **kwargs) -> SCDProcessorJob:
    config = SCDProcessorConfig(**kwargs)
    return SCDProcessorJob(spark, config)
