"""
CDC Ingestion Pipeline.

Reads Debezium-format CDC change events from Kafka topics or local JSONL
files and applies them to Bronze Delta tables using Delta MERGE. Handles
INSERT (op=c), UPDATE (op=u), and DELETE (op=d) operations with proper
ordering by operation timestamp.

Integrates with PipelineAuditLogger for consistent audit trail.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Literal, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

from delta.tables import DeltaTable
from databricks_pipeline_reliability import PipelineAuditLogger


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

DEBEZIUM_SOURCE_SCHEMA = StructType([
    StructField("version", StringType()),
    StructField("connector", StringType()),
    StructField("name", StringType()),
    StructField("ts_ms", LongType()),
    StructField("db", StringType()),
    StructField("schema", StringType()),
    StructField("table", StringType()),
    StructField("txId", StringType()),
    StructField("lsn", LongType()),
])

AIRCRAFT_RECORD_SCHEMA = StructType([
    StructField("aircraft_code", StringType()),
    StructField("aircraft_name", StringType()),
    StructField("iata_code", StringType()),
    StructField("icao_code", StringType()),
    StructField("manufacturer", StringType()),
    StructField("capacity", IntegerType()),
    StructField("range_nm", IntegerType()),
    StructField("status", StringType()),
])

AIRPORT_RECORD_SCHEMA = StructType([
    StructField("airport_code", StringType()),
    StructField("airport_name", StringType()),
    StructField("city", StringType()),
    StructField("country", StringType()),
    StructField("timezone", StringType()),
    StructField("terminal_count", IntegerType()),
    StructField("status", StringType()),
])


def _build_envelope_schema(record_schema: StructType) -> StructType:
    return StructType([
        StructField("schema", StringType()),
        StructField("payload", StructType([
            StructField("before", record_schema),
            StructField("after", record_schema),
            StructField("source", DEBEZIUM_SOURCE_SCHEMA),
            StructField("op", StringType()),
            StructField("ts_ms", LongType()),
        ])),
    ])


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class CDCSourceSpec:
    name: str
    source_table: str
    target_table: str
    key_column: str
    record_schema: StructType
    kafka_topic: str = ""
    file_path: str = ""


@dataclass
class CDCIngestionConfig:
    pipeline_name: str = "cdc_ingestion"
    pipeline_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    source_mode: Literal["kafka", "file"] = "file"
    kafka_bootstrap_servers: str = "localhost:9092"
    cdc_data_root: str = "data/cdc"


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

class CDCIngestionJob:

    def __init__(self, spark: SparkSession, config: CDCIngestionConfig | None = None):
        self._spark = spark
        self._config = config or CDCIngestionConfig()
        self._logger = PipelineAuditLogger(spark)

    def build_source_specs(self) -> list[CDCSourceSpec]:
        return [
            CDCSourceSpec(
                name="aircraft",
                source_table="aircraft_reference",
                target_table="airline_ops.bronze.aircraft_reference_raw",
                key_column="aircraft_code",
                record_schema=AIRCRAFT_RECORD_SCHEMA,
                kafka_topic="airline.cdc.aircraft",
                file_path=f"{self._config.cdc_data_root}/aircraft",
            ),
            CDCSourceSpec(
                name="airport",
                source_table="airports",
                target_table="airline_ops.bronze.airports_raw",
                key_column="airport_code",
                record_schema=AIRPORT_RECORD_SCHEMA,
                kafka_topic="airline.cdc.airport",
                file_path=f"{self._config.cdc_data_root}/airport",
            ),
        ]

    def process_all(self, specs: list[CDCSourceSpec] | None = None) -> list[dict]:
        specs = specs or self.build_source_specs()
        results = []
        for spec in specs:
            result = self.process_cdc_events(spec)
            results.append(result)
        return results

    def process_cdc_events(self, spec: CDCSourceSpec) -> dict:
        run_id = self._config.pipeline_run_id
        batch_id = f"cdc_{spec.name}_{run_id[:8]}"

        self._logger.log_event(
            pipeline_name=self._config.pipeline_name,
            stage_name=f"cdc_ingest_{spec.name}",
            target_table=spec.target_table,
            run_id=run_id,
            batch_id=batch_id,
            status="STARTED",
        )

        try:
            raw_df = self._read_cdc_events(spec)

            if raw_df.isEmpty():
                self._logger.log_event(
                    pipeline_name=self._config.pipeline_name,
                    stage_name=f"cdc_ingest_{spec.name}",
                    target_table=spec.target_table,
                    run_id=run_id,
                    batch_id=batch_id,
                    status="SUCCESS",
                    rows_written=0,
                    details={"message": "No CDC events to process"},
                )
                return {"table": spec.name, "status": "SUCCESS", "rows": 0}

            parsed_df = self._parse_debezium_events(raw_df, spec)
            deduped_df = self._deduplicate_by_key(parsed_df, spec)

            stats = self._apply_changes(deduped_df, spec, batch_id)

            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"cdc_ingest_{spec.name}",
                target_table=spec.target_table,
                run_id=run_id,
                batch_id=batch_id,
                status="SUCCESS",
                rows_written=stats["total"],
                details=stats,
            )

            return {"table": spec.name, "status": "SUCCESS", **stats}

        except Exception as exc:
            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"cdc_ingest_{spec.name}",
                target_table=spec.target_table,
                run_id=run_id,
                batch_id=batch_id,
                status="FAILED",
                error_message=str(exc),
            )
            return {"table": spec.name, "status": "FAILED", "error": str(exc)}

    def _read_cdc_events(self, spec: CDCSourceSpec) -> DataFrame:
        if self._config.source_mode == "kafka":
            return (
                self._spark
                .read
                .format("kafka")
                .option("kafka.bootstrap.servers",
                        self._config.kafka_bootstrap_servers)
                .option("subscribe", spec.kafka_topic)
                .option("startingOffsets", "earliest")
                .option("endingOffsets", "latest")
                .load()
                .selectExpr("CAST(value AS STRING) as json_value")
            )

        return (
            self._spark
            .read
            .text(spec.file_path)
            .withColumnRenamed("value", "json_value")
        )

    def _parse_debezium_events(
        self, raw_df: DataFrame, spec: CDCSourceSpec
    ) -> DataFrame:
        envelope_schema = _build_envelope_schema(spec.record_schema)

        parsed = (
            raw_df
            .withColumn("envelope",
                        F.from_json(F.col("json_value"), envelope_schema))
            .select(
                F.col("envelope.payload.op").alias("cdc_op"),
                F.col("envelope.payload.ts_ms").alias("cdc_ts_ms"),
                F.col("envelope.payload.before").alias("before"),
                F.col("envelope.payload.after").alias("after"),
                F.col("envelope.payload.source.table").alias("source_table"),
                F.col("envelope.payload.source.txId").alias("tx_id"),
            )
            .filter(F.col("cdc_op").isNotNull())
        )

        return parsed

    def _deduplicate_by_key(
        self, parsed_df: DataFrame, spec: CDCSourceSpec
    ) -> DataFrame:
        key_col = spec.key_column

        with_key = parsed_df.withColumn(
            "_merge_key",
            F.coalesce(
                F.col(f"after.{key_col}"),
                F.col(f"before.{key_col}"),
            )
        )

        from pyspark.sql.window import Window
        dedup_window = Window.partitionBy("_merge_key").orderBy(
            F.col("cdc_ts_ms").desc()
        )

        return (
            with_key
            .withColumn("_rank", F.row_number().over(dedup_window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )

    def _apply_changes(
        self, changes_df: DataFrame, spec: CDCSourceSpec, batch_id: str
    ) -> dict:
        inserts_df = changes_df.filter(F.col("cdc_op") == "c")
        updates_df = changes_df.filter(F.col("cdc_op") == "u")
        deletes_df = changes_df.filter(F.col("cdc_op") == "d")

        insert_count = inserts_df.count()
        update_count = updates_df.count()
        delete_count = deletes_df.count()

        upserts_df = inserts_df.unionByName(updates_df)

        if upserts_df.count() > 0:
            after_fields = [
                f.name for f in spec.record_schema.fields
            ]
            upsert_records = upserts_df.select(
                *[F.col(f"after.{f}").alias(f) for f in after_fields],
                F.col("cdc_ts_ms"),
            ).withColumn("cdc_batch_id", F.lit(batch_id)) \
             .withColumn("cdc_ingestion_timestamp", F.current_timestamp())

            if self._spark.catalog.tableExists(spec.target_table):
                delta_table = DeltaTable.forName(self._spark, spec.target_table)
                (
                    delta_table.alias("target")
                    .merge(
                        upsert_records.alias("source"),
                        f"target.{spec.key_column} = source.{spec.key_column}"
                    )
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                (
                    upsert_records
                    .write
                    .format("delta")
                    .mode("append")
                    .option("mergeSchema", "true")
                    .saveAsTable(spec.target_table)
                )

        if delete_count > 0:
            before_keys = deletes_df.select(
                F.col(f"before.{spec.key_column}").alias(spec.key_column)
            )

            if self._spark.catalog.tableExists(spec.target_table):
                delta_table = DeltaTable.forName(self._spark, spec.target_table)
                (
                    delta_table.alias("target")
                    .merge(
                        before_keys.alias("deletes"),
                        f"target.{spec.key_column} = deletes.{spec.key_column}"
                    )
                    .whenMatchedDelete()
                    .execute()
                )

        return {
            "inserts": insert_count,
            "updates": update_count,
            "deletes": delete_count,
            "total": insert_count + update_count + delete_count,
        }


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_job(spark: SparkSession, **kwargs) -> CDCIngestionJob:
    config = CDCIngestionConfig(**kwargs)
    return CDCIngestionJob(spark, config)
