"""
Streaming Ingestion Pipeline.

Reads flight events and weather updates from Kafka or Auto Loader (JSONL
fallback) and lands them in Bronze Delta tables. Uses foreachBatch to integrate
with the existing PipelineAuditLogger for consistent audit logging.

Supports two source modes:
    - "kafka"      : reads from Kafka topics using Spark's Kafka connector
    - "autoloader" : reads from JSONL directories using Databricks Auto Loader
                     (cloudFiles) or Spark's built-in file streaming
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from databricks_pipeline_reliability import PipelineAuditLogger

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

FLIGHT_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("flight_id", StringType()),
    StructField("flight_number", StringType()),
    StructField("airline", StringType()),
    StructField("event_type", StringType()),
    StructField("airport_code", StringType()),
    StructField("gate", StringType()),
    StructField("origin", StringType()),
    StructField("destination", StringType()),
    StructField("aircraft_type", StringType()),
    StructField("tail_number", StringType()),
    StructField("event_time", StringType()),
    StructField("scheduled_departure", StringType()),
    StructField("scheduled_arrival", StringType()),
    StructField("status", StringType()),
    StructField("delay_minutes", IntegerType()),
    StructField("reason", StringType()),
    StructField("metadata", StringType()),
])

WEATHER_UPDATE_SCHEMA = StructType([
    StructField("observation_id", StringType()),
    StructField("station_id", StringType()),
    StructField("observation_time", StringType()),
    StructField("observation_date", StringType()),
    StructField("temp_c", DoubleType()),
    StructField("dewpoint_c", DoubleType()),
    StructField("wind_dir_degrees", IntegerType()),
    StructField("wind_speed_kt", IntegerType()),
    StructField("wind_gust_kt", IntegerType()),
    StructField("visibility_statute_mi", DoubleType()),
    StructField("altim_in_hg", DoubleType()),
    StructField("flight_category", StringType()),
    StructField("sky_cover", StringType()),
    StructField("precip_in", DoubleType()),
    StructField("weather_profile", StringType()),
])


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SourceMode = Literal["kafka", "autoloader"]

@dataclass
class StreamingSourceSpec:
    name: str
    source_mode: SourceMode
    target_table: str
    schema: StructType
    kafka_topic: str = ""
    kafka_bootstrap_servers: str = "localhost:9092"
    file_path: str = ""
    file_format: str = "json"
    checkpoint_location: str = ""
    watermark_column: str = "event_time"
    watermark_delay: str = "10 minutes"
    trigger_interval: str = "30 seconds"
    max_files_per_trigger: int = 10
    starting_offsets: str = "latest"


@dataclass
class StreamingIngestionConfig:
    pipeline_name: str = "streaming_ingestion"
    pipeline_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    checkpoint_root: str = "/tmp/checkpoints/streaming_ingestion"
    kafka_bootstrap_servers: str = "localhost:9092"
    source_mode: SourceMode = "autoloader"
    flight_events_path: str = "data/streaming/flight_events"
    weather_updates_path: str = "data/streaming/weather_updates"
    flight_events_topic: str = "airline.flight_events"
    weather_updates_topic: str = "airline.weather_updates"
    trigger_interval: str = "30 seconds"


# ---------------------------------------------------------------------------
# Streaming ingestion job
# ---------------------------------------------------------------------------

class StreamingIngestionJob:

    def __init__(self, spark: SparkSession, config: StreamingIngestionConfig | None = None):
        self._spark = spark
        self._config = config or StreamingIngestionConfig()
        self._logger = PipelineAuditLogger(spark)
        self._queries = []

    def build_source_specs(self) -> list[StreamingSourceSpec]:
        mode = self._config.source_mode
        return [
            StreamingSourceSpec(
                name="flight_events",
                source_mode=mode,
                target_table="flight_delay.bronze.flight_events_raw",
                schema=FLIGHT_EVENT_SCHEMA,
                kafka_topic=self._config.flight_events_topic,
                kafka_bootstrap_servers=self._config.kafka_bootstrap_servers,
                file_path=self._config.flight_events_path,
                checkpoint_location=f"{self._config.checkpoint_root}/flight_events",
                watermark_column="event_time",
                watermark_delay="10 minutes",
                trigger_interval=self._config.trigger_interval,
            ),
            StreamingSourceSpec(
                name="weather_updates",
                source_mode=mode,
                target_table="flight_delay.bronze.weather_updates_raw",
                schema=WEATHER_UPDATE_SCHEMA,
                kafka_topic=self._config.weather_updates_topic,
                kafka_bootstrap_servers=self._config.kafka_bootstrap_servers,
                file_path=self._config.weather_updates_path,
                checkpoint_location=f"{self._config.checkpoint_root}/weather_updates",
                watermark_column="observation_time",
                watermark_delay="15 minutes",
                trigger_interval=self._config.trigger_interval,
            ),
        ]

    def start_all(self, specs: list[StreamingSourceSpec] | None = None) -> list:
        specs = specs or self.build_source_specs()
        for spec in specs:
            query = self.start_stream(spec)
            self._queries.append(query)
        return self._queries

    def start_stream(self, spec: StreamingSourceSpec):
        raw_stream = self._read_source(spec)

        parsed_stream = self._parse_and_enrich(raw_stream, spec)

        watermarked = parsed_stream.withWatermark(
            spec.watermark_column, spec.watermark_delay
        )

        def foreach_batch_fn(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            self._write_micro_batch(batch_df, batch_id, spec)

        query = (
            watermarked
            .writeStream
            .foreachBatch(foreach_batch_fn)
            .option("checkpointLocation", spec.checkpoint_location)
            .trigger(processingTime=spec.trigger_interval)
            .queryName(f"streaming_{spec.name}")
            .start()
        )

        self._logger.log_event(
            pipeline_name=self._config.pipeline_name,
            stage_name=f"start_stream_{spec.name}",
            target_table=spec.target_table,
            run_id=self._config.pipeline_run_id,
            batch_id="streaming",
            status="STARTED",
            details=f"Source: {spec.source_mode}, trigger: {spec.trigger_interval}",
        )

        return query

    def _read_source(self, spec: StreamingSourceSpec) -> DataFrame:
        if spec.source_mode == "kafka":
            return self._read_kafka(spec)
        return self._read_file_stream(spec)

    def _read_kafka(self, spec: StreamingSourceSpec) -> DataFrame:
        return (
            self._spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", spec.kafka_bootstrap_servers)
            .option("subscribe", spec.kafka_topic)
            .option("startingOffsets", spec.starting_offsets)
            .option("failOnDataLoss", "false")
            .load()
        )

    def _read_file_stream(self, spec: StreamingSourceSpec) -> DataFrame:
        try:
            return (
                self._spark
                .readStream
                .format("cloudFiles")
                .option("cloudFiles.format", spec.file_format)
                .option("cloudFiles.maxFilesPerTrigger", spec.max_files_per_trigger)
                .option("cloudFiles.schemaLocation", f"{spec.checkpoint_location}/schema")
                .schema(spec.schema)
                .load(spec.file_path)
            )
        except Exception:
            return (
                self._spark
                .readStream
                .format(spec.file_format)
                .schema(spec.schema)
                .option("maxFilesPerTrigger", spec.max_files_per_trigger)
                .load(spec.file_path)
            )

    def _parse_and_enrich(self, stream_df: DataFrame, spec: StreamingSourceSpec) -> DataFrame:
        if spec.source_mode == "kafka":
            parsed = (
                stream_df
                .selectExpr("CAST(value AS STRING) as json_value",
                            "topic as kafka_topic",
                            "partition as kafka_partition",
                            "offset as kafka_offset")
                .select(
                    F.from_json(F.col("json_value"), spec.schema).alias("data"),
                    F.col("kafka_topic"),
                    F.col("kafka_partition"),
                    F.col("kafka_offset"),
                )
                .select("data.*", "kafka_topic", "kafka_partition", "kafka_offset")
            )
        else:
            parsed = stream_df
            for col_name in ["kafka_topic", "kafka_partition", "kafka_offset"]:
                if col_name not in parsed.columns:
                    parsed = parsed.withColumn(col_name, F.lit(None))

        enriched = (
            parsed
            .withColumn("source_system", F.lit(f"streaming_{spec.name}"))
            .withColumn("ingestion_timestamp", F.current_timestamp())
            .withColumn("batch_id", F.lit(self._config.pipeline_run_id))
        )

        if spec.watermark_column in enriched.columns:
            enriched = enriched.withColumn(
                spec.watermark_column,
                F.to_timestamp(F.col(spec.watermark_column))
            )

        if spec.name == "flight_events":
            enriched = (
                enriched
                .withColumn("event_date", F.to_date(F.col("event_time")))
                .withColumn("scheduled_departure",
                            F.to_timestamp(F.col("scheduled_departure")))
                .withColumn("scheduled_arrival",
                            F.to_timestamp(F.col("scheduled_arrival")))
            )
        elif spec.name == "weather_updates":
            enriched = enriched.withColumn(
                "observation_date", F.to_date(F.col("observation_time"))
            )

        return enriched

    def _write_micro_batch(
        self, batch_df: DataFrame, batch_id: int, spec: StreamingSourceSpec
    ) -> None:
        micro_batch_id = f"{self._config.pipeline_run_id}_mb{batch_id}"
        row_count = batch_df.count()

        try:
            (
                batch_df
                .withColumn("batch_id", F.lit(micro_batch_id))
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(spec.target_table)
            )

            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"micro_batch_{spec.name}",
                target_table=spec.target_table,
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="SUCCESS",
                rows_written=row_count,
                details=f"Micro-batch {batch_id}",
            )
        except Exception as exc:
            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"micro_batch_{spec.name}",
                target_table=spec.target_table,
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="FAILED",
                rows_written=0,
                error_message=str(exc),
            )
            raise

    def await_all(self, timeout_seconds: Optional[int] = None) -> None:
        for q in self._queries:
            if timeout_seconds:
                q.awaitTermination(timeout_seconds * 1000)
            else:
                q.awaitTermination()

    def stop_all(self) -> None:
        for q in self._queries:
            try:
                q.stop()
            except Exception:
                pass
        self._queries.clear()

    def status(self) -> list[dict]:
        return [
            {
                "name": q.name,
                "id": str(q.id),
                "is_active": q.isActive,
                "last_progress": q.lastProgress,
            }
            for q in self._queries
        ]


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

def create_job(spark: SparkSession, **kwargs) -> StreamingIngestionJob:
    config = StreamingIngestionConfig(**kwargs)
    return StreamingIngestionJob(spark, config)
