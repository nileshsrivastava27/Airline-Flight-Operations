"""
Streaming Gold Transformation.

Maintains real-time Gold tables from Silver streaming data:
  - gold.realtime_flight_status : latest status per flight (MERGE upsert)
  - gold.realtime_airport_operations : aggregated airport metrics

Uses Spark Structured Streaming with foreachBatch and Delta MERGE for
upsert semantics.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from databricks_pipeline_reliability import PipelineAuditLogger


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TERMINAL_STATUSES = {"ARRIVAL", "CANCELLATION"}

CONGESTION_THRESHOLDS = {
    "CRITICAL": 15,
    "HIGH": 10,
    "MODERATE": 5,
}


@dataclass
class StreamingGoldConfig:
    pipeline_name: str = "streaming_gold"
    pipeline_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    checkpoint_root: str = "/tmp/checkpoints/streaming_gold"
    trigger_interval: str = "1 minute"
    watermark_delay: str = "10 minutes"


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

class StreamingGoldJob:

    def __init__(self, spark: SparkSession, config: StreamingGoldConfig | None = None):
        self._spark = spark
        self._config = config or StreamingGoldConfig()
        self._logger = PipelineAuditLogger(spark)
        self._queries = []

    def start_all(self) -> list:
        self._queries.append(self._start_flight_status_stream())
        self._queries.append(self._start_airport_operations_stream())
        return self._queries

    # -- real-time flight status --------------------------------------------

    def _start_flight_status_stream(self):
        events = (
            self._spark
            .readStream
            .format("delta")
            .table("flight_delay.silver.flight_events_clean")
        )

        watermarked = events.withWatermark(
            "event_time", self._config.watermark_delay
        )

        def upsert_flight_status(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            self._merge_flight_status(batch_df, batch_id)

        return (
            watermarked
            .writeStream
            .foreachBatch(upsert_flight_status)
            .option("checkpointLocation",
                    f"{self._config.checkpoint_root}/flight_status")
            .trigger(processingTime=self._config.trigger_interval)
            .queryName("gold_realtime_flight_status")
            .start()
        )

    def _merge_flight_status(self, batch_df: DataFrame, batch_id: int) -> None:
        latest_per_flight = (
            batch_df
            .groupBy("flight_id")
            .agg(
                F.first("flight_number").alias("flight_number"),
                F.first("airline").alias("airline"),
                F.first("origin").alias("origin"),
                F.first("destination").alias("destination"),
                F.first("aircraft_type").alias("aircraft_type"),
                F.first("tail_number").alias("tail_number"),
                F.last("event_type").alias("current_status"),
                F.last("gate").alias("current_gate"),
                F.first("scheduled_departure").alias("scheduled_departure"),
                F.first("scheduled_arrival").alias("scheduled_arrival"),
                F.max("event_time").alias("latest_event_time"),
                F.max("delay_minutes").alias("delay_minutes"),
                F.last("reason").alias("delay_reason"),
                F.count("*").alias("batch_event_count"),
            )
            .withColumn("is_active",
                        ~F.col("current_status").isin(list(TERMINAL_STATUSES)))
            .withColumn("flight_date", F.to_date(F.col("latest_event_time")))
            .withColumn("last_updated", F.current_timestamp())
        )

        micro_batch_id = f"{self._config.pipeline_run_id}_mb{batch_id}"
        row_count = latest_per_flight.count()

        try:
            target_table = "flight_delay.gold.realtime_flight_status"

            if self._spark.catalog.tableExists(target_table):
                delta_table = DeltaTable.forName(self._spark, target_table)
                (
                    delta_table.alias("target")
                    .merge(
                        latest_per_flight.alias("source"),
                        "target.flight_id = source.flight_id"
                    )
                    .whenMatchedUpdate(
                        condition="source.latest_event_time > target.latest_event_time",
                        set={
                            "current_status": "source.current_status",
                            "current_gate": "source.current_gate",
                            "latest_event_time": "source.latest_event_time",
                            "delay_minutes": "source.delay_minutes",
                            "delay_reason": "source.delay_reason",
                            "total_events": "target.total_events + source.batch_event_count",
                            "is_active": "source.is_active",
                            "last_updated": "source.last_updated",
                        }
                    )
                    .whenNotMatchedInsert(
                        values={
                            "flight_id": "source.flight_id",
                            "flight_number": "source.flight_number",
                            "airline": "source.airline",
                            "origin": "source.origin",
                            "destination": "source.destination",
                            "aircraft_type": "source.aircraft_type",
                            "tail_number": "source.tail_number",
                            "current_status": "source.current_status",
                            "current_gate": "source.current_gate",
                            "scheduled_departure": "source.scheduled_departure",
                            "scheduled_arrival": "source.scheduled_arrival",
                            "latest_event_time": "source.latest_event_time",
                            "delay_minutes": "source.delay_minutes",
                            "delay_reason": "source.delay_reason",
                            "total_events": "source.batch_event_count",
                            "is_active": "source.is_active",
                            "flight_date": "source.flight_date",
                            "last_updated": "source.last_updated",
                        }
                    )
                    .execute()
                )
            else:
                (
                    latest_per_flight
                    .withColumnRenamed("batch_event_count", "total_events")
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(target_table)
                )

            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name="flight_status_merge",
                target_table=target_table,
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="SUCCESS",
                rows_written=row_count,
            )
        except Exception as exc:
            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name="flight_status_merge",
                target_table="flight_delay.gold.realtime_flight_status",
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="FAILED",
                error_message=str(exc),
            )
            raise

    # -- real-time airport operations ---------------------------------------

    def _start_airport_operations_stream(self):
        flight_events = (
            self._spark
            .readStream
            .format("delta")
            .table("flight_delay.silver.flight_events_clean")
        )

        watermarked = flight_events.withWatermark(
            "event_time", self._config.watermark_delay
        )

        def compute_airport_ops(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            self._merge_airport_operations(batch_df, batch_id)

        return (
            watermarked
            .writeStream
            .foreachBatch(compute_airport_ops)
            .option("checkpointLocation",
                    f"{self._config.checkpoint_root}/airport_operations")
            .trigger(processingTime=self._config.trigger_interval)
            .queryName("gold_realtime_airport_operations")
            .start()
        )

    def _merge_airport_operations(self, batch_df: DataFrame, batch_id: int) -> None:
        departures = (
            batch_df
            .filter(F.col("event_type") == "DEPARTURE")
            .groupBy(F.col("origin").alias("airport_code"))
            .agg(F.count("*").alias("departures"))
        )
        arrivals = (
            batch_df
            .filter(F.col("event_type") == "ARRIVAL")
            .groupBy(F.col("destination").alias("airport_code"))
            .agg(F.count("*").alias("arrivals"))
        )
        delays = (
            batch_df
            .filter(F.col("is_delay_event") == True)
            .groupBy(F.col("airport_code"))
            .agg(
                F.count("*").alias("delayed"),
                F.avg("delay_minutes").alias("avg_delay"),
                F.max("delay_minutes").alias("max_delay"),
            )
        )
        cancellations = (
            batch_df
            .filter(F.col("is_cancellation_event") == True)
            .groupBy(F.col("airport_code"))
            .agg(F.count("*").alias("cancelled"))
        )

        all_airports = (
            batch_df
            .select(F.col("airport_code"))
            .distinct()
        )

        airport_metrics = (
            all_airports
            .join(departures, "airport_code", "left")
            .join(arrivals, "airport_code", "left")
            .join(delays, "airport_code", "left")
            .join(cancellations, "airport_code", "left")
            .fillna(0)
            .withColumn("snapshot_time", F.current_timestamp())
            .withColumn("total_departures", F.coalesce(F.col("departures"), F.lit(0)))
            .withColumn("total_arrivals", F.coalesce(F.col("arrivals"), F.lit(0)))
            .withColumn("active_flights",
                        F.col("total_departures") - F.col("total_arrivals"))
            .withColumn("delayed_flights", F.coalesce(F.col("delayed"), F.lit(0)))
            .withColumn("cancelled_flights", F.coalesce(F.col("cancelled"), F.lit(0)))
            .withColumn("avg_delay_minutes",
                        F.coalesce(F.col("avg_delay"), F.lit(0.0)))
            .withColumn("max_delay_minutes",
                        F.coalesce(F.col("max_delay"), F.lit(0)))
            .withColumn("congestion_level",
                        F.when(F.col("delayed_flights") >= CONGESTION_THRESHOLDS["CRITICAL"], "CRITICAL")
                        .when(F.col("delayed_flights") >= CONGESTION_THRESHOLDS["HIGH"], "HIGH")
                        .when(F.col("delayed_flights") >= CONGESTION_THRESHOLDS["MODERATE"], "MODERATE")
                        .otherwise("LOW"))
            .withColumn("current_weather", F.lit(None).cast("string"))
            .withColumn("flight_date", F.current_date())
            .withColumn("last_updated", F.current_timestamp())
            .select(
                "airport_code", "snapshot_time", "total_departures",
                "total_arrivals", "active_flights", "delayed_flights",
                "cancelled_flights", "avg_delay_minutes", "max_delay_minutes",
                "current_weather", "congestion_level", "flight_date",
                "last_updated",
            )
        )

        micro_batch_id = f"{self._config.pipeline_run_id}_mb{batch_id}"
        row_count = airport_metrics.count()

        try:
            target_table = "flight_delay.gold.realtime_airport_operations"

            if self._spark.catalog.tableExists(target_table):
                delta_table = DeltaTable.forName(self._spark, target_table)
                (
                    delta_table.alias("target")
                    .merge(
                        airport_metrics.alias("source"),
                        "target.airport_code = source.airport_code "
                        "AND target.flight_date = source.flight_date"
                    )
                    .whenMatchedUpdate(set={
                        "snapshot_time": "source.snapshot_time",
                        "total_departures":
                            "target.total_departures + source.total_departures",
                        "total_arrivals":
                            "target.total_arrivals + source.total_arrivals",
                        "active_flights": "source.active_flights",
                        "delayed_flights":
                            "target.delayed_flights + source.delayed_flights",
                        "cancelled_flights":
                            "target.cancelled_flights + source.cancelled_flights",
                        "avg_delay_minutes": "source.avg_delay_minutes",
                        "max_delay_minutes":
                            F.expr("GREATEST(target.max_delay_minutes, source.max_delay_minutes)"),
                        "congestion_level": "source.congestion_level",
                        "last_updated": "source.last_updated",
                    })
                    .whenNotMatchedInsertAll()
                    .execute()
                )
            else:
                (
                    airport_metrics
                    .write
                    .format("delta")
                    .mode("overwrite")
                    .option("overwriteSchema", "true")
                    .saveAsTable(target_table)
                )

            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name="airport_operations_merge",
                target_table=target_table,
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="SUCCESS",
                rows_written=row_count,
            )
        except Exception as exc:
            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name="airport_operations_merge",
                target_table="flight_delay.gold.realtime_airport_operations",
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="FAILED",
                error_message=str(exc),
            )
            raise

    # -- lifecycle ----------------------------------------------------------

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


def create_job(spark: SparkSession, **kwargs) -> StreamingGoldJob:
    config = StreamingGoldConfig(**kwargs)
    return StreamingGoldJob(spark, config)
