"""
Streaming Silver Transformation.

Reads raw flight events and weather updates from Bronze streaming tables,
applies cleaning, deduplication, and enrichment, and writes to Silver
streaming tables. Uses Spark Structured Streaming with foreachBatch for
consistent audit logging via PipelineAuditLogger.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from databricks_pipeline_reliability import PipelineAuditLogger


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

VALID_EVENT_TYPES = {
    "FLIGHT_SCHEDULED", "BOARDING_STARTED", "BOARDING_COMPLETED",
    "DEPARTURE", "TAKEOFF", "LANDING", "ARRIVAL",
    "DELAY", "CANCELLATION", "GATE_CHANGE",
}

WEATHER_SEVERITY_MAP = {"VFR": 0, "MVFR": 1, "IFR": 2, "LIFR": 3}


@dataclass
class StreamingSilverConfig:
    pipeline_name: str = "streaming_silver"
    pipeline_run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    checkpoint_root: str = "/tmp/checkpoints/streaming_silver"
    trigger_interval: str = "30 seconds"
    watermark_delay_flight: str = "10 minutes"
    watermark_delay_weather: str = "15 minutes"


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

class StreamingSilverJob:

    def __init__(self, spark: SparkSession, config: StreamingSilverConfig | None = None):
        self._spark = spark
        self._config = config or StreamingSilverConfig()
        self._logger = PipelineAuditLogger(spark)
        self._queries = []

    # -- public api ---------------------------------------------------------

    def start_all(self) -> list:
        self._queries.append(self._start_flight_events_stream())
        self._queries.append(self._start_weather_updates_stream())
        return self._queries

    # -- flight events ------------------------------------------------------

    def _start_flight_events_stream(self):
        raw = (
            self._spark
            .readStream
            .format("delta")
            .table("flight_delay.bronze.flight_events_raw")
        )

        watermarked = raw.withWatermark("event_time", self._config.watermark_delay_flight)

        def process_flight_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            cleaned = self._transform_flight_events(batch_df)
            self._write_batch(
                cleaned, batch_id,
                target_table="flight_delay.silver.flight_events_clean",
                stream_name="flight_events",
            )

        return (
            watermarked
            .writeStream
            .foreachBatch(process_flight_batch)
            .option("checkpointLocation",
                    f"{self._config.checkpoint_root}/flight_events")
            .trigger(processingTime=self._config.trigger_interval)
            .queryName("silver_flight_events")
            .start()
        )

    def _transform_flight_events(self, df: DataFrame) -> DataFrame:
        valid_types_list = list(VALID_EVENT_TYPES)

        cleaned = (
            df
            .filter(F.col("event_id").isNotNull())
            .filter(F.col("flight_id").isNotNull())
            .filter(F.col("event_type").isin(valid_types_list))
            .filter(F.col("airport_code").isNotNull())
            .filter(F.col("event_time").isNotNull())
        )

        dedup_window = Window.partitionBy("event_id").orderBy(
            F.col("ingestion_timestamp").desc()
        )
        deduped = (
            cleaned
            .withColumn("_row_num", F.row_number().over(dedup_window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        enriched = (
            deduped
            .withColumn("airline", F.upper(F.col("airline")))
            .withColumn("airport_code", F.upper(F.col("airport_code")))
            .withColumn("origin", F.upper(F.col("origin")))
            .withColumn("destination", F.upper(F.col("destination")))
            .withColumn("is_delay_event", F.col("event_type") == "DELAY")
            .withColumn("is_cancellation_event", F.col("event_type") == "CANCELLATION")
            .withColumn("delay_minutes",
                        F.coalesce(F.col("delay_minutes"), F.lit(0)))
            .withColumn("event_date", F.to_date(F.col("event_time")))
            .withColumn("processing_timestamp", F.current_timestamp())
        )

        output_columns = [
            "event_id", "flight_id", "flight_number", "airline",
            "event_type", "airport_code", "gate", "origin", "destination",
            "aircraft_type", "tail_number", "event_time",
            "scheduled_departure", "scheduled_arrival", "status",
            "delay_minutes", "reason",
            "is_delay_event", "is_cancellation_event",
            "event_date", "processing_timestamp",
        ]
        return enriched.select(*[c for c in output_columns if c in enriched.columns])

    # -- weather updates ----------------------------------------------------

    def _start_weather_updates_stream(self):
        raw = (
            self._spark
            .readStream
            .format("delta")
            .table("flight_delay.bronze.weather_updates_raw")
        )

        watermarked = raw.withWatermark(
            "observation_time", self._config.watermark_delay_weather
        )

        def process_weather_batch(batch_df: DataFrame, batch_id: int):
            if batch_df.isEmpty():
                return
            cleaned = self._transform_weather_updates(batch_df)
            self._write_batch(
                cleaned, batch_id,
                target_table="flight_delay.silver.weather_updates_clean",
                stream_name="weather_updates",
            )

        return (
            watermarked
            .writeStream
            .foreachBatch(process_weather_batch)
            .option("checkpointLocation",
                    f"{self._config.checkpoint_root}/weather_updates")
            .trigger(processingTime=self._config.trigger_interval)
            .queryName("silver_weather_updates")
            .start()
        )

    def _transform_weather_updates(self, df: DataFrame) -> DataFrame:
        cleaned = (
            df
            .filter(F.col("observation_id").isNotNull())
            .filter(F.col("station_id").isNotNull())
            .filter(F.col("observation_time").isNotNull())
        )

        dedup_window = Window.partitionBy(
            "station_id", "observation_time"
        ).orderBy(F.col("ingestion_timestamp").desc())

        deduped = (
            cleaned
            .withColumn("_row_num", F.row_number().over(dedup_window))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )

        severity_expr = (
            F.when(F.col("flight_category") == "LIFR", 3)
            .when(F.col("flight_category") == "IFR", 2)
            .when(F.col("flight_category") == "MVFR", 1)
            .otherwise(0)
        )

        wind_chill_expr = F.when(
            (F.col("temp_c") <= 10) & (F.col("wind_speed_kt") > 3),
            F.round(
                13.12
                + 0.6215 * F.col("temp_c")
                - 11.37 * F.pow(F.col("wind_speed_kt") * 1.852, 0.16)
                + 0.3965 * F.col("temp_c") * F.pow(F.col("wind_speed_kt") * 1.852, 0.16),
                1,
            )
        )

        enriched = (
            deduped
            .withColumn("station_id", F.upper(F.col("station_id")))
            .withColumn("observation_date", F.to_date(F.col("observation_time")))
            .withColumn("wind_chill_c", wind_chill_expr)
            .withColumn("is_gusty",
                        F.coalesce(F.col("wind_gust_kt"), F.lit(0)) > 25)
            .withColumn("is_low_visibility",
                        F.col("visibility_statute_mi") < 3.0)
            .withColumn("weather_severity", severity_expr)
            .withColumn("processing_timestamp", F.current_timestamp())
        )

        output_columns = [
            "observation_id", "station_id", "observation_time",
            "observation_date", "temp_c", "dewpoint_c",
            "wind_dir_degrees", "wind_speed_kt", "wind_gust_kt",
            "visibility_statute_mi", "altim_in_hg", "flight_category",
            "sky_cover", "precip_in", "wind_chill_c",
            "is_gusty", "is_low_visibility", "weather_severity",
            "processing_timestamp",
        ]
        return enriched.select(*[c for c in output_columns if c in enriched.columns])

    # -- shared write -------------------------------------------------------

    def _write_batch(
        self,
        df: DataFrame,
        batch_id: int,
        *,
        target_table: str,
        stream_name: str,
    ) -> None:
        micro_batch_id = f"{self._config.pipeline_run_id}_mb{batch_id}"
        row_count = df.count()

        try:
            (
                df
                .write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(target_table)
            )

            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"silver_{stream_name}",
                target_table=target_table,
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="SUCCESS",
                rows_written=row_count,
                details=f"Micro-batch {batch_id}",
            )
        except Exception as exc:
            self._logger.log_event(
                pipeline_name=self._config.pipeline_name,
                stage_name=f"silver_{stream_name}",
                target_table=target_table,
                run_id=self._config.pipeline_run_id,
                batch_id=micro_batch_id,
                status="FAILED",
                rows_written=0,
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


def create_job(spark: SparkSession, **kwargs) -> StreamingSilverJob:
    config = StreamingSilverConfig(**kwargs)
    return StreamingSilverJob(spark, config)
