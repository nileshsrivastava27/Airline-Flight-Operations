"""
Notebook-friendly Bronze ingestion helper for Databricks.

This module loads raw CSV or JSONL source files into Delta Bronze tables for
the airline flight operations Lakehouse project.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Iterable, Literal

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
else:
    DataFrame = Any
    SparkSession = Any


SourceVariant = Literal["csv", "jsonl", "mixed"]


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    source_path: str
    source_format: str
    target_table: str
    source_system: str
    source_file_extension: str
    write_mode: str = "append"
    options: dict[str, str] = field(default_factory=dict)


def normalize_source_format(source_format: str) -> str:
    cleaned = source_format.strip().lower()
    if cleaned == "jsonl":
        return "json"
    if cleaned in {"csv", "json"}:
        return cleaned
    raise ValueError(f"Unsupported source format: {source_format}")


def build_default_dataset_specs(data_root: str, variant: SourceVariant = "mixed") -> list[DatasetSpec]:
    cleaned_root = data_root.rstrip("/")
    if variant == "csv":
        raw_root = f"{cleaned_root}/data/raw"
        return [
            DatasetSpec(
                name="flight_operations_raw",
                source_path=f"{raw_root}/flight_operations",
                source_format="csv",
                target_table="airline_ops.bronze.flight_operations_raw",
                source_system="synthetic_airline_ops",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
            ),
            DatasetSpec(
                name="airports_raw",
                source_path=f"{raw_root}/airports",
                source_format="csv",
                target_table="airline_ops.bronze.airports_raw",
                source_system="synthetic_airports",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
            ),
            DatasetSpec(
                name="aircraft_reference_raw",
                source_path=f"{raw_root}/aircraft_reference",
                source_format="csv",
                target_table="airline_ops.bronze.aircraft_reference_raw",
                source_system="synthetic_aircraft",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
            ),
            DatasetSpec(
                name="weather_metar_raw",
                source_path=f"{raw_root}/weather_metar",
                source_format="csv",
                target_table="airline_ops.bronze.weather_metar_raw",
                source_system="synthetic_metar",
                source_file_extension=".csv",
                options={"header": "true", "inferSchema": "true"},
            ),
        ]

    if variant == "jsonl":
        raw_root = f"{cleaned_root}/data/raw_json"
        return [
            DatasetSpec(
                name="flight_operations_raw",
                source_path=f"{raw_root}/flight_operations",
                source_format="jsonl",
                target_table="airline_ops.bronze.flight_operations_raw",
                source_system="synthetic_airline_ops",
                source_file_extension=".jsonl",
            ),
            DatasetSpec(
                name="airports_raw",
                source_path=f"{raw_root}/airports",
                source_format="jsonl",
                target_table="airline_ops.bronze.airports_raw",
                source_system="synthetic_airports",
                source_file_extension=".jsonl",
            ),
            DatasetSpec(
                name="aircraft_reference_raw",
                source_path=f"{raw_root}/aircraft_reference",
                source_format="jsonl",
                target_table="airline_ops.bronze.aircraft_reference_raw",
                source_system="synthetic_aircraft",
                source_file_extension=".jsonl",
            ),
            DatasetSpec(
                name="weather_metar_raw",
                source_path=f"{raw_root}/weather_metar",
                source_format="jsonl",
                target_table="airline_ops.bronze.weather_metar_raw",
                source_system="synthetic_metar",
                source_file_extension=".jsonl",
            ),
        ]

    if variant == "mixed":
        csv_specs = build_default_dataset_specs(cleaned_root, "csv")
        weather_json_spec = build_default_dataset_specs(cleaned_root, "jsonl")[-1]
        return [
            csv_specs[0],
            csv_specs[1],
            csv_specs[2],
            weather_json_spec,
        ]

    raise ValueError(f"Unsupported source variant: {variant}")


class BronzeIngestionJob:
    """
    Load raw source files into Delta Bronze tables.

    Usage in Databricks notebook:

        from databricks_bronze_ingestion import BronzeIngestionJob, build_default_dataset_specs

        specs = build_default_dataset_specs("/Workspace/Repos/you/Airline-Flight-Operations", "mixed")
        job = BronzeIngestionJob(spark)
        results = job.ingest_all(specs, batch_id="batch_20260510_001")
        display(results)
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_all(self, specs: Iterable[DatasetSpec], *, batch_id: str | None = None) -> list[dict[str, Any]]:
        return [self.ingest_dataset(spec, batch_id=batch_id) for spec in specs]

    def ingest_dataset(self, spec: DatasetSpec, *, batch_id: str | None = None) -> dict[str, Any]:
        source_df = self._read_source(spec)
        enriched_df = self._apply_metadata(source_df, spec, batch_id=batch_id)
        aligned_df = self._align_to_target_table(enriched_df, spec.target_table)
        row_count = aligned_df.count()
        (
            aligned_df.write.format("delta")
            .mode(spec.write_mode)
            .option("mergeSchema", "false")
            .saveAsTable(spec.target_table)
        )
        return {
            "dataset": spec.name,
            "source_path": spec.source_path,
            "source_format": spec.source_format,
            "target_table": spec.target_table,
            "rows_written": row_count,
            "write_mode": spec.write_mode,
        }

    def _read_source(self, spec: DatasetSpec) -> DataFrame:
        reader = self.spark.read.format(normalize_source_format(spec.source_format))
        for key, value in spec.options.items():
            reader = reader.option(key, value)
        if normalize_source_format(spec.source_format) == "json":
            reader = reader.option("multiLine", "false")
        return reader.load(spec.source_path)

    def _apply_metadata(
        self,
        df: DataFrame,
        spec: DatasetSpec,
        *,
        batch_id: str | None = None,
    ) -> DataFrame:
        from pyspark.sql import functions as F

        file_name_expr = F.regexp_extract(F.input_file_name(), r"([^/]+$)", 1)

        if "source_file_name" in df.columns:
            df = df.withColumn(
                "source_file_name",
                F.when(
                    F.col("source_file_name").isNull() | (F.trim(F.col("source_file_name")) == ""),
                    file_name_expr,
                ).otherwise(F.col("source_file_name")),
            )
        else:
            df = df.withColumn("source_file_name", file_name_expr)

        if "source_system" in df.columns:
            df = df.withColumn(
                "source_system",
                F.when(
                    F.col("source_system").isNull() | (F.trim(F.col("source_system")) == ""),
                    F.lit(spec.source_system),
                ).otherwise(F.col("source_system")),
            )
        else:
            df = df.withColumn("source_system", F.lit(spec.source_system))

        if "ingestion_timestamp" in df.columns:
            df = df.withColumn(
                "ingestion_timestamp",
                F.coalesce(F.to_timestamp("ingestion_timestamp"), F.current_timestamp()),
            )
        else:
            df = df.withColumn("ingestion_timestamp", F.current_timestamp())

        if batch_id:
            if "batch_id" in df.columns:
                df = df.withColumn("batch_id", F.lit(batch_id))
            else:
                df = df.withColumn("batch_id", F.lit(batch_id))

        return df

    def _align_to_target_table(self, df: DataFrame, target_table: str) -> DataFrame:
        from pyspark.sql import functions as F

        if not self.spark.catalog.tableExists(target_table):
            raise ValueError(
                f"Target table '{target_table}' does not exist. Run the DDL scripts first."
            )

        target_schema = self.spark.table(target_table).schema
        projected_columns = []
        for field in target_schema.fields:
            if field.name in df.columns:
                projected_columns.append(F.col(field.name).cast(field.dataType).alias(field.name))
            else:
                projected_columns.append(F.lit(None).cast(field.dataType).alias(field.name))
        return df.select(*projected_columns)


def create_job(spark: SparkSession) -> BronzeIngestionJob:
    return BronzeIngestionJob(spark)


if __name__ == "__main__":
    raise SystemExit(
        "Import this module from a Databricks notebook and call BronzeIngestionJob(spark)."
    )
