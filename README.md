# Airline Flight Operations Lakehouse

Databricks and Delta Lake project for an airline flight operations analytics
pipeline using Bronze, Silver, and Gold layers.

## Project Scope

This repo contains:

- Lakehouse table DDL for Bronze, Silver, and Gold
- Synthetic raw data in CSV and JSONL formats
- A raw data generator aligned to the Bronze schema
- Databricks PySpark Bronze ingestion code for CSV and JSONL sources
- Architecture documentation for high-level and low-level design

## Repository Structure

- `sql/ddl/`: Databricks DDL scripts
- `data/raw/`: CSV raw source files
- `data/raw_json/`: JSON Lines raw source files
- `scripts/`: synthetic data generator
- `bronze/`: notebook-ready Bronze ingestion examples
- `silver/`: notebook-ready Silver transformation examples
- `gold/`: notebook-ready Gold transformation examples
- `validation/`: SQL validation checks for Bronze, Silver, and Gold
- `databricks_bronze_ingestion.py`: notebook-friendly Bronze ingestion helper
- `databricks_silver_transformation.py`: notebook-friendly Silver transformation helper
- `databricks_gold_transformation.py`: notebook-friendly Gold transformation helper
- `docs/`: architecture and design documentation

## Main Design Artifacts

- [Architecture and Schema Design](/Users/nileshs2002/Documents/Airline%20Project/docs/airline_architecture_and_schema.md)
- [SQL DDL Overview](/Users/nileshs2002/Documents/Airline%20Project/sql/README.md)
- [Synthetic Raw Data Notes](/Users/nileshs2002/Documents/Airline%20Project/data/README.md)
- [Project Checklist](/Users/nileshs2002/Documents/Airline%20Project/PROJECT_CHECKLIST.md)
- [Validation SQL](/Users/nileshs2002/Documents/Airline%20Project/validation/README.md)

## Databricks Layers

- `airline_ops.bronze`: raw ingestion tables
- `airline_ops.silver`: cleaned and conformed tables
- `airline_ops.gold`: reporting-ready aggregate tables

## Data Domains

- Flight operations
- Airport reference
- Aircraft reference
- Weather observations

## Bronze Ingestion

Use [databricks_bronze_ingestion.py](/Users/nileshs2002/Documents/Airline%20Project/databricks_bronze_ingestion.py)
from a Databricks notebook after running the DDL scripts:

```python
from databricks_bronze_ingestion import BronzeIngestionJob, build_default_dataset_specs

specs = build_default_dataset_specs(
    "/Workspace/Repos/nileshsrivastava27/Airline-Flight-Operations",
    variant="mixed",
)

job = BronzeIngestionJob(spark)
results = job.ingest_all(specs, batch_id="batch_20260510_001")
display(results)
```

For a direct notebook version with explicit `spark.read.format("csv")` and
`spark.read.format("json")` ingestion cells, use
[bronze_ingestion_notebook.py](/Users/nileshs2002/Documents/Airline%20Project/bronze/bronze_ingestion_notebook.py).

## Silver Transformations

Use [databricks_silver_transformation.py](/Users/nileshs2002/Documents/Airline%20Project/databricks_silver_transformation.py)
from a Databricks notebook after Bronze tables are populated:

```python
from databricks_silver_transformation import SilverTransformationJob

job = SilverTransformationJob(spark)
results = job.run_all()
display(results)
```

For a direct notebook version with Silver transformation cells, use
[silver_transformation_notebook.py](/Users/nileshs2002/Documents/Airline%20Project/silver/silver_transformation_notebook.py).

## Gold Transformations

Use [databricks_gold_transformation.py](/Users/nileshs2002/Documents/Airline%20Project/databricks_gold_transformation.py)
from a Databricks notebook after Silver tables are populated:

```python
from databricks_gold_transformation import GoldTransformationJob

job = GoldTransformationJob(spark)
results = job.run_all()
display(results)
```

For a direct notebook version with Gold transformation cells, use
[gold_transformation_notebook.py](/Users/nileshs2002/Documents/Airline%20Project/gold/gold_transformation_notebook.py).

## Achieved So Far

- Defined the airline flight operations Lakehouse use case and project scope
- Created architecture documentation with both high-level and low-level design
- Created Bronze, Silver, and Gold DDL scripts
- Generated synthetic raw airline source data in both CSV and JSONL formats
- Built reusable Bronze ingestion logic for Databricks
- Added notebook-style Bronze ingestion code for Databricks Free Edition
- Built Silver transformation logic for cleaned facts, reference tables, quarantine, and route derivation
- Added schema-aligned Silver writes to reduce datatype mismatch issues during inserts
- Built Gold transformation logic for on-time performance, route delay, airport delay, cancellation, and aircraft performance reporting
- Added notebook-ready Bronze, Silver, and Gold execution entry points
- Added lightweight unit tests for Bronze, Silver, and Gold modules
- Structured the repository so it can be run through Databricks Repos and later connected to Power BI

## Validation

Use the SQL files in [validation/README.md](/Users/nileshs2002/Documents/Airline%20Project/validation/README.md) to verify Bronze, Silver, and Gold outputs after each layer runs.

## Reporting Targets

The Gold layer is designed to support Power BI or Databricks SQL dashboards
for:

- on-time performance
- route delay analysis
- airport delay analysis
- cancellation trends
- aircraft performance
