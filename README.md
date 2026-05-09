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
- `databricks_bronze_ingestion.py`: notebook-friendly Bronze ingestion helper
- `docs/`: architecture and design documentation

## Main Design Artifacts

- [Architecture and Schema Design](/Users/nileshs2002/Documents/Airline%20Project/docs/airline_architecture_and_schema.md)
- [SQL DDL Overview](/Users/nileshs2002/Documents/Airline%20Project/sql/README.md)
- [Synthetic Raw Data Notes](/Users/nileshs2002/Documents/Airline%20Project/data/README.md)

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

## Reporting Targets

The Gold layer is designed to support Power BI or Databricks SQL dashboards
for:

- on-time performance
- route delay analysis
- airport delay analysis
- cancellation trends
- aircraft performance
