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
- `databricks_pipeline_reliability.py`: shared rollback, audit, rerun, schema-evolution, and rescued-data helpers
- `docs/`: architecture and design documentation

## Main Design Artifacts

- [Architecture and Schema Design](/Users/nileshs2002/Documents/Airline%20Project/docs/airline_architecture_and_schema.md)
- [SQL DDL Overview](/Users/nileshs2002/Documents/Airline%20Project/sql/README.md)
- [Synthetic Raw Data Notes](/Users/nileshs2002/Documents/Airline%20Project/data/README.md)
- [Project Checklist](/Users/nileshs2002/Documents/Airline%20Project/PROJECT_CHECKLIST.md)
- [Validation SQL](/Users/nileshs2002/Documents/Airline%20Project/validation/README.md)

## Databricks Layers

- `airline_ops.bronze`: raw ingestion tables and rejected-records tables
- `airline_ops.silver`: cleaned and conformed tables
- `airline_ops.gold`: reporting-ready aggregate tables

## Data Domains

- Flight operations
- Airport reference
- Aircraft reference
- Weather observations

## Bronze Ingestion

Run the DDL scripts first, including both `sql/ddl/005_create_audit_tables.sql`
and `sql/ddl/006_create_rejected_tables.sql`:

```sql
-- Run in order in Databricks
-- 001_create_schemas.sql
-- 002_bronze_tables.sql
-- 003_silver_tables.sql
-- 004_gold_tables.sql
-- 005_create_audit_tables.sql
-- 006_create_rejected_tables.sql  <-- required before Bronze ingestion
```

Then use [databricks_bronze_ingestion.py](/Users/nileshs2002/Documents/Airline%20Project/databricks_bronze_ingestion.py)
from a Databricks notebook:

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

The result for each dataset now includes a `rows_rescued` field showing how
many rows were routed to the rejected table rather than the main Bronze table.

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

## Pipeline Reliability

### Rollback on Failure

Every Bronze, Silver, and Gold write captures the current Delta table version
before it starts. If any step raises an exception, the affected table is
restored to that version using `RESTORE TABLE ... TO VERSION AS OF`. For
Bronze ingestion specifically, both the main Bronze table and the corresponding
rejected table are restored together so a failed run leaves no partial writes
behind in either sink.

### Rescued Data and Rejected Records

Bronze ingestion uses the Databricks `rescuedDataColumn` option when reading
source files. Any values that do not fit the expected schema, including type
mismatches, unexpected extra fields, or unparseable content, are collected into
a `_rescued_data` column instead of silently dropping those rows or aborting
the read entirely.

After reading, rows are split into two groups:

- **Clean rows** — pass to the normal Bronze target table (e.g.
  `airline_ops.bronze.flight_operations_raw`).
- **Rescued rows** — routed to a dedicated rejected-records table (e.g.
  `airline_ops.bronze.flight_operations_rejected`) with the following schema:

| Column               | Type      | Description                                        |
|----------------------|-----------|----------------------------------------------------|
| `source_table`       | STRING    | The Bronze target table the row was intended for   |
| `source_system`      | STRING    | Logical source system label                        |
| `source_file_name`   | STRING    | Originating file name                              |
| `rescued_data`       | STRING    | JSON of the values that did not match the schema   |
| `raw_payload`        | STRING    | Full incoming row snapshot as JSON                 |
| `batch_id`           | STRING    | Ingestion batch identifier                         |
| `rejection_timestamp`| TIMESTAMP | When the row was rejected                          |

Rejected tables are created by `sql/ddl/006_create_rejected_tables.sql`.

> **Note on schema inference:** `rescuedDataColumn` is most effective when
> reading against an explicit schema. When `inferSchema=true` is used, Spark
> derives the schema from the incoming data itself, which reduces how many
> mismatches it can detect. For stricter rescue coverage, consider reading
> against the Bronze table's existing schema rather than inferring.

### Idempotent Reruns

Bronze and Silver writes are batch-idempotent. Before inserting, any existing
rows matching the current `batch_id` are deleted from the target table. This
means rerunning a failed batch replays cleanly without producing duplicates.

### Schema Evolution

When a source file introduces new columns not present in the Bronze target
table, those columns are added automatically via `ALTER TABLE ADD COLUMNS`
before the write proceeds. The rescued column itself is excluded from
schema evolution to avoid polluting the target table.

### Audit Logging

Every pipeline event (STARTED, SUCCESS, FAILED) is written to
`airline_ops.audit.pipeline_run_log`. Success events now include a
`rows_rescued` count alongside `rows_written` and `rows_removed`, making
it easy to monitor data quality trends across batches.

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
- Added rollback and recovery behavior for Bronze, Silver, and Gold write paths
- Added Bronze schema evolution handling for new incoming columns
- Added audit logging hooks for pipeline runs and table-level writes, including rescued row counts
- Added idempotent rerun behavior for batch-based Bronze and Silver writes
- Added notebook-ready Bronze, Silver, and Gold execution entry points
- Added lightweight unit tests for Bronze, Silver, and Gold modules
- Added rescued-data capture via `rescuedDataColumn` during Bronze reads
- Added per-dataset rejected-records tables (`*_rejected`) in the Bronze layer
- Added dual-table rollback for Bronze ingestion (target table and rejected table restored together on failure)
- Structured the repository so it can be run through Databricks Repos and later connected to Power BI

## Validation

Use the SQL files in [validation/README.md](/Users/nileshs2002/Documents/Airline%20Project/validation/README.md) to verify Bronze, Silver, and Gold outputs after each layer runs.

To check rescued/rejected row counts after Bronze ingestion:

```sql
SELECT 'flight_operations_rejected' AS table_name, COUNT(*) AS row_count
FROM airline_ops.bronze.flight_operations_rejected
UNION ALL
SELECT 'airports_rejected', COUNT(*)
FROM airline_ops.bronze.airports_rejected
UNION ALL
SELECT 'aircraft_reference_rejected', COUNT(*)
FROM airline_ops.bronze.aircraft_reference_rejected
UNION ALL
SELECT 'weather_metar_rejected', COUNT(*)
FROM airline_ops.bronze.weather_metar_rejected;
```

## Reporting Targets

The Gold layer is designed to support Power BI or Databricks SQL dashboards
for:

- on-time performance
- route delay analysis
- airport delay analysis
- cancellation trends
- aircraft performance
