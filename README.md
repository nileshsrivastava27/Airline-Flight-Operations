# Airline Intelligent Data Platform

End-to-end Databricks Lakehouse for airline flight operations — batch and
streaming Medallion architecture, CDC/SCD, data contracts, and a GenAI layer
(RAG, Text-to-SQL, Copilot, Incident Assistant). Built on Unity Catalog
(`airline_ops`), Delta Lake, and PySpark.

## Repository Structure

```
├── pipeline/                 # Batch Medallion pipeline
│   ├── databricks_bronze_ingestion.py
│   ├── databricks_silver_transformation.py
│   ├── databricks_gold_transformation.py
│   ├── databricks_pipeline_reliability.py   # rollback, audit, idempotency
│   ├── databricks_table_agent.py            # Unity Catalog search helper
│   ├── cdc_ingestion.py                     # Change Data Capture
│   └── scd_processor.py                     # Slowly Changing Dimensions
│
├── streaming/                # Spark Structured Streaming
│   ├── streaming_ingestion.py
│   ├── streaming_silver.py
│   └── streaming_gold.py
│
├── genai/                    # GenAI / LLM layer
│   ├── airline_docs.py       # document loader & chunker
│   ├── rag_pipeline.py       # Vector Search RAG
│   ├── text_to_sql.py        # natural-language to SQL
│   ├── airline_copilot.py    # unified assistant (routes to RAG / SQL / LLM)
│   └── incident_assistant.py # root-cause analysis from audit + Gold data
│
├── contracts/                # Data contract YAML specs (from feature/data_contracts)
├── simulator/                # Event generators (flights, weather, CDC)
├── bronze/                   # Notebook-ready Bronze examples
├── silver/                   # Notebook-ready Silver examples
├── gold/                     # Notebook-ready Gold examples
├── sql/ddl/                  # DDL scripts for all schemas
├── data/                     # Synthetic raw data (CSV + JSONL)
├── scripts/                  # Data generation utilities
├── validation/               # SQL validation checks per layer
├── tests/                    # Unit tests
└── docs/                     # Architecture documentation
```

## Unity Catalog Layout

| Schema | Purpose |
|--------|---------|
| `airline_ops.bronze` | Raw ingestion + rejected-records tables |
| `airline_ops.silver` | Cleaned, conformed, and quarantine tables |
| `airline_ops.gold` | Reporting aggregates |
| `airline_ops.audit` | Pipeline run logs |
| `airline_ops.genai` | Document chunks and vector-search assets |

## Quick Start

### 1. DDL Setup

Run the scripts in `sql/ddl/` in order (001 → 006) in a Databricks SQL
warehouse or notebook.

### 2. Bronze Ingestion

```python
from pipeline.databricks_bronze_ingestion import BronzeIngestionJob, build_default_dataset_specs

specs = build_default_dataset_specs("/Workspace/Repos/<you>/Airline-Flight-Operations", variant="mixed")
job = BronzeIngestionJob(spark)
results = job.ingest_all(specs, batch_id="batch_001")
```

### 3. Silver and Gold

```python
from pipeline.databricks_silver_transformation import SilverTransformationJob
from pipeline.databricks_gold_transformation import GoldTransformationJob

SilverTransformationJob(spark).run_all()
GoldTransformationJob(spark).run_all()
```

### 4. Streaming

```python
from streaming.streaming_ingestion import start_streaming_ingestion
start_streaming_ingestion(spark, checkpoint_base="/tmp/checkpoints")
```

### 5. GenAI — Airline Copilot

```python
from genai.airline_copilot import ask

response = ask(spark, "What were the top 5 most delayed routes last month?")
print(response.answer)
```

### 6. GenAI — Incident Assistant

```python
from genai.incident_assistant import analyze

report = analyze(spark, "Multiple flight cancellations at ORD this morning")
print(report.summary)
print(report.root_causes)
```

## Key Features

- **Medallion Architecture** — Bronze → Silver → Gold with rollback, audit
  logging, idempotent reruns, rescued-data capture, and schema evolution.
- **CDC & SCD** — Change Data Capture ingestion and Slowly Changing Dimension
  (Type 1 / Type 2) processing.
- **Structured Streaming** — real-time ingestion from simulated Kafka-style
  event sources with streaming Silver and Gold aggregations.
- **Data Contracts** — YAML-based schema and quality contracts validated before
  writes proceed.
- **RAG Pipeline** — Vector Search over chunked airline documents with
  LLM-generated answers.
- **Text-to-SQL** — natural-language questions translated to validated SQL over
  Unity Catalog tables.
- **Airline Copilot** — intent-routing assistant combining RAG, SQL, and direct
  LLM capabilities.
- **Incident Assistant** — automated root-cause analysis pulling audit logs,
  delay metrics, and weather data.

## Pipeline Reliability

Shared helpers in `pipeline/databricks_pipeline_reliability.py`:

- **Rollback** — captures Delta version before each write; restores on failure.
- **Audit logging** — every run logs to `airline_ops.audit.pipeline_run_log`.
- **Idempotency** — batch-id-based dedup prevents duplicate rows on rerun.
- **Schema evolution** — new source columns added via `ALTER TABLE ADD COLUMNS`.
- **Rescued data** — malformed rows routed to `*_rejected` tables.

## Validation

SQL checks in `validation/` verify row counts, nulls, and business rules
after each layer runs. See `validation/README.md`.

## Data Domains

- Flight operations
- Airport reference
- Aircraft reference
- Weather observations (METAR)
