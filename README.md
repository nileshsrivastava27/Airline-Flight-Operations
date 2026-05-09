# Airline Flight Operations Lakehouse

Databricks and Delta Lake project for an airline flight operations analytics
pipeline using Bronze, Silver, and Gold layers.

## Project Scope

This repo contains:

- Lakehouse table DDL for Bronze, Silver, and Gold
- Synthetic raw data in CSV and JSONL formats
- A raw data generator aligned to the Bronze schema
- Architecture documentation for high-level and low-level design

## Repository Structure

- `sql/ddl/`: Databricks DDL scripts
- `data/raw/`: CSV raw source files
- `data/raw_json/`: JSON Lines raw source files
- `scripts/`: synthetic data generator
- `docs/`: architecture and design documentation

## Main Design Artifacts

- [Architecture and Schema Design](/Users/nileshs2002/Documents/New%20project/docs/airline_architecture_and_schema.md)
- [SQL DDL Overview](/Users/nileshs2002/Documents/New%20project/sql/README.md)
- [Synthetic Raw Data Notes](/Users/nileshs2002/Documents/New%20project/data/README.md)

## Databricks Layers

- `airline_ops.bronze`: raw ingestion tables
- `airline_ops.silver`: cleaned and conformed tables
- `airline_ops.gold`: reporting-ready aggregate tables

## Data Domains

- Flight operations
- Airport reference
- Aircraft reference
- Weather observations

## Reporting Targets

The Gold layer is designed to support Power BI or Databricks SQL dashboards
for:

- on-time performance
- route delay analysis
- airport delay analysis
- cancellation trends
- aircraft performance
