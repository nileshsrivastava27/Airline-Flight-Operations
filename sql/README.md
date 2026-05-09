# Airline Ops Lakehouse SQL

This folder contains the base DDL for an airline flight operations Lakehouse
project on Databricks.

## Files

- `ddl/001_create_schemas.sql`: creates catalog and schemas
- `ddl/010_bronze_tables.sql`: raw ingestion tables
- `ddl/020_silver_tables.sql`: cleaned and conformed tables
- `ddl/030_gold_tables.sql`: reporting tables for dashboards

## Recommended raw data mapping

- `bronze.flight_operations_raw`: BTS on-time / operations dataset
- `bronze.airports_raw`: OurAirports airport reference dataset
- `bronze.aircraft_reference_raw`: OpenFlights aircraft reference dataset
- `bronze.weather_metar_raw`: Aviation Weather Center METAR dataset

## Reporting path

Use `gold.on_time_performance_daily`, `gold.route_delay_summary`,
`gold.airport_delay_summary`, `gold.cancellation_summary`, and
`gold.aircraft_performance_summary` as the Power BI model sources.

## Bronze ingestion helper

After creating the Bronze tables, use
[`databricks_bronze_ingestion.py`](/Users/nileshs2002/Documents/Airline%20Project/databricks_bronze_ingestion.py)
from a Databricks notebook to load CSV, JSONL, or mixed raw sources into the
Delta Bronze tables.
