# Silver Transformations

This folder contains notebook-ready PySpark code for transforming Bronze Delta
tables into cleaned Silver Delta tables in Databricks.

## Files

- `silver_transformation_notebook.py`: Databricks notebook-style Silver
  transformation example

## Usage

1. Run the Bronze ingestion first
2. Ensure the Silver DDL has been created from `sql/ddl/020_silver_tables.sql`
3. Open `silver_transformation_notebook.py` in Databricks as a Python notebook
4. Run the notebook cells to populate the Silver tables
