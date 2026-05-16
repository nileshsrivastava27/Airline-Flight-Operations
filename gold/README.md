# Gold Transformations

This folder contains notebook-ready PySpark code for transforming Silver Delta
tables into Gold reporting tables in Databricks.

## Files

- `gold_transformation_notebook.py`: Databricks notebook-style Gold
  transformation example

## Usage

1. Run the Silver transformation first
2. Ensure the Gold DDL has been created from `sql/ddl/030_gold_tables.sql`
3. Open `gold_transformation_notebook.py` in Databricks as a Python notebook
4. Run the notebook cells to populate the Gold tables
