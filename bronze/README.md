# Bronze Ingestion

This folder contains notebook-ready PySpark code for loading raw CSV and JSONL
source files into the Bronze Delta tables in Databricks.

## Files

- `bronze_ingestion_notebook.py`: Databricks notebook-style ingestion example

## Usage

1. Run the DDL scripts in `sql/ddl/`
2. Open `bronze_ingestion_notebook.py` in Databricks as a Python notebook or
   copy the cells into a notebook
3. Update `project_root` if your Databricks Repo path is different
4. Run the Bronze ingestion cells
