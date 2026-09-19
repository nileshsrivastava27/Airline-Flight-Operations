"""
Text-to-SQL engine for airline_ops Unity Catalog tables.

Takes a natural-language question, builds a SQL query using an LLM,
validates it against the catalog schema, and executes it on Spark.

Requires: Foundation Model Serving.
Notebook guard: check genai_enabled widget before running.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from pyspark.sql import DataFrame, SparkSession
except ImportError:
    DataFrame = Any
    SparkSession = Any

CATALOG = "airline_ops"
LLM_ENDPOINT = "databricks-meta-llama-3-1-70b-instruct"

BLOCKED_KEYWORDS = {"DROP", "DELETE", "TRUNCATE", "INSERT", "UPDATE", "ALTER", "CREATE"}


@dataclass
class SQLResult:
    question: str
    sql: str
    columns: List[str]
    rows: List[List[Any]]
    row_count: int


def _get_table_schemas(spark: SparkSession, schemas: Optional[List[str]] = None) -> str:
    """Return DDL-style descriptions of tables in the catalog."""
    schemas = schemas or ["bronze", "silver", "gold"]
    ddl_parts: List[str] = []
    for schema in schemas:
        tables = spark.sql(
            f"SHOW TABLES IN {CATALOG}.{schema}"
        ).collect()
        for row in tables:
            table_name = f"{CATALOG}.{schema}.{row.tableName}"
            cols = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
            col_defs = ", ".join(f"{c.col_name} {c.data_type}" for c in cols if c.col_name and not c.col_name.startswith("#"))
            ddl_parts.append(f"-- {table_name}\n({col_defs})")
    return "\n\n".join(ddl_parts)


def _validate_sql(sql: str) -> None:
    """Reject obviously dangerous statements."""
    upper = sql.upper().strip().rstrip(";")
    first_word = upper.split()[0] if upper.split() else ""
    if first_word in BLOCKED_KEYWORDS:
        raise ValueError(f"Blocked keyword: {first_word}")
    for kw in BLOCKED_KEYWORDS:
        if f" {kw} " in f" {upper} ":
            raise ValueError(f"Blocked keyword found: {kw}")


def generate_sql(
    spark: SparkSession,
    question: str,
    schemas: Optional[List[str]] = None,
) -> str:
    """Use an LLM to translate *question* into a SQL query."""
    import mlflow.deployments

    client = mlflow.deployments.get_deploy_client("databricks")
    table_context = _get_table_schemas(spark, schemas)

    prompt = (
        "You are a SQL expert for an airline data warehouse on Databricks Unity Catalog.\n"
        "Write a single SELECT query that answers the question. Return ONLY the SQL, no explanation.\n"
        "Use fully qualified table names (catalog.schema.table).\n\n"
        f"Available tables:\n{table_context}\n\n"
        f"Question: {question}"
    )

    response = client.predict(
        endpoint=LLM_ENDPOINT,
        inputs={"messages": [{"role": "user", "content": prompt}]},
    )
    sql = response["choices"][0]["message"]["content"].strip()
    sql = sql.strip("`").removeprefix("sql").strip()
    return sql


def text_to_sql_ask(
    spark: SparkSession,
    question: str,
    schemas: Optional[List[str]] = None,
    limit: int = 100,
) -> SQLResult:
    """End-to-end Text-to-SQL: generate, validate, execute."""
    sql = generate_sql(spark, question, schemas)
    _validate_sql(sql)

    if "LIMIT" not in sql.upper():
        sql = f"{sql.rstrip(';')} LIMIT {limit}"

    df: DataFrame = spark.sql(sql)
    rows = [list(r) for r in df.collect()]

    return SQLResult(
        question=question,
        sql=sql,
        columns=df.columns,
        rows=rows,
        row_count=len(rows),
    )
