"""
Notebook-ready Databricks table search helper.

Designed for Unity Catalog first. It can optionally search `hive_metastore`
when requested.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


@dataclass
class TableMatch:
    catalog: str
    schema: str
    table: str
    table_type: str

    @property
    def full_path(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"


class DatabricksTableAgent:
    """
    Small search agent for Databricks notebooks.

    Example:
        agent = DatabricksTableAgent(spark)
        agent.find_table("orders")
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def find_table(
        self,
        table_name: str,
        *,
        exact_match: bool = False,
        include_hive_metastore: bool = False,
        limit: int = 20,
    ) -> List[TableMatch]:
        """
        Search visible tables and views by name.

        Args:
            table_name: Table name or partial name to search.
            exact_match: If True, return exact table-name matches only.
            include_hive_metastore: If True, also search the legacy metastore.
            limit: Max rows returned.
        """

        cleaned_name = table_name.strip()
        if not cleaned_name:
            raise ValueError("table_name must not be empty")

        uc_matches = self._search_unity_catalog(
            table_name=cleaned_name,
            exact_match=exact_match,
            limit=limit,
        )

        hive_matches: List[TableMatch] = []
        if include_hive_metastore and len(uc_matches) < limit:
            hive_matches = self._search_hive_metastore(
                table_name=cleaned_name,
                exact_match=exact_match,
                limit=limit - len(uc_matches),
            )

        return uc_matches + hive_matches

    def find_best_match(
        self,
        table_name: str,
        *,
        include_hive_metastore: bool = False,
    ) -> Optional[str]:
        """
        Return the best fully qualified table path, if any.
        """

        matches = self.find_table(
            table_name,
            exact_match=True,
            include_hive_metastore=include_hive_metastore,
            limit=1,
        )
        if matches:
            return matches[0].full_path

        fuzzy_matches = self.find_table(
            table_name,
            exact_match=False,
            include_hive_metastore=include_hive_metastore,
            limit=1,
        )
        return fuzzy_matches[0].full_path if fuzzy_matches else None

    def answer(
        self,
        request: str,
        *,
        include_hive_metastore: bool = False,
        limit: int = 10,
    ) -> str:
        """
        Agent-style response for notebook users.
        """

        matches = self.find_table(
            request,
            exact_match=False,
            include_hive_metastore=include_hive_metastore,
            limit=limit,
        )

        if not matches:
            return f"No table found for '{request}'."

        if len(matches) == 1:
            match = matches[0]
            return f"Found table: {match.full_path} ({match.table_type})"

        lines = [f"Found {len(matches)} matching tables for '{request}':"]
        lines.extend(f"- {match.full_path} ({match.table_type})" for match in matches)
        return "\n".join(lines)

    def _search_unity_catalog(
        self,
        *,
        table_name: str,
        exact_match: bool,
        limit: int,
    ) -> List[TableMatch]:
        """
        Search Unity Catalog through system.information_schema.tables.
        """

        tables_df = self.spark.table("system.information_schema.tables").select(
            F.col("table_catalog").alias("catalog"),
            F.col("table_schema").alias("schema"),
            F.col("table_name").alias("table"),
            F.col("table_type").alias("table_type"),
        )

        filtered = self._filter_tables(
            tables_df=tables_df,
            table_name=table_name,
            exact_match=exact_match,
        )

        ranked = (
            filtered.withColumn(
                "rank_score",
                F.when(F.lower(F.col("table")) == F.lit(table_name.lower()), F.lit(0))
                .when(F.lower(F.col("table")).startswith(table_name.lower()), F.lit(1))
                .otherwise(F.lit(2)),
            )
            .orderBy("rank_score", "catalog", "schema", "table")
            .limit(limit)
        )

        return self._collect_matches(ranked.collect())

    def _search_hive_metastore(
        self,
        *,
        table_name: str,
        exact_match: bool,
        limit: int,
    ) -> List[TableMatch]:
        """
        Search the legacy hive_metastore by iterating over visible databases.
        """

        matches: List[TableMatch] = []
        schemas = [
            row.namespace
            for row in self.spark.sql("SHOW SCHEMAS IN hive_metastore").collect()
            if row.namespace != "information_schema"
        ]

        for schema in schemas:
            if len(matches) >= limit:
                break

            tables = self.spark.sql(f"SHOW TABLES IN hive_metastore.{schema}")
            if exact_match:
                filtered = tables.filter(F.lower(F.col("tableName")) == table_name.lower())
            else:
                filtered = tables.filter(
                    F.lower(F.col("tableName")).contains(table_name.lower())
                )

            ordered_rows = filtered.orderBy("tableName").limit(limit - len(matches)).collect()

            for row in ordered_rows:
                matches.append(
                    TableMatch(
                        catalog="hive_metastore",
                        schema=schema,
                        table=row.tableName,
                        table_type="MANAGED" if not row.isTemporary else "TEMPORARY",
                    )
                )

        return matches

    @staticmethod
    def _filter_tables(tables_df, *, table_name: str, exact_match: bool):
        lowered_table = F.lower(F.col("table"))
        lowered_query = table_name.lower()

        if exact_match:
            return tables_df.filter(lowered_table == lowered_query)

        return tables_df.filter(lowered_table.contains(lowered_query))

    @staticmethod
    def _collect_matches(rows: Iterable) -> List[TableMatch]:
        return [
            TableMatch(
                catalog=row.catalog,
                schema=row.schema,
                table=row.table,
                table_type=row.table_type,
            )
            for row in rows
        ]


def create_agent(spark: SparkSession) -> DatabricksTableAgent:
    return DatabricksTableAgent(spark)


if __name__ == "__main__":
    agent = create_agent(spark)  # type: ignore[name-defined]
    print(agent.answer("orders"))
