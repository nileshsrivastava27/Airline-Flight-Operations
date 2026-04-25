import importlib.util
import sys
import types
import unittest
from pathlib import Path


def _install_pyspark_stubs() -> None:
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")

    class SparkSession:
        pass

    sql.SparkSession = SparkSession
    sql.functions = functions
    pyspark.sql = sql

    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql
    sys.modules["pyspark.sql.functions"] = functions


def _load_module():
    _install_pyspark_stubs()
    module_path = Path(__file__).resolve().parents[1] / "databricks_table_agent.py"
    spec = importlib.util.spec_from_file_location("databricks_table_agent", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MODULE = _load_module()
DatabricksTableAgent = MODULE.DatabricksTableAgent
TableMatch = MODULE.TableMatch


class DatabricksTableAgentTests(unittest.TestCase):
    def setUp(self):
        self.agent = DatabricksTableAgent(spark=object())

    def test_find_table_rejects_empty_name(self):
        with self.assertRaises(ValueError):
            self.agent.find_table("   ")

    def test_find_table_combines_unity_catalog_and_hive_results(self):
        uc_results = [
            TableMatch(
                catalog="main",
                schema="sales",
                table="orders",
                table_type="MANAGED",
            )
        ]
        hive_results = [
            TableMatch(
                catalog="hive_metastore",
                schema="legacy",
                table="orders_archive",
                table_type="MANAGED",
            )
        ]

        self.agent._search_unity_catalog = lambda **_: uc_results
        self.agent._search_hive_metastore = lambda **_: hive_results

        matches = self.agent.find_table("orders", include_hive_metastore=True, limit=5)

        self.assertEqual(
            [match.full_path for match in matches],
            ["main.sales.orders", "hive_metastore.legacy.orders_archive"],
        )

    def test_find_best_match_prefers_exact_match(self):
        expected = TableMatch(
            catalog="main",
            schema="analytics",
            table="customers",
            table_type="VIEW",
        )

        calls = []

        def fake_find_table(table_name, **kwargs):
            calls.append((table_name, kwargs["exact_match"]))
            return [expected] if kwargs["exact_match"] else []

        self.agent.find_table = fake_find_table

        best = self.agent.find_best_match("customers")

        self.assertEqual(best, "main.analytics.customers")
        self.assertEqual(calls, [("customers", True)])

    def test_find_best_match_falls_back_to_partial_search(self):
        partial = TableMatch(
            catalog="main",
            schema="analytics",
            table="customers_daily",
            table_type="MANAGED",
        )

        def fake_find_table(table_name, **kwargs):
            return [] if kwargs["exact_match"] else [partial]

        self.agent.find_table = fake_find_table

        best = self.agent.find_best_match("customer")

        self.assertEqual(best, "main.analytics.customers_daily")

    def test_answer_returns_single_match_message(self):
        self.agent.find_table = lambda *args, **kwargs: [
            TableMatch(
                catalog="main",
                schema="finance",
                table="payments",
                table_type="MANAGED",
            )
        ]

        answer = self.agent.answer("payments")

        self.assertEqual(answer, "Found table: main.finance.payments (MANAGED)")

    def test_answer_returns_multiple_matches_message(self):
        self.agent.find_table = lambda *args, **kwargs: [
            TableMatch("main", "finance", "payments", "MANAGED"),
            TableMatch("main", "finance", "payments_backup", "VIEW"),
        ]

        answer = self.agent.answer("payments")

        self.assertIn("Found 2 matching tables for 'payments':", answer)
        self.assertIn("- main.finance.payments (MANAGED)", answer)
        self.assertIn("- main.finance.payments_backup (VIEW)", answer)

    def test_answer_returns_not_found_message(self):
        self.agent.find_table = lambda *args, **kwargs: []

        answer = self.agent.answer("missing_table")

        self.assertEqual(answer, "No table found for 'missing_table'.")


if __name__ == "__main__":
    unittest.main()
