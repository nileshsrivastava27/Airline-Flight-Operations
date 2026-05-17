import unittest

from databricks_pipeline_reliability import DEFAULT_AUDIT_TABLE


class PipelineReliabilityModuleTests(unittest.TestCase):
    def test_default_audit_table_name(self):
        self.assertEqual(DEFAULT_AUDIT_TABLE, "airline_ops.audit.pipeline_run_log")


if __name__ == "__main__":
    unittest.main()
