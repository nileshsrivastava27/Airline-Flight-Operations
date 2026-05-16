import unittest

from databricks_gold_transformation import GoldWriteSpec, create_job


class GoldTransformationModuleTests(unittest.TestCase):
    def test_create_job_returns_transformation_job(self):
        spark = object()
        job = create_job(spark)

        self.assertIs(job.spark, spark)

    def test_gold_write_spec_defaults(self):
        spec = GoldWriteSpec(
            target_table="airline_ops.gold.on_time_performance_daily",
        )

        self.assertEqual(spec.write_mode, "overwrite")


if __name__ == "__main__":
    unittest.main()
