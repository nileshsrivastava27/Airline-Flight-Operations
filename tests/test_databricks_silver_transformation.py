import unittest

from databricks_silver_transformation import SilverWriteSpec, create_job


class SilverTransformationModuleTests(unittest.TestCase):
    def test_create_job_returns_transformation_job(self):
        spark = object()
        job = create_job(spark)

        self.assertIs(job.spark, spark)

    def test_silver_write_spec_defaults(self):
        spec = SilverWriteSpec(
            target_table="airline_ops.silver.airports_clean",
            temp_view="airports_stage",
        )

        self.assertEqual(spec.identity_columns, ())
        self.assertEqual(spec.temp_view, "airports_stage")


if __name__ == "__main__":
    unittest.main()
