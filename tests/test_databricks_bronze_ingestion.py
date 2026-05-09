import unittest

from databricks_bronze_ingestion import build_default_dataset_specs, normalize_source_format


class BronzeIngestionSpecTests(unittest.TestCase):
    def test_normalize_source_format_maps_jsonl_to_json(self):
        self.assertEqual(normalize_source_format("jsonl"), "json")

    def test_normalize_source_format_rejects_unknown_value(self):
        with self.assertRaises(ValueError):
            normalize_source_format("parquet")

    def test_build_default_dataset_specs_csv_variant(self):
        specs = build_default_dataset_specs("/tmp/airline-project", "csv")

        self.assertEqual(len(specs), 4)
        self.assertEqual(specs[0].source_format, "csv")
        self.assertEqual(specs[0].source_path, "/tmp/airline-project/data/raw/flight_operations")
        self.assertEqual(specs[0].target_table, "airline_ops.bronze.flight_operations_raw")

    def test_build_default_dataset_specs_jsonl_variant(self):
        specs = build_default_dataset_specs("/tmp/airline-project", "jsonl")

        self.assertEqual(len(specs), 4)
        self.assertEqual(specs[3].source_format, "jsonl")
        self.assertEqual(specs[3].source_path, "/tmp/airline-project/data/raw_json/weather_metar")

    def test_build_default_dataset_specs_mixed_variant(self):
        specs = build_default_dataset_specs("/tmp/airline-project", "mixed")

        self.assertEqual([spec.source_format for spec in specs], ["csv", "csv", "csv", "jsonl"])
        self.assertEqual(specs[3].target_table, "airline_ops.bronze.weather_metar_raw")


if __name__ == "__main__":
    unittest.main()
