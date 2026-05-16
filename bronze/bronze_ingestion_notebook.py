# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Ingestion Notebook
# MAGIC
# MAGIC This notebook loads the synthetic airline source data from CSV and JSONL
# MAGIC files into the Databricks Bronze Delta tables.

# COMMAND ----------

from pyspark.sql import functions as F

project_root = "/Workspace/Users/nileshsrivastava20@gmail.com/Airline-Flight-Operations"

csv_root = f"file:{project_root}/data/raw"
json_root = f"file:{project_root}/data/raw_json"

batch_id = "batch_20260516_001"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Operations from CSV

# COMMAND ----------

flight_csv_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{csv_root}/flight_operations")
)

flight_csv_df = (
    flight_csv_df.withColumn(
        "source_file_name",
        F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+$)", 1),
    )
    .withColumn("source_system", F.lit("synthetic_airline_ops"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
)

display(flight_csv_df.limit(10))

flight_csv_df.write.format("delta").mode("append").saveAsTable(
    "airline_ops.bronze.flight_operations_raw"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flight Operations from JSONL

# COMMAND ----------

flight_json_df = spark.read.format("json").load(f"{json_root}/flight_operations")

flight_json_df = (
    flight_json_df.withColumn(
        "source_file_name",
        F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+$)", 1),
    )
    .withColumn("source_system", F.lit("synthetic_airline_ops"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
)

display(flight_json_df.limit(10))

flight_json_df.write.format("delta").mode("append").saveAsTable(
    "airline_ops.bronze.flight_operations_raw"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Airports from CSV

# COMMAND ----------

airports_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{csv_root}/airports")
)

airports_df = (
    airports_df.withColumn(
        "source_file_name",
        F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+$)", 1),
    )
    .withColumn("source_system", F.lit("synthetic_airports"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
)

display(airports_df.limit(10))

airports_df.write.format("delta").mode("append").saveAsTable(
    "airline_ops.bronze.airports_raw"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aircraft Reference from CSV

# COMMAND ----------

aircraft_df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{csv_root}/aircraft_reference")
)

aircraft_df = (
    aircraft_df.withColumn(
        "source_file_name",
        F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+$)", 1),
    )
    .withColumn("source_system", F.lit("synthetic_aircraft"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
)

display(aircraft_df.limit(10))

aircraft_df.write.format("delta").mode("append").saveAsTable(
    "airline_ops.bronze.aircraft_reference_raw"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Weather from JSONL

# COMMAND ----------

weather_df = spark.read.format("json").load(f"{json_root}/weather_metar")

weather_df = (
    weather_df.withColumn(
        "source_file_name",
        F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+$)", 1),
    )
    .withColumn("source_system", F.lit("synthetic_metar"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("batch_id", F.lit(batch_id))
)

display(weather_df.limit(10))

weather_df.write.format("delta").mode("append").saveAsTable(
    "airline_ops.bronze.weather_metar_raw"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

spark.sql("select count(*) as flights from airline_ops.bronze.flight_operations_raw").show()
spark.sql("select count(*) as airports from airline_ops.bronze.airports_raw").show()
spark.sql("select count(*) as aircraft from airline_ops.bronze.aircraft_reference_raw").show()
spark.sql("select count(*) as weather from airline_ops.bronze.weather_metar_raw").show()
