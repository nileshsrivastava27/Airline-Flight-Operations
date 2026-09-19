# Databricks notebook source

# MAGIC %md
# MAGIC # Airline Intelligent Data Platform — Full Pipeline Execution Guide
# MAGIC
# MAGIC This notebook runs the entire Medallion pipeline end-to-end on a
# MAGIC **Databricks free trial** workspace. Follow each step in order.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Upload this entire repo to Databricks via **Repos** → Connect to Git
# MAGIC - Use a cluster with **Databricks Runtime 13.3 LTS** or later
# MAGIC - Cluster must have at least 1 worker (single-node is fine for free trial)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 0: Upload Repo to Databricks
# MAGIC
# MAGIC 1. Go to **Workspace** → **Repos** → **Add Repo**
# MAGIC 2. Paste: `https://github.com/nileshsrivastava27/Airline-Flight-Operations.git`
# MAGIC 3. Select branch: `main` (after merging feature/genai)
# MAGIC 4. Click **Create Repo**
# MAGIC
# MAGIC Your repo path will be:
# MAGIC `/Workspace/Repos/<your-email>/Airline-Flight-Operations`
# MAGIC
# MAGIC Set it below:

# COMMAND ----------

REPO_ROOT = "/Workspace/Repos/nileshsrivastava27/Airline-Flight-Operations"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Create Schemas (DDL)
# MAGIC
# MAGIC Run each DDL script to set up the Unity Catalog schemas and tables.
# MAGIC On free trial with `hive_metastore`, the catalog is already available.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a: Create schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS airline_ops;

# COMMAND ----------

# On free trial, tables go into hive_metastore. We simulate the schema
# layout by creating databases that mirror the Unity Catalog structure.
for schema in ["bronze", "silver", "gold", "audit"]:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS airline_ops_{schema}")
    print(f"Created: airline_ops_{schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b: Run DDL scripts
# MAGIC
# MAGIC **Important — Free Trial Adjustment:**
# MAGIC On free trial you use `hive_metastore` instead of Unity Catalog.
# MAGIC The DDL files reference `airline_ops.bronze.*` etc. You have two options:
# MAGIC
# MAGIC 1. **Find & replace** `airline_ops.bronze` → `airline_ops_bronze` (etc.) in the SQL files
# MAGIC 2. **Or** run the DDL manually using the cells below
# MAGIC
# MAGIC We'll use option 2 — run inline DDL that works on free trial:

# COMMAND ----------

# Bronze tables
spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_bronze.flight_operations_raw (
    flight_id STRING,
    airline_code STRING,
    flight_number STRING,
    origin_airport STRING,
    destination_airport STRING,
    scheduled_departure TIMESTAMP,
    actual_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    departure_delay_minutes INT,
    arrival_delay_minutes INT,
    flight_status STRING,
    cancellation_code STRING,
    aircraft_tail_number STRING,
    passengers INT,
    _rescued_data STRING,
    _source_file STRING,
    _ingestion_timestamp TIMESTAMP,
    _batch_id STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_bronze.airports_raw (
    airport_code STRING,
    airport_name STRING,
    city STRING,
    state STRING,
    country STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timezone STRING,
    _rescued_data STRING,
    _source_file STRING,
    _ingestion_timestamp TIMESTAMP,
    _batch_id STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_bronze.aircraft_reference_raw (
    tail_number STRING,
    aircraft_type STRING,
    manufacturer STRING,
    model STRING,
    year_built INT,
    seat_capacity INT,
    airline_code STRING,
    _rescued_data STRING,
    _source_file STRING,
    _ingestion_timestamp TIMESTAMP,
    _batch_id STRING
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_bronze.weather_metar_raw (
    station_id STRING,
    observation_time TIMESTAMP,
    temperature_c DOUBLE,
    dewpoint_c DOUBLE,
    wind_speed_kt INT,
    wind_direction_deg INT,
    visibility_miles DOUBLE,
    ceiling_ft INT,
    weather_condition STRING,
    raw_metar STRING,
    _rescued_data STRING,
    _source_file STRING,
    _ingestion_timestamp TIMESTAMP,
    _batch_id STRING
) USING DELTA
""")

print("Bronze tables created.")

# COMMAND ----------

# Rejected tables
for table in ["flight_operations", "airports", "aircraft_reference", "weather_metar"]:
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS airline_ops_bronze.{table}_rejected (
        source_table STRING,
        source_system STRING,
        source_file_name STRING,
        rescued_data STRING,
        raw_payload STRING,
        batch_id STRING,
        rejection_timestamp TIMESTAMP
    ) USING DELTA
    """)
print("Rejected tables created.")

# COMMAND ----------

# Audit table
spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_audit.pipeline_run_log (
    run_id STRING,
    pipeline_name STRING,
    target_table STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    rows_written LONG,
    rows_removed LONG,
    rows_rescued LONG,
    batch_id STRING,
    error_message STRING,
    metadata STRING
) USING DELTA
""")
print("Audit table created.")

# COMMAND ----------

# Silver tables
spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_silver.flight_operations (
    flight_id STRING,
    airline_code STRING,
    flight_number STRING,
    origin_airport STRING,
    destination_airport STRING,
    scheduled_departure TIMESTAMP,
    actual_departure TIMESTAMP,
    scheduled_arrival TIMESTAMP,
    actual_arrival TIMESTAMP,
    departure_delay_minutes INT,
    arrival_delay_minutes INT,
    flight_status STRING,
    cancellation_code STRING,
    aircraft_tail_number STRING,
    passengers INT,
    route STRING,
    is_delayed BOOLEAN,
    delay_category STRING,
    _batch_id STRING,
    _silver_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_silver.airports (
    airport_code STRING,
    airport_name STRING,
    city STRING,
    state STRING,
    country STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    timezone STRING,
    _batch_id STRING,
    _silver_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_silver.aircraft_reference (
    tail_number STRING,
    aircraft_type STRING,
    manufacturer STRING,
    model STRING,
    year_built INT,
    seat_capacity INT,
    airline_code STRING,
    _batch_id STRING,
    _silver_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_silver.weather_metar (
    station_id STRING,
    observation_time TIMESTAMP,
    temperature_c DOUBLE,
    dewpoint_c DOUBLE,
    wind_speed_kt INT,
    wind_direction_deg INT,
    visibility_miles DOUBLE,
    ceiling_ft INT,
    weather_condition STRING,
    _batch_id STRING,
    _silver_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_silver.quarantine (
    source_table STRING,
    record_json STRING,
    quality_issue STRING,
    quarantine_timestamp TIMESTAMP,
    _batch_id STRING
) USING DELTA
""")

print("Silver tables created.")

# COMMAND ----------

# Gold tables
spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_gold.ontime_performance (
    airline_code STRING,
    total_flights LONG,
    delayed_flights LONG,
    ontime_pct DOUBLE,
    avg_departure_delay DOUBLE,
    avg_arrival_delay DOUBLE,
    report_date DATE,
    _gold_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_gold.route_delay_summary (
    route STRING,
    origin_airport STRING,
    destination_airport STRING,
    total_flights LONG,
    avg_delay_minutes DOUBLE,
    max_delay_minutes INT,
    total_delayed_flights LONG,
    report_date DATE,
    _gold_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_gold.airport_delay_summary (
    airport_code STRING,
    total_departures LONG,
    total_arrivals LONG,
    avg_departure_delay DOUBLE,
    avg_arrival_delay DOUBLE,
    report_date DATE,
    _gold_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_gold.cancellation_summary (
    airline_code STRING,
    cancellation_code STRING,
    total_cancellations LONG,
    report_date DATE,
    _gold_timestamp TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS airline_ops_gold.aircraft_performance (
    tail_number STRING,
    aircraft_type STRING,
    total_flights LONG,
    avg_delay_minutes DOUBLE,
    total_passengers LONG,
    report_date DATE,
    _gold_timestamp TIMESTAMP
) USING DELTA
""")

print("Gold tables created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Bronze Ingestion
# MAGIC
# MAGIC Load raw CSV/JSONL files into Bronze Delta tables.

# COMMAND ----------

# MAGIC %run ./pipeline/databricks_pipeline_reliability

# COMMAND ----------

# MAGIC %run ./pipeline/databricks_bronze_ingestion

# COMMAND ----------

data_root = REPO_ROOT
specs = build_default_dataset_specs(data_root, variant="csv")

job = BronzeIngestionJob(spark)
results = job.ingest_all(specs, batch_id="batch_initial_001")

for r in results:
    print(f"  {r.dataset_name}: {r.rows_written} rows written, {r.rows_rescued} rescued, status={r.status}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Bronze

# COMMAND ----------

display(spark.sql("""
SELECT 'flight_operations_raw' AS table_name, COUNT(*) AS row_count FROM airline_ops_bronze.flight_operations_raw
UNION ALL
SELECT 'airports_raw', COUNT(*) FROM airline_ops_bronze.airports_raw
UNION ALL
SELECT 'aircraft_reference_raw', COUNT(*) FROM airline_ops_bronze.aircraft_reference_raw
UNION ALL
SELECT 'weather_metar_raw', COUNT(*) FROM airline_ops_bronze.weather_metar_raw
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected counts:**
# MAGIC - flight_operations_raw: ~720
# MAGIC - airports_raw: ~8
# MAGIC - aircraft_reference_raw: ~4
# MAGIC - weather_metar_raw: ~270
# MAGIC
# MAGIC Take a screenshot of this result for your portfolio.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Silver Transformations

# COMMAND ----------

# MAGIC %run ./pipeline/databricks_silver_transformation

# COMMAND ----------

silver_job = SilverTransformationJob(spark)
silver_results = silver_job.run_all()

for r in silver_results:
    print(f"  {r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Silver

# COMMAND ----------

display(spark.sql("""
SELECT 'flight_operations' AS table_name, COUNT(*) AS row_count FROM airline_ops_silver.flight_operations
UNION ALL
SELECT 'airports', COUNT(*) FROM airline_ops_silver.airports
UNION ALL
SELECT 'aircraft_reference', COUNT(*) FROM airline_ops_silver.aircraft_reference
UNION ALL
SELECT 'weather_metar', COUNT(*) FROM airline_ops_silver.weather_metar
"""))

# COMMAND ----------

# Sample: check delay categorization worked
display(spark.sql("""
SELECT delay_category, COUNT(*) AS cnt
FROM airline_ops_silver.flight_operations
GROUP BY delay_category
ORDER BY cnt DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Gold Transformations

# COMMAND ----------

# MAGIC %run ./pipeline/databricks_gold_transformation

# COMMAND ----------

gold_job = GoldTransformationJob(spark)
gold_results = gold_job.run_all()

for r in gold_results:
    print(f"  {r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Validate Gold

# COMMAND ----------

display(spark.sql("SELECT * FROM airline_ops_gold.ontime_performance ORDER BY ontime_pct ASC"))

# COMMAND ----------

display(spark.sql("SELECT * FROM airline_ops_gold.route_delay_summary ORDER BY avg_delay_minutes DESC LIMIT 10"))

# COMMAND ----------

display(spark.sql("SELECT * FROM airline_ops_gold.cancellation_summary ORDER BY total_cancellations DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Check Audit Logs

# COMMAND ----------

display(spark.sql("""
SELECT pipeline_name, target_table, status, rows_written, rows_rescued,
       start_time, end_time
FROM airline_ops_audit.pipeline_run_log
ORDER BY start_time DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: GenAI Setup (Optional)
# MAGIC
# MAGIC This step checks if your workspace supports GenAI features.
# MAGIC On free trial it will detect them as unavailable and skip gracefully.

# COMMAND ----------

# MAGIC %run ./genai/genai_setup_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Layer | Status | What to screenshot |
# MAGIC |-------|--------|--------------------|
# MAGIC | DDL | Tables created | Schema browser showing all tables |
# MAGIC | Bronze | Raw data loaded | Row count validation cell output |
# MAGIC | Silver | Data cleaned & enriched | Delay category distribution |
# MAGIC | Gold | Aggregates built | On-time performance table |
# MAGIC | Audit | Pipeline logged | Audit log showing all runs |
# MAGIC | GenAI | Setup check done | Capability detection output |
# MAGIC
# MAGIC **Next steps:**
# MAGIC 1. Screenshot each validation cell for your portfolio
# MAGIC 2. Build a Databricks SQL dashboard from Gold tables
# MAGIC 3. Connect Power BI to the Gold layer
# MAGIC 4. Create a Databricks Workflow linking these steps as tasks
