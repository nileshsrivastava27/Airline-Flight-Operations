# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Transformation Notebook
# MAGIC
# MAGIC This notebook transforms Bronze Delta tables into cleaned Silver Delta
# MAGIC tables for the airline flight operations project.

# COMMAND ----------

from databricks_silver_transformation import SilverTransformationJob

job = SilverTransformationJob(spark)
results = job.run_all()
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

spark.sql("select count(*) as flights_clean from airline_ops.silver.flight_operations_clean").show()
spark.sql("select count(*) as flights_quarantine from airline_ops.silver.flight_operations_quarantine").show()
spark.sql("select count(*) as airports_clean from airline_ops.silver.airports_clean").show()
spark.sql("select count(*) as route_reference from airline_ops.silver.route_reference").show()
spark.sql("select count(*) as weather_clean from airline_ops.silver.weather_observations_clean").show()
