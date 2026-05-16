# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Transformation Notebook
# MAGIC
# MAGIC This notebook transforms Silver Delta tables into Gold reporting tables
# MAGIC for the airline flight operations project.

# COMMAND ----------

from databricks_gold_transformation import GoldTransformationJob

job = GoldTransformationJob(spark)
results = job.run_all()
display(results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

spark.sql("select count(*) as on_time_performance_daily from airline_ops.gold.on_time_performance_daily").show()
spark.sql("select count(*) as route_delay_summary from airline_ops.gold.route_delay_summary").show()
spark.sql("select count(*) as airport_delay_summary from airline_ops.gold.airport_delay_summary").show()
spark.sql("select count(*) as cancellation_summary from airline_ops.gold.cancellation_summary").show()
spark.sql("select count(*) as aircraft_performance_summary from airline_ops.gold.aircraft_performance_summary").show()
