# Databricks notebook source

# MAGIC %md
# MAGIC # GenAI Feature Setup
# MAGIC
# MAGIC This notebook checks whether your Databricks workspace supports the
# MAGIC GenAI features (Vector Search, Foundation Model Serving) and sets up
# MAGIC the required schema and tables.
# MAGIC
# MAGIC **Free trial workspaces** do not have Vector Search or Model Serving.
# MAGIC Run this notebook to see what is available and enable/skip accordingly.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Check workspace capabilities

# COMMAND ----------

genai_available = True
missing_features = []

# Check Vector Search
try:
    from databricks.vector_search.client import VectorSearchClient
    vs_client = VectorSearchClient()
    vs_client.list_endpoints()
    print("Vector Search: AVAILABLE")
except Exception as e:
    genai_available = False
    missing_features.append("Vector Search")
    print(f"Vector Search: NOT AVAILABLE ({e})")

# COMMAND ----------

# Check Foundation Model Serving
try:
    import mlflow.deployments
    client = mlflow.deployments.get_deploy_client("databricks")
    client.list_endpoints()
    print("Foundation Model Serving: AVAILABLE")
except Exception as e:
    genai_available = False
    missing_features.append("Foundation Model Serving")
    print(f"Foundation Model Serving: NOT AVAILABLE ({e})")

# COMMAND ----------

if genai_available:
    print("All GenAI features are available. You can run the full GenAI pipeline.")
else:
    print(f"Missing features: {', '.join(missing_features)}")
    print()
    print("Your workspace does not support all GenAI features.")
    print("The GenAI notebooks will be skipped in pipeline jobs.")
    print()
    print("To enable GenAI, upgrade to a Databricks workspace with:")
    print("  - Vector Search endpoints")
    print("  - Foundation Model Serving (pay-per-token or provisioned)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create GenAI schema (if available)

# COMMAND ----------

if genai_available:
    spark.sql("CREATE SCHEMA IF NOT EXISTS airline_ops.genai")
    print("Created schema: airline_ops.genai")
else:
    print("Skipped — GenAI features not available on this workspace.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Store result for downstream notebooks
# MAGIC
# MAGIC Other notebooks can read this widget to decide whether to run GenAI steps.

# COMMAND ----------

dbutils.widgets.dropdown("genai_enabled", str(genai_available), ["True", "False"])
print(f"genai_enabled = {genai_available}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage in pipeline jobs
# MAGIC
# MAGIC In your Databricks workflow, add this notebook as the **first task**.
# MAGIC Then in each GenAI notebook, add this guard at the top:
# MAGIC
# MAGIC ```python
# MAGIC genai_enabled = dbutils.widgets.get("genai_enabled") == "True"
# MAGIC if not genai_enabled:
# MAGIC     dbutils.notebook.exit("GenAI skipped — not available on this workspace")
# MAGIC ```
# MAGIC
# MAGIC This lets you keep GenAI notebooks in your workflow definition.
# MAGIC They will exit cleanly on free-trial workspaces and run fully
# MAGIC on workspaces with Vector Search and Model Serving.
