# Databricks Table Agent

This helper is meant for a Databricks notebook or a Databricks Python file.

It searches visible tables in Unity Catalog and returns the fully qualified
table path in `catalog.schema.table` format.

## Files

- `databricks_table_agent.py`: notebook-friendly helper class

## Use in a Databricks notebook

```python
from databricks_table_agent import create_agent

agent = create_agent(spark)

# Return one best match
best_match = agent.find_best_match("customers")
print(best_match)

# Return multiple matches
matches = agent.find_table("customers")
for match in matches:
    print(match.full_path, match.table_type)

# Agent-style answer
print(agent.answer("customers"))
```

## Notes

- Unity Catalog search uses `system.information_schema.tables`.
- This only returns tables you have permission to view.
- If you also want the legacy metastore, set
  `include_hive_metastore=True`.
