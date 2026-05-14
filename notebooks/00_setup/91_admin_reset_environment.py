# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Layer — Environment Reset and Reprocessing Support
# MAGIC
# MAGIC **Notebook:** `91_admin_reset_environment`
# MAGIC
# MAGIC Resets the data pipeline environment by clearing tables and execution state.
# MAGIC
# MAGIC This notebook is used to support reprocessing scenarios, testing cycles and
# MAGIC full environment reinitialization during development, validation and pipeline
# MAGIC troubleshooting activities.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Clear configured pipeline tables and execution state
# MAGIC - Support full or partial environment reprocessing
# MAGIC - Reset operational monitoring and ingestion control data
# MAGIC - Assist development and testing workflows
# MAGIC - Preserve schemas and table structures for rapid re-execution
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Truncates data from configured schemas such as Bronze and Monitoring
# MAGIC - Does not drop schemas or table structures
# MAGIC - Should be used with caution in controlled environments only

# COMMAND ----------

schemas_to_clean = ["bronze", "monitoring"]

for schema in schemas_to_clean:
    tables = spark.sql(f"SHOW TABLES IN {schema}").collect()

    for table in tables:
        table_name = table.tableName
        full_name = f"{schema}.{table_name}"

        try:
            spark.sql(f"TRUNCATE TABLE {full_name}")
            print(f"TRUNCATED: {full_name}")
        except Exception as e:
            print(f"ERROR truncating {full_name}: {e}")