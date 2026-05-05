# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 91_admin_reset_environment
# Layer: Admin
# Author: Bruno Souza
#
# Description:
# Resets the data pipeline environment by clearing tables and execution state.
#
# Context:
# Used to support reprocessing scenarios, testing cycles or full environment
# reinitialization during development and validation.
#
# Notes:
# - Truncates data from configured schemas (e.g., bronze, monitoring)
# - Does not drop schemas or table structures
# - Should be used with caution in controlled environments only
# ------------------------------------------------------------------------------

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