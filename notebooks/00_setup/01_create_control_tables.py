# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — Control and Monitoring Tables Initialization
# MAGIC
# MAGIC **Notebook:** 01_create_control_tables
# MAGIC
# MAGIC Creates the control and monitoring tables required by the data pipeline.
# MAGIC
# MAGIC Provides the structures used to track pipeline executions, operational logs
# MAGIC and ingestion control metadata across the project.
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Safe to re-run, idempotent
# MAGIC - Must be executed after schema creation
# MAGIC - Pipeline executions are logged in `monitoring.pipeline_log`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS config.ingestion_control (
# MAGIC     pipeline_name STRING,
# MAGIC     source_endpoint STRING,
# MAGIC     target_table STRING,
# MAGIC     load_strategy STRING,
# MAGIC     last_successful_batch_id STRING,
# MAGIC     last_successful_run_at TIMESTAMP,
# MAGIC     last_processed_value STRING,
# MAGIC     is_active BOOLEAN,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS monitoring.pipeline_log (
# MAGIC     log_timestamp TIMESTAMP,
# MAGIC     batch_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     layer STRING,
# MAGIC     level STRING,
# MAGIC     event_name STRING,
# MAGIC     status STRING,
# MAGIC     message STRING,
# MAGIC     endpoint STRING,
# MAGIC     target_table STRING,
# MAGIC     records_read BIGINT,
# MAGIC     records_written BIGINT,
# MAGIC     started_at TIMESTAMP,
# MAGIC     finished_at TIMESTAMP,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA;