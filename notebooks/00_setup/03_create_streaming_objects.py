# Databricks notebook source
# MAGIC %sql
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Notebook: 03_create_streaming_objects
# MAGIC -- Layer: Setup
# MAGIC --
# MAGIC -- Description:
# MAGIC -- Creates schemas and Delta tables required by the optional real-time voting
# MAGIC -- micro-batch pipeline.
# MAGIC -- -----------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS control;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_stream;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_stream;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_stream;
# MAGIC CREATE SCHEMA IF NOT EXISTS monitoring;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.streaming_dlt;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS control.votacoes_stream_offset (
# MAGIC
# MAGIC     endpoint STRING,
# MAGIC     last_processed_id STRING,
# MAGIC     last_processed_ts TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO control.votacoes_stream_offset
# MAGIC VALUES (
# MAGIC     '/votacoes',
# MAGIC     '0',
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS bronze_stream.votacoes_raw (
# MAGIC     vota_id_votacao STRING,
# MAGIC     bronze_tx_endpoint STRING,
# MAGIC     bronze_id_batch STRING,
# MAGIC     bronze_ts_ingestao TIMESTAMP,
# MAGIC     bronze_dt_ingestao DATE,
# MAGIC     bronze_tx_payload STRING,
# MAGIC     bronze_tx_record_hash STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS monitoring.pipeline_log (
# MAGIC
# MAGIC     pipeline_name STRING,
# MAGIC     batch_id STRING,
# MAGIC     started_at TIMESTAMP,
# MAGIC     finished_at TIMESTAMP,
# MAGIC     status STRING,
# MAGIC     records_read BIGINT,
# MAGIC     records_written BIGINT,
# MAGIC     records_discarded BIGINT,
# MAGIC     error_message STRING
# MAGIC )
# MAGIC USING DELTA;