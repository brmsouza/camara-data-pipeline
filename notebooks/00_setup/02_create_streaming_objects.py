# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — Streaming Objects Initialization
# MAGIC
# MAGIC **Notebook:** `03_create_streaming_objects`
# MAGIC
# MAGIC Creates schemas and Delta tables required by the optional real-time voting
# MAGIC micro-batch pipeline.
# MAGIC
# MAGIC This notebook prepares the supporting structures used by the streaming voting
# MAGIC architecture, enabling controlled ingestion, processing and monitoring of
# MAGIC near real-time parliamentary voting data.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Create schemas required by the streaming pipeline
# MAGIC - Create Delta tables used by the voting micro-batch workflow
# MAGIC - Support incremental ingestion of voting data
# MAGIC - Prepare storage structures for Bronze, Silver and Gold streaming outputs
# MAGIC - Enable operational monitoring of the optional real-time voting pipeline
# MAGIC - Support replay and reprocessing strategies for voting micro-batches
# MAGIC
# MAGIC **Target:** Streaming schemas and Delta control objects

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