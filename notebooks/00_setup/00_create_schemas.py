# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Layer — Streaming Objects Initialization
# MAGIC
# MAGIC **Notebook:** 00_create_schemas
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

# ------------------------------------------------------------------------------
# Notebook: 00_create_schemas
# Layer: Setup
# Author: Bruno Souza
#
# Description:
# Creates the schemas required for the data pipeline.
#
# Context:
# Ensures that all layers (Bronze, Silver, Gold) and support schemas
# (monitoring, config) exist before any ingestion or transformation runs.
#
# Notes:
# - Safe to re-run (idempotent)
# - Must be executed before pipeline execution
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS config;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS monitoring;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_curated;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_base;