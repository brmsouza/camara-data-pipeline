# Databricks notebook source
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