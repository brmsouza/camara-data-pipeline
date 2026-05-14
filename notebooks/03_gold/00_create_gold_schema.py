# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Gold Schema Initialization
# MAGIC
# MAGIC **Notebook:** `00_create_gold_schema`
# MAGIC
# MAGIC Initializes the Gold analytical layer used by the dimensional Star Schema.
# MAGIC
# MAGIC This notebook creates the Gold database/schema responsible for storing
# MAGIC curated analytical dimensions and fact tables consumed by BI, dashboards,
# MAGIC KPIs and analytical workloads.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Create the Gold database if it does not exist
# MAGIC - Establish the analytical layer for dimensional modeling
# MAGIC - Support Star Schema organization for business analytics
# MAGIC - Provide centralized storage for dimensions and fact tables
# MAGIC
# MAGIC ## Gold Layer Scope
# MAGIC
# MAGIC - Conformed dimensions
# MAGIC - Analytical fact tables
# MAGIC - Business-oriented aggregations
# MAGIC - BI-ready datasets
# MAGIC
# MAGIC **Target:** `gold`

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gold")