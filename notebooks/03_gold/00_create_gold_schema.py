# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 00_create_gold_schema
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Initializes the Gold analytical layer used by the dimensional Star Schema.
#
# Context:
# This notebook creates the Gold database/schema responsible for storing
# curated analytical dimensions and fact tables consumed by BI, dashboards,
# KPIs and analytical workloads.
#
# Responsibilities:
# - Create the Gold database if it does not exist
# - Establish the analytical layer for dimensional modeling
# - Support Star Schema organization for business analytics
# - Provide centralized storage for dimensions and fact tables
#
# Gold Layer Scope:
# - Conformed dimensions
# - Analytical fact tables
# - Business-oriented aggregations
# - BI-ready datasets
#
# Target:
# gold
# ------------------------------------------------------------------------------

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS gold")