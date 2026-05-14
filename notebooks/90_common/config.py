# Databricks notebook source
# MAGIC %md
# MAGIC # Core Layer — Global Pipeline Configuration
# MAGIC
# MAGIC **Notebook:** `config`
# MAGIC
# MAGIC Defines global configuration parameters used across the data pipeline.
# MAGIC
# MAGIC This notebook centralizes environment variables, API settings, ingestion
# MAGIC parameters and default values to ensure consistency, reusability and simplified
# MAGIC maintenance across ingestion, transformation and analytical workflows.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Centralize global pipeline configuration parameters
# MAGIC - Define API connection and pagination settings
# MAGIC - Configure ingestion execution parameters
# MAGIC - Store analysis configuration values such as selected years and legislatures
# MAGIC - Define environment-specific paths and volume locations
# MAGIC - Standardize reusable configuration across notebooks and utility modules
# MAGIC - Support centralized maintenance of operational parameters
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Includes API base URL, timeout and pagination settings
# MAGIC - Defines analytical parameters such as `SELECT_ANOS` and legislaturas
# MAGIC - Contains volume paths and environment-specific configurations
# MAGIC - Imported by ingestion and utility notebooks

# COMMAND ----------

#Legislatura = 57  # 2023-2027
#Legislatura = 56  # 2019-2023

# Legislaturas padrão do projeto
LEGISLATURAS_PADRAO = [57, 56]

# Padrões de paginação
DEFAULT_PAGE_SIZE = 100
DEFAULT_TIMEOUT = 60

SELECT_ANOS = [2022, 2023, 2024, 2025, 2026]

VOLUME_RAW_CAMARA = "/Volumes/camara/bronze/raw_camara"