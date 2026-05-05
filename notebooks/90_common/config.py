# Databricks notebook source
# ------------------------------------------------------------------------------
# Module: config
# Layer: Core
# Author: Bruno Souza
#
# Description:
# Defines global configuration parameters used across the data pipeline.
#
# Context:
# Centralizes environment variables, API settings, ingestion parameters
# and default values to ensure consistency and simplify maintenance.
#
# Notes:
# - Includes API base URL, timeouts and pagination settings
# - Defines analysis parameters (e.g., SELECT_ANOS, legislaturas)
# - Contains volume paths and environment-specific configurations
# - Imported by ingestion and utility modules
# ------------------------------------------------------------------------------

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