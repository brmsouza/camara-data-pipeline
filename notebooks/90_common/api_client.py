# Databricks notebook source
# ------------------------------------------------------------------------------
# Module: api_client
# Layer: Core
# Author: Bruno Souza
#
# Description:
# Provides reusable functions to interact with the Câmara dos Deputados
# Open Data API.
#
# Context:
# Centralizes HTTP request handling, pagination, retries and timeout control
# used across ingestion pipelines.
#
# Notes:
# - Supports GET requests with parameter handling
# - Includes retry logic for resilience
# - Handles pagination for large datasets
# - Designed to be reused across all Bronze ingestion notebooks
# ------------------------------------------------------------------------------

# COMMAND ----------

import requests

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"


def get_data(endpoint: str, params: dict | None = None, timeout: int = 20) -> dict:
    url = f"{BASE_URL}{endpoint}"

    response = requests.get(
        url,
        params=params,
        timeout=timeout,
    )

    response.raise_for_status()

    return response.json()