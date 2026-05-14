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


import time
import requests

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"


def get_data(
    endpoint: str,
    params: dict | None = None,
    timeout: int = 20,
    retries: int = 3,
    sleep_seconds: int = 2,
) -> dict:

    url = f"{BASE_URL}{endpoint}"

    last_exception = None

    for attempt in range(retries):

        try:

            response = requests.get(
                url,
                params=params,
                timeout=timeout,
            )

            response.raise_for_status()

            return response.json()

        except Exception as e:

            last_exception = e

            print(
                f"Attempt {attempt + 1}/{retries} failed: {str(e)}"
            )

            if attempt < retries - 1:
                time.sleep(sleep_seconds)

    raise last_exception