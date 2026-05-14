# Databricks notebook source
# MAGIC %md
# MAGIC # Core Layer — Câmara Open Data API Client
# MAGIC
# MAGIC **Notebook:** `api_client`
# MAGIC
# MAGIC Provides reusable functions to interact with the Câmara dos Deputados
# MAGIC Open Data API.
# MAGIC
# MAGIC This module centralizes HTTP request handling, pagination, retries and timeout
# MAGIC control used across Bronze ingestion pipelines and operational data collection
# MAGIC workflows.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Execute HTTP GET requests against the Câmara Open Data API
# MAGIC - Handle API parameter construction and transmission
# MAGIC - Support paginated API extraction workflows
# MAGIC - Implement retry logic for resilient ingestion execution
# MAGIC - Control request timeout and connection handling
# MAGIC - Standardize API interaction logic across ingestion notebooks
# MAGIC - Support reusable ingestion patterns across Bronze pipelines
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Supports GET requests with parameter handling
# MAGIC - Includes retry logic for resilience
# MAGIC - Handles pagination for large datasets
# MAGIC - Designed to be reused across all Bronze ingestion notebooks

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