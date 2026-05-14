# Databricks notebook source
# MAGIC %md
# MAGIC # Admin Layer — Open Data API Connectivity Validation
# MAGIC
# MAGIC **Notebook:** `90_admin_test_api_connection`
# MAGIC
# MAGIC Validates connectivity with the Câmara dos Deputados Open Data API.
# MAGIC
# MAGIC This notebook is used to verify network access, endpoint availability and
# MAGIC basic API response structure before executing ingestion pipelines.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Validate connectivity with the Câmara dos Deputados Open Data API
# MAGIC - Verify endpoint availability and accessibility
# MAGIC - Test API response structure and connectivity latency
# MAGIC - Support operational troubleshooting of ingestion pipelines
# MAGIC - Assist environment validation before pipeline execution
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Intended for operational validation and troubleshooting
# MAGIC - Does not persist data
# MAGIC - Can be executed independently at any time

# COMMAND ----------

import requests

url = "https://dadosabertos.camara.leg.br/api/v2/deputados"

print("Testando conexão...")

try:
    response = requests.get(
        url,
        params={"itens": 1},
        timeout=30
    )

    print("Status:", response.status_code)
    print("Resposta:", response.text[:200])

except Exception as e:
    print("Erro:", str(e))