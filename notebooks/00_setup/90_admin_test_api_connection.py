# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 90_admin_test_api_connection
# Layer: Admin
# Author: Bruno Souza
#
# Description:
# Validates connectivity with the Câmara dos Deputados Open Data API.
#
# Context:
# Used to verify network access, endpoint availability and basic API response
# structure before executing ingestion pipelines.
#
# Notes:
# - Intended for operational validation and troubleshooting
# - Does not persist data
# - Can be executed independently at any time
# ------------------------------------------------------------------------------

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