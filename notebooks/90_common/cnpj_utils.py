# Databricks notebook source
# MAGIC %md
# MAGIC # Core Layer — CNPJ Validation Utilities
# MAGIC
# MAGIC **Notebook:** `cnpj_utils`
# MAGIC
# MAGIC Provides reusable utilities for CNPJ validation, normalization and supplier risk enrichment.
# MAGIC
# MAGIC This notebook centralizes CNPJ-related helper functions used to standardize
# MAGIC supplier documents, validate Brazilian legal entity identifiers and support
# MAGIC supplier quality checks across CEAP expense and supplier pipelines.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Normalize CNPJ values into a consistent format
# MAGIC - Remove punctuation and non-numeric characters from CNPJ fields
# MAGIC - Validate CNPJ structure and check digits
# MAGIC - Identify invalid, missing or suspicious supplier documents
# MAGIC - Support supplier data quality rules in CEAP expense pipelines
# MAGIC - Provide reusable utilities for supplier enrichment and risk analysis
# MAGIC - Standardize CNPJ handling across Bronze, Silver and Gold workflows
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Designed to support supplier and CEAP expense processing
# MAGIC - Used by ingestion, curation and Gold supplier dimension notebooks
# MAGIC - Does not persist data directly
# MAGIC - Provides reusable validation functions for CNPJ-related transformations

# COMMAND ----------

import re
import time
import requests


API_BASE_URL = "https://brasilapi.com.br/api/cnpj/v1"

REQUEST_TIMEOUT_SECONDS = 15
MAX_RETRIES = 3
RETRY_SLEEP_SECONDS = 2


def clean_document(document):
    if document is None:
        return None

    digits = re.sub(r"[^0-9]", "", str(document))

    if len(digits) == 0:
        return None

    return digits


def is_repeated_document(document):
    document = clean_document(document)

    if document is None:
        return False

    return document == document[0] * len(document)


def fetch_cnpj_data(cnpj):
    cnpj = clean_document(cnpj)

    if cnpj is None:
        return {
            "cnpj_consulta_status": "INVALID_FORMAT",
            "cnpj_situacao_cadastral": None,
            "cnpj_razao_social": None,
            "cnpj_nome_fantasia": None,
            "cnpj_cnae_principal": None,
            "cnpj_uf": None,
            "cnpj_municipio": None,
            "cnpj_porte": None,
            "cnpj_capital_social": None,
            "cnpj_api_http_status": None,
            "cnpj_api_error": "Invalid CNPJ format"
        }

    if len(cnpj) != 14:
        return {
            "cnpj_consulta_status": "INVALID_FORMAT",
            "cnpj_situacao_cadastral": None,
            "cnpj_razao_social": None,
            "cnpj_nome_fantasia": None,
            "cnpj_cnae_principal": None,
            "cnpj_uf": None,
            "cnpj_municipio": None,
            "cnpj_porte": None,
            "cnpj_capital_social": None,
            "cnpj_api_http_status": None,
            "cnpj_api_error": "CNPJ must have 14 digits"
        }

    if is_repeated_document(cnpj):
        return {
            "cnpj_consulta_status": "INVALID_FORMAT",
            "cnpj_situacao_cadastral": None,
            "cnpj_razao_social": None,
            "cnpj_nome_fantasia": None,
            "cnpj_cnae_principal": None,
            "cnpj_uf": None,
            "cnpj_municipio": None,
            "cnpj_porte": None,
            "cnpj_capital_social": None,
            "cnpj_api_http_status": None,
            "cnpj_api_error": "Repeated digits CNPJ"
        }

    url = f"{API_BASE_URL}/{cnpj}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                timeout=REQUEST_TIMEOUT_SECONDS
            )

            if response.status_code == 200:
                payload = response.json()

                return {
                    "cnpj_consulta_status": "FOUND",
                    "cnpj_situacao_cadastral": payload.get("descricao_situacao_cadastral"),
                    "cnpj_razao_social": payload.get("razao_social"),
                    "cnpj_nome_fantasia": payload.get("nome_fantasia"),
                    "cnpj_cnae_principal": payload.get("cnae_fiscal_descricao"),
                    "cnpj_uf": payload.get("uf"),
                    "cnpj_municipio": payload.get("municipio"),
                    "cnpj_porte": payload.get("porte"),
                    "cnpj_capital_social": payload.get("capital_social"),
                    "cnpj_api_http_status": response.status_code,
                    "cnpj_api_error": None
                }

            if response.status_code == 404:
                return {
                    "cnpj_consulta_status": "NOT_FOUND",
                    "cnpj_situacao_cadastral": None,
                    "cnpj_razao_social": None,
                    "cnpj_nome_fantasia": None,
                    "cnpj_cnae_principal": None,
                    "cnpj_uf": None,
                    "cnpj_municipio": None,
                    "cnpj_porte": None,
                    "cnpj_capital_social": None,
                    "cnpj_api_http_status": response.status_code,
                    "cnpj_api_error": "CNPJ not found"
                }

            if attempt == MAX_RETRIES:
                return {
                    "cnpj_consulta_status": "ERROR",
                    "cnpj_situacao_cadastral": None,
                    "cnpj_razao_social": None,
                    "cnpj_nome_fantasia": None,
                    "cnpj_cnae_principal": None,
                    "cnpj_uf": None,
                    "cnpj_municipio": None,
                    "cnpj_porte": None,
                    "cnpj_capital_social": None,
                    "cnpj_api_http_status": response.status_code,
                    "cnpj_api_error": response.text[:500]
                }

        except Exception as e:
            if attempt == MAX_RETRIES:
                return {
                    "cnpj_consulta_status": "ERROR",
                    "cnpj_situacao_cadastral": None,
                    "cnpj_razao_social": None,
                    "cnpj_nome_fantasia": None,
                    "cnpj_cnae_principal": None,
                    "cnpj_uf": None,
                    "cnpj_municipio": None,
                    "cnpj_porte": None,
                    "cnpj_capital_social": None,
                    "cnpj_api_http_status": None,
                    "cnpj_api_error": str(e)[:500]
                }

        time.sleep(RETRY_SLEEP_SECONDS * attempt)