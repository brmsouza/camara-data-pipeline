# Databricks notebook source
# ------------------------------------------------------------------------------
# Module: pagination
# Layer: Core
# Author: Bruno Souza
#
# Description:
# Provides reusable functions to handle API pagination.
#
# Context:
# Centralizes logic to iterate through paginated endpoints of the
# Câmara dos Deputados API, ensuring complete data retrieval.
#
# Notes:
# - Supports page-based pagination (pagina / itens)
# - Handles loop termination based on API response size
# - Designed to be used with or without retry logic
# - Used across ingestion pipelines for large datasets
# ------------------------------------------------------------------------------

# COMMAND ----------

def paginate(
    endpoint: str,
    params: dict | None = None,
    limit: int | None = None,
    page_size: int = 100,
    timeout: int = 20,
):
    records = []
    page = 1

    while True:
        current_params = dict(params or {})
        current_params["pagina"] = page
        current_params["itens"] = page_size

        payload = get_data(
            endpoint=endpoint,
            params=current_params,
            timeout=timeout,
        )

        page_records = payload.get("dados", [])

        if not page_records:
            break

        records.extend(page_records)

        if len(page_records) < page_size:
            break

        if limit and len(records) >= limit:
            break

        page += 1

    return records if limit is None else records[:limit]

    
import time


def paginate_with_retry(
    endpoint: str,
    params: dict | None = None,
    limit: int | None = None,
    page_size: int = 100,
    timeout: int = 120,
    retries: int = 3,
    sleep_seconds: float = 0.5,
) -> list[dict]:

    records = []
    page = 1

    while True:
        current_params = dict(params or {})
        current_params["pagina"] = page
        current_params["itens"] = page_size

        last_error = None

        for attempt in range(1, retries + 1):
            try:
                payload = get_data(
                    endpoint=endpoint,
                    params=current_params,
                    timeout=timeout,
                )
                break

            except Exception as e:
                last_error = e

                if attempt == retries:
                    raise last_error

                time.sleep(sleep_seconds * attempt)

        page_records = payload.get("dados", [])

        if not page_records:
            break

        records.extend(page_records)

        if len(page_records) < page_size:
            break

        if limit is not None and len(records) >= limit:
            break

        page += 1

        time.sleep(sleep_seconds)

    return records if limit is None else records[:limit]    