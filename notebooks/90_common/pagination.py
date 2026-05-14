# Databricks notebook source
# MAGIC %md
# MAGIC # Core Layer — API Pagination Utilities
# MAGIC
# MAGIC **Notebook:** `pagination`
# MAGIC
# MAGIC Provides reusable functions to handle API pagination.
# MAGIC
# MAGIC This notebook centralizes the logic required to iterate through paginated
# MAGIC endpoints of the Câmara dos Deputados Open Data API, ensuring complete and
# MAGIC consistent retrieval of large datasets across ingestion pipelines.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Handle page-based API pagination workflows
# MAGIC - Support iterative retrieval of large API datasets
# MAGIC - Control pagination parameters such as `pagina` and `itens`
# MAGIC - Detect pagination termination conditions based on API responses
# MAGIC - Standardize pagination logic across ingestion notebooks
# MAGIC - Support reusable ingestion execution patterns for large-volume endpoints
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Supports page-based pagination using `pagina` and `itens`
# MAGIC - Handles loop termination based on API response size
# MAGIC - Designed to be used with or without retry logic
# MAGIC - Used across ingestion pipelines for large datasets

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