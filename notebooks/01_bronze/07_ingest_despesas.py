# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 07_ingest_despesas
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests parliamentary expense data from the Câmara dos Deputados API
# using the /deputados/{id}/despesas endpoint.
#
# Context:
# Expenses are extracted for each deputy and configured year, supporting
# CEAP analysis by deputy, supplier, expense category and reference period.
#
# Notes:
# - Full load by deputy and year
# - Data retrieved with pagination and retry support
# - Each record is enriched with id_deputado and ano_referencia
# - One API extraction cycle per deputy/year combination
# - Data persisted in Delta (append mode)
# - Execution logged in monitoring.pipeline_log
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

# MAGIC %run ../90_common/api_client

# COMMAND ----------

# MAGIC %run ../90_common/bronze_writer

# COMMAND ----------

# MAGIC %run ../90_common/config

# COMMAND ----------

# MAGIC %run ../90_common/pagination

# COMMAND ----------

import time
import uuid
from datetime import datetime

# Pipeline configuration
SOURCE_TABLE = "bronze.deputados_detalhes"
TARGET_TABLE = "bronze.despesas"
PIPELINE_NAME = "bronze_ingest_despesas"
SOURCE_ENDPOINT = "/deputados/{id}/despesas"

# Execution metadata
batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
failed_requests = []

try:
    # Register pipeline start
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_started",
        message=f"start | anos={SELECT_ANOS}",
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
        started_at=started_at,
    )

    # Retrieve distinct deputy IDs from detailed deputies table
    deputados_df = spark.table(SOURCE_TABLE).select("source_id").distinct()
    deputados_ids = [row["source_id"] for row in deputados_df.collect()]

    all_records = []

    # Retrieve expenses for each deputy/year combination
    for deputado_id in deputados_ids:
        for ano in SELECT_ANOS:
            try:
                records = paginate_with_retry(
                    endpoint=f"/deputados/{deputado_id}/despesas",
                    params={
                        "ano": ano,
                    },
                    limit=None,
                    page_size=100,
                    timeout=120,
                    retries=3,
                    sleep_seconds=0.5,
                )

                records_read += len(records)

                # Preserve extraction context for downstream relationships
                for record in records:
                    record["id_deputado"] = deputado_id
                    record["ano_referencia"] = ano
                    all_records.append(record)

                # Throttle requests to avoid API rate limiting
                time.sleep(0.2)

            except Exception:
                # Track failed deputy/year combinations for replay
                failed_requests.append(
                    {
                        "id_deputado": deputado_id,
                        "ano": ano,
                    }
                )
                continue

    if all_records:
        # Convert API payloads into standardized Bronze structure
        df = build_bronze_dataframe(
            records=all_records,
            source_endpoint=SOURCE_ENDPOINT,
            source_id_field="codDocumento",
            batch_id=batch_id,
        )

        records_written = df.count()

        # Persist data into Bronze Delta table
        write_bronze_delta(
            df=df,
            table_name=TARGET_TABLE,
            mode="append",
        )

    finished_at = datetime.now()

    # Register successful completion
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_completed",
        status="success",
        message=f"anos={SELECT_ANOS} | deputados={len(deputados_ids)} | failed_requests={len(failed_requests)}",
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

except Exception as e:
    finished_at = datetime.now()

    # Register failure details for troubleshooting and replay
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="ERROR",
        event_name="job_failed",
        status="failed",
        message=f"anos={SELECT_ANOS} | failed_requests={len(failed_requests)}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

    raise