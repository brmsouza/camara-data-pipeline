# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 02_ingest_deputados_detalhes
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Retrieves detailed information for each deputy using the
# /deputados/{id} endpoint based on previously ingested IDs.
#
# Context:
# Complements the base deputies dataset by enriching records with
# additional attributes at the individual level.
#
# Notes:
# - Full load (non-incremental)
# - One API call per deputy (higher latency expected)
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

# MAGIC %run ../90_common/pagination

# COMMAND ----------

# MAGIC %run ../90_common/config

# COMMAND ----------

import time
import uuid
from datetime import datetime

# Pipeline configuration
SOURCE_TABLE = "bronze.deputados"
ENDPOINT = "/deputados/{id}"
TARGET_TABLE = "bronze.deputados_detalhes"
PIPELINE_NAME = "bronze_ingest_deputados_detalhes"

# Execution metadata
batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
failed_deputados = []

try:
    # Register pipeline start
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_started",
        message="start",
        endpoint=ENDPOINT,
        target_table=TARGET_TABLE,
        started_at=started_at,
    )

    # Retrieve distinct deputy IDs from base Bronze table
    deputados_df = spark.table(SOURCE_TABLE).select("source_id").distinct()

    # Collect IDs to driver for sequential API calls
    deputados_ids = [row["source_id"] for row in deputados_df.collect()]

    records = []

    # Iterate over each deputy and request detailed data
    for deputado_id in deputados_ids:
        try:
            payload = get_data(
                endpoint=f"/deputados/{deputado_id}",
                params=None,
                timeout=DEFAULT_TIMEOUT,
            )

            deputado = payload.get("dados")

            # Append only valid responses
            if deputado:
                records.append(deputado)

            # Throttle requests to avoid API rate limiting
            time.sleep(0.3)

        except Exception:
            # Track failed requests for observability
            failed_deputados.append(deputado_id)
            continue

    records_read = len(records)

    if records:
        # Convert API payloads into standardized Bronze structure
        df = build_bronze_dataframe(
            records=records,
            source_endpoint=ENDPOINT,
            source_id_field="id",
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
        message=f"failed_deputados={len(failed_deputados)}",
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
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
        message=f"failed_deputados={len(failed_deputados)}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TARGET_TABLE,
    )

    raise