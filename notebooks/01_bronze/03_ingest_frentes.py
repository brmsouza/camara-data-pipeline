# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 03_ingest_frentes
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests parliamentary fronts data from the Câmara dos Deputados API
# using the /frentes endpoint.
#
# Context:
# Provides the base dataset for parliamentary front analysis, including
# downstream joins with members, deputies, parties and legislatures.
#
# Notes:
# - Full load (non-incremental)
# - Data retrieved with pagination support
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

import uuid
from datetime import datetime

# Pipeline configuration
ENDPOINT = "/frentes"
TABLE_NAME = "bronze.frentes"
PIPELINE_NAME = "bronze_ingest_frentes"

# Execution metadata
batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0

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
        target_table=TABLE_NAME,
        started_at=started_at,
    )

    # Retrieve all parliamentary fronts from the API
    records = paginate(
        endpoint=ENDPOINT,
        limit=None,
        page_size=DEFAULT_PAGE_SIZE,
        timeout=DEFAULT_TIMEOUT,
    )

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
            table_name=TABLE_NAME,
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
        message="full_load",
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
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
        message="full_load",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
    )

    raise