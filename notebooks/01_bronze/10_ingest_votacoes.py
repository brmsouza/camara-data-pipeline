# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 10_ingest_votacoes
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests voting sessions data from the Câmara dos Deputados API
# using the /votacoes endpoint.
#
# Context:
# Voting data is extracted by year using date windows to support downstream
# analysis of voting behavior, party alignment and parliamentary engagement.
#
# Notes:
# - Full load by configured years
# - Data retrieved with pagination and retry support
# - Each record is enriched with ano_referencia
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
ENDPOINT = "/votacoes"
TABLE_NAME = "bronze.votacoes"
PIPELINE_NAME = "bronze_ingest_votacoes"

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
        message=f"start | anos={SELECT_ANOS}",
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
        started_at=started_at,
    )

    records = []

    # Retrieve voting sessions for each configured year
    for ano in SELECT_ANOS:
        votacoes = paginate_with_retry(
            endpoint=ENDPOINT,
            params={
                "dataInicio": f"{ano}-01-01",
                "dataFim": f"{ano}-12-31",
            },
            limit=None,
            page_size=DEFAULT_PAGE_SIZE,
            timeout=120,
            retries=3,
            sleep_seconds=0.5,
        )

        # Preserve extraction year for traceability
        for votacao in votacoes:
            votacao["ano_referencia"] = ano
            records.append(votacao)

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
        message=f"full_load | anos={SELECT_ANOS}",
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
        message=f"anos={SELECT_ANOS}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
    )

    raise