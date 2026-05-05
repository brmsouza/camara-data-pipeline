# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 12_ingest_votacoes_votos
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests individual voting records from the Câmara dos Deputados API
# using the /votacoes/{id}/votos endpoint.
#
# Context:
# Complements the voting sessions dataset by retrieving each deputy vote
# for previously ingested voting sessions, enabling voting behavior and
# alignment analysis.
#
# Notes:
# - Full load based on previously ingested voting IDs
# - One API request per voting session
# - Each record is enriched with id_votacao
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

import time
import uuid
from datetime import datetime

# Pipeline configuration
SOURCE_TABLE = "bronze.votacoes"
TARGET_TABLE = "bronze.votacoes_votos"
PIPELINE_NAME = "bronze_ingest_votacoes_votos"
SOURCE_ENDPOINT = "/votacoes/{id}/votos"

# Execution metadata
batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
failed_votacoes = []

try:
    # Register pipeline start
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_started",
        message="start",
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
        started_at=started_at,
    )

    # Retrieve distinct voting IDs from base Bronze table
    votacoes_df = spark.table(SOURCE_TABLE).select("source_id").distinct()
    votacoes_ids = [row["source_id"] for row in votacoes_df.collect()]

    all_records = []

    # Retrieve individual votes for each voting session
    for votacao_id in votacoes_ids:
        try:
            payload = get_data(
                endpoint=f"/votacoes/{votacao_id}/votos",
                params=None,
                timeout=DEFAULT_TIMEOUT,
            )

            records = payload.get("dados", [])
            records_read += len(records)

            # Preserve voting identifier for downstream relationships
            for record in records:
                record["id_votacao"] = votacao_id
                all_records.append(record)

            # Throttle requests to avoid API rate limiting
            time.sleep(0.2)

        except Exception:
            # Track failed voting sessions for observability and replay
            failed_votacoes.append(votacao_id)
            continue

    if all_records:
        # Convert API payloads into standardized Bronze structure
        df = build_bronze_dataframe(
            records=all_records,
            source_endpoint=SOURCE_ENDPOINT,
            source_id_field="id_votacao",
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
        message=f"votacoes_processadas={len(votacoes_ids)} | failed_votacoes={len(failed_votacoes)}",
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
        message=f"failed_votacoes={len(failed_votacoes)}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

    raise