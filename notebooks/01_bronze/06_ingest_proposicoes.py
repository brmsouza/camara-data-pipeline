# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 06_ingest_proposicoes
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests legislative propositions data from the Câmara dos Deputados API
# using the /proposicoes endpoint.
#
# Context:
# Propositions are extracted by year to control API volume, improve
# reliability and support partial reprocessing when a specific year fails.
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

# MAGIC %run ../90_common/config

# COMMAND ----------

# MAGIC %run ../90_common/pagination

# COMMAND ----------

import uuid
from datetime import datetime

# Pipeline configuration
ENDPOINT = "/proposicoes"
TABLE_NAME = "bronze.proposicoes"
PIPELINE_NAME = "bronze_ingest_proposicoes"

# Execution metadata
batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
failed_anos = []

try:
    # Register pipeline start
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_started",
        message=f"start | anos={SELECT_ANOS} | write_strategy=by_year",
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
        started_at=started_at,
    )

    # Process each configured year independently to allow partial recovery
    for ano in SELECT_ANOS:
        try:
            proposicoes = paginate_with_retry(
                endpoint=ENDPOINT,
                params={
                    "ano": ano,
                },
                limit=None,
                page_size=50,
                timeout=180,
                retries=5,
                sleep_seconds=2,
            )

            # Preserve extraction year for traceability
            for proposicao in proposicoes:
                proposicao["ano_referencia"] = ano

            records_read += len(proposicoes)

            if proposicoes:
                # Convert API payloads into standardized Bronze structure
                df = build_bronze_dataframe(
                    records=proposicoes,
                    source_endpoint=ENDPOINT,
                    source_id_field="id",
                    batch_id=batch_id,
                )

                rows_written = df.count()
                records_written += rows_written

                # Persist each year independently into Bronze Delta table
                write_bronze_delta(
                    df=df,
                    table_name=TABLE_NAME,
                    mode="append",
                )

            # Register year-level completion
            log_pipeline_event(
                batch_id=batch_id,
                pipeline_name=PIPELINE_NAME,
                layer="bronze",
                level="INFO",
                event_name="year_completed",
                status="success",
                message=f"ano={ano}",
                records_read=len(proposicoes),
                records_written=len(proposicoes),
                started_at=started_at,
                finished_at=datetime.now(),
                endpoint=ENDPOINT,
                target_table=TABLE_NAME,
            )

        except Exception as e:
            # Track failed years without interrupting the remaining execution
            failed_anos.append(ano)

            log_pipeline_event(
                batch_id=batch_id,
                pipeline_name=PIPELINE_NAME,
                layer="bronze",
                level="ERROR",
                event_name="year_failed",
                status="failed",
                message=f"ano={ano}",
                error_message=str(e),
                records_read=records_read,
                records_written=records_written,
                started_at=started_at,
                finished_at=datetime.now(),
                endpoint=ENDPOINT,
                target_table=TABLE_NAME,
            )

            continue

    finished_at = datetime.now()

    # Register successful completion
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_completed",
        status="success",
        message=f"full_load | anos={SELECT_ANOS} | failed_anos={failed_anos}",
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
        message=f"anos={SELECT_ANOS} | failed_anos={failed_anos}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
    )

    raise