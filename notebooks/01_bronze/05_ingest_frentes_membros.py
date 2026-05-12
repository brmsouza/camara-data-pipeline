# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 05_ingest_frentes_membros
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests members of parliamentary fronts from the Câmara dos Deputados API
# using the /frentes/{id}/membros endpoint.
#
# Context:
# Complements the parliamentary fronts dataset by retrieving the deputies
# associated with each front previously ingested in the Bronze layer.
#
# Notes:
# - Full load (non-incremental)
# - One API call per parliamentary front
# - Each record is enriched with id_frente
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
SOURCE_TABLE = "bronze.frentes"
TARGET_TABLE = "bronze.frentes_membros"
PIPELINE_NAME = "bronze_ingest_frentes_membros"
SOURCE_ENDPOINT = "/frentes/{id}/membros"

# Execution metadata
batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
failed_frentes = []

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

    # Retrieve distinct front IDs from base Bronze table
    frentes_df = spark.table(SOURCE_TABLE).select("source_id").distinct()
    frentes_ids = [row["source_id"] for row in frentes_df.collect()]

    all_records = []

    # Retrieve members for each parliamentary front
    for frente_id in frentes_ids:
        try:
            payload = get_data(
                endpoint=f"/frentes/{frente_id}/membros",
                params=None,
                timeout=DEFAULT_TIMEOUT,
            )

            records = payload.get("dados", [])
            records_read += len(records)

            # Preserve front identifier for downstream relationships
            for record in records:
                record["id_frente"] = frente_id

                # ---------------------------------------------------
                # Preserve original deputy ID from API
                # ---------------------------------------------------
                record["id_deputado_api"] = record.get("id")

                # ---------------------------------------------------
                # Fallback technical identifier
                # Some historical members do not contain deputy ID
                # in the Câmara API response.
                #
                # This fallback is used only to avoid null source_id
                # in the Bronze layer.
                # ---------------------------------------------------
                if record.get("id") is None:
                    record["id"] = (
                        f"{frente_id}_"
                        f"{record.get('nome')}_"
                        f"{record.get('siglaPartido')}_"
                        f"{record.get('siglaUf')}_"
                        f"{record.get('titulo')}_"
                        f"{record.get('idLegislatura')}"
                    )
                    record["id_deputado_original_ausente"] = True
                else:
                    record["id_deputado_original_ausente"] = False

                all_records.append(record)

            # Throttle requests to avoid API rate limiting
            time.sleep(0.3)

        except Exception:
            # Track failed requests for observability
            failed_frentes.append(frente_id)
            continue

    if all_records:
        # Convert API payloads into standardized Bronze structure
        df = build_bronze_dataframe(
            records=all_records,
            source_endpoint=SOURCE_ENDPOINT,
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
        message=f"frentes_processadas={len(frentes_ids)} | failed_frentes={len(failed_frentes)}",
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
        message=f"failed_frentes={len(failed_frentes)}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

    raise