# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Body Members API Ingestion
# MAGIC
# MAGIC **Notebook:** `09_ingest_orgaos_membros`  
# MAGIC **Endpoint:** `/orgaos/{id}/membros`
# MAGIC
# MAGIC Ingests members of legislative bodies from the Câmara dos Deputados Open Data
# MAGIC API using the legislative body members endpoint.
# MAGIC
# MAGIC This notebook complements the legislative bodies dataset by retrieving
# MAGIC membership relationships for each organizational entity previously ingested
# MAGIC in the Bronze layer. The extraction uses the configured analysis period
# MAGIC because membership is treated as a temporal relationship.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Retrieve legislative body membership records from the Câmara Open Data API
# MAGIC - Execute one API extraction cycle per legislative body
# MAGIC - Handle paginated API extraction workflows
# MAGIC - Apply retry logic for resilient ingestion execution
# MAGIC - Enrich records with legislative body identifiers and reference date ranges
# MAGIC - Preserve raw API payloads with minimal transformation
# MAGIC - Add ingestion metadata for traceability and auditing
# MAGIC - Persist Bronze Delta ingestion tables
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and reprocessing scenarios
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Full load using the configured analysis period
# MAGIC - One API extraction cycle per legislative body
# MAGIC - Data retrieved with pagination and retry support
# MAGIC - Each record enriched with `id_orgao` and extraction reference date ranges
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze legislative body members ingestion tables

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
SOURCE_TABLE = "bronze.orgaos"
TARGET_TABLE = "bronze.orgaos_membros"
PIPELINE_NAME = "bronze_ingest_orgaos_membros"
SOURCE_ENDPOINT = "/orgaos/{id}/membros"

# Execution metadata
batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
failed_orgaos = []

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

    # Retrieve distinct legislative body IDs from base Bronze table
    orgaos_df = spark.table(SOURCE_TABLE).select("source_id").distinct()
    orgaos_ids = [row["source_id"] for row in orgaos_df.collect()]

    all_records = []

    # Membership is a temporal relationship; apply the configured analysis period
    data_inicio = f"{min(SELECT_ANOS)}-01-01"
    data_fim = f"{max(SELECT_ANOS)}-12-31"

    # Retrieve members for each legislative body
    for orgao_id in orgaos_ids:
        try:
            records = paginate_with_retry(
                endpoint=f"/orgaos/{orgao_id}/membros",
                params={
                    "dataInicio": data_inicio,
                    "dataFim": data_fim,
                },
                limit=None,
                page_size=DEFAULT_PAGE_SIZE,
                timeout=120,
                retries=3,
                sleep_seconds=0.5,
            )

            records_read += len(records)

            # Preserve relationship context for downstream processing
            for record in records:
                record["id_orgao"] = orgao_id
                record["data_inicio_referencia"] = data_inicio
                record["data_fim_referencia"] = data_fim
                all_records.append(record)

            # Throttle requests to avoid API rate limiting
            time.sleep(0.2)

        except Exception:
            # Track failed legislative bodies for observability and replay
            failed_orgaos.append(orgao_id)
            continue

    # Register collected volume before persistence
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="records_collected",
        status="success",
        message=f"orgaos_processados={len(orgaos_ids)} | records={len(all_records)} | failed_orgaos={len(failed_orgaos)}",
        records_read=records_read,
        records_written=0,
        started_at=started_at,
        finished_at=datetime.now(),
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

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

    else:
        # Ensure target table creation when no membership records are returned
        empty_records = [{"id": None, "id_orgao": None}]

        df_empty = build_bronze_dataframe(
            records=empty_records,
            source_endpoint=SOURCE_ENDPOINT,
            source_id_field="id",
            batch_id=batch_id,
        ).limit(0)

        write_bronze_delta(
            df=df_empty,
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
        message=f"orgaos_processados={len(orgaos_ids)} | failed_orgaos={len(failed_orgaos)}",
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
        message=f"failed_orgaos={len(failed_orgaos)}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

    raise