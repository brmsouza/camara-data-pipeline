# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Deputies API Ingestion
# MAGIC
# MAGIC **Notebook:** `01_ingest_deputados`  
# MAGIC **Endpoint:** `/deputados`
# MAGIC
# MAGIC Ingests deputies data from the Câmara dos Deputados Open Data API,
# MAGIC retrieving records per legislature defined in `LEGISLATURAS_PADRAO`.
# MAGIC
# MAGIC This notebook is part of the Bronze ingestion layer, persisting raw deputy
# MAGIC records with minimal transformation and preserving technical metadata required
# MAGIC for traceability, auditing and reprocessing workflows.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract deputy records from the Câmara Open Data API
# MAGIC - Retrieve deputies by configured legislatures
# MAGIC - Preserve raw API payloads with minimal transformation
# MAGIC - Add ingestion metadata for traceability and auditing
# MAGIC - Persist Bronze Delta ingestion tables
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and reprocessing scenarios
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Full load, non-incremental ingestion
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze deputy ingestion tables

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
ENDPOINT = "/deputados"
TABLE_NAME = "bronze.deputados"
PIPELINE_NAME = "bronze_ingest_deputados"

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
        message=f"start | legislaturas={LEGISLATURAS_PADRAO}",
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
        started_at=started_at,
    )

    records = []

    # Retrieve deputies for each configured legislature
    for id_legislatura in LEGISLATURAS_PADRAO:
        deputados = paginate(
            endpoint=ENDPOINT,
            params={"idLegislatura": id_legislatura},
            limit=None,
            page_size=DEFAULT_PAGE_SIZE,
            timeout=DEFAULT_TIMEOUT,
        )

        # Preserve the legislature used as extraction context
        for deputado in deputados:
            deputado["id_legislatura"] = id_legislatura
            records.append(deputado)

    records_read = len(records)

    if records:
        # Convert API payloads into the standardized Bronze structure
        df = build_bronze_dataframe(
            records=records,
            source_endpoint=ENDPOINT,
            source_id_field="id",
            batch_id=batch_id,
        )

        records_written = df.count()

        # Persist raw data into the Bronze Delta table
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
        message=f"legislaturas={LEGISLATURAS_PADRAO}",
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
        message=f"legislaturas={LEGISLATURAS_PADRAO}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
    )

    raise