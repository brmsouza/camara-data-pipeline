# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Parliamentary Fronts API Ingestion
# MAGIC
# MAGIC **Notebook:** `03_ingest_frentes`  
# MAGIC **Endpoint:** `/frentes`
# MAGIC
# MAGIC Ingests parliamentary fronts data from the Câmara dos Deputados Open Data API
# MAGIC using the parliamentary fronts endpoint.
# MAGIC
# MAGIC This notebook provides the base dataset required for parliamentary front
# MAGIC analysis, supporting downstream joins with members, deputies, political parties
# MAGIC and legislatures across Silver and Gold analytical layers.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract parliamentary front records from the Câmara Open Data API
# MAGIC - Handle paginated API retrieval workflows
# MAGIC - Preserve raw API payloads with minimal transformation
# MAGIC - Add ingestion metadata for traceability and auditing
# MAGIC - Persist Bronze Delta ingestion tables
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and reprocessing scenarios
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Full load, non-incremental ingestion
# MAGIC - Data retrieved with pagination support
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze parliamentary fronts ingestion tables

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