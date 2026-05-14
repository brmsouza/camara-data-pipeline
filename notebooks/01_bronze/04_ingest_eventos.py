# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Events API Ingestion
# MAGIC
# MAGIC **Notebook:** `04_ingest_eventos`  
# MAGIC **Endpoint:** `/eventos`
# MAGIC
# MAGIC Ingests legislative events data from the Câmara dos Deputados Open Data API
# MAGIC using the legislative events endpoint.
# MAGIC
# MAGIC This notebook extracts parliamentary events by daily windows for each configured
# MAGIC reference year, reducing API response size, improving extraction reliability
# MAGIC and preserving the original extraction period used during ingestion.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract legislative event records from the Câmara Open Data API
# MAGIC - Retrieve events using daily extraction windows by reference year
# MAGIC - Handle paginated API extraction workflows
# MAGIC - Apply retry logic for resilient ingestion execution
# MAGIC - Enrich records with reference year and extraction window metadata
# MAGIC - Preserve raw API payloads with minimal transformation
# MAGIC - Add ingestion metadata for traceability and auditing
# MAGIC - Persist Bronze Delta ingestion tables
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and reprocessing scenarios
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Full load using yearly and daily extraction windows
# MAGIC - Data retrieved with pagination and retry support
# MAGIC - Each record enriched with `ano_referencia` and extraction window dates
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze legislative events ingestion tables

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
from datetime import datetime, timedelta

# Pipeline configuration
ENDPOINT = "/eventos"
TABLE_NAME = "bronze.eventos"
PIPELINE_NAME = "bronze_ingest_eventos"

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
        message=f"start | anos={SELECT_ANOS} | granularity=daily",
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
        started_at=started_at,
    )

    records = []

    # Retrieve events using daily extraction windows for each configured year
    for ano in SELECT_ANOS:
        data_inicio = datetime(ano, 1, 1)
        data_fim_ano = datetime(ano, 12, 31)

        while data_inicio <= data_fim_ano:
            data_janela = data_inicio.strftime("%Y-%m-%d")

            eventos = paginate_with_retry(
                endpoint=ENDPOINT,
                params={
                    "dataInicio": data_janela,
                    "dataFim": data_janela,
                },
                limit=None,
                page_size=DEFAULT_PAGE_SIZE,
                timeout=120,
                retries=3,
                sleep_seconds=0.5,
            )

            # Preserve extraction context for traceability
            for evento in eventos:
                evento["ano_referencia"] = ano
                evento["data_inicio_janela"] = data_janela
                evento["data_fim_janela"] = data_janela
                records.append(evento)

            data_inicio = data_inicio + timedelta(days=1)

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
        message=f"full_load | anos={SELECT_ANOS} | granularity=daily",
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
        message=f"anos={SELECT_ANOS} | granularity=daily",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
    )

    raise