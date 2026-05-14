# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Bodies API Ingestion
# MAGIC
# MAGIC **Notebook:** `08_ingest_orgaos`  
# MAGIC **Endpoint:** `/orgaos`
# MAGIC
# MAGIC Ingests legislative bodies data from the Câmara dos Deputados Open Data API
# MAGIC using the legislative bodies endpoint.
# MAGIC
# MAGIC This notebook extracts legislative bodies as reference entities with historical
# MAGIC validity periods. A broad historical extraction range is used instead of
# MAGIC `SELECT_ANOS` to avoid losing inactive, archived or historical organizational
# MAGIC records.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Extract legislative body records from the Câmara Open Data API
# MAGIC - Retrieve historical and active organizational entities
# MAGIC - Handle manual pagination using `pagina` and `itens`
# MAGIC - Preserve stable extraction ordering using organizational identifiers
# MAGIC - Preserve raw API payloads with minimal transformation
# MAGIC - Add ingestion metadata for traceability and auditing
# MAGIC - Persist Bronze Delta ingestion tables
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and reprocessing scenarios
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Full load using broad historical date ranges
# MAGIC - Manual pagination using `pagina` and `itens` parameters
# MAGIC - Data ordered by identifier for stable extraction
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze legislative bodies ingestion tables

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
ENDPOINT = "/orgaos"
TABLE_NAME = "bronze.orgaos"
PIPELINE_NAME = "bronze_ingest_orgaos"

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
        message="start | source=/orgaos | full_load | historical_range",
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
        started_at=started_at,
    )

    records = []
    pagina = 1
    itens = 100

    # Retrieve all available pages using a broad validity period
    while True:
        payload = get_data(
            endpoint=ENDPOINT,
            params={
                "pagina": pagina,
                "itens": itens,
                "dataInicio": "1900-01-01",
                "dataFim": "2100-01-01",
                "ordenarPor": "id",
                "ordem": "ASC",
            },
            timeout=DEFAULT_TIMEOUT,
        )

        dados = payload.get("dados", [])

        if not dados:
            break

        records.extend(dados)
        pagina += 1

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
        message=f"full_load | pages={pagina - 1} | records_read={records_read} | records_written={records_written}",
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
        message="source=/orgaos | full_load | historical_range",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=ENDPOINT,
        target_table=TABLE_NAME,
    )

    raise