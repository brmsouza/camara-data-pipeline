# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Voting Guidance API Ingestion
# MAGIC
# MAGIC **Notebook:** `11_ingest_votacoes_orientacoes`  
# MAGIC **Endpoint:** `/votacoes/{id}/orientacoes`
# MAGIC
# MAGIC Ingests voting guidance data from the Câmara dos Deputados Open Data API
# MAGIC using the voting guidance endpoint.
# MAGIC
# MAGIC This notebook complements the voting sessions dataset by retrieving political
# MAGIC party and parliamentary bloc voting guidance for each voting session previously
# MAGIC ingested in the Bronze layer.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Retrieve voting guidance records from the Câmara Open Data API
# MAGIC - Execute one API request per voting session identifier
# MAGIC - Support controlled parallel extraction workflows
# MAGIC - Enrich records with voting session identifiers
# MAGIC - Preserve raw API payloads with minimal transformation
# MAGIC - Add ingestion metadata for traceability and auditing
# MAGIC - Persist Bronze Delta ingestion tables
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and reprocessing scenarios
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Full load based on previously ingested voting session identifiers
# MAGIC - One API request per voting session
# MAGIC - Controlled parallelism used to reduce execution time
# MAGIC - Each record enriched with `id_votacao`
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze voting guidance ingestion tables

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

# MAGIC %run ../90_common/api_client

# COMMAND ----------

# MAGIC %run ../90_common/bronze_writer

# COMMAND ----------

# MAGIC %run ../90_common/config

# COMMAND ----------

import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Pipeline configuration
SOURCE_TABLE = "bronze.votacoes"
TARGET_TABLE = "bronze.votacoes_orientacoes"
PIPELINE_NAME = "bronze_ingest_votacoes_orientacoes"
SOURCE_ENDPOINT = "/votacoes/{id}/orientacoes"
MAX_WORKERS = 10

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
        message=f"start | parallel_workers={MAX_WORKERS}",
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
        started_at=started_at,
    )

    # Retrieve distinct voting IDs from base Bronze table
    votacoes_df = spark.table(SOURCE_TABLE).select("source_id").distinct()
    votacoes_ids = [row["source_id"] for row in votacoes_df.collect()]

    def fetch_orientacoes(votacao_id):
        payload = get_data(
            endpoint=f"/votacoes/{votacao_id}/orientacoes",
            params=None,
            timeout=DEFAULT_TIMEOUT,
        )

        records = payload.get("dados", [])

        # Preserve voting identifier for downstream relationships
        for record in records:
            record["id_votacao"] = votacao_id

        return votacao_id, records

    all_records = []

    # Execute API requests concurrently using controlled parallelism
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_orientacoes, votacao_id): votacao_id
            for votacao_id in votacoes_ids
        }

        for future in as_completed(futures):
            votacao_id = futures[future]

            try:
                _, records = future.result()

                records_read += len(records)
                all_records.extend(records)

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

    else:
        # Ensure target table creation when no guidance records are returned
        empty_records = [{"id_votacao": None}]

        df_empty = build_bronze_dataframe(
            records=empty_records,
            source_endpoint=SOURCE_ENDPOINT,
            source_id_field="id_votacao",
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
        message=f"votacoes_processadas={len(votacoes_ids)} | failed_votacoes={len(failed_votacoes)} | workers={MAX_WORKERS}",
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
        message=f"failed_votacoes={len(failed_votacoes)} | workers={MAX_WORKERS}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

    raise