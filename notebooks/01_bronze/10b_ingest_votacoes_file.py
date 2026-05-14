# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Voting Sessions File Ingestion
# MAGIC
# MAGIC **Notebook:** `10b_ingest_votacoes_file`  
# MAGIC **Source:** Unity Catalog volume CSV files
# MAGIC
# MAGIC Ingests voting sessions data from CSV files stored in the
# MAGIC Unity Catalog volume.
# MAGIC
# MAGIC This notebook provides a file-based ingestion alternative to the API extraction
# MAGIC pipeline for parliamentary voting data loads, reducing execution time while
# MAGIC preserving Bronze metadata, traceability and replayability standards.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read voting session CSV files from Unity Catalog volumes
# MAGIC - Support high-volume voting data ingestion workflows
# MAGIC - Capture source file paths using Unity Catalog metadata
# MAGIC - Enrich records with reference year extracted from file names
# MAGIC - Preserve raw ingestion payload structure with minimal transformation
# MAGIC - Add ingestion metadata for traceability and auditing
# MAGIC - Persist Bronze Delta ingestion tables
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and reprocessing scenarios
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Full load from CSV files
# MAGIC - Source files read from Unity Catalog volumes
# MAGIC - File paths captured using Unity Catalog metadata
# MAGIC - Each record enriched with `ano_referencia` extracted from the file name
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze voting sessions ingestion tables

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

# MAGIC %run ../90_common/bronze_writer

# COMMAND ----------

# MAGIC %run ../90_common/config

# COMMAND ----------

import uuid
from datetime import datetime
from pyspark.sql.functions import regexp_extract, col

# Pipeline configuration
SOURCE_PATH = f"{VOLUME_RAW_CAMARA}/votacoes/*.csv"
TABLE_NAME = "bronze.votacoes"
PIPELINE_NAME = "bronze_ingest_votacoes_file"
SOURCE_ENDPOINT = "file://votacoes"

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
        message=f"start | source_path={SOURCE_PATH} | source_type=file",
        endpoint=SOURCE_ENDPOINT,
        target_table=TABLE_NAME,
        started_at=started_at,
    )

    # Read source CSV files from Unity Catalog volume
    df_file = (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .csv(SOURCE_PATH)
    )

    # Add source file metadata for lineage
    df_file = df_file.withColumn(
        "_source_file",
        col("_metadata.file_path")
    )

    # Extract reference year from file path
    df_file = df_file.withColumn(
        "ano_referencia",
        regexp_extract(col("_source_file"), r"votacoes-(\d{4})\.csv", 1)
    )

    records_read = df_file.count()

    if records_read > 0:
        df = build_bronze_dataframe_from_df(
            df_raw=df_file,
            source_endpoint=SOURCE_ENDPOINT,
            source_id_field="id",
            batch_id=batch_id,
            source_system="camara_file",
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
        message=f"file_load | source_path={SOURCE_PATH}",
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
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
        message=f"file_load_failed | source_path={SOURCE_PATH}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TABLE_NAME,
    )

    raise