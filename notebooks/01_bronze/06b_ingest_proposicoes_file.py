# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 06b_ingest_proposicoes_file
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests legislative propositions data from CSV files stored in the
# Unity Catalog volume.
#
# Context:
# File-based ingestion is used as an alternative to the API pipeline for
# large-volume loads, reducing execution time while preserving Bronze metadata.
#
# Notes:
# - Full load from CSV files
# - Source files are read from Unity Catalog volume
# - File path is captured using Unity Catalog metadata
# - Each record is enriched with ano_referencia extracted from the file name
# - Data persisted in Delta (append mode)
# - Execution logged in monitoring.pipeline_log
# ------------------------------------------------------------------------------

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
SOURCE_PATH = f"{VOLUME_RAW_CAMARA}/proposicoes/*.csv"
TABLE_NAME = "bronze.proposicoes"
PIPELINE_NAME = "bronze_ingest_proposicoes_file"
SOURCE_ENDPOINT = "file://proposicoes"

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
    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("sep", ",")
        .option("encoding", "UTF-8")
        .csv(SOURCE_PATH)
    )

    # Add source file metadata for lineage
    df_raw = df_raw.withColumn(
        "_source_file",
        col("_metadata.file_path")
    )

    # Extract reference year from file path
    df_raw = df_raw.withColumn(
        "ano_referencia",
        regexp_extract(col("_source_file"), r"(\d{4})", 1)
    )

    records_read = df_raw.count()

    if records_read > 0:       

        # Convert file payloads into standardized Bronze structure
        df = build_bronze_dataframe_from_df(
            df_raw=df_raw,
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