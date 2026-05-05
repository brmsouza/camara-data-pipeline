# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 07b_ingest_despesas_file
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests parliamentary expense data from CSV files stored in the
# Unity Catalog volume.
#
# Context:
# File-based ingestion is used as an alternative to the API pipeline for
# high-volume expense loads, reducing execution time while preserving Bronze metadata.
#
# Notes:
# - Full load from CSV files
# - Source files are processed by reference year
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
from pyspark.sql.functions import regexp_extract, col, when

# Pipeline configuration
SOURCE_PATH = f"{VOLUME_RAW_CAMARA}/despesas/*.csv"
TARGET_TABLE = "bronze.despesas"
PIPELINE_NAME = "bronze_ingest_despesas_file"
SOURCE_ENDPOINT = "file://despesas"

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
        message=f"start | source_path={SOURCE_PATH} | write_strategy=by_year",
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
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
        regexp_extract(col("_source_file"), r"(?i)ano-(\d{4})", 1)
    )

    # Normalize empty year values
    df_raw = df_raw.withColumn(
        "ano_referencia",
        when(col("ano_referencia") == "", None)
        .otherwise(col("ano_referencia"))
    )

    anos = [
        row["ano_referencia"]
        for row in df_raw.select("ano_referencia").distinct().collect()
        if row["ano_referencia"] is not None
    ]

    # Process each file year independently to allow partial recovery
    for ano in sorted(anos):
        try:
            df_ano = df_raw.filter(col("ano_referencia") == ano)

            records_ano_read = df_ano.count()
            records_read += records_ano_read

            if records_ano_read > 0:
                df = build_bronze_dataframe_from_df(
                    df_raw=df_ano,
                    source_endpoint=SOURCE_ENDPOINT,
                    source_id_field="codDocumento",
                    batch_id=batch_id,
                    source_system="camara_file",
                )

                rows_written = df.count()
                records_written += rows_written

                # Persist each year independently into Bronze Delta table
                write_bronze_delta(
                    df=df,
                    table_name=TARGET_TABLE,
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
                records_read=records_ano_read,
                records_written=rows_written if records_ano_read > 0 else 0,
                started_at=started_at,
                finished_at=datetime.now(),
                endpoint=SOURCE_ENDPOINT,
                target_table=TARGET_TABLE,
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
                endpoint=SOURCE_ENDPOINT,
                target_table=TARGET_TABLE,
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
        message=f"file_load | anos={anos} | failed_anos={failed_anos}",
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
        message=f"file_load_failed | failed_anos={failed_anos}",
        error_message=str(e),
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

    raise