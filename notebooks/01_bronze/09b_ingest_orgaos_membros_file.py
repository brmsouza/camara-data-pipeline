# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Legislative Body Members File Ingestion
# MAGIC
# MAGIC **Notebook:** `09b_ingest_orgaos_membros_file`  
# MAGIC **Source:** Unity Catalog volume CSV files
# MAGIC
# MAGIC Ingests members of legislative bodies from CSV files stored in the
# MAGIC Unity Catalog volume.
# MAGIC
# MAGIC This notebook provides a file-based ingestion alternative to the API extraction
# MAGIC pipeline for high-volume legislative body membership loads, reducing execution
# MAGIC time while preserving Bronze metadata, traceability and replayability standards.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislative body membership CSV files from Unity Catalog volumes
# MAGIC - Support high-volume membership ingestion workflows
# MAGIC - Capture source file paths using Unity Catalog metadata
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
# MAGIC - Data persisted in Delta Lake using append mode
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze legislative body members ingestion tables

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

# MAGIC %run ../90_common/bronze_writer

# COMMAND ----------

# MAGIC %run ../90_common/config

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    regexp_extract,
    when,
    concat_ws,
)

# COMMAND ----------

SOURCE_PATH = f"{VOLUME_RAW_CAMARA}/orgaos_membros/*.csv"
TARGET_TABLE = "bronze.orgaos_membros"
PIPELINE_NAME = "bronze_ingest_orgaos_membros_file"
SOURCE_ENDPOINT = "file://orgaos_membros"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0

# COMMAND ----------

try:
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_started",
        message=f"start | source_path={SOURCE_PATH} | source_type=file",
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
        started_at=started_at,
    )

    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("quote", "\"")
        .option("escape", "\"")
        .option("multiLine", True)
        .option("mode", "PERMISSIVE")
        .csv(SOURCE_PATH)
    )

    df_raw = df_raw.withColumn(
        "_source_file",
        col("_metadata.file_path")
    )

    df_raw = df_raw.withColumn(
        "legislatura_referencia",
        regexp_extract(
            col("_source_file"),
            r"(?i)L(\d+)",
            1
        )
    )

    df_raw = df_raw.withColumn(
        "legislatura_referencia",
        when(col("legislatura_referencia") == "", None)
        .otherwise(col("legislatura_referencia"))
    )

    df_raw = df_raw.withColumn(
        "id_membro_orgao",
        concat_ws(
            "_",
            col("uriOrgao"),
            col("siglaOrgao"),
            col("uriDeputado"),
            col("nomeDeputado"),
            col("siglaPartido"),
            col("siglaUF"),
            col("cargo"),
            col("dataInicio"),
            col("dataFim"),
            col("legislatura_referencia"),
            col("_source_file")
        )
    )

    records_read = df_raw.count()

    if records_read > 0:
        df = build_bronze_dataframe_from_df(
            df_raw=df_raw,
            source_endpoint=SOURCE_ENDPOINT,
            source_id_field="id_membro_orgao",
            batch_id=batch_id,
            source_system="camara_file",
        )

        records_written = df.count()

        write_bronze_delta(
            df=df,
            table_name=TARGET_TABLE,
            mode="append",
        )

    else:
        empty_records = [{"id_membro_orgao": None}]

        df_empty = build_bronze_dataframe(
            records=empty_records,
            source_endpoint=SOURCE_ENDPOINT,
            source_id_field="id_membro_orgao",
            batch_id=batch_id,
            source_system="camara_file",
        ).limit(0)

        write_bronze_delta(
            df=df_empty,
            table_name=TARGET_TABLE,
            mode="append",
        )

    finished_at = datetime.now()

    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer="bronze",
        level="INFO",
        event_name="job_completed",
        status="success",
        message=f"file_load | records_read={records_read} | records_written={records_written}",
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=finished_at,
        endpoint=SOURCE_ENDPOINT,
        target_table=TARGET_TABLE,
    )

except Exception as e:
    finished_at = datetime.now()

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
        target_table=TARGET_TABLE,
    )

    raise