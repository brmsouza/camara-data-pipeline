# Databricks notebook source
# ------------------------------------------------------------------------------
# Module: table_logger
# Layer: Core
# Author: Bruno Souza
#
# Description:
# Provides utilities to log pipeline execution events into Delta tables.
#
# Context:
# Implements the persistence layer for operational logging, writing structured
# events into monitoring.pipeline_log to support observability and auditing.
#
# Notes:
# - Writes structured logs (start, progress, success, failure)
# - Standardizes log schema across all pipelines
# - Supports batch-level traceability via batch_id
# - Used by all ingestion and admin notebooks
# ------------------------------------------------------------------------------

# COMMAND ----------

from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

pipeline_log_schema = StructType([
    StructField("log_timestamp", TimestampType(), True),
    StructField("batch_id", StringType(), True),
    StructField("pipeline_name", StringType(), True),
    StructField("layer", StringType(), True),
    StructField("level", StringType(), True),
    StructField("event_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("message", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_written", LongType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("finished_at", TimestampType(), True),
    StructField("error_message", StringType(), True),
])


def log_pipeline_event(
    batch_id: str,
    pipeline_name: str,
    layer: str,
    level: str,
    event_name: str,
    message: str | None = None,
    endpoint: str | None = None,
    target_table: str | None = None,
    status: str | None = None,
    records_read: int | None = None,
    records_written: int | None = None,
    started_at=None,
    finished_at=None,
    error_message: str | None = None,
) -> None:

    rows = [{
        "log_timestamp": datetime.now(),
        "batch_id": batch_id,
        "pipeline_name": pipeline_name,
        "layer": layer,
        "level": level,
        "event_name": event_name,
        "status": status,
        "message": message,
        "endpoint": endpoint,
        "target_table": target_table,
        "records_read": records_read,
        "records_written": records_written,
        "started_at": started_at,
        "finished_at": finished_at,
        "error_message": error_message,
    }]

    df_log = spark.createDataFrame(rows, schema=pipeline_log_schema)

    (
        df_log.write
        .format("delta")
        .mode("append")
        .saveAsTable("monitoring.pipeline_log")
    )