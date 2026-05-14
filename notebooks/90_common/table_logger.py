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
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType
)

LOG_TABLE = "monitoring.pipeline_log"


def log_pipeline_event(
    batch_id,
    pipeline_name,
    layer,
    level,
    event_name,
    message,
    endpoint=None,
    target_table=None,
    records_read=None,
    records_written=None,
    started_at=None,
    finished_at=None,
    error_message=None,
    status=None
):
    log_schema = StructType([
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
        StructField("created_at", TimestampType(), True),
        StructField("records_discarded", LongType(), True),
    ])

    log_df = spark.createDataFrame(
        [(
            datetime.now(),
            batch_id,
            pipeline_name,
            layer,
            level,
            event_name,
            status,
            message,
            endpoint,
            target_table,
            int(records_read) if records_read is not None else None,
            int(records_written) if records_written is not None else None,
            started_at,
            finished_at,
            error_message,
            datetime.now(),
            None
        )],
        log_schema
    )

    if spark.catalog.tableExists(LOG_TABLE):
        target_schema = spark.table(LOG_TABLE).schema
        target_columns = [field.name for field in target_schema]

        for field in target_schema:
            if field.name not in log_df.columns:
                log_df = log_df.withColumn(
                    field.name,
                    F.lit(None).cast(field.dataType)
                )

        log_df = log_df.select([
            F.col(field.name).cast(field.dataType).alias(field.name)
            for field in target_schema
        ])

        log_df.write.mode("append").saveAsTable(LOG_TABLE)

    else:
        (
            log_df
            .select(
                "log_timestamp",
                "batch_id",
                "pipeline_name",
                "layer",
                "level",
                "event_name",
                "status",
                "message",
                "endpoint",
                "target_table",
                "records_read",
                "records_written",
                "started_at",
                "finished_at",
                "error_message"
            )
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(LOG_TABLE)
        )