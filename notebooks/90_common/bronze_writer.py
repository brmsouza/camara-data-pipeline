# Databricks notebook source
# MAGIC %md
# MAGIC # Core Layer — Bronze Layer Persistence Utilities
# MAGIC
# MAGIC **Notebook:** `bronze_writer`
# MAGIC
# MAGIC Provides reusable functions to standardize the creation and persistence
# MAGIC of Bronze layer DataFrames.
# MAGIC
# MAGIC This notebook centralizes the transformation of raw ingestion payloads into the
# MAGIC standardized Bronze structure and handles persistence into Delta Lake tables
# MAGIC used by Bronze ingestion pipelines.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Standardize raw ingestion payload structures
# MAGIC - Support ingestion from Python records and Spark DataFrames
# MAGIC - Add ingestion metadata such as batch identifiers and timestamps
# MAGIC - Preserve raw API payloads as JSON for replayability and auditing
# MAGIC - Persist Bronze datasets using Delta Lake
# MAGIC - Support reusable Bronze ingestion patterns across the project
# MAGIC - Ensure consistent Bronze schema structure across ingestion pipelines
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Supports both Python records and Spark DataFrames
# MAGIC - Adds ingestion metadata such as batch ID, source and ingestion timestamps
# MAGIC - Preserves the raw payload as JSON
# MAGIC - Writes data using Delta Lake

# COMMAND ----------

import json
import uuid

from pyspark.sql.functions import (
    col,
    concat_ws,
    current_date,
    current_timestamp,
    lit,
    sha2,
    struct,
    to_json,
)
from pyspark.sql.types import StructType, StructField, StringType


def build_bronze_dataframe(
    records: list[dict],
    source_endpoint: str,
    source_id_field: str = "id",
    batch_id: str | None = None,
    source_system: str = "camara_api",
):
    if batch_id is None:
        batch_id = str(uuid.uuid4())

    rows = [
        {
            "source_id": (
                str(record.get(source_id_field))
                if record.get(source_id_field) is not None
                else None
            ),
            "raw_payload": json.dumps(record, ensure_ascii=False, sort_keys=True),
            "source_endpoint": source_endpoint,
            "batch_id": batch_id,
        }
        for record in records
    ]

    schema = StructType([
        StructField("source_id", StringType(), True),
        StructField("raw_payload", StringType(), True),
        StructField("source_endpoint", StringType(), True),
        StructField("batch_id", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema=schema)

    return add_bronze_metadata(
        df=df,
        source_system=source_system,
    )


def build_bronze_dataframe_from_df(
    df_raw,
    source_endpoint: str,
    source_id_field: str = "id",
    batch_id: str | None = None,
    source_system: str = "camara_file",
):
    if batch_id is None:
        batch_id = str(uuid.uuid4())

    source_id_expr = (
        col(source_id_field).cast("string")
        if source_id_field in df_raw.columns
        else lit(None).cast("string")
    )

    df = (
        df_raw
        .withColumn("source_id", source_id_expr)
        .withColumn("raw_payload", to_json(struct(*[col(c) for c in df_raw.columns])))
        .withColumn("source_endpoint", lit(source_endpoint))
        .withColumn("batch_id", lit(batch_id))
        .select(
            "source_id",
            "raw_payload",
            "source_endpoint",
            "batch_id",
        )
    )

    return add_bronze_metadata(
        df=df,
        source_system=source_system,
    )


def add_bronze_metadata(
    df,
    source_system: str,
):
    return (
        df.withColumn(
            "record_hash",
            sha2(
                concat_ws(
                    "||",
                    col("source_endpoint"),
                    col("source_id"),
                    col("raw_payload"),
                ),
                256,
            ),
        )
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_system", lit(source_system))
    )


def write_bronze_delta(
    df,
    table_name: str,
    mode: str = "append",
) -> None:
    (
        df.write
        .format("delta")
        .mode(mode)
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )