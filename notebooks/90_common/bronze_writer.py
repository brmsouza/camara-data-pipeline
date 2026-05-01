# Databricks notebook source

from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_date, current_timestamp, lit


def build_bronze_dataframe(
    records: list[dict[str, Any]],
    source_endpoint: str,
) -> DataFrame:
    """
    Cria um DataFrame Bronze a partir dos registros retornados pela API.
    """
    rows = [
        {
            "raw_payload": json.dumps(record, ensure_ascii=False),
            "source_endpoint": source_endpoint,
        }
        for record in records
    ]

    df = spark.createDataFrame(rows)

    return (
        df.withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_system", lit("camara_api"))
    )


def write_bronze_delta(
    df: DataFrame,
    table_name: str,
    mode: str = "append",
) -> None:
    """
    Escreve o DataFrame Bronze em uma tabela Delta.
    """
    records = df.count()

    logger.info(
        "bronze_write_started | table=%s | mode=%s | records=%s",
        table_name,
        mode,
        records,
    )

    (
        df.write.format("delta")
        .mode(mode)
        .partitionBy("ingestion_date")
        .option("mergeSchema", "true")
        .saveAsTable(table_name)
    )

    logger.info(
        "bronze_write_completed | table=%s | mode=%s | records=%s",
        table_name,
        mode,
        records,
    )