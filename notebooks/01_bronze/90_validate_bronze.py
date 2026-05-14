# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 99_validate_bronze
# Layer: Validation
# Author: Bruno Souza
#
# Description:
# Validates the Bronze layer after ingestion.
#
# Context:
# Used to verify data availability, volume and basic consistency across
# all Bronze tables before promoting data to Silver layer.
#
# Notes:
# - Executes dynamic validation checks across Bronze tables
# - Checks row counts, null source IDs and duplicate payload hashes
# - Does not modify data
# - Intended to be executed after all Bronze pipelines
# ------------------------------------------------------------------------------

# COMMAND ----------

from pyspark.sql.functions import col, count, max as spark_max

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
)

# COMMAND ----------


schema = "bronze"
tables = spark.sql(f"SHOW TABLES IN {schema}").collect()

results = []

for table in tables:
    table_name = table.tableName
    full_name = f"{schema}.{table_name}"

    try:
        df = spark.table(full_name)
        columns = df.columns

        total_rows = df.count()

        null_source_id = (
            df.filter(col("source_id").isNull()).count()
            if "source_id" in columns
            else None
        )

        distinct_source_id = (
            df.select("source_id").distinct().count()
            if "source_id" in columns
            else None
        )

        distinct_record_hash = (
            df.select("record_hash").distinct().count()
            if "record_hash" in columns
            else None
        )

        duplicate_hash_count = (
            total_rows - distinct_record_hash
            if distinct_record_hash is not None
            else None
        )

        latest_ingestion_timestamp = (
            df.select(
                spark_max("ingestion_timestamp").alias("latest_ingestion_timestamp")
            ).collect()[0]["latest_ingestion_timestamp"]
            if "ingestion_timestamp" in columns
            else None
        )

        results.append({
            "table_name": full_name,
            "total_rows": total_rows,
            "null_source_id": null_source_id,
            "distinct_source_id": distinct_source_id,
            "distinct_record_hash": distinct_record_hash,
            "duplicate_hash_count": duplicate_hash_count,
            "latest_ingestion_timestamp": latest_ingestion_timestamp,
            "status": "OK",
            "error_message": None,
        })

    except Exception as e:
        results.append({
            "table_name": full_name,
            "total_rows": None,
            "null_source_id": None,
            "distinct_source_id": None,
            "distinct_record_hash": None,
            "duplicate_hash_count": None,
            "latest_ingestion_timestamp": None,
            "status": "ERROR",
            "error_message": str(e),
        })

validation_schema = StructType([
    StructField("table_name", StringType(), True),
    StructField("total_rows", LongType(), True),
    StructField("null_source_id", LongType(), True),
    StructField("distinct_source_id", LongType(), True),
    StructField("distinct_record_hash", LongType(), True),
    StructField("duplicate_hash_count", LongType(), True),
    StructField("latest_ingestion_timestamp", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True),
])

df_validation = spark.createDataFrame(results, schema=validation_schema)

display(df_validation.orderBy("table_name"))

display(
    df_validation
    .filter(
        (col("status") == "ERROR") |
        (col("total_rows") == 0) |
        (col("null_source_id") > 0) |
        (col("duplicate_hash_count") > 0)
    )
    .orderBy("table_name")
)

for table in tables:
    full_name = f"{schema}.{table.tableName}"

    try:
        df = spark.table(full_name)

        if "source_system" in df.columns and "source_endpoint" in df.columns:
            print(f"Volume by source: {full_name}")

            display(
                df.groupBy("source_system", "source_endpoint")
                .agg(count("*").alias("total_rows"))
                .orderBy("source_system", "source_endpoint")
            )

    except Exception as e:
        print(f"ERROR reading source distribution for {full_name}: {e}")