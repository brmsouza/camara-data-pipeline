# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 01_base_deputados
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Performs standardization, typing, deduplication and quality validation
# for deputies data from the Bronze layer.
#
# Context:
# This notebook transforms raw ingestion data from bronze.deputados into
# a structured and validated Silver Base table following enterprise
# engineering standards and Medallion Architecture principles.
#
# Responsibilities:
# - Apply schema standardization
# - Cast and normalize fields
# - Remove invalid records
# - Perform technical deduplication
# - Add traceability columns
# - Persist curated Delta table
#
# Source:
# bronze.deputados
#
# Target:
# silver_base.deputados
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Ready for Silver Curated consumption
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    row_number,
    count,
    from_json,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

# COMMAND ----------

SOURCE_TABLE = "bronze.deputados"
TARGET_TABLE = "silver_base.deputados"

PIPELINE_NAME = "silver_base_deputados"
LAYER = "silver_base"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer="silver",
    level="INFO",
    event_name="job_started",
    message=f"source={PIPELINE_NAME} | start successfully",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

df_bronze = spark.table(SOURCE_TABLE)

records_read = df_bronze.count()


# COMMAND ----------

deputados_schema = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("siglaPartido", StringType(), True),
    StructField("siglaUf", StringType(), True),
    StructField("email", StringType(), True),
    StructField("urlFoto", StringType(), True),
])

df_parsed = (
    df_bronze
    .withColumn(
        "json_data",
        from_json(col("raw_payload"), deputados_schema)
    )
)

df = (
    df_parsed
    .select(
        col("json_data.id").alias("dept_id_deputado"),
        trim(col("json_data.nome")).alias("dept_tx_nome"),
        trim(col("json_data.siglaPartido")).alias("part_sg_partido"),
        upper(trim(col("json_data.siglaUf"))).alias("uf_sg_uf"),
        trim(col("json_data.email")).alias("dept_tx_email"),
        trim(col("json_data.urlFoto")).alias("dept_tx_url_foto"),

        col("ingestion_timestamp").alias("bronze_ts_ingestao"),
        col("source_endpoint").alias("bronze_tx_endpoint"),
        col("source_id").alias("bronze_id_origem"),
        col("batch_id").alias("bronze_id_batch"),
        col("record_hash").alias("bronze_tx_record_hash"),

        current_timestamp().alias("silver_ts_processamento")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("dept_id_deputado")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

#display(df_bronze)

# COMMAND ----------

invalid_null_id = df_dedup.filter(col("dept_id_deputado").isNull()).count()

duplicated_ids = (
    df_dedup
    .groupBy("dept_id_deputado")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if invalid_null_id > 0:
    raise Exception(f"Data quality error: {invalid_null_id} records without deputy ID.")

if duplicated_ids > 0:
    raise Exception(f"Data quality error: {duplicated_ids} duplicated deputy IDs.")

# COMMAND ----------

df_valid = df_dedup.filter(col("dept_id_deputado").isNotNull())

records_written = df_valid.count()

#print(f"Records valid for Silver Base: {records_written}")

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

#display(spark.table(TARGET_TABLE))

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer="silver",
    level="INFO",
    event_name="job_finished",
    message=f"source={PIPELINE_NAME} | finished successfully | records_read={records_read} | records_written={records_written}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
    finished_at=datetime.now(),
)

# COMMAND ----------

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Layer: {LAYER}")
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")