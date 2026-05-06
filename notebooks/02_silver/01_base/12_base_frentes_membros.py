# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 12_base_frentes_membros
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates parliamentary front
# membership data from the Bronze layer.
#
# Context:
# This notebook transforms raw parliamentary front membership payloads from
# bronze.frentes_membros into a structured Silver Base table. The resulting
# dataset represents the relationship between deputies and parliamentary fronts,
# supporting downstream analysis of political groups, thematic coalitions,
# parliamentary participation and future correlation with expenses and votes.
#
# Responsibilities:
# - Parse raw JSON payload
# - Apply schema standardization
# - Cast identifiers and dates
# - Preserve deputy, party and parliamentary front relationships
# - Preserve membership role information
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Delta table for curated consumption
#
# Source:
# bronze.frentes_membros
#
# Target:
# silver_base.frentes_membros
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Source for parliamentary front membership analytics
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
    from_json,
    sha2,
    concat_ws,
)

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
)

from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.frentes_membros"
TARGET_TABLE = "silver_base.frentes_membros"

PIPELINE_NAME = "silver_base_frentes_membros"

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

#print(f"Records read from Bronze: {records_read}")

# COMMAND ----------

frentes_membros_schema = StructType([
    StructField("codTitulo", IntegerType(), True),
    StructField("dataFim", StringType(), True),
    StructField("dataInicio", StringType(), True),
    StructField("email", StringType(), True),
    StructField("id", LongType(), True),
    StructField("idLegislatura", IntegerType(), True),
    StructField("id_frente", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("siglaPartido", StringType(), True),
    StructField("siglaUf", StringType(), True),
    StructField("titulo", StringType(), True),
    StructField("uri", StringType(), True),
    StructField("uriPartido", StringType(), True),
    StructField("urlFoto", StringType(), True),
])

df_parsed = (
    df_bronze
    .withColumn(
        "json_data",
        from_json(col("raw_payload"), frentes_membros_schema)
    )
)

# COMMAND ----------

df = (
    df_parsed
    .select(
        col("json_data.id_frente")
            .try_cast("long")
            .alias("frente_id_frente"),

        col("json_data.id")
            .alias("dept_id_deputado"),

        trim(col("json_data.uri"))
            .alias("dept_tx_uri"),

        trim(col("json_data.nome"))
            .alias("dept_tx_nome"),

        trim(col("json_data.email"))
            .alias("dept_tx_email"),

        upper(trim(col("json_data.siglaPartido")))
            .alias("part_sg_partido"),

        trim(col("json_data.uriPartido"))
            .alias("part_tx_uri"),

        upper(trim(col("json_data.siglaUf")))
            .alias("uf_sg_uf"),

        col("json_data.idLegislatura")
            .alias("leg_id_legislatura"),

        col("json_data.codTitulo")
            .alias("memb_cd_titulo"),

        trim(col("json_data.titulo"))
            .alias("memb_tx_titulo"),

        col("json_data.dataInicio")
            .try_cast("date")
            .alias("memb_dt_inicio"),

        col("json_data.dataFim")
            .try_cast("date")
            .alias("memb_dt_fim"),

        trim(col("json_data.urlFoto"))
            .alias("dept_tx_url_foto"),

        sha2(
            concat_ws(
                "||",
                col("json_data.id_frente"),
                col("json_data.id"),
                col("json_data.codTitulo"),
                col("json_data.titulo")
            ),
            256
        ).alias("memb_tx_dedup_key"),

        col("ingestion_timestamp")
            .alias("bronze_ts_ingestao"),

        col("ingestion_date")
            .alias("bronze_dt_ingestao"),

        col("source_endpoint")
            .alias("bronze_tx_endpoint"),

        col("source_id")
            .alias("bronze_id_origem"),

        col("batch_id")
            .alias("bronze_id_batch"),

        col("record_hash")
            .alias("bronze_tx_record_hash"),

        current_timestamp()
            .alias("silver_ts_processamento")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("memb_tx_dedup_key")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("frente_id_frente").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
)

records_written = df_valid.count()

##print(f"Records valid for Silver Base: {records_written}")

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

display(spark.table(TARGET_TABLE).limit(50))

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

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")