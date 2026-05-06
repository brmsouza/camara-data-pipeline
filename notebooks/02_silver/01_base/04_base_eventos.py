# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 04_base_eventos
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates legislative events data
# from the Bronze layer.
#
# Context:
# This notebook transforms raw API payloads from bronze.eventos into a structured
# Silver Base table. The resulting dataset supports future event analytics,
# presence analysis, legislative calendar and engagement metrics.
#
# Responsibilities:
# - Parse raw JSON payload
# - Apply schema standardization
# - Cast dates and timestamps
# - Preserve event location and related bodies
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Delta table for curated consumption
#
# Source:
# bronze.eventos
#
# Target:
# silver_base.eventos
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Source for legislative calendar and attendance analytics
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
    to_date,
    to_timestamp,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    ArrayType,
)

# COMMAND ----------

SOURCE_TABLE = "bronze.eventos"
TARGET_TABLE = "silver_base.eventos"

PIPELINE_NAME = "silver_base_eventos"
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

#print(f"Records read from Bronze: {records_read}")

# COMMAND ----------

#display(df_bronze.limit(10))
#df_bronze.printSchema()

# COMMAND ----------

local_camara_schema = StructType([
    StructField("nome", StringType(), True),
    StructField("predio", StringType(), True),
    StructField("sala", StringType(), True),
    StructField("andar", StringType(), True),
])

orgao_evento_schema = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("sigla", StringType(), True),
    StructField("nome", StringType(), True),
    StructField("apelido", StringType(), True),
    StructField("codTipoOrgao", IntegerType(), True),
    StructField("tipoOrgao", StringType(), True),
    StructField("nomePublicacao", StringType(), True),
    StructField("nomeResumido", StringType(), True),
])

eventos_schema = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("ano_referencia", IntegerType(), True),
    StructField("dataHoraInicio", StringType(), True),
    StructField("dataHoraFim", StringType(), True),
    StructField("data_inicio_janela", StringType(), True),
    StructField("data_fim_janela", StringType(), True),
    StructField("descricao", StringType(), True),
    StructField("descricaoTipo", StringType(), True),
    StructField("situacao", StringType(), True),
    StructField("urlRegistro", StringType(), True),
    StructField("localCamara", local_camara_schema, True),
    StructField("localExterno", StringType(), True),
    StructField("orgaos", ArrayType(orgao_evento_schema), True),
])

# COMMAND ----------

df_parsed = (
    df_bronze
    .withColumn(
        "json_data",
        from_json(col("raw_payload"), eventos_schema)
    )
)

df = (
    df_parsed
    .select(
        col("json_data.id").alias("evt_id_evento"),
        trim(col("json_data.uri")).alias("evt_tx_uri"),
        col("json_data.ano_referencia").cast("int").alias("evt_nr_ano_referencia"),

        to_timestamp(col("json_data.dataHoraInicio")).alias("evt_ts_inicio"),
        to_timestamp(col("json_data.dataHoraFim")).alias("evt_ts_fim"),
        to_date(col("json_data.data_inicio_janela")).alias("evt_dt_inicio_janela"),
        to_date(col("json_data.data_fim_janela")).alias("evt_dt_fim_janela"),

        trim(col("json_data.descricao")).alias("evt_tx_descricao"),
        trim(col("json_data.descricaoTipo")).alias("evt_tx_tipo"),
        trim(col("json_data.situacao")).alias("evt_tx_situacao"),
        trim(col("json_data.urlRegistro")).alias("evt_tx_url_registro"),

        trim(col("json_data.localCamara.nome")).alias("evt_tx_local_camara"),
        trim(col("json_data.localCamara.predio")).alias("evt_tx_predio"),
        trim(col("json_data.localCamara.sala")).alias("evt_tx_sala"),
        trim(col("json_data.localCamara.andar")).alias("evt_tx_andar"),
        trim(col("json_data.localExterno")).alias("evt_tx_local_externo"),

        col("json_data.orgaos").alias("evt_arr_orgaos"),

        col("ingestion_timestamp").alias("bronze_ts_ingestao"),
        col("ingestion_date").alias("bronze_dt_ingestao"),
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
    .partitionBy("evt_id_evento")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

invalid_null_id = df_dedup.filter(col("evt_id_evento").isNull()).count()
invalid_null_inicio = df_dedup.filter(col("evt_ts_inicio").isNull()).count()

duplicated_ids = (
    df_dedup
    .groupBy("evt_id_evento")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if invalid_null_id > 0:
    raise Exception(f"Data quality error: {invalid_null_id} records without event ID.")

if duplicated_ids > 0:
    raise Exception(f"Data quality error: {duplicated_ids} duplicated event IDs.")

#print(f"Data quality warning: {invalid_null_inicio} records without event start timestamp.")

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("evt_id_evento").isNotNull())
)

records_written = df_valid.count()

#print(f"Records valid for Silver Base: {records_written}")

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("evt_nr_ano_referencia")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

#display(spark.table(TARGET_TABLE).limit(50))

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