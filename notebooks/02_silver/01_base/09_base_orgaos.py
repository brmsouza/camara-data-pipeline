# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 09_base_orgaos
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates legislative organization
# data from the Bronze layer.
#
# Context:
# This notebook transforms raw organization payloads from bronze.orgaos into a
# structured Silver Base table. The resulting dataset centralizes Câmara
# organizations such as plenary, committees and other legislative bodies,
# supporting proposition, event and voting relationship analysis.
#
# Responsibilities:
# - Parse raw JSON payload
# - Apply schema standardization
# - Cast identifiers
# - Preserve organization classification fields
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Silver Base Delta table
#
# Source:
# bronze.orgaos
#
# Target:
# silver_base.orgaos
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Source for legislative organization dimensions
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
    initcap,
    count,
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

SOURCE_TABLE = "bronze.orgaos"
TARGET_TABLE = "silver_base.orgaos"

PIPELINE_NAME = "silver_base_orgaos"
LAYER = "silver_base"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
records_discarded = 0

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
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

orgaos_schema = StructType([
    StructField("apelido", StringType(), True),
    StructField("codTipoOrgao", IntegerType(), True),
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("nomePublicacao", StringType(), True),
    StructField("nomeResumido", StringType(), True),
    StructField("sigla", StringType(), True),
    StructField("tipoOrgao", StringType(), True),
    StructField("uri", StringType(), True),
])

df_parsed = (
    df_bronze
    .withColumn(
        "json_data",
        from_json(col("raw_payload"), orgaos_schema)
    )
)

# COMMAND ----------

df_standardized = (
    df_parsed
    .select(
        col("json_data.id")
            .alias("org_id_orgao"),

        trim(col("json_data.uri"))
            .alias("org_tx_uri"),

        upper(trim(col("json_data.sigla")))
            .alias("org_sg_orgao"),

        initcap(trim(col("json_data.nome")))
            .alias("org_tx_nome"),

        initcap(trim(col("json_data.apelido")))
            .alias("org_tx_apelido"),

        initcap(trim(col("json_data.nomePublicacao")))
            .alias("org_tx_nome_publicacao"),

        initcap(trim(col("json_data.nomeResumido")))
            .alias("org_tx_nome_resumido"),

        col("json_data.codTipoOrgao")
            .alias("org_cd_tipo_orgao"),

        initcap(trim(col("json_data.tipoOrgao")))
            .alias("org_tx_tipo_orgao"),

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
    .partitionBy("org_id_orgao")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df_standardized
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

invalid_null_orgao = (
    df_dedup
    .filter(col("org_id_orgao").isNull())
    .count()
)

duplicated_orgaos = (
    df_dedup
    .groupBy("org_id_orgao")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if duplicated_orgaos > 0:
    raise Exception(
        f"Data quality error: {duplicated_orgaos} duplicated organizations."
    )
df_valid = (
    df_dedup
    .filter(col("org_id_orgao").isNotNull())
)

records_written = df_valid.count()

records_discarded = records_read - records_written


# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=f"source={PIPELINE_NAME} | finished successfully | records_read={records_read} | records_written={records_written} | records_discarded={records_discarded}",
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
print(f"Records discarded: {records_discarded}")