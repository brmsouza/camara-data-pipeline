# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 10_base_orgaos_membros
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates legislative organization
# membership data from the Bronze layer.
#
# Context:
# This notebook transforms raw organization membership payloads from
# bronze.orgaos_membros into a structured Silver Base table. The resulting
# dataset represents the relationship between deputies and legislative bodies,
# supporting committee participation, institutional role analysis and future
# dimensional modeling.
#
# Responsibilities:
# - Parse raw CSV-like payload embedded in JSON structure
# - Apply schema standardization
# - Cast dates
# - Preserve organization and deputy relationships
# - Preserve role and membership period information
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Delta table for curated consumption
#
# Source:
# bronze.orgaos_membros
#
# Target:
# silver_base.orgaos_membros
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by ingestion date
# - Source for organization membership and committee analytics
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
    from_csv,
    expr,
    sha2,
    concat_ws,
    regexp_extract,
)

from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.orgaos_membros"
TARGET_TABLE = "silver_base.orgaos_membros"

PIPELINE_NAME = "silver_base_orgaos_membros"

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

df_map = (
    df_bronze
    .withColumn(
        "payload_map",
        from_json(col("raw_payload"), MapType(StringType(), StringType()))
    )
    .withColumn(
        "payload_data",
        expr("""
            element_at(
                map_values(
                    map_filter(
                        payload_map,
                        (k, v) -> k NOT IN ('_source_file', 'ano_referencia')
                    )
                ),
                1
            )
        """)
    )
)

# COMMAND ----------

orgaos_membros_csv_schema = """
uriOrgao STRING,
siglaOrgao STRING,
nomeOrgao STRING,
nomePublicacaoOrgao STRING,
uriDeputado STRING,
nomeDeputado STRING,
siglaPartido STRING,
siglaUF STRING,
cargo STRING,
dataInicio STRING,
dataFim STRING
"""

df_parsed = (
    df_map
    .withColumn(
        "csv_data",
        from_csv(
            col("payload_data"),
            orgaos_membros_csv_schema,
            {
                "sep": ";",
                "quote": '"',
                "escape": '"',
                "header": "false"
            }
        )
    )
)

# COMMAND ----------

df = (
    df_parsed
    .select(
        regexp_extract(
            trim(col("csv_data.uriOrgao")),
            r"/orgaos/([0-9]+)",
            1
        ).try_cast("long").alias("org_id_orgao"),

        trim(col("csv_data.uriOrgao"))
            .alias("org_tx_uri"),

        upper(trim(col("csv_data.siglaOrgao")))
            .alias("org_sg_orgao"),

        trim(col("csv_data.nomeOrgao"))
            .alias("org_tx_nome"),

        trim(col("csv_data.nomePublicacaoOrgao"))
            .alias("org_tx_nome_publicacao"),

        regexp_extract(
            trim(col("csv_data.uriDeputado")),
            r"/deputados/([0-9]+)",
            1
        ).try_cast("long").alias("dept_id_deputado"),

        trim(col("csv_data.uriDeputado"))
            .alias("dept_tx_uri"),

        trim(col("csv_data.nomeDeputado"))
            .alias("dept_tx_nome"),

        upper(trim(col("csv_data.siglaPartido")))
            .alias("part_sg_partido"),

        upper(trim(col("csv_data.siglaUF")))
            .alias("uf_sg_uf"),

        trim(col("csv_data.cargo"))
            .alias("memb_tx_cargo"),

        col("csv_data.dataInicio")
            .try_cast("date")
            .alias("memb_dt_inicio"),

        col("csv_data.dataFim")
            .try_cast("date")
            .alias("memb_dt_fim"),

        sha2(
            concat_ws(
                "||",
                trim(col("csv_data.uriOrgao")),
                trim(col("csv_data.uriDeputado")),
                trim(col("csv_data.cargo")),
                trim(col("csv_data.dataInicio")),
                trim(col("csv_data.dataFim"))
            ),
            256
        ).alias("memb_tx_dedup_key"),

        col("ingestion_timestamp")
            .alias("bronze_ts_ingestao"),

        col("ingestion_date")
            .alias("bronze_dt_ingestao"),

        col("source_endpoint")
            .alias("bronze_tx_endpoint"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_id_origem"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_tx_source_file"),

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
    .filter(col("org_id_orgao").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
)

records_written = df_valid.count()

#print(f"Records valid for Silver Base: {records_written}")

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("bronze_dt_ingestao")
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

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")