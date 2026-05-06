# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 07_base_votacoes_votos
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates parliamentary voting
# records from the Bronze layer.
#
# Context:
# This notebook transforms raw voting vote payloads from bronze.votacoes_votos
# into a structured Silver Base table. The resulting dataset represents the
# relationship between deputies and voting sessions, enabling downstream
# analytical models related to parliamentary behavior, political alignment,
# party cohesion and voting analytics.
#
# Responsibilities:
# - Parse raw CSV-like payload embedded in JSON structure
# - Apply schema standardization
# - Cast identifiers and timestamps
# - Preserve deputy and voting relationships
# - Preserve party and federation information
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Delta table for curated consumption
#
# Source:
# bronze.votacoes_votos
#
# Target:
# silver_base.votacoes_votos
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by voting year
# - Source for voting behavior and political analytics
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
    regexp_replace,
    expr,
)

from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.votacoes_votos"
TARGET_TABLE = "silver_base.votacoes_votos"

PIPELINE_NAME = "silver_base_votacoes_votos"

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

print(f"Records read from Bronze: {records_read}")

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

votacoes_votos_csv_schema = """
idVotacao STRING,
uriVotacao STRING,
dataHoraVoto STRING,
voto STRING,
deputado_id STRING,
deputado_uri STRING,
deputado_nome STRING,
deputado_siglaPartido STRING,
deputado_uriPartido STRING,
deputado_siglaUf STRING,
deputado_idLegislatura STRING,
deputado_urlFoto STRING
"""

df_parsed = (
    df_map
    .withColumn(
        "csv_data",
        from_csv(
            col("payload_data"),
            votacoes_votos_csv_schema,
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
        # ---------------------------------------------------
        # Voting relationship
        # ---------------------------------------------------

        trim(col("csv_data.idVotacao"))
            .alias("vot_id_votacao"),

        trim(col("csv_data.uriVotacao"))
            .alias("vot_tx_uri"),

        col("csv_data.dataHoraVoto")
            .try_cast("timestamp")
            .alias("vot_ts_voto"),

        trim(col("csv_data.voto"))
            .alias("vot_tx_voto"),

        # ---------------------------------------------------
        # Deputy relationship
        # ---------------------------------------------------

        col("csv_data.deputado_id")
            .try_cast("long")
            .alias("dept_id_deputado"),

        trim(col("csv_data.deputado_uri"))
            .alias("dept_tx_uri"),

        trim(col("csv_data.deputado_nome"))
            .alias("dept_tx_nome"),

        upper(trim(col("csv_data.deputado_siglaPartido")))
            .alias("part_sg_partido"),

        trim(col("csv_data.deputado_uriPartido"))
            .alias("part_tx_uri"),

        upper(trim(col("csv_data.deputado_siglaUf")))
            .alias("uf_sg_uf"),

        col("csv_data.deputado_idLegislatura")
            .try_cast("int")
            .alias("leg_id_legislatura"),

        trim(col("csv_data.deputado_urlFoto"))
            .alias("dept_tx_url_foto"),

        # ---------------------------------------------------
        # Technical dedup key
        # ---------------------------------------------------

        expr("""
            sha2(
                concat_ws(
                    '||',
                    csv_data.idVotacao,
                    csv_data.deputado_id,
                    csv_data.voto
                ),
                256
            )
        """).alias("vot_tx_dedup_key"),

        # ---------------------------------------------------
        # Bronze lineage / traceability
        # ---------------------------------------------------

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

        # ---------------------------------------------------
        # Silver metadata
        # ---------------------------------------------------

        current_timestamp()
            .alias("silver_ts_processamento")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("vot_tx_dedup_key")
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
    .filter(col("vot_id_votacao").isNotNull())
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