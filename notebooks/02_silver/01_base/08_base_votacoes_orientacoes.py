# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 08_base_votacoes_orientacoes
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates voting orientation data
# from the Bronze layer.
#
# Context:
# This notebook transforms raw voting orientation payloads from
# bronze.votacoes_orientacoes into a structured Silver Base table. The resulting
# dataset represents party, bloc and bench orientations for each voting session,
# enabling downstream analysis of political alignment, party discipline and
# comparison between official orientation and individual deputy votes.
#
# Responsibilities:
# - Parse raw CSV-like payload embedded in JSON structure
# - Apply schema standardization
# - Cast identifiers where applicable
# - Preserve voting and political bench relationships
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Silver Base Delta table
#
# Source:
# bronze.votacoes_orientacoes
#
# Target:
# silver_base.votacoes_orientacoes
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by ingestion date
# - Source for voting alignment and party orientation analytics
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
    initcap,
    count,
)

from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.votacoes_orientacoes"
TARGET_TABLE = "silver_base.votacoes_orientacoes"

PIPELINE_NAME = "silver_base_votacoes_orientacoes"
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

votacoes_orientacoes_csv_schema = """
idVotacao STRING,
uriVotacao STRING,
siglaOrgao STRING,
descricao STRING,
siglaBancada STRING,
uriBancada STRING,
orientacao STRING
"""

df_parsed = (
    df_map
    .withColumn(
        "csv_data",
        from_csv(
            col("payload_data"),
            votacoes_orientacoes_csv_schema,
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

df_standardized = (
    df_parsed
    .select(
        trim(col("csv_data.idVotacao"))
            .alias("vot_id_votacao"),

        trim(col("csv_data.uriVotacao"))
            .alias("vot_tx_uri"),

        upper(trim(col("csv_data.siglaOrgao")))
            .alias("org_sg_orgao"),

        initcap(trim(col("csv_data.descricao")))
            .alias("vot_tx_descricao_resultado"),

        upper(trim(col("csv_data.siglaBancada")))
            .alias("banc_tx_sigla_bancada"),

        trim(col("csv_data.uriBancada"))
            .alias("banc_tx_uri"),

        initcap(trim(col("csv_data.orientacao")))
            .alias("vot_tx_orientacao"),

        sha2(
            concat_ws(
                "||",
                trim(col("csv_data.idVotacao")),
                upper(trim(col("csv_data.siglaOrgao"))),
                upper(trim(col("csv_data.siglaBancada"))),
                initcap(trim(col("csv_data.orientacao")))
            ),
            256
        ).alias("vot_tx_dedup_key"),

        col("ingestion_timestamp")
            .alias("bronze_ts_ingestao"),

        col("ingestion_date")
            .alias("bronze_dt_ingestao"),

        col("source_endpoint")
            .alias("bronze_tx_endpoint"),

        col("source_id")
            .alias("bronze_id_origem"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_tx_source_file"),

        col("payload_map")
            .getItem("ano_referencia")
            .cast("int")
            .alias("bronze_nr_ano_referencia"),

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
    .partitionBy("vot_tx_dedup_key")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df_standardized
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

invalid_null_votacao = (
    df_dedup
    .filter(col("vot_id_votacao").isNull())
    .count()
)

invalid_null_bancada = (
    df_dedup
    .filter(col("banc_tx_sigla_bancada").isNull())
    .count()
)

duplicated_orientations = (
    df_dedup
    .groupBy("vot_tx_dedup_key")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)


if duplicated_orientations > 0:
    raise Exception(
        f"Data quality error: {duplicated_orientations} duplicated orientation records."
    )
    
df_valid = (
    df_dedup
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("banc_tx_sigla_bancada").isNotNull())
)

records_written = df_valid.count()

records_discarded = records_read - records_written

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("bronze_nr_ano_referencia")
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