# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 08_curated_votacoes_orientacoes
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates voting orientation data from Silver Base.
#
# Context:
# This notebook transforms silver_base.votacoes_orientacoes into a curated and
# analytics-ready dataset representing party, bloc and bench voting orientations.
# The resulting table supports downstream comparison between official political
# orientation and individual deputy votes.
#
# Responsibilities:
# - Consolidate standardized voting orientation attributes from Silver Base
# - Normalize voting orientation values into curated analytical categories
# - Create analytical orientation flags
# - Preserve voting, organization and bench relationships
# - Preserve lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.votacoes_orientacoes
#
# Target:
# silver_curated.votacoes_orientacoes
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Source for political alignment and party discipline modeling
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    upper,
    trim,
    regexp_replace,
    current_timestamp,
    when,
    lit,
    length,
)

# COMMAND ----------

SOURCE_TABLE = "silver_base.votacoes_orientacoes"
TARGET_TABLE = "silver_curated.votacoes_orientacoes"

PIPELINE_NAME = "silver_curated_votacoes_orientacoes"
LAYER = "silver_curated"

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

df_base = spark.table(SOURCE_TABLE)

records_read = df_base.count()


# COMMAND ----------

df_enriched = (
    df_base
    .withColumn(
        "banc_tx_sigla_original",
        col("banc_tx_sigla_bancada")
    )
    .withColumn(
        "banc_tx_sigla_normalizada",
        upper(trim(col("banc_tx_sigla_bancada")))
    )
    .withColumn(
        "banc_tx_sigla_normalizada",
        regexp_replace(col("banc_tx_sigla_normalizada"), r"\.$", "")
    )
    .withColumn(
        "banc_tx_sigla_normalizada",
        regexp_replace(col("banc_tx_sigla_normalizada"), r"\s+", " ")
    )
    .withColumn(
        "banc_tx_sigla_normalizada",
        when(col("banc_tx_sigla_normalizada") == "REPUBLICAN", lit("REPUBLICANOS"))
        .when(col("banc_tx_sigla_normalizada") == "SOLIDARIED", lit("SOLIDARIEDADE"))
        .when(col("banc_tx_sigla_normalizada").isin("GOV", "GOVERNO"), lit("GOVERNO"))
        .when(col("banc_tx_sigla_normalizada").contains("E TOTAL DE VOTANTES"), lit(None))
        .when(col("banc_tx_sigla_normalizada").contains("TOTAL DE VOTANTES"), lit(None))
        .when(col("banc_tx_sigla_normalizada").contains("TOTAL:"), lit(None))
        .when(col("banc_tx_sigla_normalizada") == "", lit(None))
        .otherwise(col("banc_tx_sigla_normalizada"))
    )
)

# COMMAND ----------

df_enriched = (
    df_enriched
    .withColumn(
        "banc_fl_bancada_valida",
        when(col("banc_tx_sigla_normalizada").isNull(), lit(0))
        .when(upper(col("banc_tx_sigla_normalizada")).contains("VOTOS"), lit(0))
        .when(upper(col("banc_tx_sigla_normalizada")).contains("VOTO"), lit(0))
        .when(upper(col("banc_tx_sigla_normalizada")).contains("EMENDA"), lit(0))
        .when(upper(col("banc_tx_sigla_normalizada")).contains("PROJETO"), lit(0))
        .when(upper(col("banc_tx_sigla_normalizada")).contains("DEPUTAD"), lit(0))
        .when(upper(col("banc_tx_sigla_normalizada")).contains("ART."), lit(0))
        .when(upper(col("banc_tx_sigla_normalizada")).rlike("^[0-9]"), lit(0))
        .when(length(col("banc_tx_sigla_normalizada")) > 40, lit(0))
        .otherwise(lit(1))
    )
)

# COMMAND ----------

df_curated = (
    df_enriched
    .select(
        col("vot_id_votacao").alias("vot_id_votacao"),
        col("vot_tx_uri").alias("vot_tx_uri"),
        col("org_sg_orgao").alias("org_sg_orgao"),

        col("banc_tx_sigla_original").alias("banc_tx_sigla_bancada"),
        col("banc_tx_sigla_normalizada").alias("banc_tx_bancada_curada"),
        col("banc_tx_uri").alias("banc_tx_uri"),

        when(col("banc_tx_sigla_normalizada").isNull(), lit("Inválida"))
            .when(col("banc_tx_sigla_normalizada").contains("GOVERNO"), lit("Governo"))
            .when(col("banc_tx_sigla_normalizada").contains("OPOSI"), lit("Oposição"))
            .when(col("banc_tx_sigla_normalizada").contains("MINORIA"), lit("Minoria"))
            .when(col("banc_tx_sigla_normalizada").contains("MAIORIA"), lit("Maioria"))
            .when(col("banc_tx_sigla_normalizada").startswith("FDR"), lit("Federação"))
            .when(col("banc_tx_sigla_normalizada").isin("PSCPTB", "NOVOCIDPV"), lit("Bloco/Federação"))
            .otherwise(lit("Partido"))
            .alias("banc_tx_tipo_bancada"),
            col("banc_fl_bancada_valida").alias("banc_fl_bancada_valida"),

        col("vot_tx_orientacao").alias("vot_tx_orientacao"),

        when(upper(col("vot_tx_orientacao")) == "SIM", lit("Sim"))
            .when(
                (upper(col("vot_tx_orientacao")) == "NÃO") |
                (upper(col("vot_tx_orientacao")) == "NAO"),
                lit("Não")
            )
            .when(upper(col("vot_tx_orientacao")).contains("LIBER"), lit("Liberado"))
            .when(upper(col("vot_tx_orientacao")).contains("OBSTRU"), lit("Obstrução"))
            .when(upper(col("vot_tx_orientacao")).contains("ABST"), lit("Abstenção"))
            .otherwise(col("vot_tx_orientacao"))
            .alias("vot_tx_orientacao_curada"),

        col("vot_tx_descricao_resultado").alias("vot_tx_descricao_resultado"),

        when(upper(col("vot_tx_orientacao")) == "SIM", lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_orientacao_sim"),

        when(
            (upper(col("vot_tx_orientacao")) == "NÃO") |
            (upper(col("vot_tx_orientacao")) == "NAO"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_orientacao_nao"),

        when(upper(col("vot_tx_orientacao")).contains("LIBER"), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_orientacao_liberado"),

        when(upper(col("vot_tx_orientacao")).contains("OBSTRU"), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_orientacao_obstrucao"),

        when(upper(col("vot_tx_orientacao")).contains("ABST"), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_orientacao_abstencao"),

        col("vot_tx_dedup_key").alias("vot_tx_dedup_key"),
        col("bronze_nr_ano_referencia").alias("bronze_nr_ano_referencia"),

        col("bronze_ts_ingestao").alias("bronze_ts_ingestao"),
        col("bronze_dt_ingestao").alias("bronze_dt_ingestao"),
        col("bronze_tx_endpoint").alias("bronze_tx_endpoint"),
        col("bronze_id_origem").alias("bronze_id_origem"),
        col("bronze_tx_source_file").alias("bronze_tx_source_file"),
        col("bronze_id_batch").alias("bronze_id_batch"),
        col("bronze_tx_record_hash").alias("bronze_tx_record_hash"),

        col("silver_ts_processamento").alias("silver_base_ts_processamento"),

        current_timestamp().alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

duplicated_orientations = (
    df_curated
    .groupBy("vot_tx_dedup_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_orientations > 0:
    raise Exception(
        f"Data quality error: {duplicated_orientations} duplicated orientation records in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

df_discarded = (
    df_dedup
    .filter(
        col("vot_tx_dedup_key").isNull()
        |
        col("vot_id_votacao").isNull()
        |
        col("banc_tx_bancada_curada").isNull()
        |
        (col("banc_tx_tipo_bancada") == "Inválida")
        |
        (col("banc_fl_bancada_valida") != 1)
    )
    .withColumn(
        "rejection_reason",
        when(col("vot_tx_dedup_key").isNull(), lit("vot_tx_dedup_key_is_null"))
        .when(col("vot_id_votacao").isNull(), lit("vot_id_votacao_is_null"))
        .when(col("banc_tx_bancada_curada").isNull(), lit("banc_tx_bancada_curada_is_null"))
        .when(col("banc_tx_tipo_bancada") == "Inválida", lit("bancada_invalida"))
        .when(col("banc_fl_bancada_valida") != 1, lit("bancada_texto_invalido"))
        .otherwise(lit("unknown"))
    )
)

df_valid = (
    df_dedup
    .filter(col("vot_tx_dedup_key").isNotNull())
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("banc_tx_bancada_curada").isNotNull())
    .filter(col("banc_tx_tipo_bancada") != "Inválida")
    .filter(col("banc_fl_bancada_valida") == 1)
)

records_written = df_valid.count()
records_discarded = df_discarded.count()

# COMMAND ----------

(
    df_discarded.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET_TABLE}_rejeitadas")
)

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