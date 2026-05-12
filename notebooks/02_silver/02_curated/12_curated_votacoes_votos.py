# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 07_curated_votacoes_votos
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates deputy voting records from Silver Base.
#
# Context:
# This notebook transforms silver_base.votacoes_votos into a curated and
# analytics-ready voting behavior dataset. The resulting table represents the
# relationship between deputies and voting sessions, preserving party, UF,
# legislature and vote information for downstream Gold fact modeling.
#
# Responsibilities:
# - Consolidate standardized deputy voting attributes from Silver Base
# - Normalize vote values into curated analytical categories
# - Create analytical voting behavior flags
# - Preserve deputy, party, UF, legislature and voting relationships
# - Preserve lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.votacoes_votos
#
# Target:
# silver_curated.votacoes_votos
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Source for deputy voting behavior and political alignment modeling
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    upper,
    current_timestamp,
    when,
    lit,
)

# COMMAND ----------

SOURCE_TABLE = "silver_base.votacoes_votos"
TARGET_TABLE = "silver_curated.votacoes_votos"

PIPELINE_NAME = "silver_curated_votacoes_votos"
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

df_curated = (
    df_base
    .select(
        # ---------------------------------------------------
        # Voting relationship
        # ---------------------------------------------------

        col("vot_id_votacao")
            .alias("vot_id_votacao"),

        col("vot_tx_uri")
            .alias("vot_tx_uri"),

        col("vot_ts_voto")
            .alias("vot_ts_voto"),

        # ---------------------------------------------------
        # Vote value
        # ---------------------------------------------------

        col("vot_tx_voto")
            .alias("vot_tx_voto"),

        when(upper(col("vot_tx_voto")) == "SIM", lit("Sim"))
            .when(upper(col("vot_tx_voto")) == "NÃO", lit("Não"))
            .when(upper(col("vot_tx_voto")) == "NAO", lit("Não"))
            .when(upper(col("vot_tx_voto")).contains("ABST"), lit("Abstenção"))
            .when(upper(col("vot_tx_voto")).contains("OBSTRU"), lit("Obstrução"))
            .otherwise(col("vot_tx_voto"))
            .alias("vot_tx_voto_curado"),

        when(upper(col("vot_tx_voto")) == "SIM", lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_sim"),

        when(
            (upper(col("vot_tx_voto")) == "NÃO") |
            (upper(col("vot_tx_voto")) == "NAO"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_nao"),

        when(upper(col("vot_tx_voto")).contains("ABST"), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_abstencao"),

        when(upper(col("vot_tx_voto")).contains("OBSTRU"), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_obstrucao"),

        # ---------------------------------------------------
        # Deputy relationship
        # ---------------------------------------------------

        col("dept_id_deputado")
            .alias("dept_id_deputado"),

        col("dept_tx_uri")
            .alias("dept_tx_uri"),

        col("dept_tx_nome")
            .alias("dept_tx_nome_parlamentar"),

        col("dept_tx_url_foto")
            .alias("dept_tx_url_foto"),

        # ---------------------------------------------------
        # Party / UF / legislature
        # ---------------------------------------------------

        col("part_sg_partido")
            .alias("part_sg_partido"),

        col("part_tx_uri")
            .alias("part_tx_uri"),

        col("uf_sg_uf")
            .alias("uf_sg_uf"),

        col("leg_id_legislatura")
            .alias("leg_id_legislatura"),

        # ---------------------------------------------------
        # Technical key
        # ---------------------------------------------------

        col("vot_tx_dedup_key")
            .alias("vot_tx_dedup_key"),

        col("bronze_nr_ano_referencia")
            .alias("bronze_nr_ano_referencia"),

        # ---------------------------------------------------
        # Bronze lineage / traceability
        # ---------------------------------------------------

        col("bronze_ts_ingestao")
            .alias("bronze_ts_ingestao"),

        col("bronze_dt_ingestao")
            .alias("bronze_dt_ingestao"),

        col("bronze_tx_endpoint")
            .alias("bronze_tx_endpoint"),

        col("bronze_id_origem")
            .alias("bronze_id_origem"),

        col("bronze_id_batch")
            .alias("bronze_id_batch"),

        col("bronze_tx_record_hash")
            .alias("bronze_tx_record_hash"),

        # ---------------------------------------------------
        # Silver Base lineage
        # ---------------------------------------------------

        col("silver_ts_processamento")
            .alias("silver_base_ts_processamento"),

        # ---------------------------------------------------
        # Silver Curated metadata
        # ---------------------------------------------------

        current_timestamp()
            .alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

duplicated_votes = (
    df_curated
    .groupBy("vot_tx_dedup_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_votes > 0:
    raise Exception(
        f"Data quality error: {duplicated_votes} duplicated voting records in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("vot_tx_dedup_key").isNull()
        |
        col("vot_id_votacao").isNull()
        |
        col("dept_id_deputado").isNull()
    )
    .withColumn(
        "rejection_reason",
        when(
            col("vot_tx_dedup_key").isNull(),
            lit("vot_tx_dedup_key_is_null")
        )
        .when(
            col("vot_id_votacao").isNull(),
            lit("vot_id_votacao_is_null")
        )
        .when(
            col("dept_id_deputado").isNull(),
            lit("dept_id_deputado_is_null")
        )
        .otherwise(lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("vot_tx_dedup_key").isNotNull())
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()

# COMMAND ----------

duplicated_votes = (
    df_curated
    .groupBy("vot_tx_dedup_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_votes > 0:
    raise Exception(
        f"Data quality error: {duplicated_votes} duplicated voting records in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------



# COMMAND ----------

(
    df_discarded.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET_TABLE}_rejeitadas")
)

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("vot_tx_dedup_key").isNotNull())
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
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