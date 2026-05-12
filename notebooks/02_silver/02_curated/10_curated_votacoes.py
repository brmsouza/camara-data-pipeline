# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 05_curated_votacoes
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, standardizes, enriches and validates voting session data
# from Silver Base.
#
# Context:
# This notebook transforms silver_base.votacoes into a curated and
# analytics-ready voting dataset. The resulting table centralizes voting
# metadata, voting results, approval status, proposition relationship, event
# relationship and legislative organization context.
#
# Responsibilities:
# - Consolidate standardized voting attributes from Silver Base
# - Create analytical voting flags and voting result indicators
# - Preserve proposition, event and organization relationships
# - Preserve voting result counts
# - Preserve complete lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.votacoes
#
# Target:
# silver_curated.votacoes
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by voting reference year
# - Ready for voting analytics and Gold fact modeling
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col, 
    current_timestamp,
    when,
    lit,
    coalesce,
)
 

# COMMAND ----------

SOURCE_TABLE = "silver_base.votacoes"
TARGET_TABLE = "silver_curated.votacoes"

PIPELINE_NAME = "silver_curated_votacoes"
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
        # Voting identity
        # ---------------------------------------------------

        col("vot_id_votacao")
            .alias("vot_id_votacao"),

        col("vot_tx_uri")
            .alias("vot_tx_uri"),

        # ---------------------------------------------------
        # Voting temporal attributes
        # ---------------------------------------------------

        col("vot_dt_votacao")
            .alias("vot_dt_votacao"),

        col("vot_fl_data_valida")
            .alias("vot_fl_data_valida"),

        col("vot_ts_registro")
            .alias("vot_ts_registro"),

        col("vot_fl_timestamp_registro_valido")
            .alias("vot_fl_timestamp_registro_valido"),

        col("vot_fl_periodo_valido")
            .alias("vot_fl_periodo_valido"),

        col("vot_nr_ano_referencia")
            .alias("vot_nr_ano_referencia"),

        # ---------------------------------------------------
        # Voting description and status
        # ---------------------------------------------------

        col("vot_tx_descricao")
            .alias("vot_tx_descricao"),

        col("vot_fl_aprovacao")
            .alias("vot_fl_aprovacao"),

        when(col("vot_fl_aprovacao") == 1, lit("Aprovada"))
            .when(col("vot_fl_aprovacao") == 0, lit("Não aprovada"))
            .otherwise(lit("Não informado"))
            .alias("vot_tx_status_aprovacao"),

        when(col("vot_fl_aprovacao") == 0, lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_rejeitada"),

        # ---------------------------------------------------
        # Voting result counts
        # ---------------------------------------------------

        coalesce(col("vot_qt_sim"), lit(0))
            .alias("vot_qt_sim"),

        coalesce(col("vot_qt_nao"), lit(0))
            .alias("vot_qt_nao"),

        coalesce(col("vot_qt_outros"), lit(0))
            .alias("vot_qt_outros"),

        (
            coalesce(col("vot_qt_sim"), lit(0)) +
            coalesce(col("vot_qt_nao"), lit(0)) +
            coalesce(col("vot_qt_outros"), lit(0))
        ).alias("vot_qt_total"),

        when(
            coalesce(col("vot_qt_sim"), lit(0)) >
            coalesce(col("vot_qt_nao"), lit(0)),
            lit("Maioria Sim")
        )
        .when(
            coalesce(col("vot_qt_nao"), lit(0)) >
            coalesce(col("vot_qt_sim"), lit(0)),
            lit("Maioria Não")
        )
        .when(
            (
                coalesce(col("vot_qt_sim"), lit(0)) +
                coalesce(col("vot_qt_nao"), lit(0)) +
                coalesce(col("vot_qt_outros"), lit(0))
            ) == 0,
            lit("Sem votos contabilizados")
        )
        .otherwise(lit("Empate ou indeterminado"))
        .alias("vot_tx_resultado_curado"),

        # ---------------------------------------------------
        # Event relationship
        # ---------------------------------------------------

        col("evt_id_evento")
            .alias("evt_id_evento"),

        col("evt_tx_uri")
            .alias("evt_tx_uri"),

        # ---------------------------------------------------
        # Organization relationship
        # ---------------------------------------------------

        col("org_id_orgao")
            .alias("org_id_orgao"),

        col("org_sg_orgao")
            .alias("org_sg_orgao"),

        col("org_tx_uri")
            .alias("org_tx_uri"),

        # ---------------------------------------------------
        # Proposition relationship
        # ---------------------------------------------------

        col("prop_id_proposicao")
            .alias("prop_id_proposicao"),

        col("prop_tx_uri")
            .alias("prop_tx_uri"),

        col("prop_tx_descricao")
            .alias("prop_tx_descricao_votada"),

        # ---------------------------------------------------
        # Analytical flags
        # ---------------------------------------------------

        when(col("prop_id_proposicao").isNotNull(), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_possui_proposicao"),

        when(col("evt_id_evento").isNotNull(), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_possui_evento"),

        when(col("org_id_orgao").isNotNull(), lit(1))
            .otherwise(lit(0))
            .alias("vot_fl_possui_orgao"),

        when(
            (
                coalesce(col("vot_qt_sim"), lit(0)) +
                coalesce(col("vot_qt_nao"), lit(0)) +
                coalesce(col("vot_qt_outros"), lit(0))
            ) > 0,
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_possui_votos_contabilizados"),

        # ---------------------------------------------------
        # Bronze lineage / traceability
        # ---------------------------------------------------

        col("bronze_tx_endpoint")
            .alias("bronze_tx_endpoint"),

        col("bronze_id_origem")
            .alias("bronze_id_origem"),

        col("bronze_id_batch")
            .alias("bronze_id_batch"),

        col("bronze_tx_record_hash")
            .alias("bronze_tx_record_hash"),

        col("bronze_ts_ingestao")
            .alias("bronze_ts_ingestao"),

        col("bronze_dt_ingestao")
            .alias("bronze_dt_ingestao"),

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

duplicated_ids = (
    df_curated
    .groupBy("vot_id_votacao")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_ids > 0:
    raise Exception(
        f"Data quality error: {duplicated_ids} duplicated voting IDs in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("vot_id_votacao").isNull()
        |
        col("vot_nr_ano_referencia").isNull()
    )
    .withColumn(
        "rejection_reason",
        when(
            col("vot_id_votacao").isNull(),
            lit("vot_id_votacao_is_null")
        )
        .when(
            col("vot_nr_ano_referencia").isNull(),
            lit("vot_nr_ano_referencia_is_null")
        )
        .otherwise(lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("vot_nr_ano_referencia").isNotNull())
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("vot_nr_ano_referencia").isNotNull())
)

records_written = df_valid.count()

records_discarded = records_read - records_written

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
    .partitionBy("vot_nr_ano_referencia")
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