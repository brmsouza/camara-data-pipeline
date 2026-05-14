# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Curated Layer — Legislative Proposition Consolidation and Enrichment
# MAGIC
# MAGIC **Notebook:** `06_curated_proposicoes`
# MAGIC
# MAGIC Consolidates, enriches and validates legislative proposition data
# MAGIC from the Silver Base layer.
# MAGIC
# MAGIC This notebook transforms `silver_base.proposicoes` into a curated and
# MAGIC analytics-ready proposition dataset. The resulting table centralizes
# MAGIC proposition metadata, legislative status, proposition lifecycle and
# MAGIC parliamentary processing information used downstream in Gold fact tables,
# MAGIC legislative analytics and voting correlation analysis.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Consolidate standardized proposition attributes from Silver Base
# MAGIC - Curate legislative status and proposition type indicators
# MAGIC - Create analytical proposition flags
# MAGIC - Create proposition lifecycle indicators
# MAGIC - Preserve lineage and traceability columns
# MAGIC - Validate curated-level uniqueness
# MAGIC - Persist curated Delta tables for Gold consumption
# MAGIC
# MAGIC **Source of truth:** `silver_base.proposicoes`  
# MAGIC
# MAGIC **Target:** `silver_curated.proposicoes`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake format
# MAGIC - Ready for legislative analytics and Gold modeling

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
    year,
    month
)

from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "silver_base.proposicoes"
TARGET_TABLE = "silver_curated.proposicoes"

PIPELINE_NAME = "silver_curated_proposicoes"
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
        col("prop_id_proposicao").alias("prop_id_proposicao"),
        col("prop_tx_uri").alias("prop_tx_uri"),
        col("prop_sg_tipo").alias("prop_sg_tipo"),
        col("prop_tx_descricao_tipo").alias("prop_tx_descricao_tipo"),
        col("prop_nr_numero").alias("prop_nr_numero"),
        col("prop_nr_ano").alias("prop_nr_ano"),
        col("prop_cd_tipo").alias("prop_cd_tipo"),

        col("prop_tx_ementa").alias("prop_tx_ementa"),
        col("prop_tx_ementa_detalhada").alias("prop_tx_ementa_detalhada"),
        col("prop_tx_keywords").alias("prop_tx_keywords"),

        col("prop_ts_apresentacao").alias("prop_ts_apresentacao"),
        col("prop_fl_data_apresentacao_valida").alias("prop_fl_data_apresentacao_valida"),
        year(col("prop_ts_apresentacao")).alias("prop_nr_ano_apresentacao"),
        month(col("prop_ts_apresentacao")).alias("prop_nr_mes_apresentacao"),

        col("org_tx_uri_numerador").alias("prop_tx_uri_orgao_numerador"),
        col("prop_tx_uri_anterior").alias("prop_tx_uri_anterior"),
        col("prop_tx_uri_principal").alias("prop_tx_uri_principal"),
        col("prop_tx_uri_posterior").alias("prop_tx_uri_posterior"),
        col("prop_tx_url_inteiro_teor").alias("prop_tx_url_inteiro_teor"),
        col("prop_tx_urn_final").alias("prop_tx_urn_final"),

        col("status_ts_data_hora").alias("prop_ts_status_data_hora"),
        col("status_fl_data_hora_valida").alias("prop_fl_data_status_valida"),
        col("status_fl_periodo_valido").alias("prop_fl_periodo_status_valido"),
        col("status_nr_sequencia").alias("prop_nr_status_sequencia"),
        col("status_tx_uri_relator").alias("prop_tx_status_uri_relator"),
        col("status_id_orgao").alias("prop_id_status_orgao"),
        col("status_sg_orgao").alias("prop_sg_status_orgao"),
        col("status_tx_uri_orgao").alias("prop_tx_status_uri_orgao"),
        col("status_tx_regime").alias("prop_tx_status_regime"),
        col("status_tx_descricao_tramitacao").alias("prop_tx_status_descricao_tramitacao"),
        col("status_id_tipo_tramitacao").alias("prop_id_status_tipo_tramitacao"),
        col("status_tx_descricao_situacao").alias("prop_tx_status_descricao_situacao"),
        col("status_id_situacao").alias("prop_id_status_situacao"),
        col("status_tx_despacho").alias("prop_tx_status_despacho"),
        col("status_tx_apreciacao").alias("prop_tx_status_apreciacao"),
        col("status_tx_url").alias("prop_tx_status_url"),

        when(upper(col("status_tx_descricao_situacao")).contains("ARQUIV"), lit("Arquivada"))
            .when(upper(col("status_tx_descricao_situacao")).contains("APROV"), lit("Aprovada"))
            .when(upper(col("status_tx_descricao_situacao")).contains("REJEIT"), lit("Rejeitada"))
            .otherwise(lit("Em tramitação"))
            .alias("prop_tx_status_curado"),

        when(upper(col("status_tx_descricao_situacao")).contains("ARQUIV"), lit(0))
            .otherwise(lit(1))
            .alias("prop_fl_tramitando"),

        when(upper(col("status_tx_descricao_situacao")).contains("APROV"), lit(1))
            .otherwise(lit(0))
            .alias("prop_fl_aprovada"),

        when(upper(col("status_tx_descricao_situacao")).contains("REJEIT"), lit(1))
            .otherwise(lit(0))
            .alias("prop_fl_rejeitada"),

        when(upper(col("prop_sg_tipo")) == "PL", lit("Projeto de Lei"))
            .when(upper(col("prop_sg_tipo")) == "PEC", lit("Proposta de Emenda Constitucional"))
            .otherwise(col("prop_tx_descricao_tipo"))
            .alias("prop_tx_tipo_curado"),

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

duplicated_ids = (
    df_curated
    .groupBy("prop_id_proposicao")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_ids > 0:
    raise Exception(
        f"Data quality error: {duplicated_ids} duplicated proposition IDs in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("prop_id_proposicao").isNull()
        |
        col("prop_nr_ano_apresentacao").isNull()
    )
    .withColumn(
        "rejection_reason",
        when(
            col("prop_id_proposicao").isNull(),
            lit("prop_id_proposicao_is_null")
        )
        .when(
            col("prop_nr_ano_apresentacao").isNull(),
            lit("prop_nr_ano_apresentacao_is_null")
        )
        .otherwise(lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("prop_id_proposicao").isNotNull())
    .filter(col("prop_nr_ano_apresentacao").isNotNull())
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

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
    .partitionBy("prop_nr_ano_apresentacao")
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