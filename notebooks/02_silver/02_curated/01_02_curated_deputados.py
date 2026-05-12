# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 01_02_curated_deputados
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates deputy data from Silver Base.
#
# Context:
# This notebook joins silver_base.deputados with silver_base.deputados_detalhes
# to create a curated and analytics-ready deputy dataset. The resulting table
# centralizes parliamentary identity, party, federation, personal profile,
# contact, office and status information.
#
# Responsibilities:
# - Join base deputy and deputy detail datasets
# - Consolidate standardized deputy attributes from Silver Base
# - Resolve fallback attributes between deputados and deputados_detalhes
# - Preserve deputy, party and legislature relationships
# - Create business-friendly descriptive attributes
# - Preserve technical validation flags from Silver Base
# - Preserve lineage and processing metadata
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Sources:
# silver_base.deputados
# silver_base.deputados_detalhes
#
# Target:
# silver_curated.deputados
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Ready for Gold dimension modeling
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
    concat_ws,
    coalesce,
)


# COMMAND ----------

SOURCE_TABLE_DEPUTADOS = "silver_base.deputados"
SOURCE_TABLE_DETALHES = "silver_base.deputados_detalhes"
TARGET_TABLE = "silver_curated.deputados"

PIPELINE_NAME = "silver_curated_deputados"
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
    endpoint=f"{SOURCE_TABLE_DEPUTADOS}, {SOURCE_TABLE_DETALHES}",
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

df_deputados = spark.table(SOURCE_TABLE_DEPUTADOS)
df_detalhes = spark.table(SOURCE_TABLE_DETALHES)

records_read = df_deputados.count()


# COMMAND ----------

df_joined = (
    df_deputados.alias("dep")
    .join(
        df_detalhes.alias("det"),
        col("dep.dept_id_deputado") == col("det.dept_id_deputado"),
        "left"
    )
)

# COMMAND ----------

df_curated = (
    df_joined
    .select(
        # ---------------------------------------------------
        # Deputy identity
        # ---------------------------------------------------

        col("dep.dept_id_deputado")
            .alias("dept_id_deputado"),

        coalesce(col("det.dept_tx_nome_parlamentar"), col("dep.dept_tx_nome"))
            .alias("dept_tx_nome_parlamentar"),

        col("det.dept_tx_nome_civil")
            .alias("dept_tx_nome_civil"),

        col("det.dept_tx_nome_eleitoral")
            .alias("dept_tx_nome_eleitoral"),

        # ---------------------------------------------------
        # Party / federation / legislature
        # ---------------------------------------------------

        coalesce(col("det.part_sg_partido"), col("dep.part_sg_partido"))
            .alias("part_sg_partido"),

        coalesce(col("det.uf_sg_uf"), col("dep.uf_sg_uf"))
            .alias("uf_sg_uf"),

        col("det.leg_id_legislatura")
            .alias("leg_id_legislatura"),

        # ---------------------------------------------------
        # Personal profile
        # ---------------------------------------------------

        when(col("det.dept_sg_sexo") == "M", lit("Masculino"))
            .when(col("det.dept_sg_sexo") == "F", lit("Feminino"))
            .otherwise(col("det.dept_sg_sexo"))
            .alias("dept_tx_sexo"),

        col("det.dept_dt_nascimento")
            .alias("dept_dt_nascimento"),

        col("det.dept_fl_data_nascimento_valida")
            .alias("dept_fl_data_nascimento_valida"),

        col("det.dept_dt_falecimento")
            .alias("dept_dt_falecimento"),

        col("det.dept_fl_data_falecimento_valida")
            .alias("dept_fl_data_falecimento_valida"),

        col("det.uf_sg_nascimento")
            .alias("dept_sg_uf_nascimento"),

        col("det.dept_tx_municipio_nascimento")
            .alias("dept_tx_municipio_nascimento"),

        col("det.dept_tx_escolaridade")
            .alias("dept_tx_escolaridade"),

        # ---------------------------------------------------
        # Mandate status
        # ---------------------------------------------------

        col("det.dept_id_ultimo_status")
            .alias("dept_id_status_mandato"),

        col("det.dept_dt_ultimo_status")
            .alias("dept_dt_status_mandato"),

        col("det.dept_fl_data_status_valida")
            .alias("dept_fl_data_status_mandato_valida"),

        col("det.dept_tx_situacao")
            .alias("dept_tx_situacao_mandato"),

        col("det.dept_tx_condicao_eleitoral")
            .alias("dept_tx_condicao_eleitoral"),

        col("det.dept_tx_descricao_status")
            .alias("dept_tx_descricao_status_mandato"),

        when(col("det.dept_dt_falecimento").isNotNull(), lit("Falecido"))
            .when(
                upper(col("det.dept_tx_situacao")).contains("EXERC"),
                lit("Em exercício")
            )
            .otherwise(col("det.dept_tx_situacao"))
            .alias("dept_tx_status_mandato_curado"),

        col("det.dept_nr_cpf")
            .alias("dept_nr_cpf"),

        col("det.dept_fl_cpf_valido")
            .alias("dept_fl_cpf_valido"),

        # ---------------------------------------------------
        # Contact and digital presence
        # ---------------------------------------------------

        coalesce(col("det.dept_tx_email"), col("dep.dept_tx_email"))
            .alias("dept_tx_email"),

        coalesce(
            col("det.dept_fl_email_valido"),
            col("dep.dept_fl_email_valido"),
            lit(0)
        ).alias("dept_fl_email_valido"),

        coalesce(col("det.dept_tx_url_website"), col("dep.dept_tx_url_foto"))
            .alias("dept_tx_url_referencia"),

        col("dep.dept_tx_url_foto")
            .alias("dept_tx_url_foto"),

        concat_ws(" | ", col("det.dept_arr_rede_social"))
            .alias("dept_tx_redes_sociais"),

        # ---------------------------------------------------
        # Office / gabinete
        # ---------------------------------------------------

        col("det.gab_tx_nome")
            .alias("gab_tx_nome"),

        col("det.gab_tx_predio")
            .alias("gab_tx_predio"),

        col("det.gab_tx_sala")
            .alias("gab_tx_sala"),

        col("det.gab_tx_andar")
            .alias("gab_tx_andar"),

        col("det.gab_tx_telefone")
            .alias("gab_tx_telefone"),

        col("det.gab_fl_telefone_valido")
            .alias("gab_fl_telefone_valido"),

        col("det.gab_tx_email")
            .alias("gab_tx_email"),

        col("det.gab_fl_email_valido")
            .alias("gab_fl_email_valido"),

        # ---------------------------------------------------
        # Lineage
        # ---------------------------------------------------

        col("dep.bronze_ts_ingestao")
            .alias("bronze_ts_ingestao_deputados"),

        col("det.bronze_ts_ingestao")
            .alias("bronze_ts_ingestao_detalhes"),

        col("dep.bronze_id_batch")
            .alias("bronze_id_batch_deputados"),

        col("det.bronze_id_batch")
            .alias("bronze_id_batch_detalhes"),

        col("dep.bronze_tx_record_hash")
            .alias("bronze_tx_record_hash_deputados"),

        col("det.bronze_tx_record_hash")
            .alias("bronze_tx_record_hash_detalhes"),

        current_timestamp()
            .alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

duplicated_ids = (
    df_curated
    .groupBy("dept_id_deputado")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_ids > 0:
    raise Exception(
        f"Data quality error: {duplicated_ids} duplicated deputy IDs in curated layer."
    )

df_dedup = df_curated


# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("dept_id_deputado").isNotNull())
    .filter(col("dept_tx_nome_parlamentar").isNotNull())
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
    endpoint=f"{SOURCE_TABLE_DEPUTADOS}, {SOURCE_TABLE_DETALHES}",
    target_table=TARGET_TABLE,
    started_at=started_at,
    finished_at=datetime.now(),
)

# COMMAND ----------

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Layer: {LAYER}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")
print(f"Records discarded: {records_discarded}")