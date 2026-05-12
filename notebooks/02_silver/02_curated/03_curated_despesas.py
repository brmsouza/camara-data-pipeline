# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 03_curated_despesas
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates parliamentary expense data
# from Silver Base.
#
# Context:
# This notebook transforms silver_base.despesas into a curated and
# analytics-ready expense dataset. The resulting table centralizes CEAP expense
# records with financial values, supplier information, deputy references,
# document metadata and analytical flags required for Gold fact modeling.
#
# Responsibilities:
# - Consolidate standardized expense attributes from Silver Base
# - Preserve financial values and document references
# - Preserve supplier, deputy and legislature relationships
# - Preserve technical validation flags from Silver Base
# - Create analytical expense flags
# - Preserve lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.despesas
#
# Target:
# silver_curated.despesas
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by expense year
# - Ready for CEAP financial analytics and Gold modeling
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

SOURCE_TABLE = "silver_base.despesas"
TARGET_TABLE = "silver_curated.despesas"

PIPELINE_NAME = "silver_curated_despesas"
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
        # Deputy / legislature / party
        # ---------------------------------------------------

        col("dept_id_deputado")
            .alias("dept_id_deputado"),

        col("desp_tx_nome_parlamentar")
            .alias("dept_tx_nome_parlamentar"),

        col("dept_id_cadastro")
            .alias("dept_id_cadastro"),

        col("dept_nr_cpf")
            .alias("dept_nr_cpf"),

        col("dept_nr_carteira_parlamentar")
            .alias("dept_nr_carteira_parlamentar"),

        col("uf_sg_uf")
            .alias("uf_sg_uf"),

        col("part_sg_partido")
            .alias("part_sg_partido"),

        coalesce(col("leg_nr_legislatura"), col("leg_cd_legislatura"))
            .alias("leg_id_legislatura"),

        # ---------------------------------------------------
        # Expense classification
        # ---------------------------------------------------

        col("desp_cd_subcota")
            .alias("desp_cd_subcota"),

        col("desp_tx_descricao")
            .alias("desp_tx_tipo_despesa"),

        col("desp_cd_especificacao_subcota")
            .alias("desp_cd_especificacao_subcota"),

        col("desp_tx_descricao_especificacao")
            .alias("desp_tx_especificacao"),

        # ---------------------------------------------------
        # Supplier
        # ---------------------------------------------------

        col("forn_tx_nome")
            .alias("forn_tx_nome"),

        col("forn_nr_cnpj_cpf")
            .alias("forn_nr_cnpj_cpf"),

        when(col("forn_tx_tipo_documento") == "NA", lit("Não identificado"))
            .otherwise(col("forn_tx_tipo_documento"))
            .alias("forn_tx_tipo_documento"),

        col("forn_fl_documento_valido")
            .alias("forn_fl_documento_valido"),
            
        col("dept_fl_cpf_valido")
            .alias("dept_fl_cpf_valido"),

        col("desp_fl_data_emissao_valida")
            .alias("desp_fl_data_emissao_valida"),

        col("desp_fl_data_restituicao_valida")
            .alias("desp_fl_data_restituicao_valida"),            

        # ---------------------------------------------------
        # Document / fiscal metadata
        # ---------------------------------------------------

        col("desp_nr_documento")
            .alias("desp_nr_documento"),

        col("desp_cd_tipo_documento")
            .alias("desp_cd_tipo_documento"),

        col("desp_id_documento")
            .alias("desp_id_documento"),

        col("desp_tx_url_documento")
            .alias("desp_tx_url_documento"),

        when(col("desp_tx_url_documento").isNotNull(), lit(1))
            .otherwise(lit(0))
            .alias("desp_fl_possui_documento_url"),

        # ---------------------------------------------------
        # Dates / competence
        # ---------------------------------------------------

        col("desp_dt_emissao")
            .alias("desp_dt_emissao"),

        col("desp_nr_mes")
            .alias("desp_nr_mes"),

        col("desp_nr_ano")
            .alias("desp_nr_ano"),

        col("desp_nr_parcela")
            .alias("desp_nr_parcela"),

        # ---------------------------------------------------
        # Financial values
        # ---------------------------------------------------

        col("desp_vl_documento")
            .alias("desp_vl_documento"),

        col("desp_vl_glosa")
            .alias("desp_vl_glosa"),

        col("desp_vl_liquido")
            .alias("desp_vl_liquido"),

        col("desp_vl_restituicao")
            .alias("desp_vl_restituicao"),

        when(col("desp_vl_glosa") > 0, lit(1))
            .otherwise(lit(0))
            .alias("desp_fl_possui_glosa"),

        when(col("desp_vl_restituicao") > 0, lit(1))
            .otherwise(lit(0))
            .alias("desp_fl_possui_restituicao"),

        when(col("desp_vl_liquido") < 0, lit(1))
            .otherwise(lit(0))
            .alias("desp_fl_valor_negativo"),

        # ---------------------------------------------------
        # Travel / reimbursement
        # ---------------------------------------------------

        col("desp_tx_passageiro")
            .alias("desp_tx_passageiro"),

        col("desp_tx_trecho")
            .alias("desp_tx_trecho"),

        col("desp_nr_lote")
            .alias("desp_nr_lote"),

        col("desp_nr_ressarcimento")
            .alias("desp_nr_ressarcimento"),

        col("desp_dt_pagamento_restituicao")
            .alias("desp_dt_pagamento_restituicao"),

        # ---------------------------------------------------
        # Technical keys
        # ---------------------------------------------------

        col("desp_tx_dedup_key")
            .alias("desp_tx_dedup_key"),

        # ---------------------------------------------------
        # Lineage
        # ---------------------------------------------------

        col("bronze_ts_ingestao")
            .alias("bronze_ts_ingestao"),

        col("bronze_dt_ingestao")
            .alias("bronze_dt_ingestao"),

        col("bronze_tx_endpoint")
            .alias("bronze_tx_endpoint"),

        col("bronze_id_origem")
            .alias("bronze_id_origem"),

        col("bronze_tx_source_file")
            .alias("bronze_tx_source_file"),

        col("bronze_nr_ano_referencia")
            .alias("bronze_nr_ano_referencia"),

        col("bronze_id_batch")
            .alias("bronze_id_batch"),

        col("bronze_tx_record_hash")
            .alias("bronze_tx_record_hash"),

        col("silver_ts_processamento")
            .alias("silver_base_ts_processamento"),

        current_timestamp()
            .alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

duplicated_keys = (
    df_curated
    .groupBy("desp_tx_dedup_key")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_keys > 0:
    raise Exception(
        f"Data quality error: {duplicated_keys} duplicated expense keys in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("desp_tx_dedup_key").isNotNull())
    .filter(col("desp_nr_ano").isNotNull())
    .filter(col("desp_vl_liquido").isNotNull())
)

records_written = df_valid.count()

records_discarded = records_read - records_written

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("desp_nr_ano")
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
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")
print(f"Records discarded: {records_discarded}")