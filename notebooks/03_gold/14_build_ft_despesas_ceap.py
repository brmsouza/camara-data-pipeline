# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 14_build_ft_despesas_ceap
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the CEAP expenses fact table for the Gold Star Schema.
#
# Context:
# This notebook creates the parliamentary expense fact table by joining curated
# expense records with Gold conformed dimensions such as deputy, party,
# legislature, supplier, expense type, UF and date.
#
# Responsibilities:
# - Read curated expense records
# - Join Gold dimensions
# - Create dimensional foreign keys
# - Preserve CEAP analytical measures and flags
# - Persist a partitioned Gold Delta fact table
# - Optimize the Delta table for analytical queries
# - Register operational execution metrics
#
# Source:
# silver_curated.despesas
#
# Target:
# gold.ft_despesas_ceap
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_ft_despesas_ceap"

SOURCE_TABLE = "silver_curated.despesas"
TARGET_TABLE = "gold.ft_despesas_ceap"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={SOURCE_TABLE} | started | target_table={TARGET_TABLE}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

df_source = spark.table(SOURCE_TABLE)

df_dm_deputado = spark.table("gold.dm_deputado")
df_dm_partido = spark.table("gold.dm_partido")
df_dm_legislatura = spark.table("gold.dm_legislatura")
df_dm_fornecedor = spark.table("gold.dm_fornecedor")
df_dm_tipo_despesa = spark.table("gold.dm_tipo_despesa")
df_dm_uf = spark.table("gold.dm_uf")

records_read = df_source.count()

# COMMAND ----------

df_fact = (
    df_source.alias("desp")
    .join(
        df_dm_deputado.alias("dept"),
        F.col("desp.dept_id_deputado") == F.col("dept.id_deputado"),
        "left"
    )
    .join(
        df_dm_partido.alias("part"),
        F.col("desp.part_sg_partido") == F.col("part.part_sg_partido"),
        "left"
    )
    .join(
        df_dm_legislatura.alias("leg"),
        F.col("desp.leg_id_legislatura") == F.col("leg.leg_id_legislatura"),
        "left"
    )
    .join(
        df_dm_fornecedor.alias("forn"),
        F.col("desp.forn_nr_cnpj_cpf") == F.col("forn.forn_nr_cnpj_cpf"),
        "left"
    )
    .join(
        df_dm_tipo_despesa.alias("tipo"),
        (
            (F.col("desp.desp_cd_subcota") == F.col("tipo.desp_cd_subcota")) &
            (F.col("desp.desp_cd_especificacao_subcota") == F.col("tipo.desp_cd_especificacao_subcota"))
        ),
        "left"
    )
    .join(
        df_dm_uf.alias("uf"),
        F.col("desp.uf_sg_uf") == F.col("uf.uf_sg_uf"),
        "left"
    )
    .select(
        F.col("dept.sk_dept"),
        F.col("part.sk_part"),
        F.col("leg.sk_leg"),
        F.col("forn.sk_forn"),
        F.col("tipo.sk_desp_tipo"),
        F.col("uf.sk_uf"),

        F.date_format(
            F.col("desp.desp_dt_emissao"),
            "yyyyMMdd"
        ).cast("int").alias("sk_data_emissao"),

        F.col("desp.dept_id_deputado"),
        F.col("desp.part_sg_partido"),
        F.col("desp.leg_id_legislatura"),
        F.col("desp.uf_sg_uf"),

        F.col("desp.desp_id_documento"),
        F.col("desp.desp_nr_documento"),
        F.col("desp.desp_cd_tipo_documento"),
        F.col("desp.desp_tx_url_documento"),
        F.col("desp.desp_fl_possui_documento_url"),

        F.col("desp.desp_cd_subcota"),
        F.col("desp.desp_cd_especificacao_subcota"),

        F.col("desp.desp_dt_emissao"),
        F.col("desp.desp_nr_ano"),
        F.col("desp.desp_nr_mes"),
        F.col("desp.desp_nr_parcela"),

        F.col("desp.desp_vl_documento"),
        F.col("desp.desp_vl_glosa"),
        F.col("desp.desp_vl_liquido"),
        F.col("desp.desp_vl_restituicao"),

        F.col("desp.desp_fl_possui_glosa"),
        F.col("desp.desp_fl_possui_restituicao"),
        F.col("desp.desp_fl_valor_negativo"),

        F.col("desp.desp_tx_passageiro"),
        F.col("desp.desp_tx_trecho"),
        F.col("desp.desp_nr_lote"),
        F.col("desp.desp_nr_ressarcimento"),
        F.col("desp.desp_dt_pagamento_restituicao"),

        F.col("desp.desp_tx_dedup_key"),

        F.col("desp.bronze_ts_ingestao"),
        F.col("desp.bronze_dt_ingestao"),
        F.col("desp.bronze_tx_endpoint"),
        F.col("desp.bronze_id_origem"),
        F.col("desp.bronze_tx_source_file"),
        F.col("desp.bronze_nr_ano_referencia"),
        F.col("desp.bronze_id_batch"),
        F.col("desp.bronze_tx_record_hash"),

        F.current_timestamp().alias("gold_ts_processamento"),
        F.lit(batch_id).alias("gold_id_batch")
    )
)

# COMMAND ----------

records_written = df_fact.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.despesas has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: ft_despesas_ceap has no records."
    )

null_required_keys = (
    df_fact
    .filter(
        F.col("sk_dept").isNull() |
        F.col("sk_data_emissao").isNull()
    )
    .count()
)

if null_required_keys > 0:
    raise Exception(
        f"Gold validation failed: required dimensional keys are null = {null_required_keys}"
    )

# COMMAND ----------

(
    df_fact
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("desp_nr_ano", "desp_nr_mes")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

spark.sql(f"""
OPTIMIZE {TARGET_TABLE}
ZORDER BY (sk_dept, sk_data_emissao)
""")

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=f"source={SOURCE_TABLE} | finished successfully | records_read={records_read} | records_written={records_written} | records_discarded={records_discarded}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
    finished_at=datetime.now(),
)