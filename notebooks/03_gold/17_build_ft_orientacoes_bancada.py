# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 17_build_ft_orientacoes_bancada
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the voting orientation fact table for the Gold Star Schema.
#
# Context:
# This notebook creates the bancada voting orientation fact table, enabling
# analysis of party bloc recommendations across legislative voting events.
#
# Source:
# silver_curated.votacoes_orientacoes
#
# Target:
# gold.ft_orientacoes_bancada
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_ft_orientacoes_bancada"

SOURCE_TABLE = "silver_curated.votacoes_orientacoes"
TARGET_TABLE = "gold.ft_orientacoes_bancada"

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

df_dm_bancada = spark.table("gold.dm_bancada")
df_dm_orgao = spark.table("gold.dm_orgao")

records_read = df_source.count()

# COMMAND ----------

df_fact = (
    df_source.alias("ori")
    .join(
        df_dm_bancada.alias("banc"),
        F.col("ori.banc_tx_sigla_bancada") == F.col("banc.banc_tx_sigla_bancada"),
        "left"
    )
    .join(
        df_dm_orgao.alias("org"),
        F.col("ori.org_sg_orgao") == F.col("org.org_sg_orgao"),
        "left"
    )
    .select(
        F.col("banc.sk_banc"),
        F.col("org.sk_org"),

        F.col("ori.vot_id_votacao"),
        F.col("ori.vot_tx_uri"),

        F.col("ori.org_sg_orgao"),
        F.col("ori.banc_tx_sigla_bancada"),

        F.col("ori.vot_tx_orientacao"),
        F.col("ori.vot_tx_orientacao_curada"),
        F.col("ori.vot_tx_descricao_resultado"),

        F.col("ori.vot_fl_orientacao_sim"),
        F.col("ori.vot_fl_orientacao_nao"),
        F.col("ori.vot_fl_orientacao_liberado"),
        F.col("ori.vot_fl_orientacao_obstrucao"),
        F.col("ori.vot_fl_orientacao_abstencao"),

        F.col("ori.vot_tx_dedup_key"),
        F.col("ori.bronze_nr_ano_referencia"),

        F.col("ori.bronze_ts_ingestao"),
        F.col("ori.bronze_dt_ingestao"),
        F.col("ori.bronze_tx_endpoint"),
        F.col("ori.bronze_id_origem"),
        F.col("ori.bronze_tx_source_file"),
        F.col("ori.bronze_id_batch"),
        F.col("ori.bronze_tx_record_hash"),

        F.current_timestamp().alias("gold_ts_processamento"),
        F.lit(batch_id).alias("gold_id_batch")
    )
)

# COMMAND ----------

records_written = df_fact.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.votacoes_orientacoes has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: ft_orientacoes_bancada has no records."
    )

# COMMAND ----------

(
    df_fact
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("bronze_nr_ano_referencia")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

spark.sql(f"""
OPTIMIZE {TARGET_TABLE}
ZORDER BY (sk_banc, vot_id_votacao)
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