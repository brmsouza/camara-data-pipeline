# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 16_build_ft_votacoes
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the voting summary fact table for the Gold Star Schema.
#
# Context:
# This notebook creates an analytical fact table with voting-level measures,
# voting result indicators and dimensional foreign keys.
#
# Source:
# silver_curated.votacoes
#
# Target:
# gold.ft_votacoes
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_ft_votacoes"

SOURCE_TABLE = "silver_curated.votacoes"
TARGET_TABLE = "gold.ft_votacoes"

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

df_dm_evento = spark.table("gold.dm_evento")
df_dm_orgao = spark.table("gold.dm_orgao")
df_dm_proposicao = spark.table("gold.dm_proposicao")

records_read = df_source.count()

# COMMAND ----------

df_fact = (
    df_source.alias("vot")
    .join(
        df_dm_evento.alias("evt"),
        F.col("vot.evt_id_evento") == F.col("evt.evt_id_evento"),
        "left"
    )
    .join(
        df_dm_orgao.alias("org"),
        F.col("vot.org_id_orgao") == F.col("org.org_id_orgao"),
        "left"
    )
    .join(
        df_dm_proposicao.alias("prop"),
        F.col("vot.prop_id_proposicao") == F.col("prop.prop_id_proposicao"),
        "left"
    )
    .select(
        F.date_format(
            F.col("vot.vot_dt_votacao"),
            "yyyyMMdd"
        ).cast("int").alias("sk_data_votacao"),

        F.col("evt.sk_evt"),
        F.col("org.sk_org"),
        F.col("prop.sk_prop"),

        F.col("vot.vot_id_votacao"),
        F.col("vot.vot_tx_uri"),

        F.col("vot.vot_dt_votacao"),
        F.col("vot.vot_ts_registro"),
        F.col("vot.vot_nr_ano_referencia"),

        F.col("vot.vot_tx_descricao"),
        F.col("vot.vot_tx_status_aprovacao"),
        F.col("vot.vot_tx_resultado_curado"),

        F.col("vot.vot_fl_data_valida"),
        F.col("vot.vot_fl_timestamp_registro_valido"),
        F.col("vot.vot_fl_periodo_valido"),

        F.col("vot.vot_fl_aprovacao").alias("vot_fl_aprovada"),

        F.col("vot.vot_fl_rejeitada"),

        F.col("vot.vot_qt_sim"),
        F.col("vot.vot_qt_nao"),
        F.col("vot.vot_qt_outros"),
        F.col("vot.vot_qt_total"),

        F.col("vot.evt_id_evento"),
        F.col("vot.org_id_orgao"),
        F.col("vot.prop_id_proposicao"),

        F.col("vot.vot_fl_possui_proposicao"),
        F.col("vot.vot_fl_possui_evento"),
        F.col("vot.vot_fl_possui_orgao"),
        F.col("vot.vot_fl_possui_votos_contabilizados"),

        # ---------------------------------------------------
        # Lineage / traceability
        # ---------------------------------------------------

        F.col("vot.bronze_tx_endpoint"),
        F.col("vot.bronze_id_origem"),
        F.col("vot.bronze_id_batch"),
        F.col("vot.bronze_tx_record_hash"),
        F.col("vot.bronze_ts_ingestao"),
        F.col("vot.bronze_dt_ingestao"),

        F.current_timestamp().alias("gold_ts_processamento"),
        F.lit(batch_id).alias("gold_id_batch")
    )
)

# COMMAND ----------

records_written = df_fact.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.votacoes has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: ft_votacoes has no records."
    )

null_sk_data_votacao = (
    df_fact
    .filter(F.col("sk_data_votacao").isNull())
    .count()
)

print(f"Null sk_data_votacao: {null_sk_data_votacao}")

duplicated_business_keys = (
    df_fact
    .groupBy("vot_id_votacao")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated vot_id_votacao found = {duplicated_business_keys}"
    )

# COMMAND ----------

(
    df_fact
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("vot_nr_ano_referencia")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

spark.sql(f"""
OPTIMIZE {TARGET_TABLE}
ZORDER BY (sk_data_votacao, sk_prop, sk_org)
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