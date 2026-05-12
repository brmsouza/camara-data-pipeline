# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 17_build_ft_votos
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the individual deputy votes fact table for the Gold Star Schema.
#
# Context:
# This notebook creates the deputy voting fact table by joining curated vote
# records with deputy, party, legislature, UF and voting dimensions.
#
# Source:
# silver_curated.votacoes_votos
#
# Target:
# gold.ft_votos
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_ft_votos"

SOURCE_TABLE = "silver_curated.votacoes_votos"
TARGET_TABLE = "gold.ft_votos"

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
df_dm_uf = spark.table("gold.dm_uf")

records_read = df_source.count()

# COMMAND ----------

df_fact = (
    df_source.alias("voto")
    .join(
        df_dm_deputado.alias("dept"),
        F.col("voto.dept_id_deputado") == F.col("dept.id_deputado"),
        "left"
    )
    .join(
        df_dm_partido.alias("part"),
        F.col("voto.part_sg_partido") == F.col("part.part_sg_partido"),
        "left"
    )
    .join(
        df_dm_legislatura.alias("leg"),
        F.col("voto.leg_id_legislatura") == F.col("leg.leg_id_legislatura"),
        "left"
    )
    .join(
        df_dm_uf.alias("uf"),
        F.col("voto.uf_sg_uf") == F.col("uf.uf_sg_uf"),
        "left"
    )
    .select(
        F.col("dept.sk_dept"),
        F.col("part.sk_part"),
        F.col("leg.sk_leg"),
        F.col("uf.sk_uf"),

        F.date_format(
            F.to_date(F.col("voto.vot_ts_voto")),
            "yyyyMMdd"
        ).cast("int").alias("sk_data_voto"),

        F.col("voto.vot_id_votacao"),
        F.col("voto.vot_tx_uri"),
        F.col("voto.vot_ts_voto"),

        F.col("voto.dept_id_deputado"),
        F.col("voto.part_sg_partido"),
        F.col("voto.uf_sg_uf"),
        F.col("voto.leg_id_legislatura"),

        F.col("voto.vot_tx_voto"),
        F.col("voto.vot_tx_voto_curado"),
        F.col("voto.vot_fl_sim"),
        F.col("voto.vot_fl_nao"),
        F.col("voto.vot_fl_abstencao"),
        F.col("voto.vot_fl_obstrucao"),

        F.col("voto.vot_tx_dedup_key"),
        F.col("voto.bronze_nr_ano_referencia"),

        F.col("voto.bronze_ts_ingestao"),
        F.col("voto.bronze_dt_ingestao"),
        F.col("voto.bronze_tx_endpoint"),
        F.col("voto.bronze_id_origem"),
        F.col("voto.bronze_id_batch"),
        F.col("voto.bronze_tx_record_hash"),

        F.current_timestamp().alias("gold_ts_processamento"),
        F.lit(batch_id).alias("gold_id_batch")
    )
)

# COMMAND ----------

records_written = df_fact.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.votacoes_votos has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: ft_votos has no records."
    )

null_sk_dept = (
    df_fact
    .filter(F.col("sk_dept").isNull())
    .count()
)

if null_sk_dept > 0:
    raise Exception(
        f"Gold validation failed: null sk_dept found = {null_sk_dept}"
    )

null_sk_data_voto = (
    df_fact
    .filter(F.col("sk_data_voto").isNull())
    .count()
)

print(f"Null sk_data_voto: {null_sk_data_voto}")

duplicated_business_keys = (
    df_fact
    .groupBy(
        "vot_id_votacao",
        "dept_id_deputado"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated vote grain found = {duplicated_business_keys}"
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
ZORDER BY (vot_id_votacao, sk_dept, sk_part)
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