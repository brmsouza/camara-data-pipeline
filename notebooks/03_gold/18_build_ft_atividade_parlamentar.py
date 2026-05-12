# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 18_build_ft_atividade_parlamentar
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the parliamentary activity fact table for the Gold Star Schema.
#
# Context:
# This notebook consolidates deputy activity indicators from curated datasets,
# creating a wide analytical fact table for parliamentary engagement.
#
# Source:
# silver_curated.deputados
# silver_curated.despesas
# silver_curated.votacoes_votos
# silver_curated.frentes_membros
#
# Target:
# gold.ft_atividade_parlamentar
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_ft_atividade_parlamentar"

SOURCE_TABLE_DEPUTADOS = "silver_curated.deputados"
SOURCE_TABLE_DESPESAS = "silver_curated.despesas"
SOURCE_TABLE_VOTOS = "silver_curated.votacoes_votos"
SOURCE_TABLE_FRENTES = "silver_curated.frentes_membros"

TARGET_TABLE = "gold.ft_atividade_parlamentar"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

SOURCE_TABLE = (
    f"{SOURCE_TABLE_DEPUTADOS}, "
    f"{SOURCE_TABLE_DESPESAS}, "
    f"{SOURCE_TABLE_VOTOS}, "
    f"{SOURCE_TABLE_FRENTES}"
)

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

df_deputados = spark.table(SOURCE_TABLE_DEPUTADOS)
df_despesas = spark.table(SOURCE_TABLE_DESPESAS)
df_votos = spark.table(SOURCE_TABLE_VOTOS)
df_frentes = spark.table(SOURCE_TABLE_FRENTES)

df_dm_deputado = spark.table("gold.dm_deputado")
df_dm_partido = spark.table("gold.dm_partido")
df_dm_legislatura = spark.table("gold.dm_legislatura")
df_dm_uf = spark.table("gold.dm_uf")

records_read = df_deputados.count()

# COMMAND ----------

df_metricas_despesas = (
    df_despesas
    .groupBy("dept_id_deputado")
    .agg(
        F.count("*").alias("qt_despesas"),
        F.sum("desp_vl_documento").alias("vl_total_documento"),
        F.sum("desp_vl_glosa").alias("vl_total_glosa"),
        F.sum("desp_vl_liquido").alias("vl_total_liquido"),
        F.sum("desp_vl_restituicao").alias("vl_total_restituicao"),
        F.max("desp_fl_possui_glosa").alias("fl_possui_glosa"),
        F.max("desp_fl_possui_restituicao").alias("fl_possui_restituicao")
    )
)

df_metricas_votos = (
    df_votos
    .groupBy("dept_id_deputado")
    .agg(
        F.count("*").alias("qt_votos"),
        F.sum("vot_fl_sim").alias("qt_votos_sim"),
        F.sum("vot_fl_nao").alias("qt_votos_nao"),
        F.sum("vot_fl_abstencao").alias("qt_votos_abstencao"),
        F.sum("vot_fl_obstrucao").alias("qt_votos_obstrucao")
    )
)

df_metricas_frentes = (
    df_frentes
    .groupBy("dept_id_deputado")
    .agg(
        F.countDistinct("frente_id_frente").alias("qt_frentes"),
        F.max("memb_fl_coordenador").alias("fl_coordenador_frente"),
        F.max("memb_fl_presidente").alias("fl_presidente_frente"),
        F.max("memb_fl_vice").alias("fl_vice_frente")
    )
)

# COMMAND ----------

df_fact = (
    df_deputados.alias("dept_src")
    .join(df_metricas_despesas.alias("desp"), "dept_id_deputado", "left")
    .join(df_metricas_votos.alias("vot"), "dept_id_deputado", "left")
    .join(df_metricas_frentes.alias("frente"), "dept_id_deputado", "left")
    .join(
        df_dm_deputado.alias("dept"),
        F.col("dept_src.dept_id_deputado") == F.col("dept.id_deputado"),
        "left"
    )
    .join(
        df_dm_partido.alias("part"),
        F.col("dept_src.part_sg_partido") == F.col("part.part_sg_partido"),
        "left"
    )
    .join(
        df_dm_legislatura.alias("leg"),
        F.col("dept_src.leg_id_legislatura") == F.col("leg.leg_id_legislatura"),
        "left"
    )
    .join(
        df_dm_uf.alias("uf"),
        F.col("dept_src.uf_sg_uf") == F.col("uf.uf_sg_uf"),
        "left"
    )
    .select(
        F.col("dept.sk_dept"),
        F.col("part.sk_part"),
        F.col("leg.sk_leg"),
        F.col("uf.sk_uf"),

        F.col("dept_src.dept_id_deputado"),
        F.col("dept_src.part_sg_partido"),
        F.col("dept_src.uf_sg_uf"),
        F.col("dept_src.leg_id_legislatura"),

        F.coalesce(F.col("qt_despesas"), F.lit(0)).alias("qt_despesas"),
        F.coalesce(F.col("vl_total_documento"), F.lit(0)).alias("vl_total_documento"),
        F.coalesce(F.col("vl_total_glosa"), F.lit(0)).alias("vl_total_glosa"),
        F.coalesce(F.col("vl_total_liquido"), F.lit(0)).alias("vl_total_liquido"),
        F.coalesce(F.col("vl_total_restituicao"), F.lit(0)).alias("vl_total_restituicao"),
        F.coalesce(F.col("fl_possui_glosa"), F.lit(0)).alias("fl_possui_glosa"),
        F.coalesce(F.col("fl_possui_restituicao"), F.lit(0)).alias("fl_possui_restituicao"),

        F.coalesce(F.col("qt_votos"), F.lit(0)).alias("qt_votos"),
        F.coalesce(F.col("qt_votos_sim"), F.lit(0)).alias("qt_votos_sim"),
        F.coalesce(F.col("qt_votos_nao"), F.lit(0)).alias("qt_votos_nao"),
        F.coalesce(F.col("qt_votos_abstencao"), F.lit(0)).alias("qt_votos_abstencao"),
        F.coalesce(F.col("qt_votos_obstrucao"), F.lit(0)).alias("qt_votos_obstrucao"),

        F.coalesce(F.col("qt_frentes"), F.lit(0)).alias("qt_frentes"),
        F.coalesce(F.col("fl_coordenador_frente"), F.lit(0)).alias("fl_coordenador_frente"),
        F.coalesce(F.col("fl_presidente_frente"), F.lit(0)).alias("fl_presidente_frente"),
        F.coalesce(F.col("fl_vice_frente"), F.lit(0)).alias("fl_vice_frente"),

        F.current_timestamp().alias("gold_ts_processamento"),
        F.lit(batch_id).alias("gold_id_batch")
    )
)

# COMMAND ----------

records_written = df_fact.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.deputados has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: ft_atividade_parlamentar has no records."
    )

null_required_keys = (
    df_fact
    .filter(F.col("sk_dept").isNull())
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
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

spark.sql(f"""
OPTIMIZE {TARGET_TABLE}
ZORDER BY (sk_dept, sk_part, sk_leg)
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