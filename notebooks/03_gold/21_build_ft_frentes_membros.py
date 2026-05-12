# Databricks notebook source
# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 21_build_ft_frentes_membros
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the parliamentary front membership fact table for the Gold Star Schema.
#
# Context:
# This notebook creates the analytical fact table that represents the relationship
# between parliamentary fronts and their deputy members. It integrates curated
# front membership records with Gold conformed dimensions such as front, deputy,
# party, UF and legislature.
#
# The resulting table supports the Atlas of Parliamentary Fronts use case from
# the final challenge, enabling analysis of:
# - active parliamentary fronts
# - front membership composition
# - party and UF diversity within fronts
# - deputies participating in multiple fronts
# - front evolution across legislatures
# - overlap of members between different fronts
#
# Grain:
# One row per deputy membership in a parliamentary front.
#
# Responsibilities:
# - Read curated parliamentary front membership records
# - Join Gold conformed dimensions
# - Resolve dimensional surrogate keys
# - Preserve front, deputy, party, UF and legislature relationships
# - Preserve membership dates, roles, status and analytical flags
# - Preserve lineage and audit metadata
# - Validate Gold fact consistency
# - Persist a partitioned Gold Delta fact table
# - Optimize the Delta table for analytical workloads
# - Register operational execution metrics
#
# Source:
# silver_curated.frentes_membros
#
# Dimensions:
# gold.dm_frente
# gold.dm_deputado
# gold.dm_partido
# gold.dm_uf
# gold.dm_legislatura
#
# Target:
# gold.ft_frentes_membros
# ------------------------------------------------------------------------------


# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

# MAGIC %run ../90_common/config

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_ft_frentes_membros"

SOURCE_TABLE = "silver_curated.frentes_membros"
TARGET_TABLE = "gold.ft_frentes_membros"

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

df_dm_frente = spark.table("gold.dm_frente")
df_dm_deputado = spark.table("gold.dm_deputado")
df_dm_partido = spark.table("gold.dm_partido")
df_dm_uf = spark.table("gold.dm_uf")
df_dm_legislatura = spark.table("gold.dm_legislatura")

records_read = df_source.count()

# COMMAND ----------

df_fact = (
    df_source.alias("memb")
    .join(
        df_dm_frente.alias("frente"),
        F.col("memb.frente_id_frente") == F.col("frente.frente_id_frente"),
        "left"
    )
    .join(
        df_dm_deputado.alias("dept"),
        F.col("memb.dept_id_deputado") == F.col("dept.id_deputado"),
        "left"
    )
    .join(
        df_dm_partido.alias("part"),
        F.col("memb.part_sg_partido") == F.col("part.part_sg_partido"),
        "left"
    )
    .join(
        df_dm_uf.alias("uf"),
        F.col("memb.uf_sg_uf") == F.col("uf.uf_sg_uf"),
        "left"
    )
    .join(
        df_dm_legislatura.alias("leg"),
        F.col("memb.leg_id_legislatura") == F.col("leg.leg_id_legislatura"),
        "left"
    )
    .select(
        F.col("frente.sk_frente").alias("sk_frente"),
        F.col("dept.sk_dept").alias("sk_dept"),
        F.col("part.sk_part").alias("sk_part"),
        F.col("uf.sk_uf").alias("sk_uf"),
        F.col("leg.sk_leg").alias("sk_leg"),

        F.col("memb.frente_id_frente").alias("frente_id_frente"),
        F.col("memb.dept_id_deputado").alias("dept_id_deputado"),
        F.col("memb.part_sg_partido").alias("part_sg_partido"),
        F.col("memb.uf_sg_uf").alias("uf_sg_uf"),
        F.col("memb.leg_id_legislatura").alias("leg_id_legislatura"),

        F.col("memb.memb_tx_dedup_key").alias("memb_tx_dedup_key"),

        F.col("frente.frente_tx_uri").alias("frente_tx_uri"),
        F.col("frente.frente_tx_titulo").alias("frente_tx_titulo"),
        F.col("frente.frente_fl_tema_saude").alias("frente_fl_tema_saude"),
        F.col("frente.frente_fl_tema_educacao").alias("frente_fl_tema_educacao"),
        F.col("frente.frente_fl_tema_seguranca").alias("frente_fl_tema_seguranca"),
        F.col("frente.frente_fl_tema_agro").alias("frente_fl_tema_agro"),
        F.col("frente.frente_fl_tema_mulher").alias("frente_fl_tema_mulher"),
        F.col("frente.frente_fl_tema_meio_ambiente").alias("frente_fl_tema_meio_ambiente"),

        F.col("memb.dept_tx_uri").alias("dept_tx_uri"),
        F.col("memb.dept_tx_nome_parlamentar").alias("dept_tx_nome_parlamentar"),
        F.col("memb.dept_tx_email").alias("dept_tx_email"),
        F.col("memb.dept_fl_email_valido").alias("dept_fl_email_valido"),
        F.col("memb.dept_tx_url_foto").alias("dept_tx_url_foto"),

        F.col("memb.memb_cd_titulo").alias("memb_cd_titulo"),
        F.col("memb.memb_tx_titulo").alias("memb_tx_titulo"),
        F.col("memb.memb_tx_status").alias("memb_tx_status"),
        F.col("memb.memb_fl_ativo").alias("memb_fl_ativo"),
        F.col("memb.memb_fl_coordenador").alias("memb_fl_coordenador"),
        F.col("memb.memb_fl_presidente").alias("memb_fl_presidente"),
        F.col("memb.memb_fl_vice").alias("memb_fl_vice"),
        F.col("memb.memb_fl_membro").alias("memb_fl_membro"),

        F.lit(1).cast("int").alias("qt_membro_frente"),

        F.when(F.col("memb.memb_fl_ativo") == 1, F.lit(1))
            .otherwise(F.lit(0))
            .cast("int")
            .alias("qt_membro_ativo"),

        F.when(F.col("memb.memb_fl_coordenador") == 1, F.lit(1))
            .otherwise(F.lit(0))
            .cast("int")
            .alias("qt_coordenador"),

        F.when(F.col("memb.memb_fl_presidente") == 1, F.lit(1))
            .otherwise(F.lit(0))
            .cast("int")
            .alias("qt_presidente"),

        F.col("memb.bronze_ts_ingestao").alias("bronze_ts_ingestao"),
        F.col("memb.bronze_dt_ingestao").alias("bronze_dt_ingestao"),
        F.col("memb.bronze_tx_endpoint").alias("bronze_tx_endpoint"),
        F.col("memb.bronze_id_origem").alias("bronze_id_origem"),
        F.col("memb.bronze_id_batch").alias("bronze_id_batch"),
        F.col("memb.bronze_tx_record_hash").alias("bronze_tx_record_hash"),

        F.col("memb.silver_base_ts_processamento").alias("silver_base_ts_processamento"),
        F.col("memb.silver_curated_ts_processamento").alias("silver_curated_ts_processamento"),

        F.current_timestamp().alias("gold_ts_processamento"),
        F.lit(batch_id).alias("gold_id_batch")
    )
)


# COMMAND ----------

df_fact = (
    df_fact
    .filter(
        F.col("leg_id_legislatura").isin(LEGISLATURAS_PADRAO)
    )
)


# COMMAND ----------

records_written = df_fact.count()
records_discarded = records_read - records_written

# COMMAND ----------

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.frentes_membros has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: ft_frentes_membros has no records."
    )

null_sk_frente = (
    df_fact
    .filter(F.col("sk_frente").isNull())
    .count()
)

if null_sk_frente > 0:
    raise Exception(
        f"Gold validation failed: null sk_frente found = {null_sk_frente}"
    )

null_sk_dept = (
    df_fact
    .filter(F.col("sk_dept").isNull())
    .count()
)

if null_sk_dept > 0:
    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer=LAYER,
        level="WARN",
        event_name="quality_validation",
        message=(
            "ft_frentes_membros contains records without resolved deputy dimension "
            f"| null_sk_dept={null_sk_dept}"
        ),
        endpoint=SOURCE_TABLE,
        target_table=TARGET_TABLE,
        started_at=started_at,
    )

null_sk_leg = (
    df_fact
    .filter(F.col("sk_leg").isNull())
    .count()
)

if null_sk_leg > 0:
    raise Exception(
        f"Gold validation failed: null sk_leg found = {null_sk_leg}"
    )

duplicated_memberships = (
    df_fact
    .groupBy("memb_tx_dedup_key")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_memberships > 0:
    raise Exception(
        f"Gold validation failed: duplicated front membership keys found = {duplicated_memberships}"
    )


# COMMAND ----------

(
    df_fact
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("leg_id_legislatura")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

spark.sql(f"OPTIMIZE {TARGET_TABLE}")

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