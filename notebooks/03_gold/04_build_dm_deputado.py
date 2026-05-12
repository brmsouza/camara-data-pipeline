# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 04_build_dm_deputado
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed deputy dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the deputy dimension used across analytical fact tables
# such as CEAP expenses, voting behavior, event attendance and parliamentary
# activity.
#
# Responsibilities:
# - Read curated deputy records
# - Extract analytical deputy attributes
# - Create a surrogate key for dimensional modeling
# - Ensure one record per deputy
# - Preserve lineage and Gold processing metadata
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.deputados
#
# Target:
# gold.dm_deputado
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_dm_deputado"
SOURCE_TABLE = "silver_curated.deputados"
TARGET_TABLE = "gold.dm_deputado"

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

records_read = df_source.count()

# COMMAND ----------

df_dm_deputado = (
    df_source
    .select(
        F.col("dept_id_deputado")
            .alias("id_deputado"),

        F.col("dept_tx_nome_parlamentar")
            .alias("dept_tx_nome_parlamentar"),

        F.col("dept_tx_nome_civil")
            .alias("dept_tx_nome_civil"),

        F.col("dept_tx_nome_eleitoral")
            .alias("dept_tx_nome_eleitoral"),

        F.col("uf_sg_uf")
            .alias("uf_sg_uf"),

        F.col("part_sg_partido")
            .alias("part_sg_partido"),

        F.col("leg_id_legislatura")
            .alias("leg_id_legislatura"),

        F.col("dept_tx_sexo")
            .alias("dept_tx_sexo"),

        F.col("dept_dt_nascimento")
            .alias("dept_dt_nascimento"),

        F.when(
            F.col("dept_dt_nascimento").isNotNull(),
            F.floor(
                F.months_between(
                    F.current_date(),
                    F.col("dept_dt_nascimento")
                ) / 12
            )
        ).otherwise(None).cast("int")
            .alias("dept_qt_idade"),

        F.col("dept_tx_escolaridade")
            .alias("dept_tx_escolaridade"),

        F.col("dept_tx_situacao_mandato")
            .alias("dept_tx_situacao_mandato"),

        F.col("dept_tx_condicao_eleitoral")
            .alias("dept_tx_condicao_eleitoral"),

        F.col("dept_tx_status_mandato_curado")
            .alias("dept_tx_status_mandato_curado"),

        F.col("dept_tx_email")
            .alias("dept_tx_email"),

        F.col("dept_fl_email_valido")
            .alias("dept_fl_email_valido"),

        F.col("dept_tx_url_foto")
            .alias("dept_tx_url_foto"),

        F.col("dept_tx_url_referencia")
            .alias("dept_tx_url_referencia"),

        F.col("gab_tx_nome")
            .alias("gab_tx_nome"),

        F.col("gab_tx_predio")
            .alias("gab_tx_predio"),

        F.col("gab_tx_sala")
            .alias("gab_tx_sala"),

        F.col("gab_tx_andar")
            .alias("gab_tx_andar"),

        F.col("gab_tx_telefone")
            .alias("gab_tx_telefone"),

        F.col("gab_fl_telefone_valido")
            .alias("gab_fl_telefone_valido"),

        F.col("bronze_ts_ingestao_deputados")
            .alias("bronze_ts_ingestao"),

        F.col("bronze_id_batch_deputados")
            .alias("bronze_id_batch"),

        F.col("bronze_tx_record_hash_deputados")
            .alias("bronze_tx_record_hash")
    )
    .filter(F.col("id_deputado").isNotNull())
    .dropDuplicates(["id_deputado"])
)

# COMMAND ----------

window_deputado = Window.orderBy("id_deputado")

df_dm_deputado = (
    df_dm_deputado
    .withColumn(
        "sk_dept",
        F.row_number().over(window_deputado)
    )
    .select(
        "sk_dept",
        "id_deputado",
        "dept_tx_nome_parlamentar",
        "dept_tx_nome_civil",
        "dept_tx_nome_eleitoral",
        "uf_sg_uf",
        "part_sg_partido",
        "leg_id_legislatura",
        "dept_tx_sexo",
        "dept_dt_nascimento",
        "dept_qt_idade",
        "dept_tx_escolaridade",
        "dept_tx_situacao_mandato",
        "dept_tx_condicao_eleitoral",
        "dept_tx_status_mandato_curado",
        "dept_tx_email",
        "dept_fl_email_valido",
        "dept_tx_url_foto",
        "dept_tx_url_referencia",
        "gab_tx_nome",
        "gab_tx_predio",
        "gab_tx_sala",
        "gab_tx_andar",
        "gab_tx_telefone",
        "gab_fl_telefone_valido",
        "bronze_ts_ingestao",
        "bronze_id_batch",
        "bronze_tx_record_hash"
    )
    .withColumn(
        "gold_ts_processamento",
        F.current_timestamp()
    )
    .withColumn(
        "gold_id_batch",
        F.lit(batch_id)
    )
)

# COMMAND ----------

records_written = df_dm_deputado.count()

records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.deputados has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: dm_deputado has no records."
    )

duplicated_business_keys = (
    df_dm_deputado
    .groupBy("id_deputado")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated id_deputado found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_deputado
    .groupBy("sk_dept")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_dept found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_deputado
    .filter(F.col("sk_dept").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_dept found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_deputado
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
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