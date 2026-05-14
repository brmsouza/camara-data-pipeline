# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Proposition Dimension (dm_proposicao)
# MAGIC
# MAGIC **Notebook:** 05_build_dm_proposicao
# MAGIC
# MAGIC Builds the conformed proposition dimension for the Gold Star Schema.
# MAGIC
# MAGIC This notebook creates the proposition dimension used across analytical fact
# MAGIC tables and data marts related to voting behavior, proposition lifecycle,
# MAGIC legislative activity and parliamentary productivity.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read curated proposition records
# MAGIC - Extract analytical proposition attributes
# MAGIC - Create a surrogate key for dimensional modeling
# MAGIC - Ensure one record per proposition
# MAGIC - Preserve lineage and Gold processing metadata
# MAGIC - Validate dimension consistency
# MAGIC - Persist the Gold Delta dimension table
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC **Source of truth:** `silver_curated.proposicoes`  
# MAGIC
# MAGIC **Target:** `gold.dm_proposicao`

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_dm_proposicao"
SOURCE_TABLE = "silver_curated.proposicoes"
TARGET_TABLE = "gold.dm_proposicao"

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

df_dm_proposicao = (
    df_source
    .select(
        F.col("prop_id_proposicao"),

        F.col("prop_tx_uri"),

        F.col("prop_sg_tipo"),

        F.col("prop_tx_descricao_tipo"),

        F.col("prop_nr_numero"),

        F.col("prop_nr_ano"),

        F.col("prop_cd_tipo"),

        F.col("prop_tx_ementa"),

        F.col("prop_tx_keywords"),

        F.col("prop_ts_apresentacao"),

        F.date_format(
            F.to_date(F.col("prop_ts_apresentacao")),
            "yyyyMMdd"
        ).cast("int").alias("sk_data_apresentacao"),

        F.col("prop_fl_data_apresentacao_valida"),

        F.col("prop_nr_ano_apresentacao"),

        F.col("prop_nr_mes_apresentacao"),

        F.col("prop_tx_url_inteiro_teor"),

        F.col("prop_tx_urn_final"),

        F.col("prop_ts_status_data_hora"),

        F.date_format(
            F.to_date(F.col("prop_ts_status_data_hora")),
            "yyyyMMdd"
        ).cast("int").alias("sk_data_status"),

        F.col("prop_sg_status_orgao"),

        F.col("prop_tx_status_regime"),

        F.col("prop_tx_status_descricao_tramitacao"),

        F.col("prop_tx_status_descricao_situacao"),

        F.col("prop_tx_status_apreciacao"),

        F.col("prop_tx_status_curado"),

        F.col("prop_fl_tramitando"),

        F.col("prop_fl_aprovada"),

        F.col("prop_fl_rejeitada"),

        F.col("prop_tx_tipo_curado"),

        F.col("bronze_ts_ingestao"),

        F.col("bronze_dt_ingestao"),

        F.col("bronze_tx_endpoint"),

        F.col("bronze_id_origem"),

        F.col("bronze_tx_source_file"),

        F.col("bronze_id_batch"),

        F.col("bronze_tx_record_hash")
    )
    .filter(F.col("prop_id_proposicao").isNotNull())
    .dropDuplicates(["prop_id_proposicao"])
)

# COMMAND ----------

window_proposicao = Window.orderBy("prop_id_proposicao")

df_dm_proposicao = (
    df_dm_proposicao
    .withColumn(
        "sk_prop",
        F.row_number().over(window_proposicao)
    )
    .select(
        "sk_prop",
        "prop_id_proposicao",
        "prop_tx_uri",
        "prop_sg_tipo",
        "prop_tx_descricao_tipo",
        "prop_nr_numero",
        "prop_nr_ano",
        "prop_cd_tipo",
        "prop_tx_ementa",
        "prop_tx_keywords",
        "prop_ts_apresentacao",
        "sk_data_apresentacao",
        "prop_fl_data_apresentacao_valida",
        "prop_nr_ano_apresentacao",
        "prop_nr_mes_apresentacao",
        "prop_tx_url_inteiro_teor",
        "prop_tx_urn_final",
        "prop_ts_status_data_hora",
        "sk_data_status",
        "prop_sg_status_orgao",
        "prop_tx_status_regime",
        "prop_tx_status_descricao_tramitacao",
        "prop_tx_status_descricao_situacao",
        "prop_tx_status_apreciacao",
        "prop_tx_status_curado",
        "prop_fl_tramitando",
        "prop_fl_aprovada",
        "prop_fl_rejeitada",
        "prop_tx_tipo_curado",
        "bronze_ts_ingestao",
        "bronze_dt_ingestao",
        "bronze_tx_endpoint",
        "bronze_id_origem",
        "bronze_tx_source_file",
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

records_written = df_dm_proposicao.count()

records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.proposicoes has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: dm_proposicao has no records."
    )

duplicated_business_keys = (
    df_dm_proposicao
    .groupBy("prop_id_proposicao")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated prop_id_proposicao found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_proposicao
    .groupBy("sk_prop")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_prop found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_proposicao
    .filter(F.col("sk_prop").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_prop found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_proposicao
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