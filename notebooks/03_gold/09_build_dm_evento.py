# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Legislative Event Dimension (dm_evento)
# MAGIC
# MAGIC **Notebook:** 09_build_dm_evento
# MAGIC
# MAGIC Builds the conformed event dimension for the Gold Star Schema.
# MAGIC
# MAGIC This notebook creates the legislative event dimension used by attendance,
# MAGIC activity and event-based analytical fact tables.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read curated event records
# MAGIC - Extract analytical event attributes
# MAGIC - Create a surrogate key for dimensional modeling
# MAGIC - Ensure one record per event
# MAGIC - Validate dimension consistency
# MAGIC - Persist the Gold Delta dimension table
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC **Source of truth:** `silver_curated.eventos`  
# MAGIC
# MAGIC **Target:** `gold.dm_evento`

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_dm_evento"
SOURCE_TABLE = "silver_curated.eventos"
TARGET_TABLE = "gold.dm_evento"

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

df_dm_evento = (
    df_source
    .select(
        F.col("evt_id_evento"),
        F.col("evt_tx_uri"),
        F.col("evt_nr_ano_referencia"),
        F.col("evt_ts_inicio"),
        F.col("evt_ts_fim"),
        F.col("evt_dt_inicio"),
        F.col("evt_dt_fim"),
        F.date_format(F.col("evt_dt_inicio"), "yyyyMMdd").cast("int").alias("sk_data_inicio"),
        F.date_format(F.col("evt_dt_fim"), "yyyyMMdd").cast("int").alias("sk_data_fim"),
        F.col("evt_nr_ano_inicio"),
        F.col("evt_nr_mes_inicio"),
        F.col("evt_fl_inicio_valido"),
        F.col("evt_fl_fim_valido"),
        F.col("evt_fl_periodo_valido"),
        F.col("evt_tx_descricao"),
        F.col("evt_tx_tipo"),
        F.col("evt_tx_situacao"),
        F.col("evt_tx_tipo_curado"),
        F.col("evt_tx_situacao_curada"),
        F.col("evt_fl_sessao"),
        F.col("evt_fl_audiencia_publica"),
        F.col("evt_fl_reuniao"),
        F.col("evt_fl_encerrado"),
        F.col("evt_fl_cancelado"),
        F.col("evt_fl_possui_registro"),
        F.col("evt_tx_local_interno"),
        F.col("evt_tx_predio"),
        F.col("evt_tx_sala"),
        F.col("evt_tx_andar"),
        F.col("evt_tx_local_externo"),
        F.col("evt_tx_tipo_local"),
        F.col("evt_tx_url_registro"),
        F.col("evt_qt_orgaos"),
        F.col("org_id_orgao_principal"),
        F.col("org_sg_orgao_principal"),
        F.col("org_tx_nome_principal"),
        F.col("org_tx_tipo_principal"),
        F.col("org_tx_siglas_relacionadas"),
        F.col("bronze_ts_ingestao"),
        F.col("bronze_dt_ingestao"),
        F.col("bronze_tx_endpoint"),
        F.col("bronze_id_origem"),
        F.col("bronze_id_batch"),
        F.col("bronze_tx_record_hash")
    )
    .filter(F.col("evt_id_evento").isNotNull())
    .dropDuplicates(["evt_id_evento"])
)

# COMMAND ----------

window_evento = Window.orderBy("evt_id_evento")

df_dm_evento = (
    df_dm_evento
    .withColumn("sk_evt", F.row_number().over(window_evento))
    .select(
        "sk_evt",
        "evt_id_evento",
        "evt_tx_uri",
        "evt_nr_ano_referencia",
        "evt_ts_inicio",
        "evt_ts_fim",
        "evt_dt_inicio",
        "evt_dt_fim",
        "sk_data_inicio",
        "sk_data_fim",
        "evt_nr_ano_inicio",
        "evt_nr_mes_inicio",
        "evt_fl_inicio_valido",
        "evt_fl_fim_valido",
        "evt_fl_periodo_valido",
        "evt_tx_descricao",
        "evt_tx_tipo",
        "evt_tx_situacao",
        "evt_tx_tipo_curado",
        "evt_tx_situacao_curada",
        "evt_fl_sessao",
        "evt_fl_audiencia_publica",
        "evt_fl_reuniao",
        "evt_fl_encerrado",
        "evt_fl_cancelado",
        "evt_fl_possui_registro",
        "evt_tx_local_interno",
        "evt_tx_predio",
        "evt_tx_sala",
        "evt_tx_andar",
        "evt_tx_local_externo",
        "evt_tx_tipo_local",
        "evt_tx_url_registro",
        "evt_qt_orgaos",
        "org_id_orgao_principal",
        "org_sg_orgao_principal",
        "org_tx_nome_principal",
        "org_tx_tipo_principal",
        "org_tx_siglas_relacionadas",
        "bronze_ts_ingestao",
        "bronze_dt_ingestao",
        "bronze_tx_endpoint",
        "bronze_id_origem",
        "bronze_id_batch",
        "bronze_tx_record_hash"
    )
    .withColumn("gold_ts_processamento", F.current_timestamp())
    .withColumn("gold_id_batch", F.lit(batch_id))
)

# COMMAND ----------

records_written = df_dm_evento.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception("Gold validation failed: source silver_curated.eventos has no records.")

if records_written == 0:
    raise Exception("Gold validation failed: dm_evento has no records.")

duplicated_business_keys = (
    df_dm_evento
    .groupBy("evt_id_evento")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated evt_id_evento found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_evento
    .groupBy("sk_evt")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_evt found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_evento
    .filter(F.col("sk_evt").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_evt found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_evento
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