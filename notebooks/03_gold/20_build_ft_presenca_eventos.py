# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Parliamentary Event Attendance Fact Table (ft_presenca_eventos)
# MAGIC
# MAGIC **Notebook:** 20_ft_build_ft_presenca_eventos
# MAGIC
# MAGIC Builds the parliamentary event attendance fact table for the Gold Star Schema.
# MAGIC
# MAGIC This notebook creates the deputy attendance fact table by combining event,
# MAGIC deputy and organizational participation records for analytical reporting.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read curated event participation records
# MAGIC - Join conformed Gold dimensions
# MAGIC - Create dimensional foreign keys
# MAGIC - Preserve attendance analytical attributes
# MAGIC - Support parliamentary participation and engagement analytics
# MAGIC - Persist the Gold Delta fact table
# MAGIC - Optimize analytical query performance
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC **Source of truth:** `silver_curated.eventos`  
# MAGIC
# MAGIC **Target:** `gold.ft_presenca_eventos`

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_ft_presenca_eventos"

SOURCE_TABLE = "silver_curated.eventos"
TARGET_TABLE = "gold.ft_presenca_eventos"

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

records_read = df_source.count()

# COMMAND ----------

df_fact = (
    df_source.alias("evt")
    .join(
        df_dm_evento.alias("dm_evt"),
        F.col("evt.evt_id_evento") == F.col("dm_evt.evt_id_evento"),
        "left"
    )
    .join(
        df_dm_orgao.alias("org"),
        F.col("evt.org_id_orgao_principal") == F.col("org.org_id_orgao"),
        "left"
    )
    .select(
        F.col("dm_evt.sk_evt"),
        F.col("org.sk_org"),

        F.date_format(
            F.col("evt.evt_dt_inicio"),
            "yyyyMMdd"
        ).cast("int").alias("sk_data_inicio"),

        F.date_format(
            F.col("evt.evt_dt_fim"),
            "yyyyMMdd"
        ).cast("int").alias("sk_data_fim"),

        F.col("evt.evt_id_evento"),
        F.col("evt.org_id_orgao_principal"),

        F.col("evt.evt_tx_uri"),
        F.col("evt.evt_ts_inicio"),
        F.col("evt.evt_ts_fim"),
        F.col("evt.evt_dt_inicio"),
        F.col("evt.evt_dt_fim"),

        F.col("evt.evt_nr_ano_referencia"),
        F.col("evt.evt_nr_ano_inicio"),
        F.col("evt.evt_nr_mes_inicio"),

        F.col("evt.evt_tx_descricao"),
        F.col("evt.evt_tx_tipo"),
        F.col("evt.evt_tx_situacao"),
        F.col("evt.evt_tx_tipo_curado"),
        F.col("evt.evt_tx_situacao_curada"),

        F.col("evt.evt_fl_inicio_valido"),
        F.col("evt.evt_fl_fim_valido"),
        F.col("evt.evt_fl_periodo_valido"),

        F.col("evt.evt_fl_sessao"),
        F.col("evt.evt_fl_audiencia_publica"),
        F.col("evt.evt_fl_reuniao"),
        F.col("evt.evt_fl_encerrado"),
        F.col("evt.evt_fl_cancelado"),

        F.col("evt.evt_fl_possui_registro"),
        F.col("evt.evt_tx_url_registro"),

        F.col("evt.evt_tx_local_interno"),
        F.col("evt.evt_tx_predio"),
        F.col("evt.evt_tx_sala"),
        F.col("evt.evt_tx_andar"),
        F.col("evt.evt_tx_local_externo"),
        F.col("evt.evt_tx_tipo_local"),

        F.col("evt.evt_qt_orgaos"),

        F.col("evt.org_sg_orgao_principal"),
        F.col("evt.org_tx_nome_principal"),
        F.col("evt.org_tx_tipo_principal"),
        F.col("evt.org_tx_siglas_relacionadas"),

        F.col("evt.bronze_ts_ingestao"),
        F.col("evt.bronze_dt_ingestao"),
        F.col("evt.bronze_tx_endpoint"),
        F.col("evt.bronze_id_origem"),
        F.col("evt.bronze_id_batch"),
        F.col("evt.bronze_tx_record_hash"),

        F.current_timestamp().alias("gold_ts_processamento"),
        F.lit(batch_id).alias("gold_id_batch")
    )
)

# COMMAND ----------

records_written = df_fact.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.eventos has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: ft_presenca_eventos has no records."
    )

null_sk_evt = (
    df_fact
    .filter(F.col("sk_evt").isNull())
    .count()
)

if null_sk_evt > 0:
    raise Exception(
        f"Gold validation failed: null sk_evt found = {null_sk_evt}"
    )

null_sk_data_inicio = (
    df_fact
    .filter(F.col("sk_data_inicio").isNull())
    .count()
)

print(f"Null sk_data_inicio: {null_sk_data_inicio}")

duplicated_business_keys = (
    df_fact
    .groupBy("evt_id_evento")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated evt_id_evento found = {duplicated_business_keys}"
    )

# COMMAND ----------

(
    df_fact
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("evt_nr_ano_referencia")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

spark.sql(f"""
OPTIMIZE {TARGET_TABLE}
ZORDER BY (sk_evt, sk_org, sk_data_inicio)
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