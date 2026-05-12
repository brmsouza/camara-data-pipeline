# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 10_build_dm_frente
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed parliamentary front dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the parliamentary front dimension used by activity and
# engagement analytical fact tables.
#
# Responsibilities:
# - Read curated parliamentary front records
# - Extract analytical front attributes
# - Create a surrogate key for dimensional modeling
# - Ensure one record per parliamentary front
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.frentes
#
# Target:
# gold.dm_frente
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
PIPELINE_NAME = "gold_build_dm_frente"
SOURCE_TABLE = "silver_curated.frentes"
TARGET_TABLE = "gold.dm_frente"

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

df_dm_frente = (
    df_source
    .select(
        F.col("frente_id_frente"),
        F.col("frente_tx_uri"),
        F.col("frente_tx_titulo"),
        F.col("leg_id_legislatura"),
        F.col("frente_fl_tema_saude"),
        F.col("frente_fl_tema_educacao"),
        F.col("frente_fl_tema_seguranca"),
        F.col("frente_fl_tema_agro"),
        F.col("frente_fl_tema_mulher"),
        F.col("frente_fl_tema_meio_ambiente"),
        F.col("bronze_ts_ingestao"),
        F.col("bronze_dt_ingestao"),
        F.col("bronze_tx_endpoint"),
        F.col("bronze_id_origem"),
        F.col("bronze_id_batch"),
        F.col("bronze_tx_record_hash")
    )
    .filter(F.col("frente_id_frente").isNotNull())
    .dropDuplicates(["frente_id_frente"])
)

# COMMAND ----------

window_frente = Window.orderBy("frente_id_frente")

df_dm_frente = (
    df_dm_frente
    .withColumn("sk_frente", F.row_number().over(window_frente))
    .select(
        "sk_frente",
        "frente_id_frente",
        "frente_tx_uri",
        "frente_tx_titulo",
        "leg_id_legislatura",
        "frente_fl_tema_saude",
        "frente_fl_tema_educacao",
        "frente_fl_tema_seguranca",
        "frente_fl_tema_agro",
        "frente_fl_tema_mulher",
        "frente_fl_tema_meio_ambiente",
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

records_written = df_dm_frente.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception("Gold validation failed: source silver_curated.frentes has no records.")

if records_written == 0:
    raise Exception("Gold validation failed: dm_frente has no records.")

duplicated_business_keys = (
    df_dm_frente
    .groupBy("frente_id_frente")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated frente_id_frente found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_frente
    .groupBy("sk_frente")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_frente found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_frente
    .filter(F.col("sk_frente").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_frente found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_frente
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.dm_frente