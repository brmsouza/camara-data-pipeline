# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 06_build_dm_orgao
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed legislative body dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the legislative body dimension used across analytical
# fact tables related to events, attendance, voting, committees and
# parliamentary activity.
#
# Responsibilities:
# - Read curated legislative body records
# - Extract analytical organization attributes
# - Create a surrogate key for dimensional modeling
# - Ensure one record per legislative body
# - Preserve lineage and Gold processing metadata
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.orgaos
#
# Target:
# gold.dm_orgao
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
PIPELINE_NAME = "gold_build_dm_orgao"
SOURCE_TABLE = "silver_curated.orgaos"
TARGET_TABLE = "gold.dm_orgao"

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

df_dm_orgao = (
    df_source
    .select(
        F.col("org_id_orgao"),
        F.col("org_tx_uri"),
        F.col("org_sg_orgao"),
        F.col("org_tx_nome"),
        F.col("org_tx_apelido"),
        F.col("org_tx_nome_publicacao"),
        F.col("org_tx_nome_resumido"),
        F.col("org_cd_tipo_orgao"),
        F.col("org_tx_tipo_orgao"),
        F.col("org_tx_tipo_curado"),
        F.col("org_fl_plenario"),
        F.col("org_fl_comissao"),
        F.col("org_fl_mesa"),
        F.col("bronze_ts_ingestao"),
        F.col("bronze_dt_ingestao"),
        F.col("bronze_tx_endpoint"),
        F.col("bronze_id_origem"),
        F.col("bronze_id_batch"),
        F.col("bronze_tx_record_hash")
    )
    .filter(F.col("org_id_orgao").isNotNull())
    .dropDuplicates(["org_id_orgao"])
)

# COMMAND ----------

window_orgao = Window.orderBy("org_id_orgao")

df_dm_orgao = (
    df_dm_orgao
    .withColumn(
        "sk_org",
        F.row_number().over(window_orgao)
    )
    .select(
        "sk_org",
        "org_id_orgao",
        "org_tx_uri",
        "org_sg_orgao",
        "org_tx_nome",
        "org_tx_apelido",
        "org_tx_nome_publicacao",
        "org_tx_nome_resumido",
        "org_cd_tipo_orgao",
        "org_tx_tipo_orgao",
        "org_tx_tipo_curado",
        "org_fl_plenario",
        "org_fl_comissao",
        "org_fl_mesa",
        "bronze_ts_ingestao",
        "bronze_dt_ingestao",
        "bronze_tx_endpoint",
        "bronze_id_origem",
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

records_written = df_dm_orgao.count()

records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.orgaos has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: dm_orgao has no records."
    )

duplicated_business_keys = (
    df_dm_orgao
    .groupBy("org_id_orgao")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated org_id_orgao found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_orgao
    .groupBy("sk_org")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_org found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_orgao
    .filter(F.col("sk_org").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_org found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_orgao
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