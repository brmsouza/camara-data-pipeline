# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 13_build_dm_bancada
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed bench/party bloc dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the bancada dimension used by voting orientation fact
# tables and political alignment analytics.
#
# Responsibilities:
# - Read curated voting orientation records
# - Extract bancada attributes
# - Create a surrogate key for dimensional modeling
# - Ensure one record per bancada
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.votacoes_orientacoes
#
# Target:
# gold.dm_bancada
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
PIPELINE_NAME = "gold_build_dm_bancada"
SOURCE_TABLE = "silver_curated.votacoes_orientacoes"
TARGET_TABLE = "gold.dm_bancada"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

df_source = spark.table(SOURCE_TABLE)

records_read = df_source.count()

# COMMAND ----------

df_dm_bancada = (
    df_source
    .select(
        F.col("banc_tx_sigla_bancada"),
        F.col("banc_tx_uri")
    )
    .filter(F.col("banc_tx_sigla_bancada").isNotNull())
    .dropDuplicates(["banc_tx_sigla_bancada"])
)

# COMMAND ----------

window_bancada = Window.orderBy("banc_tx_sigla_bancada")

df_dm_bancada = (
    df_dm_bancada
    .withColumn("sk_banc", F.row_number().over(window_bancada))
    .select(
        "sk_banc",
        "banc_tx_sigla_bancada",
        "banc_tx_uri"
    )
    .withColumn("gold_ts_processamento", F.current_timestamp())
    .withColumn("gold_id_batch", F.lit(batch_id))
)

# COMMAND ----------

records_written = df_dm_bancada.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception("Gold validation failed: source silver_curated.votacoes_orientacoes has no records.")

if records_written == 0:
    raise Exception("Gold validation failed: dm_bancada has no records.")

duplicated_business_keys = (
    df_dm_bancada
    .groupBy("banc_tx_sigla_bancada")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated banc_tx_sigla_bancada found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_bancada
    .groupBy("sk_banc")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_banc found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_bancada
    .filter(F.col("sk_banc").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_banc found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_bancada
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