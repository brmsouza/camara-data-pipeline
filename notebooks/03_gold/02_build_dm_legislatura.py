# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 02_build_dm_legislatura
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed legislature dimension for the Gold Star Schema.
#
# Context:
# This notebook creates a reusable legislature dimension from curated deputy
# data. It supports analytical joins across CEAP expenses, voting behavior,
# event attendance and parliamentary activity by legislature.
#
# Responsibilities:
# - Read curated legislature records
# - Extract valid legislature identifiers
# - Ensure one record per legislature
# - Preserve Gold processing metadata
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.curated_deputados
#
# Target:
# gold.dm_legislatura
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
PIPELINE_NAME = "gold_build_dm_legislatura"
SOURCE_TABLE = "silver_curated.legislaturas"
TARGET_TABLE = "gold.dm_legislatura"

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

df_dm_legislatura = (
    df_source
    .select(
        F.col("leg_id_legislatura"),

        F.col("leg_nr_ano_eleicao"),

        F.col("leg_nr_ano_inicio"),

        F.col("leg_nr_ano_fim"),

        F.col("leg_dt_inicio"),

        F.col("leg_dt_fim"),

        F.col("leg_qt_meses_duracao"),

        F.col("leg_fl_legislatura_atual"),

        F.col("leg_tx_descricao")
    )
    .filter(F.col("leg_id_legislatura").isNotNull())
    .dropDuplicates(["leg_id_legislatura"])
)

# COMMAND ----------

window_legislatura = Window.orderBy("leg_id_legislatura")

df_dm_legislatura = (
    df_dm_legislatura
    .withColumn(
        "sk_leg",
        F.row_number().over(window_legislatura)
    )
    .select(
        "sk_leg",
        "leg_id_legislatura",
        "leg_nr_ano_eleicao",
        "leg_nr_ano_inicio",
        "leg_nr_ano_fim",
        "leg_dt_inicio",
        "leg_dt_fim",
        "leg_qt_meses_duracao",
        "leg_fl_legislatura_atual",
        "leg_tx_descricao"
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

duplicated_surrogate_keys = (
    df_dm_legislatura
    .groupBy("sk_leg")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_leg found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_legislatura
    .filter(F.col("sk_leg").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_leg found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_legislatura
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