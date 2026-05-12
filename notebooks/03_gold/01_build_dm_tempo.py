# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 01_build_dm_data
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed date dimension for the Gold Star Schema.
#
# Context:
# This notebook creates a reusable analytical date dimension used by Gold fact
# tables, data marts and BI views. It supports time-based analysis required by
# the final challenge, including CEAP expenses by month, event density by week,
# voting timelines and parliamentary engagement series.
#
# Responsibilities:
# - Generate a complete analytical calendar
# - Create a surrogate date key for dimensional joins
# - Create date hierarchy attributes for BI consumption
# - Support Star Schema modeling in the Gold layer
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Target:
# gold.dm_data
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_dm_data"
TARGET_TABLE = "gold.dm_data"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={PIPELINE_NAME} | started | target_table={TARGET_TABLE}",
    endpoint=TARGET_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

df_dm_data = (
    spark.sql("""
        SELECT explode(
            sequence(
                to_date('2019-01-01'),
                to_date('2027-12-31'),
                interval 1 day
            )
        ) AS dt_data
    """)
    .select(
        F.date_format("dt_data", "yyyyMMdd")
            .cast("int")
            .alias("sk_data"),

        F.col("dt_data")
            .alias("data_dt_data"),

        F.year("dt_data")
            .alias("data_nr_ano"),

        F.month("dt_data")
            .alias("data_nr_mes"),

        F.dayofmonth("dt_data")
            .alias("data_nr_dia"),

        F.weekofyear("dt_data")
            .alias("data_nr_semana_ano"),

        F.quarter("dt_data")
            .alias("data_nr_trimestre"),

        F.when(
            F.month("dt_data") <= 6,
            F.lit(1)
        ).otherwise(
            F.lit(2)
        ).alias("data_nr_semestre"),

        F.date_format("dt_data", "yyyy-MM")
            .alias("data_tx_ano_mes"),

        F.date_format("dt_data", "yyyyMM")
            .cast("int")
            .alias("data_nr_ano_mes"),

        F.date_format("dt_data", "EEEE")
            .alias("data_tx_nome_dia_semana"),

        F.date_format("dt_data", "MMMM")
            .alias("data_tx_nome_mes"),

        F.dayofweek("dt_data")
            .alias("data_nr_dia_semana"),

        F.when(
            F.dayofweek("dt_data").isin(1, 7),
            F.lit(1)
        ).otherwise(
            F.lit(0)
        ).alias("data_fl_fim_semana"),

        F.current_timestamp()
            .alias("gold_ts_processamento"),

        F.lit(batch_id)
            .alias("gold_id_batch")
    )
)

# COMMAND ----------

records_written = df_dm_data.count()

records_read = records_written
records_discarded = 0

if records_written == 0:
    raise Exception(
        "Gold validation failed: dm_data has no records."
    )

duplicated_keys = (
    df_dm_data
    .groupBy("sk_data")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_data found = {duplicated_keys}"
    )

null_keys = (
    df_dm_data
    .filter(F.col("sk_data").isNull())
    .count()
)

if null_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_data found = {null_keys}"
    )

# COMMAND ----------

(
    df_dm_data
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
    message=f"source={PIPELINE_NAME} | finished successfully | records_read={records_read} | records_written={records_written} | records_discarded={records_discarded}",
    endpoint=TARGET_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
    finished_at=datetime.now(),
)