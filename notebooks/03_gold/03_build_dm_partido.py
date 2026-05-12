# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 03_build_dm_partido
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed political party dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the political party dimension used across analytical
# fact tables such as expenses, voting behavior and parliamentary activity.
#
# Responsibilities:
# - Read curated deputy records
# - Extract valid political party attributes
# - Ensure one record per political party
# - Preserve Gold processing metadata
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.deputados
#
# Target:
# gold.dm_partido
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
PIPELINE_NAME = "gold_build_dm_partido"
SOURCE_TABLE = "silver_curated.deputados"
TARGET_TABLE = "gold.dm_partido"

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

df_dm_partido = (
    df_source
    .select(
        F.col("part_sg_partido")
            .alias("part_sg_partido")
    )
    .filter(F.col("part_sg_partido").isNotNull())
    .dropDuplicates(["part_sg_partido"])
)

# COMMAND ----------

window_partido = Window.orderBy("part_sg_partido")

df_dm_partido = (
    df_dm_partido
    .withColumn(
        "sk_part",
        F.row_number().over(window_partido)
    )
    .select(
        "sk_part",
        "part_sg_partido"
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

records_written = df_dm_partido.count()

records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.deputados has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: dm_partido has no records."
    )

duplicated_business_keys = (
    df_dm_partido
    .groupBy("sk_part")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_part found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_partido
    .groupBy("sk_part")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_part found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_partido
    .filter(F.col("sk_part").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_part found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_partido
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
# MAGIC select * from gold.dm_partido