# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Parliamentary Office Dimension (dm_gabinete)
# MAGIC
# MAGIC **Notebook:**07_build_dm_gabinete
# MAGIC
# MAGIC Builds the conformed office dimension for the Gold Star Schema.
# MAGIC
# MAGIC This notebook creates the deputy office dimension from curated deputy data.
# MAGIC It supports analytical joins related to parliamentary office location,
# MAGIC contact information and cabinet-level segmentation.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read curated deputy records
# MAGIC - Extract cabinet/office attributes
# MAGIC - Create a surrogate key for dimensional modeling
# MAGIC - Ensure one record per deputy office
# MAGIC - Preserve lineage and Gold processing metadata
# MAGIC - Validate dimension consistency
# MAGIC - Persist the Gold Delta dimension table
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC **Source of truth:** `silver_curated.deputados`  
# MAGIC
# MAGIC **Target:** `gold.dm_gabinete`

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_dm_gabinete"
SOURCE_TABLE = "silver_curated.deputados"
TARGET_TABLE = "gold.dm_gabinete"

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

df_dm_gabinete = (
    df_source
    .select(
        F.col("dept_id_deputado"),
        F.col("gab_tx_nome"),
        F.col("gab_tx_predio"),
        F.col("gab_tx_sala"),
        F.col("gab_tx_andar"),
        F.col("gab_tx_telefone"),
        F.col("gab_fl_telefone_valido"),
        F.col("gab_tx_email"),
        F.col("gab_fl_email_valido"),
        F.col("bronze_ts_ingestao_deputados").alias("bronze_ts_ingestao"),
        F.col("bronze_id_batch_deputados").alias("bronze_id_batch"),
        F.col("bronze_tx_record_hash_deputados").alias("bronze_tx_record_hash")
    )
    .filter(F.col("dept_id_deputado").isNotNull())
    .dropDuplicates(["dept_id_deputado"])
)

# COMMAND ----------

window_gabinete = Window.orderBy("dept_id_deputado")

df_dm_gabinete = (
    df_dm_gabinete
    .withColumn(
        "sk_gab",
        F.row_number().over(window_gabinete)
    )
    .select(
        "sk_gab",
        "dept_id_deputado",
        "gab_tx_nome",
        "gab_tx_predio",
        "gab_tx_sala",
        "gab_tx_andar",
        "gab_tx_telefone",
        "gab_fl_telefone_valido",
        "gab_tx_email",
        "gab_fl_email_valido",
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

records_written = df_dm_gabinete.count()

records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Gold validation failed: source silver_curated.deputados has no records."
    )

if records_written == 0:
    raise Exception(
        "Gold validation failed: dm_gabinete has no records."
    )

duplicated_business_keys = (
    df_dm_gabinete
    .groupBy("dept_id_deputado")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated dept_id_deputado found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_gabinete
    .groupBy("sk_gab")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_gab found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_gabinete
    .filter(F.col("sk_gab").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_gab found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_gabinete
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