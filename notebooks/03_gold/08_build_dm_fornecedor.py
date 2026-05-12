# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 08_build_dm_fornecedor
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed supplier dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the supplier dimension used by CEAP expense fact tables.
#
# Responsibilities:
# - Read curated expense records
# - Extract supplier attributes
# - Create a surrogate key for dimensional modeling
# - Ensure one record per supplier document
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.despesas
#
# Target:
# gold.dm_fornecedor
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
PIPELINE_NAME = "gold_build_dm_fornecedor"
SOURCE_TABLE = "silver_curated.despesas"
TARGET_TABLE = "gold.dm_fornecedor"

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

df_dm_fornecedor = (
    df_source
    .select(
        F.col("forn_nr_cnpj_cpf"),
        F.col("forn_tx_nome"),
        F.col("forn_tx_tipo_documento"),
        F.col("forn_fl_documento_valido"),
        F.col("bronze_ts_ingestao"),
        F.col("bronze_dt_ingestao"),
        F.col("bronze_tx_endpoint"),
        F.col("bronze_id_origem"),
        F.col("bronze_tx_source_file"),
        F.col("bronze_id_batch"),
        F.col("bronze_tx_record_hash")
    )
    .filter(F.col("forn_nr_cnpj_cpf").isNotNull())
    .dropDuplicates(["forn_nr_cnpj_cpf"])
)

# COMMAND ----------

window_fornecedor = Window.orderBy("forn_nr_cnpj_cpf")

df_dm_fornecedor = (
    df_dm_fornecedor
    .withColumn("sk_forn", F.row_number().over(window_fornecedor))
    .select(
        "sk_forn",
        "forn_nr_cnpj_cpf",
        "forn_tx_nome",
        "forn_tx_tipo_documento",
        "forn_fl_documento_valido",
        "bronze_ts_ingestao",
        "bronze_dt_ingestao",
        "bronze_tx_endpoint",
        "bronze_id_origem",
        "bronze_tx_source_file",
        "bronze_id_batch",
        "bronze_tx_record_hash"
    )
    .withColumn("gold_ts_processamento", F.current_timestamp())
    .withColumn("gold_id_batch", F.lit(batch_id))
)

# COMMAND ----------

records_written = df_dm_fornecedor.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception("Gold validation failed: source silver_curated.despesas has no records.")

if records_written == 0:
    raise Exception("Gold validation failed: dm_fornecedor has no records.")

duplicated_business_keys = (
    df_dm_fornecedor
    .groupBy("forn_nr_cnpj_cpf")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated forn_nr_cnpj_cpf found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_fornecedor
    .groupBy("sk_forn")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_forn found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_fornecedor
    .filter(F.col("sk_forn").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_forn found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_fornecedor
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
# MAGIC select * from gold.dm_fornecedor