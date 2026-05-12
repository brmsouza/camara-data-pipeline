# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 12_build_dm_tipo_despesa
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed expense type dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the CEAP expense type dimension used by expense fact
# tables and spending analytics.
#
# Responsibilities:
# - Read curated expense records
# - Extract CEAP expense type and specification attributes
# - Create a surrogate key for dimensional modeling
# - Ensure one record per expense type/specification combination
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Source:
# silver_curated.despesas
#
# Target:
# gold.dm_tipo_despesa
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
PIPELINE_NAME = "gold_build_dm_tipo_despesa"
SOURCE_TABLE = "silver_curated.despesas"
TARGET_TABLE = "gold.dm_tipo_despesa"

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

df_dm_tipo_despesa = (
    df_source
    .select(
        F.col("desp_cd_subcota"),
        F.col("desp_tx_tipo_despesa"),
        F.col("desp_cd_especificacao_subcota"),
        F.col("desp_tx_especificacao")
    )
    .filter(F.col("desp_cd_subcota").isNotNull())
    .dropDuplicates([
        "desp_cd_subcota",
        "desp_cd_especificacao_subcota"
    ])
    .withColumn(
        "desp_tx_segmento_despesa",
        F.when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("COMBUST"),
            F.lit("Combustível")
        )
        .when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("PASSAGEM") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("AÉREA") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("AEREA"),
            F.lit("Viagens")
        )
        .when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("HOSPED"),
            F.lit("Hotéis")
        )
        .when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("ALIMENT"),
            F.lit("Alimentação")
        )
        .when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("DIVULGA") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("PUBLIC"),
            F.lit("Publicidade")
        )
        .when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("TELEFON") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("INTERNET"),
            F.lit("Comunicação")
        )
        .when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("LOCAÇÃO") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("LOCACAO") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("VEÍCULO") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("VEICULO"),
            F.lit("Locomoção")
        )
        .when(
            F.upper(F.col("desp_tx_tipo_despesa")).contains("CONSULTORIA") |
            F.upper(F.col("desp_tx_tipo_despesa")).contains("ASSESSORIA"),
            F.lit("Serviços profissionais")
        )
        .otherwise(F.lit("Outros"))
    )
)

# COMMAND ----------

window_tipo_despesa = Window.orderBy(
    "desp_cd_subcota",
    "desp_cd_especificacao_subcota"
)

df_dm_tipo_despesa = (
    df_dm_tipo_despesa
    .withColumn("sk_desp_tipo", F.row_number().over(window_tipo_despesa))
    .select(
        "sk_desp_tipo",
        "desp_cd_subcota",
        "desp_tx_tipo_despesa",
        "desp_cd_especificacao_subcota",
        "desp_tx_especificacao",
        "desp_tx_segmento_despesa"
    )
    .withColumn("gold_ts_processamento", F.current_timestamp())
    .withColumn("gold_id_batch", F.lit(batch_id))
)

# COMMAND ----------

records_written = df_dm_tipo_despesa.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception("Gold validation failed: source silver_curated.despesas has no records.")

if records_written == 0:
    raise Exception("Gold validation failed: dm_tipo_despesa has no records.")

duplicated_business_keys = (
    df_dm_tipo_despesa
    .groupBy("desp_cd_subcota", "desp_cd_especificacao_subcota")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated expense type keys found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_tipo_despesa
    .groupBy("sk_desp_tipo")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_desp_tipo found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_tipo_despesa
    .filter(F.col("sk_desp_tipo").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_desp_tipo found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_tipo_despesa
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
# MAGIC select * from gold.dm_tipo_despesa