# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 11_build_dm_uf
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds the conformed Brazilian state dimension for the Gold Star Schema.
#
# Context:
# This notebook creates the UF dimension from curated deputy, expense and voting
# datasets. It supports regional analysis across Gold fact tables.
#
# Responsibilities:
# - Read curated datasets with UF attributes
# - Consolidate unique UF values
# - Create a surrogate key for dimensional modeling
# - Validate dimension consistency
# - Persist the Gold Delta dimension table
# - Register operational execution metrics
#
# Sources:
# silver_curated.deputados
# silver_curated.despesas
# silver_curated.votacoes_votos
# silver_curated.frentes_membros
#
# Target:
# gold.dm_uf
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
PIPELINE_NAME = "gold_build_dm_uf"

SOURCE_TABLE_DEPUTADOS = "silver_curated.deputados"
SOURCE_TABLE_DESPESAS = "silver_curated.despesas"
SOURCE_TABLE_VOTOS = "silver_curated.votacoes_votos"
SOURCE_TABLE_FRENTES_MEMBROS = "silver_curated.frentes_membros"

TARGET_TABLE = "gold.dm_uf"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_dm_uf"

SOURCE_TABLE_DEPUTADOS = "silver_curated.deputados"
SOURCE_TABLE_DESPESAS = "silver_curated.despesas"
SOURCE_TABLE_VOTOS = "silver_curated.votacoes_votos"
SOURCE_TABLE_FRENTES_MEMBROS = "silver_curated.frentes_membros"

TARGET_TABLE = "gold.dm_uf"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

SOURCE_TABLE = (
    f"{SOURCE_TABLE_DEPUTADOS}, "
    f"{SOURCE_TABLE_DESPESAS}, "
    f"{SOURCE_TABLE_VOTOS}, "
    f"{SOURCE_TABLE_FRENTES_MEMBROS}"
)

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

df_deputados = spark.table(SOURCE_TABLE_DEPUTADOS).select(F.col("uf_sg_uf"))
df_despesas = spark.table(SOURCE_TABLE_DESPESAS).select(F.col("uf_sg_uf"))
df_votos = spark.table(SOURCE_TABLE_VOTOS).select(F.col("uf_sg_uf"))
df_frentes_membros = spark.table(SOURCE_TABLE_FRENTES_MEMBROS).select(F.col("uf_sg_uf"))

records_read = (
    df_deputados.count()
    + df_despesas.count()
    + df_votos.count()
    + df_frentes_membros.count()
)

# COMMAND ----------

df_dm_uf = (
    df_deputados
    .unionByName(df_despesas)
    .unionByName(df_votos)
    .unionByName(df_frentes_membros)
    .filter(F.col("uf_sg_uf").isNotNull())
    .dropDuplicates(["uf_sg_uf"])
)

# COMMAND ----------

window_uf = Window.orderBy("uf_sg_uf")

df_dm_uf = (
    df_dm_uf
    .withColumn("sk_uf", F.row_number().over(window_uf))
    .select(
        "sk_uf",
        "uf_sg_uf"
    )
    .withColumn("gold_ts_processamento", F.current_timestamp())
    .withColumn("gold_id_batch", F.lit(batch_id))
)

# COMMAND ----------

records_written = df_dm_uf.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception("Gold validation failed: UF source tables have no records.")

if records_written == 0:
    raise Exception("Gold validation failed: dm_uf has no records.")

duplicated_business_keys = (
    df_dm_uf
    .groupBy("uf_sg_uf")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated uf_sg_uf found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_uf
    .groupBy("sk_uf")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_uf found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_uf
    .filter(F.col("sk_uf").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_uf found = {null_surrogate_keys}"
    )

# COMMAND ----------

(
    df_dm_uf
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
# MAGIC select * from gold.dm_uf