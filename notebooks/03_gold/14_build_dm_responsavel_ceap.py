# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — CEAP Responsible Dimension (dm_responsavel_ceap)
# MAGIC
# MAGIC **Notebook:** 14_build_dm_responsavel_ceap
# MAGIC
# MAGIC Builds the conformed CEAP expense responsible dimension for the Gold Star Schema.
# MAGIC
# MAGIC This notebook creates the CEAP responsible dimension used by CEAP expense fact
# MAGIC tables and parliamentary expenditure analytics.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read curated CEAP expense records
# MAGIC - Extract CEAP responsible attributes
# MAGIC - Classify responsible type
# MAGIC - Create a sequential surrogate key for dimensional modeling
# MAGIC - Ensure one record per CEAP responsible
# MAGIC - Validate dimension consistency
# MAGIC - Persist the Gold Delta dimension table
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC **Source of truth:** `silver_curated.despesas`  
# MAGIC
# MAGIC **Target:** `gold.dm_responsavel_ceap`

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_dm_responsavel_ceap"
SOURCE_TABLE = "silver_curated.despesas"
TARGET_TABLE = "gold.dm_responsavel_ceap"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

df_source = spark.table(SOURCE_TABLE)

records_read = df_source.count()

# COMMAND ----------

df_dm_responsavel_ceap = (
    df_source
    .select(
        F.when(
            F.upper(F.coalesce(F.col("dept_tx_nome_parlamentar"), F.lit("")))
            .contains("LIDERANÇA"),
            F.lit("LIDERANCA")
        )
        .when(
            F.col("dept_id_deputado_resolvido").isNotNull(),
            F.lit("DEPUTADO")
        )
        .otherwise(F.lit("NAO_IDENTIFICADO"))
        .alias("resp_tx_tipo_responsavel"),

        F.when(
            F.upper(F.coalesce(F.col("dept_tx_nome_parlamentar"), F.lit("")))
            .contains("LIDERANÇA"),
            F.col("dept_tx_nome_parlamentar")
        )
        .when(
            F.col("dept_id_deputado_resolvido").isNotNull(),
            F.col("dept_tx_nome_parlamentar")
        )
        .otherwise(F.lit("Não identificado"))
        .alias("resp_tx_nome_responsavel"),

        F.col("dept_id_deputado_resolvido").alias("id_deputado"),
        F.col("dept_id_cadastro").alias("id_cadastro_ceap"),
        F.col("dept_id_deputado").alias("id_deputado_ceap"),
        F.col("dept_nr_cpf").alias("resp_nr_cpf"),
        F.col("part_sg_partido"),
        F.col("uf_sg_uf")
    )
    .dropDuplicates([
        "resp_tx_tipo_responsavel",
        "resp_tx_nome_responsavel",
        "id_deputado",
        "id_cadastro_ceap",
        "id_deputado_ceap"
    ])
)


# COMMAND ----------

window_resp = Window.orderBy(
    "resp_tx_tipo_responsavel",
    "resp_tx_nome_responsavel",
    "id_deputado",
    "id_cadastro_ceap",
    "id_deputado_ceap"
)

df_dm_responsavel_ceap = (
    df_dm_responsavel_ceap
    .withColumn(
        "sk_resp_ceap",
        F.row_number().over(window_resp)
    )
    .select(
        "sk_resp_ceap",
        "resp_tx_tipo_responsavel",
        "resp_tx_nome_responsavel",
        "id_deputado",
        "id_cadastro_ceap",
        "id_deputado_ceap",
        "resp_nr_cpf",
        "part_sg_partido",
        "uf_sg_uf"
    )
    .withColumn("gold_ts_processamento", F.current_timestamp())
    .withColumn("gold_id_batch", F.lit(batch_id))
)

# COMMAND ----------

records_written = df_dm_responsavel_ceap.count()
records_discarded = records_read - records_written

# COMMAND ----------

if records_read == 0:
    raise Exception("Gold validation failed: source silver_curated.despesas has no records.")

if records_written == 0:
    raise Exception("Gold validation failed: dm_responsavel_ceap has no records.")

duplicated_business_keys = (
    df_dm_responsavel_ceap
    .groupBy(
        "resp_tx_tipo_responsavel",
        "resp_tx_nome_responsavel",
        "id_deputado",
        "id_cadastro_ceap",
        "id_deputado_ceap"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_business_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated CEAP responsible business keys found = {duplicated_business_keys}"
    )

duplicated_surrogate_keys = (
    df_dm_responsavel_ceap
    .groupBy("sk_resp_ceap")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: duplicated sk_resp_ceap found = {duplicated_surrogate_keys}"
    )

null_surrogate_keys = (
    df_dm_responsavel_ceap
    .filter(F.col("sk_resp_ceap").isNull())
    .count()
)

if null_surrogate_keys > 0:
    raise Exception(
        f"Gold validation failed: null sk_resp_ceap found = {null_surrogate_keys}"
    )

null_responsible_names = (
    df_dm_responsavel_ceap
    .filter(F.col("resp_tx_nome_responsavel").isNull())
    .count()
)

if null_responsible_names > 0:
    raise Exception(
        f"Gold validation failed: null resp_tx_nome_responsavel found = {null_responsible_names}"
    )


# COMMAND ----------

(
    df_dm_responsavel_ceap
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