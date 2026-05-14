# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Base Layer — Legislature Reference Standardization
# MAGIC
# MAGIC **Notebook:** `13_base_legislaturas`
# MAGIC
# MAGIC Standardizes legislature reference data from the Bronze layer.
# MAGIC
# MAGIC This notebook reads raw legislature reference records from Bronze and creates
# MAGIC a clean, typed and deduplicated Silver Base table. The resulting dataset
# MAGIC preserves Bronze lineage metadata and prepares legislature attributes for
# MAGIC downstream curated dimensional modeling.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read legislature reference records from Bronze
# MAGIC - Parse and type source fields
# MAGIC - Standardize column names
# MAGIC - Deduplicate by legislature identifiers
# MAGIC - Preserve Bronze lineage metadata
# MAGIC - Validate Silver Base consistency
# MAGIC - Persist Silver Base Delta table
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC **Source of truth:** `bronze.legislaturas`  
# MAGIC
# MAGIC **Target:** `silver_base.legislaturas`

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "silver_base"
PIPELINE_NAME = "silver_base_legislaturas"
SOURCE_TABLE = "bronze.legislaturas"
TARGET_TABLE = "silver_base.legislaturas"

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

df_standardized = (
    df_source
    .select(
        F.get_json_object(F.col("raw_payload"), "$.id")
            .cast("long")
            .alias("leg_id_legislatura"),

        F.get_json_object(F.col("raw_payload"), "$.uri")
            .alias("leg_tx_uri"),

        F.to_date(
            F.get_json_object(F.col("raw_payload"), "$.dataInicio")
        ).alias("leg_dt_inicio"),

        F.to_date(
            F.get_json_object(F.col("raw_payload"), "$.dataFim")
        ).alias("leg_dt_fim"),

        F.col("ingestion_timestamp").alias("bronze_ts_ingestao"),
        F.col("ingestion_date").alias("bronze_dt_ingestao"),
        F.col("source_endpoint").alias("bronze_tx_endpoint"),
        F.col("source_id").alias("bronze_id_origem"),
        F.col("batch_id").alias("bronze_id_batch"),
        F.sha2(F.col("raw_payload"), 256).alias("bronze_tx_record_hash")
    )
    .withColumn(
        "leg_fl_data_inicio_valida",
        F.when(F.col("leg_dt_inicio").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "leg_fl_data_fim_valida",
        F.when(F.col("leg_dt_fim").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "leg_fl_periodo_valido",
        F.when(
            (F.col("leg_dt_inicio").isNotNull()) &
            (F.col("leg_dt_fim").isNotNull()) &
            (F.col("leg_dt_inicio") <= F.col("leg_dt_fim")),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "silver_base_ts_processamento",
        F.current_timestamp()
    )
)

# COMMAND ----------

window_legislatura = (
    Window
    .partitionBy("leg_id_legislatura")
    .orderBy(F.col("bronze_ts_ingestao").desc_nulls_last())
)

df_deduplicated = (
    df_standardized
    .withColumn("rn", F.row_number().over(window_legislatura))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_validated = (
    df_deduplicated
    .filter(F.col("leg_id_legislatura").isNotNull())
    .filter(F.col("leg_fl_data_inicio_valida") == 1)
    .filter(F.col("leg_fl_data_fim_valida") == 1)
    .filter(F.col("leg_fl_periodo_valido") == 1)
)

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_deduplicated
    .filter(
        F.col("leg_id_legislatura").isNull()
        |
        (F.col("leg_fl_data_inicio_valida") != 1)
        |
        (F.col("leg_fl_data_fim_valida") != 1)
        |
        (F.col("leg_fl_periodo_valido") != 1)
    )
    .withColumn(
        "rejection_reason",
        F.when(
            F.col("leg_id_legislatura").isNull(),
            F.lit("leg_id_legislatura_is_null")
        )
        .when(
            F.col("leg_fl_data_inicio_valida") != 1,
            F.lit("leg_dt_inicio_invalid")
        )
        .when(
            F.col("leg_fl_data_fim_valida") != 1,
            F.lit("leg_dt_fim_invalid")
        )
        .when(
            F.col("leg_fl_periodo_valido") != 1,
            F.lit("leg_period_invalid")
        )
        .otherwise(F.lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_validated = (
    df_deduplicated
    .filter(F.col("leg_id_legislatura").isNotNull())
    .filter(F.col("leg_fl_data_inicio_valida") == 1)
    .filter(F.col("leg_fl_data_fim_valida") == 1)
    .filter(F.col("leg_fl_periodo_valido") == 1)
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_validated.count()
records_discarded = df_discarded.count()

if records_read == 0:
    raise Exception(
        "Silver Base validation failed: bronze.legislaturas has no records."
    )

if records_written == 0:
    raise Exception(
        "Silver Base validation failed: silver_base.legislaturas has no records."
    )

duplicated_keys = (
    df_validated
    .groupBy("leg_id_legislatura")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_keys > 0:
    raise Exception(
        f"Silver Base validation failed: duplicated leg_id_legislatura found = {duplicated_keys}"
    )

# COMMAND ----------

(
    df_discarded
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET_TABLE}_rejeitadas")
)

# COMMAND ----------

(
    df_validated
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

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Layer: {LAYER}")
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")
print(f"Records discarded: {records_discarded}")