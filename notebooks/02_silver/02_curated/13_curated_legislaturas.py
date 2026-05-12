# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 13_curated_legislaturas
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Builds the curated legislature entity for downstream dimensional modeling.
#
# Context:
# This notebook reads standardized legislature records from Silver Base and
# creates a curated business entity with descriptive and analytical attributes.
# The output is used by the Gold dm_legislatura dimension.
#
# Responsibilities:
# - Read standardized legislature records from Silver Base
# - Preserve valid legislature attributes
# - Create analytical period attributes
# - Preserve lineage metadata
# - Validate curated entity consistency
# - Persist Silver Curated Delta table
# - Register operational execution metrics
#
# Source:
# silver_base.legislaturas
#
# Target:
# silver_curated.legislaturas
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "silver_curated"
PIPELINE_NAME = "silver_curated_legislaturas"
SOURCE_TABLE = "silver_base.legislaturas"
TARGET_TABLE = "silver_curated.legislaturas"

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

df_curated = (
    df_source
    .select(
        F.col("leg_id_legislatura"),
        F.col("leg_tx_uri"),
        F.col("leg_dt_inicio"),
        F.col("leg_dt_fim"),

        (F.year("leg_dt_inicio") - 1)
            .alias("leg_nr_ano_eleicao"),

        F.year("leg_dt_inicio")
            .alias("leg_nr_ano_inicio"),

        F.year("leg_dt_fim")
            .alias("leg_nr_ano_fim"),

        F.months_between(
            F.col("leg_dt_fim"),
            F.col("leg_dt_inicio")
        ).cast("int").alias("leg_qt_meses_duracao"),

        F.when(
            (F.current_date() >= F.col("leg_dt_inicio")) &
            (F.current_date() <= F.col("leg_dt_fim")),
            F.lit(1)
        ).otherwise(F.lit(0)).alias("leg_fl_legislatura_atual"),

        F.concat(
            F.lit("Legislatura "),
            F.col("leg_id_legislatura").cast("string"),
            F.lit(" - Eleição "),
            (F.year("leg_dt_inicio") - 1).cast("string")
        ).alias("leg_tx_descricao"),

        F.col("leg_fl_data_inicio_valida"),
        F.col("leg_fl_data_fim_valida"),
        F.col("leg_fl_periodo_valido"),

        F.col("bronze_ts_ingestao"),
        F.col("bronze_dt_ingestao"),
        F.col("bronze_tx_endpoint"),
        F.col("bronze_id_origem"),
        F.col("bronze_id_batch"),
        F.col("bronze_tx_record_hash"),

        F.current_timestamp().alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

records_written = df_curated.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Silver Curated validation failed: silver_base.legislaturas has no records."
    )

if records_written == 0:
    raise Exception(
        "Silver Curated validation failed: silver_curated.legislaturas has no records."
    )

duplicated_keys = (
    df_curated
    .groupBy("leg_id_legislatura")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_keys > 0:
    raise Exception(
        f"Silver Curated validation failed: duplicated leg_id_legislatura found = {duplicated_keys}"
    )

# COMMAND ----------

(
    df_curated
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