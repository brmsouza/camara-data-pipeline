# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 11_curated_frentes
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates parliamentary front data
# from Silver Base.
#
# Context:
# This notebook transforms silver_base.frentes into a curated and analytics-ready
# parliamentary front dataset. The resulting table supports downstream analysis
# of thematic political groups, parliamentary coalitions and relationships
# between fronts, deputies, expenses and voting behavior.
#
# Responsibilities:
# - Consolidate standardized parliamentary front attributes from Silver Base
# - Preserve legislature relationships
# - Create analytical thematic classification flags
# - Preserve complete lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.frentes
#
# Target:
# silver_curated.frentes
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Ready for parliamentary front dimension modeling
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    upper,
    current_timestamp,
    when,
    lit,
    count,
)

# COMMAND ----------

SOURCE_TABLE = "silver_base.frentes"
TARGET_TABLE = "silver_curated.frentes"

PIPELINE_NAME = "silver_curated_frentes"
LAYER = "silver_curated"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
records_discarded = 0

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={PIPELINE_NAME} | start successfully",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

df_base = spark.table(SOURCE_TABLE)

records_read = df_base.count()


# COMMAND ----------

df_curated = (
    df_base
    .select(
        # ---------------------------------------------------
        # Parliamentary front identity
        # ---------------------------------------------------

        col("frente_id_frente")
            .alias("frente_id_frente"),

        col("frente_tx_uri")
            .alias("frente_tx_uri"),

        col("frente_tx_titulo")
            .alias("frente_tx_titulo"),

        col("leg_id_legislatura")
            .alias("leg_id_legislatura"),

        # ---------------------------------------------------
        # Analytical classification
        # ---------------------------------------------------

        when(
            upper(col("frente_tx_titulo")).contains("SAÚDE") |
            upper(col("frente_tx_titulo")).contains("SAUDE"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("frente_fl_tema_saude"),

        when(
            upper(col("frente_tx_titulo")).contains("EDUCA"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("frente_fl_tema_educacao"),

        when(
            upper(col("frente_tx_titulo")).contains("SEGURAN"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("frente_fl_tema_seguranca"),

        when(
            upper(col("frente_tx_titulo")).contains("AGRO") |
            upper(col("frente_tx_titulo")).contains("RURAL"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("frente_fl_tema_agro"),

        when(
            upper(col("frente_tx_titulo")).contains("MULHER"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("frente_fl_tema_mulher"),

        when(
            upper(col("frente_tx_titulo")).contains("AMBIENT") |
            upper(col("frente_tx_titulo")).contains("SUSTENT"),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("frente_fl_tema_meio_ambiente"),

        # ---------------------------------------------------
        # Bronze lineage / traceability
        # ---------------------------------------------------

        col("bronze_ts_ingestao")
            .alias("bronze_ts_ingestao"),

        col("bronze_dt_ingestao")
            .alias("bronze_dt_ingestao"),

        col("bronze_tx_endpoint")
            .alias("bronze_tx_endpoint"),

        col("bronze_id_origem")
            .alias("bronze_id_origem"),

        col("bronze_id_batch")
            .alias("bronze_id_batch"),

        col("bronze_tx_record_hash")
            .alias("bronze_tx_record_hash"),

        # ---------------------------------------------------
        # Silver Base lineage
        # ---------------------------------------------------

        col("silver_ts_processamento")
            .alias("silver_base_ts_processamento"),

        # ---------------------------------------------------
        # Silver Curated metadata
        # ---------------------------------------------------

        current_timestamp()
            .alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

df_dedup = df_curated

duplicated_frentes = (
    df_dedup
    .groupBy("frente_id_frente")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if duplicated_frentes > 0:
    raise Exception(
        f"Data quality error: {duplicated_frentes} duplicated parliamentary fronts."
    )

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("frente_id_frente").isNull()
    )
    .withColumn(
        "rejection_reason",
        lit("frente_id_frente_is_null")
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("frente_id_frente").isNotNull())
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()

# COMMAND ----------

(
    df_discarded.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{TARGET_TABLE}_rejeitadas")
)

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=f"source={PIPELINE_NAME} | finished successfully | records_read={records_read} | records_written={records_written} | records_discarded={records_discarded}",
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