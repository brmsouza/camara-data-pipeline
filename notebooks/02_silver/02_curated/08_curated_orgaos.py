# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 09_curated_orgaos
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates legislative organization
# data from Silver Base.
#
# Context:
# This notebook transforms silver_base.orgaos into a curated and analytics-ready
# organization dataset. The resulting table centralizes legislative bodies such
# as plenary, committees and other Câmara organizations for downstream
# dimensional modeling and relationship analysis.
#
# Responsibilities:
# - Consolidate standardized organization attributes from Silver Base
# - Curate organization type classification indicators
# - Create analytical organization flags
# - Preserve organization identifiers and relationships
# - Preserve lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.orgaos
#
# Target:
# silver_curated.orgaos
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Ready for organization dimension modeling
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
)

# COMMAND ----------

SOURCE_TABLE = "silver_base.orgaos"
TARGET_TABLE = "silver_curated.orgaos"

PIPELINE_NAME = "silver_curated_orgaos"
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
        # Organization identity
        # ---------------------------------------------------

        col("org_id_orgao")
            .alias("org_id_orgao"),

        col("org_tx_uri")
            .alias("org_tx_uri"),

        col("org_sg_orgao")
            .alias("org_sg_orgao"),

        col("org_tx_nome")
            .alias("org_tx_nome"),

        col("org_tx_apelido")
            .alias("org_tx_apelido"),

        col("org_tx_nome_publicacao")
            .alias("org_tx_nome_publicacao"),

        col("org_tx_nome_resumido")
            .alias("org_tx_nome_resumido"),

        # ---------------------------------------------------
        # Organization classification
        # ---------------------------------------------------

        col("org_cd_tipo_orgao")
            .alias("org_cd_tipo_orgao"),

        col("org_tx_tipo_orgao")
            .alias("org_tx_tipo_orgao"),

        when(
            upper(col("org_tx_tipo_orgao")).contains("PLEN"),
            lit("Plenário")
        )
        .when(
            upper(col("org_tx_tipo_orgao")).contains("COMISS"),
            lit("Comissão")
        )
        .when(
            upper(col("org_tx_tipo_orgao")).contains("MESA"),
            lit("Mesa Diretora")
        )
        .when(
            upper(col("org_tx_tipo_orgao")).contains("FRENTE"),
            lit("Frente Parlamentar")
        )
        .otherwise(col("org_tx_tipo_orgao"))
        .alias("org_tx_tipo_curado"),

        when(upper(col("org_tx_tipo_orgao")).contains("PLEN"), lit(1))
            .otherwise(lit(0))
            .alias("org_fl_plenario"),

        when(upper(col("org_tx_tipo_orgao")).contains("COMISS"), lit(1))
            .otherwise(lit(0))
            .alias("org_fl_comissao"),

        when(upper(col("org_tx_tipo_orgao")).contains("MESA"), lit(1))
            .otherwise(lit(0))
            .alias("org_fl_mesa"),

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

duplicated_orgaos = (
    df_curated
    .groupBy("org_id_orgao")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_orgaos > 0:
    raise Exception(
        f"Data quality error: {duplicated_orgaos} duplicated organizations in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("org_id_orgao").isNull()
    )
    .withColumn(
        "rejection_reason",
        when(
            col("org_id_orgao").isNull(),
            lit("org_id_orgao_is_null")
        )
        .otherwise(lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("org_id_orgao").isNotNull())
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