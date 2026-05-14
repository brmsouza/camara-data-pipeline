# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 15_base_proposicoes_tramitacoes_cdc
# Layer: Silver Base CDC
# Author: Bruno Souza
#
# Description:
# Normalizes raw proposicoes tramitacoes CDC payloads from Bronze into a
# structured Silver Base table, preparing data for SCD Type 2 processing.
#
# Context:
# This notebook reads raw tramitacao payloads from Bronze CDC, extracts
# structured attributes from JSON, applies data quality rules, deduplicates
# technical duplicates and persists a standardized Silver CDC table.
#
# Responsibilities:
# - Read raw CDC tramitacao records from Bronze CDC
# - Parse JSON payload into structured columns
# - Preserve CDC hash and lineage metadata
# - Validate required business and CDC fields
# - Persist rejected records
# - Persist Silver CDC Delta table
# - Register operational execution metrics
#
# Source:
# bronze_cdc.proposicoes_tramitacoes_raw
#
# Target:
# silver_cdc.proposicoes_tramitacoes_base
# ------------------------------------------------------------------------------


# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "silver_base_cdc"
PIPELINE_NAME = "silver_base_proposicoes_tramitacoes_cdc"
SOURCE_TABLE = "bronze_cdc.proposicoes_tramitacoes_raw"
TARGET_TABLE = "silver_cdc.proposicoes_tramitacoes_base"

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
        F.col("prop_id_proposicao").cast("long").alias("prop_id_proposicao"),
        F.col("tram_id_evento").cast("string").alias("tram_id_evento"),

        F.to_timestamp(
            F.get_json_object("bronze_tx_payload", "$.dataHora")
        ).alias("tram_ts_tramitacao"),

        F.to_date(
            F.get_json_object("bronze_tx_payload", "$.dataHora")
        ).alias("tram_dt_tramitacao"),

        F.get_json_object("bronze_tx_payload", "$.sequencia")
            .cast("string")
            .alias("tram_tx_sequencia"),

        F.upper(
            F.get_json_object("bronze_tx_payload", "$.siglaOrgao")
        ).alias("tram_tx_sigla_orgao"),

        F.get_json_object("bronze_tx_payload", "$.uriOrgao")
            .alias("tram_tx_uri_orgao"),

        F.initcap(
            F.get_json_object("bronze_tx_payload", "$.regime")
        ).alias("tram_tx_regime"),

        F.initcap(
            F.get_json_object("bronze_tx_payload", "$.descricaoTramitacao")
        ).alias("tram_tx_descricao_tramitacao"),

        F.initcap(
            F.get_json_object("bronze_tx_payload", "$.descricaoSituacao")
        ).alias("tram_tx_descricao_situacao"),

        F.get_json_object("bronze_tx_payload", "$.despacho")
            .alias("tram_tx_despacho"),

        F.get_json_object("bronze_tx_payload", "$.url")
            .alias("tram_tx_url"),

        F.col("bronze_tx_payload_hash").alias("cdc_payload_hash"),

        F.col("bronze_id_batch"),
        F.col("bronze_ts_ingestao"),

        F.current_timestamp().alias("silver_ts_processamento")
    )
)


# COMMAND ----------

df_discarded = (
    df_standardized
    .filter(
        F.col("prop_id_proposicao").isNull()
        |
        F.col("tram_ts_tramitacao").isNull()
        |
        F.col("cdc_payload_hash").isNull()
    )
    .withColumn(
        "rejection_reason",
        F.when(
            F.col("prop_id_proposicao").isNull(),
            F.lit("prop_id_proposicao_is_null")
        )
        .when(
            F.col("tram_ts_tramitacao").isNull(),
            F.lit("tram_ts_tramitacao_is_null")
        )
        .when(
            F.col("cdc_payload_hash").isNull(),
            F.lit("cdc_payload_hash_is_null")
        )
        .otherwise(F.lit("unknown"))
    )
)


# COMMAND ----------

df_valid = (
    df_standardized
    .filter(F.col("prop_id_proposicao").isNotNull())
    .filter(F.col("tram_ts_tramitacao").isNotNull())
    .filter(F.col("cdc_payload_hash").isNotNull())
)

df_deduplicated = (
    df_valid
    .dropDuplicates([
        "prop_id_proposicao",
        "tram_id_evento",
        "cdc_payload_hash"
    ])
)

# COMMAND ----------

records_written = df_deduplicated.count()
records_discarded = df_discarded.count()

if records_read == 0:
    raise Exception(
        "Silver Base CDC validation failed: bronze_cdc.proposicoes_tramitacoes_raw has no records."
    )

if records_written == 0:
    raise Exception(
        "Silver Base CDC validation failed: silver_cdc.proposicoes_tramitacoes_base has no valid records."
    )

duplicated_keys = (
    df_deduplicated
    .groupBy(
        "prop_id_proposicao",
        "tram_id_evento",
        "cdc_payload_hash"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_keys > 0:
    raise Exception(
        f"Silver Base CDC validation failed: duplicated CDC records found = {duplicated_keys}"
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
    df_deduplicated
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
    message=(
        f"source={SOURCE_TABLE} | finished successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| records_discarded={records_discarded}"
    ),
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    records_read=records_read,
    records_written=records_written,
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