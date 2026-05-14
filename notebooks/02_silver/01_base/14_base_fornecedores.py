# Databricks notebook source
# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 14_curated_fornecedores
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Builds the curated supplier dataset enriched with public CNPJ validation data.
#
# Context:
# This notebook reads supplier records from Silver Base, prioritizes CNPJ
# suppliers based on CEAP expense usage, validates selected CNPJs using a public
# CNPJ API utility, and creates analytical flags for supplier registration
# status and potential suspicious documents.
#
# Grain:
# One row per supplier document.
#
# Responsibilities:
# - Read standardized supplier records from Silver Base
# - Prioritize CNPJ suppliers based on CEAP usage
# - Validate selected CNPJs using public API enrichment
# - Create analytical supplier status and suspicion flags
# - Preserve lineage metadata
# - Validate curated entity consistency
# - Persist Silver Curated Delta table
# - Register operational execution metrics
#
# Source:
# silver_base.fornecedores
#
# Target:
# silver_curated.fornecedores
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "silver_base"
PIPELINE_NAME = "silver_base_fornecedores"

SOURCE_TABLE = "silver_base.despesas"
TARGET_TABLE = "silver_base.fornecedores"

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
        F.upper(F.trim(F.col("forn_tx_nome"))).alias("forn_tx_nome"),

        F.regexp_replace(
            F.col("forn_nr_cnpj_cpf").cast("string"),
            "[^0-9]",
            ""
        ).alias("forn_nr_documento_original_limpo"),

        F.col("bronze_ts_ingestao"),
        F.col("bronze_dt_ingestao"),
        F.col("bronze_tx_endpoint"),
        F.col("bronze_id_origem"),
        F.col("bronze_id_batch"),
        F.col("bronze_tx_record_hash"),
    )
    .filter(
        F.col("forn_nr_documento_original_limpo").isNotNull()
        & (F.length(F.col("forn_nr_documento_original_limpo")) > 0)
    )
    .withColumn(
        "forn_fl_documento_repetido",
        F.col("forn_nr_documento_original_limpo")
        .rlike(r"^(\d)\1+$")
        .cast("int")
    )
    .withColumn(
        "forn_tx_tipo_documento",
        F.when(
            (
                F.length(F.col("forn_nr_documento_original_limpo")) == 14
            )
            &
            (
                F.col("forn_fl_documento_repetido") == 0
            ),
            F.lit("CNPJ")
        )
        .when(
            (
                F.length(F.col("forn_nr_documento_original_limpo")) == 11
            )
            &
            (
                F.col("forn_fl_documento_repetido") == 0
            ),
            F.lit("CPF")
        )
        .otherwise(F.lit("OUTRO"))
    )
    .withColumn(
        "forn_nr_documento_limpo",
        F.col("forn_nr_documento_original_limpo")
    )
    .withColumn(
        "forn_fl_documento_valido_formato",
        F.when(
            F.col("forn_tx_tipo_documento").isin("CNPJ", "CPF"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "forn_tx_dedup_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.coalesce(F.col("forn_nr_documento_limpo"), F.lit("")),
                F.coalesce(F.col("forn_tx_nome"), F.lit(""))
            ),
            256
        )
    )
)

# COMMAND ----------

window_dedup = Window.partitionBy("forn_nr_documento_limpo").orderBy(
    F.col("bronze_ts_ingestao").desc_nulls_last(),
    F.col("forn_tx_nome").asc_nulls_last()
)

df_deduplicated = (
    df_standardized
    .withColumn("rn", F.row_number().over(window_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_discarded = (
    df_deduplicated
    .filter(
        F.col("forn_tx_nome").isNull()
        |
        F.col("forn_nr_documento_limpo").isNull()
    )
    .withColumn(
        "rejection_reason",
        F.when(
            F.col("forn_tx_nome").isNull(),
            F.lit("forn_tx_nome_is_null")
        )
        .when(
            F.col("forn_nr_documento_limpo").isNull(),
            F.lit("forn_nr_documento_limpo_is_null")
        )
        .otherwise(F.lit("unknown"))
    )
)

df_valid = (
    df_deduplicated
    .filter(F.col("forn_tx_nome").isNotNull())
    .filter(F.col("forn_nr_documento_limpo").isNotNull())
)

# COMMAND ----------

records_written = df_valid.count()
records_discarded = df_discarded.count()
records_deduplicated = records_read - records_written - records_discarded

if records_read == 0:
    raise Exception(
        "Silver Base validation failed: source silver_base.despesas has no records."
    )

if records_written == 0:
    raise Exception(
        "Silver Base validation failed: silver_base.fornecedores has no records."
    )

duplicated_documents = (
    df_valid
    .groupBy("forn_nr_documento_limpo")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_documents > 0:
    raise Exception(
        f"Silver Base validation failed: duplicated supplier documents found = {duplicated_documents}"
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
    df_valid
    .withColumn("silver_base_ts_processamento", F.current_timestamp())
    .withColumn("silver_base_id_batch", F.lit(batch_id))
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
        f"| records_discarded={records_discarded} "
        f"| records_deduplicated={records_deduplicated}"
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