# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Base Layer — Voting Orientations Standardization
# MAGIC
# MAGIC **Notebook:** `11_base_votacoes_orientacoes`
# MAGIC
# MAGIC Parses, structures, types, deduplicates and validates voting orientation data
# MAGIC from the Bronze layer.
# MAGIC
# MAGIC This notebook transforms raw voting orientation payloads from
# MAGIC `bronze.votacoes_orientacoes` into a structured Silver Base table. The resulting
# MAGIC dataset represents party, parliamentary bloc and bancada orientations for each
# MAGIC voting session, enabling downstream analysis of political alignment, party
# MAGIC discipline and comparison between official orientations and individual deputy
# MAGIC votes.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Parse raw CSV-like payloads embedded in JSON structures
# MAGIC - Apply schema standardization
# MAGIC - Cast identifiers where applicable
# MAGIC - Preserve voting session and political bancada relationships
# MAGIC - Preserve lineage and traceability columns
# MAGIC - Apply technical deduplication
# MAGIC - Persist Silver Base Delta table
# MAGIC
# MAGIC **Source of truth:** `bronze.votacoes_orientacoes`  
# MAGIC
# MAGIC **Target:** `silver_base.votacoes_orientacoes`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake format
# MAGIC - Partitioned by ingestion date
# MAGIC - Source for voting alignment and party orientation analytics

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    row_number,
    sha2,
    concat_ws,
    initcap,
    count,
    when,
    lit,
)

from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.votacoes_orientacoes"
TARGET_TABLE = "silver_base.votacoes_orientacoes"

PIPELINE_NAME = "silver_base_votacoes_orientacoes"
LAYER = "silver_base"

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

df_bronze = spark.table(SOURCE_TABLE)

records_read = df_bronze.count()


# COMMAND ----------

from pyspark.sql.functions import get_json_object

df_standardized = (
    df_bronze
    .select(
        trim(get_json_object(col("raw_payload"), "$.idVotacao"))
            .alias("vot_id_votacao"),

        trim(get_json_object(col("raw_payload"), "$.uriVotacao"))
            .alias("vot_tx_uri"),

        upper(trim(get_json_object(col("raw_payload"), "$.siglaOrgao")))
            .alias("org_sg_orgao"),

        initcap(trim(get_json_object(col("raw_payload"), "$.descricao")))
            .alias("vot_tx_descricao_resultado"),

        upper(trim(get_json_object(col("raw_payload"), "$.siglaBancada")))
            .alias("banc_tx_sigla_bancada"),

        trim(get_json_object(col("raw_payload"), "$.uriBancada"))
            .alias("banc_tx_uri"),

        initcap(trim(get_json_object(col("raw_payload"), "$.orientacao")))
            .alias("vot_tx_orientacao"),

        sha2(
            concat_ws(
                "||",
                trim(get_json_object(col("raw_payload"), "$.idVotacao")),
                upper(trim(get_json_object(col("raw_payload"), "$.siglaOrgao"))),
                upper(trim(get_json_object(col("raw_payload"), "$.siglaBancada"))),
                initcap(trim(get_json_object(col("raw_payload"), "$.orientacao")))
            ),
            256
        ).alias("vot_tx_dedup_key"),

        col("ingestion_timestamp").alias("bronze_ts_ingestao"),
        col("ingestion_date").alias("bronze_dt_ingestao"),
        col("source_endpoint").alias("bronze_tx_endpoint"),
        col("source_id").alias("bronze_id_origem"),

        get_json_object(col("raw_payload"), "$._source_file")
            .alias("bronze_tx_source_file"),

        get_json_object(col("raw_payload"), "$.ano_referencia")
            .cast("int")
            .alias("bronze_nr_ano_referencia"),

        col("batch_id").alias("bronze_id_batch"),
        col("record_hash").alias("bronze_tx_record_hash"),

        current_timestamp().alias("silver_ts_processamento")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("vot_tx_dedup_key")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df_standardized
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

duplicated_orientations = (
    df_dedup
    .groupBy("vot_tx_dedup_key")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if duplicated_orientations > 0:
    raise Exception(
        f"Data quality error: {duplicated_orientations} duplicated orientation records."
    )

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("vot_id_votacao").isNull()
        |
        col("vot_tx_dedup_key").isNull()
        |
        (
            col("banc_tx_sigla_bancada").isNull()
            &
            col("vot_tx_orientacao").isNull()
        )
    )
    .withColumn(
        "rejection_reason",
        when(
            col("vot_id_votacao").isNull(),
            lit("vot_id_votacao_is_null")
        )
        .when(
            col("vot_tx_dedup_key").isNull(),
            lit("vot_tx_dedup_key_is_null")
        )
        .when(
            col("banc_tx_sigla_bancada").isNull()
            &
            col("vot_tx_orientacao").isNull(),
            lit("empty_orientation_record")
        )
        .otherwise(lit("unknown"))
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("vot_tx_dedup_key").isNotNull())
    .filter(
        ~(
            col("banc_tx_sigla_bancada").isNull()
            &
            col("vot_tx_orientacao").isNull()
        )
    )
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()

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
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("bronze_nr_ano_referencia")
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