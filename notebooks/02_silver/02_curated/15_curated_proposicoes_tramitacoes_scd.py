# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Curated CDC Layer — Proposition Tramitacoes SCD Type 2 Historization
# MAGIC
# MAGIC **Notebook:** `15_curated_proposicoes_tramitacoes_scd2`
# MAGIC
# MAGIC Builds the SCD Type 2 historical table for proposition tramitacoes.
# MAGIC
# MAGIC This notebook reads normalized CDC records from the Silver CDC Base layer and
# MAGIC creates a temporal historical table using SCD Type 2 historization logic.
# MAGIC Each detected change is tracked using payload hash comparison, preserving
# MAGIC historical versions together with validity intervals.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read normalized proposition tramitacao CDC records from Silver CDC Base
# MAGIC - Validate required CDC and temporal attributes
# MAGIC - Preserve historical changes using SCD Type 2 logic
# MAGIC - Close previous active versions when changes are detected
# MAGIC - Insert new current versions
# MAGIC - Persist rejected records for quality auditing
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC **Source of truth:** `silver_cdc.proposicoes_tramitacoes_base`  
# MAGIC
# MAGIC **Target:** `silver_cdc.proposicoes_tramitacoes_scd2`

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "silver_curated_cdc"
PIPELINE_NAME = "silver_curated_proposicoes_tramitacoes_scd2"
SOURCE_TABLE = "silver_cdc.proposicoes_tramitacoes_base"
TARGET_TABLE = "silver_cdc.proposicoes_tramitacoes_scd2"

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
        F.col("prop_id_proposicao").cast("long").alias("prop_id_proposicao"),
        F.col("tram_id_evento").cast("string").alias("tram_id_evento"),

        F.col("tram_ts_tramitacao").cast("timestamp").alias("tram_ts_tramitacao"),
        F.col("tram_dt_tramitacao").cast("date").alias("tram_dt_tramitacao"),

        F.upper(F.col("tram_tx_sigla_orgao")).alias("tram_tx_sigla_orgao"),
        F.initcap(F.col("tram_tx_regime")).alias("tram_tx_regime"),
        F.initcap(F.col("tram_tx_descricao_tramitacao")).alias("tram_tx_descricao_tramitacao"),
        F.initcap(F.col("tram_tx_descricao_situacao")).alias("tram_tx_descricao_situacao"),
        F.col("tram_tx_despacho"),
        F.col("cdc_payload_hash"),

        F.col("tram_ts_tramitacao").cast("timestamp").alias("valid_from"),
        F.lit(None).cast("timestamp").alias("valid_to"),
        F.lit(True).alias("is_current"),

        F.current_timestamp().alias("scd_ts_processamento")
    )
)

# COMMAND ----------

df_discarded = (
    df_curated
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
    df_curated
    .filter(F.col("prop_id_proposicao").isNotNull())
    .filter(F.col("tram_ts_tramitacao").isNotNull())
    .filter(F.col("cdc_payload_hash").isNotNull())
    .dropDuplicates([
        "prop_id_proposicao",
        "tram_id_evento",
        "cdc_payload_hash"
    ])
)

# COMMAND ----------


records_discarded = df_discarded.count()

if records_read == 0:
    raise Exception(
        "Silver Curated CDC validation failed: silver_cdc.proposicoes_tramitacoes_base has no records."
    )

if df_valid.count() == 0:
    raise Exception(
        "Silver Curated CDC validation failed: no valid records available for SCD2 processing."
    )

duplicated_versions = (
    df_valid
    .groupBy(
        "prop_id_proposicao",
        "tram_id_evento",
        "cdc_payload_hash"
    )
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_versions > 0:
    raise Exception(
        f"Silver Curated CDC validation failed: duplicated CDC versions found = {duplicated_versions}"
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

target_exists = spark.catalog.tableExists(TARGET_TABLE)

# COMMAND ----------

if not target_exists or spark.table(TARGET_TABLE).count() == 0:

    (
        df_valid
        .write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    records_written = df_valid.count()
    records_closed = 0

else:

    df_current = (
        spark.table(TARGET_TABLE)
        .filter(F.col("is_current") == True)
        .select(
            F.col("prop_id_proposicao").alias("cur_prop_id_proposicao"),
            F.col("tram_id_evento").alias("cur_tram_id_evento"),
            F.col("cdc_payload_hash").alias("cur_cdc_payload_hash")
        )
    )

    df_changes = (
        df_valid.alias("src")
        .join(
            df_current.alias("cur"),
            (
                (F.col("src.prop_id_proposicao") == F.col("cur.cur_prop_id_proposicao"))
                &
                (F.col("src.tram_id_evento") == F.col("cur.cur_tram_id_evento"))
            ),
            "left"
        )
        .filter(
            F.col("cur.cur_cdc_payload_hash").isNull()
            |
            (F.col("src.cdc_payload_hash") != F.col("cur.cur_cdc_payload_hash"))
        )
        .select("src.*")
    )

    records_written = df_changes.count()

    if records_written > 0:

        df_changes_keys = (
            df_changes
            .select(
                "prop_id_proposicao",
                "tram_id_evento",
                "valid_from"
            )
            .dropDuplicates()
        )

        df_changes_keys.createOrReplaceTempView("tmp_proposicoes_tramitacoes_scd2_changes")

        spark.sql(f"""
            MERGE INTO {TARGET_TABLE} AS target
            USING tmp_proposicoes_tramitacoes_scd2_changes AS source
            ON target.prop_id_proposicao = source.prop_id_proposicao
               AND target.tram_id_evento = source.tram_id_evento
               AND target.is_current = true
            WHEN MATCHED THEN
              UPDATE SET
                target.valid_to = source.valid_from,
                target.is_current = false,
                target.scd_ts_processamento = current_timestamp()
        """)

        records_closed = df_changes_keys.count()

        (
            df_changes
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(TARGET_TABLE)
        )

    else:
        records_closed = 0

# COMMAND ----------

current_duplicates = (
    spark.table(TARGET_TABLE)
    .filter(F.col("is_current") == True)
    .groupBy("prop_id_proposicao", "tram_id_evento")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if current_duplicates > 0:
    raise Exception(
        f"SCD2 validation failed: multiple current records found = {current_duplicates}"
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
        f"| records_closed={records_closed}"
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
print(f"Records closed: {records_closed}")