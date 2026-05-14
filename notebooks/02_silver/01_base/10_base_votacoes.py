# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Base Layer — Voting Sessions Standardization
# MAGIC
# MAGIC **Notebook:** `10_base_votacoes`
# MAGIC
# MAGIC Parses, structures, types, deduplicates and validates voting session data
# MAGIC from the Bronze layer.
# MAGIC
# MAGIC This notebook transforms raw voting payloads from `bronze.votacoes` into a
# MAGIC structured Silver Base table. The resulting dataset supports downstream voting
# MAGIC fact construction, political alignment analytics and correlation analysis
# MAGIC between parliamentary fronts, propositions and voting behavior.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Parse raw JSON payloads
# MAGIC - Apply schema standardization
# MAGIC - Cast dates, timestamps and voting counts
# MAGIC - Preserve voting-event and proposition relationships
# MAGIC - Preserve lineage and traceability columns
# MAGIC - Apply technical deduplication
# MAGIC - Persist Silver Base Delta table
# MAGIC - Validate technical date quality
# MAGIC - Validate voting period consistency
# MAGIC
# MAGIC **Source of truth:** `bronze.votacoes`  
# MAGIC
# MAGIC **Target:** `silver_base.votacoes`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake format
# MAGIC - Source for voting analytics and political alignment analysis

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    row_number,
    get_json_object,
    upper,
    initcap,
    count,
    when,
)

from pyspark.sql.window import Window


# COMMAND ----------

SOURCE_TABLE = "bronze.votacoes"
TARGET_TABLE = "silver_base.votacoes"

PIPELINE_NAME = "silver_base_votacoes"
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

df_bronze = (
    spark.table(SOURCE_TABLE)
    .filter(col("source_id").rlike("^[0-9]+-[0-9]+$"))
    .filter(col("raw_payload").startswith("{"))
)

records_read = df_bronze.count()


# COMMAND ----------

vot_dt_votacao_expr = (
    get_json_object(col("raw_payload"), "$.data")
    .try_cast("date")
)

vot_ts_registro_expr = (
    get_json_object(col("raw_payload"), "$.dataHoraRegistro")
    .try_cast("timestamp")
)

df_standardized = (
    df_bronze
    .select(
        col("source_id").alias("vot_id_votacao"),

        get_json_object(col("raw_payload"), "$.uri")
            .alias("vot_tx_uri"),

        vot_dt_votacao_expr
            .alias("vot_dt_votacao"),

        when(
            vot_dt_votacao_expr.isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_data_valida"),

        vot_ts_registro_expr
            .alias("vot_ts_registro"),

        when(
            vot_ts_registro_expr.isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_timestamp_registro_valido"),

        when(
            vot_ts_registro_expr.isNull()
            |
            (
                vot_ts_registro_expr.cast("date")
                >= vot_dt_votacao_expr
            ),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_periodo_valido"),

        initcap(get_json_object(col("raw_payload"), "$.descricao"))
            .alias("vot_tx_descricao"),

        get_json_object(col("raw_payload"), "$.aprovacao")
            .try_cast("int")
            .alias("vot_fl_aprovacao"),

        get_json_object(col("raw_payload"), "$.ano_referencia")
            .try_cast("int")
            .alias("vot_nr_ano_referencia"),

        get_json_object(col("raw_payload"), "$.idEvento")
            .try_cast("long")
            .alias("evt_id_evento"),

        get_json_object(col("raw_payload"), "$.uriEvento")
            .alias("evt_tx_uri"),

        get_json_object(col("raw_payload"), "$.idOrgao")
            .try_cast("long")
            .alias("org_id_orgao"),

        upper(get_json_object(col("raw_payload"), "$.siglaOrgao"))
            .alias("org_sg_orgao"),

        get_json_object(col("raw_payload"), "$.uriOrgao")
            .alias("org_tx_uri"),

        get_json_object(col("raw_payload"), "$.votosSim")
            .try_cast("int")
            .alias("vot_qt_sim"),

        get_json_object(col("raw_payload"), "$.votosNao")
            .try_cast("int")
            .alias("vot_qt_nao"),

        get_json_object(col("raw_payload"), "$.votosOutros")
            .try_cast("int")
            .alias("vot_qt_outros"),

        get_json_object(
            col("raw_payload"),
            "$.ultimaApresentacaoProposicao_idProposicao"
        )
        .try_cast("long")
        .alias("prop_id_proposicao"),

        get_json_object(
            col("raw_payload"),
            "$.ultimaApresentacaoProposicao_uriProposicao"
        )
        .alias("prop_tx_uri"),

        initcap(
            get_json_object(
                col("raw_payload"),
                "$.ultimaApresentacaoProposicao_descricao"
            )
        )
        .alias("prop_tx_descricao"),

        col("source_endpoint").alias("bronze_tx_endpoint"),
        col("source_id").alias("bronze_id_origem"),
        col("batch_id").alias("bronze_id_batch"),
        col("record_hash").alias("bronze_tx_record_hash"),
        col("ingestion_timestamp").alias("bronze_ts_ingestao"),
        col("ingestion_date").alias("bronze_dt_ingestao"),

        current_timestamp().alias("silver_ts_processamento")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("vot_id_votacao")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df_standardized
    .withColumn(
        "row_num",
        row_number().over(window_spec)
    )
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

invalid_null_id = (
    df_dedup
    .filter(col("vot_id_votacao").isNull())
    .count()
)

duplicated_ids = (
    df_dedup
    .groupBy("vot_id_votacao")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if duplicated_ids > 0:
    raise Exception(
        f"Data quality error: {duplicated_ids} duplicated voting IDs."
    )

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("vot_id_votacao").isNull()
        |
        (~col("vot_id_votacao").rlike("^[0-9]+-[0-9]+$"))
        |
        (col("vot_fl_data_valida") != 1)
        |
        (col("vot_fl_periodo_valido") != 1)
    )
    .withColumn(
        "rejection_reason",
        when(
            col("vot_id_votacao").isNull(),
            lit("vot_id_votacao_is_null")
        )
        .when(
            ~col("vot_id_votacao").rlike("^[0-9]+-[0-9]+$"),
            lit("vot_id_votacao_invalid_format")
        )
        .when(
            col("vot_fl_data_valida") != 1,
            lit("vot_dt_votacao_invalid")
        )
        .when(
            col("vot_fl_periodo_valido") != 1,
            lit("vot_period_invalid")
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
    .filter(col("vot_id_votacao").rlike("^[0-9]+-[0-9]+$"))
    .filter(col("vot_fl_data_valida") == 1)
    .filter(col("vot_fl_periodo_valido") == 1)
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()


records_deduplicated = records_read - df_dedup.count()

print(f"Records deduplicated: {records_deduplicated}")


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
    .partitionBy("vot_nr_ano_referencia")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

rejection_summary = (
    df_discarded
    .groupBy("rejection_reason")
    .count()
    .orderBy(col("count").desc())
)

rejection_summary.show(truncate=False)

rejection_message = ", ".join([
    f"{row['rejection_reason']}={row['count']}"
    for row in rejection_summary.collect()
])

if rejection_message == "":
    rejection_message = "no_rejections"

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