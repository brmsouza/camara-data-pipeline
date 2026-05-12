# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 05_base_votacoes
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates voting session data
# from the Bronze layer.
#
# Context:
# This notebook transforms raw voting payloads from bronze.votacoes into a
# structured Silver Base table. The resulting dataset supports future voting
# facts, political alignment analytics and correlation between parliamentary
# fronts and votes.
#
# Responsibilities:
# - Parse raw JSON payload
# - Apply schema standardization
# - Cast dates, timestamps and vote counts
# - Preserve voting-event and proposition relationships
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Silver Base Delta table
# - Validate technical date quality
# - Validate voting period consistency
#
# Source:
# bronze.votacoes
#
# Target:
# silver_base.votacoes
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Source for voting analytics and alignment analysis
# ------------------------------------------------------------------------------

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

df_standardized = (
    df_bronze
    .select(
        # ---------------------------------------------------
        # Voting identifiers
        # ---------------------------------------------------

        col("source_id")
            .alias("vot_id_votacao"),

        get_json_object(col("raw_payload"), "$.uri")
            .alias("vot_tx_uri"),

        # ---------------------------------------------------
        # Voting dates / timestamps
        # ---------------------------------------------------

        get_json_object(col("raw_payload"), "$.data")
            .try_cast("date")
            .alias("vot_dt_votacao"),

        when(
            get_json_object(col("raw_payload"), "$.data")
                .try_cast("date")
                .isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_data_valida"),     
        get_json_object(col("raw_payload"), "$.dataHoraRegistro")
            .try_cast("timestamp")
            .alias("vot_ts_registro"),
       
        when(
            get_json_object(col("raw_payload"), "$.dataHoraRegistro")
                .try_cast("timestamp")
                .isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_timestamp_registro_valido"),

        when(
            (
                get_json_object(col("raw_payload"), "$.dataHoraRegistro")
                    .try_cast("timestamp")
                    .cast("date")
                >=
                get_json_object(col("raw_payload"), "$.data")
                    .try_cast("date")
            ),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("vot_fl_periodo_valido"),

        # ---------------------------------------------------
        # Voting attributes
        # ---------------------------------------------------

        initcap(get_json_object(col("raw_payload"), "$.descricao"))
            .alias("vot_tx_descricao"),

        get_json_object(col("raw_payload"), "$.aprovacao")
            .try_cast("int")
            .alias("vot_fl_aprovacao"),

        get_json_object(col("raw_payload"), "$.ano_referencia")
            .try_cast("int")
            .alias("vot_nr_ano_referencia"),

        # ---------------------------------------------------
        # Event relationship
        # ---------------------------------------------------

        get_json_object(col("raw_payload"), "$.idEvento")
            .try_cast("long")
            .alias("evt_id_evento"),

        get_json_object(col("raw_payload"), "$.uriEvento")
            .alias("evt_tx_uri"),

        # ---------------------------------------------------
        # Organization relationship
        # ---------------------------------------------------

        get_json_object(col("raw_payload"), "$.idOrgao")
            .try_cast("long")
            .alias("org_id_orgao"),

        upper(get_json_object(col("raw_payload"), "$.siglaOrgao"))
            .alias("org_sg_orgao"),

        get_json_object(col("raw_payload"), "$.uriOrgao")
            .alias("org_tx_uri"),

        # ---------------------------------------------------
        # Voting results
        # ---------------------------------------------------

        get_json_object(col("raw_payload"), "$.votosSim")
            .try_cast("int")
            .alias("vot_qt_sim"),

        get_json_object(col("raw_payload"), "$.votosNao")
            .try_cast("int")
            .alias("vot_qt_nao"),

        get_json_object(col("raw_payload"), "$.votosOutros")
            .try_cast("int")
            .alias("vot_qt_outros"),

        # ---------------------------------------------------
        # Proposition relationship
        # ---------------------------------------------------

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

        initcap(get_json_object(
            col("raw_payload"),
            "$.ultimaApresentacaoProposicao_descricao"
        ))
        .alias("prop_tx_descricao"),

        # ---------------------------------------------------
        # Bronze lineage / traceability
        # ---------------------------------------------------

        col("source_endpoint")
            .alias("bronze_tx_endpoint"),

        col("source_id")
            .alias("bronze_id_origem"),

        col("batch_id")
            .alias("bronze_id_batch"),

        col("record_hash")
            .alias("bronze_tx_record_hash"),

        col("ingestion_timestamp")
            .alias("bronze_ts_ingestao"),

        col("ingestion_date")
            .alias("bronze_dt_ingestao"),

        # ---------------------------------------------------
        # Silver metadata
        # ---------------------------------------------------

        current_timestamp()
            .alias("silver_ts_processamento"),

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

df_valid = (
    df_dedup
    .filter(col("vot_id_votacao").isNotNull())
    .filter(col("vot_id_votacao").rlike("^[0-9]+-[0-9]+$"))
    .filter(col("vot_fl_data_valida") == 1)
)

records_written = df_valid.count()

records_discarded = records_read - records_written

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