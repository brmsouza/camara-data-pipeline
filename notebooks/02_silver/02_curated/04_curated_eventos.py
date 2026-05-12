# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 04_curated_eventos
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Consolidates, enriches and validates legislative event data
# from Silver Base.
#
# Context:
# This notebook transforms silver_base.eventos into a curated and
# analytics-ready event dataset. The resulting table centralizes Câmara event
# information such as sessions, meetings, public hearings and committee events,
# enabling downstream analysis of parliamentary presence, legislative agenda,
# voting context and institutional activity.
#
# Responsibilities:
# - Consolidate standardized event attributes from Silver Base
# - Curate event type, situation and location indicators
# - Create analytical event flags
# - Extract primary organization information from event organization array
# - Preserve event temporal attributes and technical validation flags
# - Preserve lineage and traceability columns
# - Validate curated-level uniqueness
# - Persist Delta table for Gold consumption
#
# Source:
# silver_base.eventos
#
# Target:
# silver_curated.eventos
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by event reference year
# - Ready for presence, agenda and voting-context Gold modeling
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
    year,
    month,
    to_date,
    size,
    element_at,
    concat_ws,
    transform,
)


# COMMAND ----------

SOURCE_TABLE = "silver_base.eventos"
TARGET_TABLE = "silver_curated.eventos"

PIPELINE_NAME = "silver_curated_eventos"
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
        # Event identity
        # ---------------------------------------------------

        col("evt_id_evento")
            .alias("evt_id_evento"),

        col("evt_tx_uri")
            .alias("evt_tx_uri"),

        col("evt_nr_ano_referencia")
            .alias("evt_nr_ano_referencia"),

        # ---------------------------------------------------
        # Event temporal attributes
        # ---------------------------------------------------

        col("evt_ts_inicio")
            .alias("evt_ts_inicio"),

        col("evt_ts_fim")
            .alias("evt_ts_fim"),

        to_date(col("evt_ts_inicio"))
            .alias("evt_dt_inicio"),

        to_date(col("evt_ts_fim"))
            .alias("evt_dt_fim"),

        year(col("evt_ts_inicio"))
            .alias("evt_nr_ano_inicio"),

        month(col("evt_ts_inicio"))
            .alias("evt_nr_mes_inicio"),

        col("evt_dt_inicio_janela")
            .alias("evt_dt_inicio_janela"),

        col("evt_dt_fim_janela")
            .alias("evt_dt_fim_janela"),

        col("evt_fl_inicio_valido")
            .alias("evt_fl_inicio_valido"),

        col("evt_fl_fim_valido")
            .alias("evt_fl_fim_valido"),

        col("evt_fl_periodo_valido")
            .alias("evt_fl_periodo_valido"),

        # ---------------------------------------------------
        # Event description and classification
        # ---------------------------------------------------

        col("evt_tx_descricao")
            .alias("evt_tx_descricao"),

        col("evt_tx_tipo")
            .alias("evt_tx_tipo"),

        col("evt_tx_situacao")
            .alias("evt_tx_situacao"),

        when(
            upper(col("evt_tx_tipo")).contains("SESS"),
            lit("Sessão")
        )
        .when(
            upper(col("evt_tx_tipo")).contains("AUDI"),
            lit("Audiência Pública")
        )
        .when(
            upper(col("evt_tx_tipo")).contains("REUNI"),
            lit("Reunião")
        )
        .when(
            upper(col("evt_tx_tipo")).contains("SEMIN"),
            lit("Seminário")
        )
        .otherwise(col("evt_tx_tipo"))
        .alias("evt_tx_tipo_curado"),

        when(
            upper(col("evt_tx_situacao")).contains("ENCERR"),
            lit("Encerrado")
        )
        .when(
            upper(col("evt_tx_situacao")).contains("CANCEL"),
            lit("Cancelado")
        )
        .when(
            upper(col("evt_tx_situacao")).contains("CONVOC"),
            lit("Convocado")
        )
        .otherwise(col("evt_tx_situacao"))
        .alias("evt_tx_situacao_curada"),

        # ---------------------------------------------------
        # Event analytical flags
        # ---------------------------------------------------

        when(upper(col("evt_tx_tipo")).contains("SESS"), lit(1))
            .otherwise(lit(0))
            .alias("evt_fl_sessao"),

        when(upper(col("evt_tx_tipo")).contains("AUDI"), lit(1))
            .otherwise(lit(0))
            .alias("evt_fl_audiencia_publica"),

        when(upper(col("evt_tx_tipo")).contains("REUNI"), lit(1))
            .otherwise(lit(0))
            .alias("evt_fl_reuniao"),

        when(upper(col("evt_tx_situacao")).contains("ENCERR"), lit(1))
            .otherwise(lit(0))
            .alias("evt_fl_encerrado"),

        when(upper(col("evt_tx_situacao")).contains("CANCEL"), lit(1))
            .otherwise(lit(0))
            .alias("evt_fl_cancelado"),

        when(col("evt_tx_url_registro").isNotNull(), lit(1))
            .otherwise(lit(0))
            .alias("evt_fl_possui_registro"),

        # ---------------------------------------------------
        # Location
        # ---------------------------------------------------

        col("evt_tx_local_camara")
            .alias("evt_tx_local_interno"),

        col("evt_tx_predio")
            .alias("evt_tx_predio"),

        col("evt_tx_sala")
            .alias("evt_tx_sala"),

        col("evt_tx_andar")
            .alias("evt_tx_andar"),

        col("evt_tx_local_externo")
            .alias("evt_tx_local_externo"),

        when(col("evt_tx_local_externo").isNotNull(), lit("Externo"))
            .otherwise(lit("Câmara"))
            .alias("evt_tx_tipo_local"),

        col("evt_tx_url_registro")
            .alias("evt_tx_url_registro"),

        # ---------------------------------------------------
        # Organization relationship
        # ---------------------------------------------------

        size(col("evt_arr_orgaos"))
            .alias("evt_qt_orgaos"),

        element_at(col("evt_arr_orgaos"), 1).getField("id")
            .alias("org_id_orgao_principal"),

        element_at(col("evt_arr_orgaos"), 1).getField("sigla")
            .alias("org_sg_orgao_principal"),

        element_at(col("evt_arr_orgaos"), 1).getField("nome")
            .alias("org_tx_nome_principal"),

        element_at(col("evt_arr_orgaos"), 1).getField("tipoOrgao")
            .alias("org_tx_tipo_principal"),

        concat_ws(
            " | ",
            transform(
                col("evt_arr_orgaos"),
                lambda x: x.getField("sigla")
            )
        ).alias("org_tx_siglas_relacionadas"),

        # ---------------------------------------------------
        # Lineage
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

        col("silver_ts_processamento")
            .alias("silver_base_ts_processamento"),

        current_timestamp()
            .alias("silver_curated_ts_processamento")
    )
)

# COMMAND ----------

duplicated_ids = (
    df_curated
    .groupBy("evt_id_evento")
    .count()
    .filter(col("count") > 1)
    .count()
)

if duplicated_ids > 0:
    raise Exception(
        f"Data quality error: {duplicated_ids} duplicated event IDs in curated layer."
    )

df_dedup = df_curated

# COMMAND ----------

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("evt_id_evento").isNull()
    )
    .withColumn(
        "rejection_reason",
        lit("evt_id_evento_is_null")
    )
)

# ---------------------------------------------------
# Valid records
# ---------------------------------------------------

df_valid = (
    df_dedup
    .filter(col("evt_id_evento").isNotNull())
)

# ---------------------------------------------------
# Metrics
# ---------------------------------------------------

records_written = df_valid.count()

records_discarded = df_discarded.count()

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("evt_id_evento").isNotNull())
)

records_written = df_valid.count()

records_discarded = records_read - records_written

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
    .partitionBy("evt_nr_ano_referencia")
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