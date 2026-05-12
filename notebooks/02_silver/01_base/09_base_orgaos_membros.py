# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 10_base_orgaos_membros
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates legislative organization
# membership data from the Bronze layer.
#
# Context:
# This notebook transforms raw organization membership payloads from
# bronze.orgaos_membros into a structured Silver Base table. The resulting
# dataset represents the relationship between deputies and legislative bodies,
# supporting committee participation, institutional role analysis and future
# dimensional modeling.
#
# Responsibilities:
# - Parse raw CSV-like payload embedded in JSON structure
# - Apply schema standardization
# - Cast dates
# - Preserve organization and deputy relationships
# - Preserve role and membership period information
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Silver Base Delta table
# - Validate technical date quality
# - Validate membership period consistency
#
# Source:
# bronze.orgaos_membros
#
# Target:
# silver_base.orgaos_membros
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by ingestion date
# - Source for organization membership and committee analytics
# ------------------------------------------------------------------------------

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
    from_json,
    sha2,
    concat_ws,
    regexp_extract,
    initcap,
    count,
    when,
    lit,
)

from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.orgaos_membros"
TARGET_TABLE = "silver_base.orgaos_membros"

PIPELINE_NAME = "silver_base_orgaos_membros"
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

df_map = (
    df_bronze
    .withColumn(
        "payload_map",
        from_json(
            col("raw_payload"),
            MapType(StringType(), StringType())
        )
    )
)

# COMMAND ----------

df_standardized = (
    df_map
    .select(
        regexp_extract(
            trim(col("payload_map.uriOrgao")),
            r"/orgaos/([0-9]+)",
            1
        ).try_cast("long").alias("org_id_orgao"),

        trim(col("payload_map.uriOrgao"))
            .alias("org_tx_uri"),

        upper(trim(col("payload_map.siglaOrgao")))
            .alias("org_sg_orgao"),

        initcap(trim(col("payload_map.nomeOrgao")))
            .alias("org_tx_nome"),

        initcap(trim(col("payload_map.nomePublicacaoOrgao")))
            .alias("org_tx_nome_publicacao"),

        regexp_extract(
            trim(col("payload_map.uriDeputado")),
            r"/deputados/([0-9]+)",
            1
        ).try_cast("long").alias("dept_id_deputado"),

        trim(col("payload_map.uriDeputado"))
            .alias("dept_tx_uri"),

        initcap(trim(col("payload_map.nomeDeputado")))
            .alias("dept_tx_nome"),

        upper(trim(col("payload_map.siglaPartido")))
            .alias("part_sg_partido"),

        upper(trim(col("payload_map.siglaUF")))
            .alias("uf_sg_uf"),

        initcap(trim(col("payload_map.cargo")))
            .alias("memb_tx_cargo"),

        col("payload_map.dataInicio")
            .try_cast("date")
            .alias("memb_dt_inicio"),

        when(
            col("payload_map.dataInicio")
                .try_cast("date")
                .isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_data_inicio_valida"),

        col("payload_map.dataFim")
            .try_cast("date")
            .alias("memb_dt_fim"),

        when(
            col("payload_map.dataFim").isNull()
            |
            col("payload_map.dataFim")
                .try_cast("date")
                .isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_data_fim_valida"),

        when(
            (
                col("payload_map.dataFim").isNull()
            )
            |
            (
                col("payload_map.dataFim").try_cast("date")
                >=
                col("payload_map.dataInicio").try_cast("date")
            ),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("memb_fl_periodo_valido"),

        sha2(
            concat_ws(
                "||",
                trim(col("payload_map.uriOrgao")),
                trim(col("payload_map.uriDeputado")),
                initcap(trim(col("payload_map.cargo"))),
                trim(col("payload_map.dataInicio")),
                trim(col("payload_map.dataFim"))
            ),
            256
        ).alias("memb_tx_dedup_key"),

        col("ingestion_timestamp")
            .alias("bronze_ts_ingestao"),

        col("ingestion_date")
            .alias("bronze_dt_ingestao"),

        col("source_endpoint")
            .alias("bronze_tx_endpoint"),

        col("source_id")
            .alias("bronze_id_origem"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_tx_source_file"),

        col("batch_id")
            .alias("bronze_id_batch"),

        col("record_hash")
            .alias("bronze_tx_record_hash"),

        current_timestamp()
            .alias("silver_ts_processamento")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("memb_tx_dedup_key")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df_standardized
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

invalid_null_orgao = (
    df_dedup
    .filter(col("org_id_orgao").isNull())
    .count()
)

invalid_null_deputado = (
    df_dedup
    .filter(col("dept_id_deputado").isNull())
    .count()
)

invalid_data_inicio = (
    df_dedup
    .filter(col("memb_fl_data_inicio_valida") != 1)
    .count()
)

invalid_data_fim = (
    df_dedup
    .filter(col("memb_fl_data_fim_valida") != 1)
    .count()
)

invalid_periodo = (
    df_dedup
    .filter(col("memb_fl_periodo_valido") != 1)
    .count()
)

duplicated_members = (
    df_dedup
    .groupBy("memb_tx_dedup_key")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if duplicated_members > 0:
    raise Exception(
        f"Data quality error: {duplicated_members} duplicated organization members."
    )

# ---------------------------------------------------
# Discarded records
# ---------------------------------------------------

df_discarded = (
    df_dedup
    .filter(
        col("org_id_orgao").isNull()
        |
        col("dept_id_deputado").isNull()
        |
        (col("memb_fl_data_inicio_valida") != 1)
        |
        (col("memb_fl_data_fim_valida") != 1)
        |
        (col("memb_fl_periodo_valido") != 1)
    )
    .withColumn(
        "rejection_reason",
        when(
            col("org_id_orgao").isNull(),
            lit("org_id_orgao_is_null")
        )
        .when(
            col("dept_id_deputado").isNull(),
            lit("dept_id_deputado_is_null")
        )
        .when(
            col("memb_fl_data_inicio_valida") != 1,
            lit("memb_dt_inicio_invalid")
        )
        .when(
            col("memb_fl_data_fim_valida") != 1,
            lit("memb_dt_fim_invalid")
        )
        .when(
            col("memb_fl_periodo_valido") != 1,
            lit("memb_period_invalid")
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
    .filter(col("dept_id_deputado").isNotNull())
    .filter(col("memb_fl_data_inicio_valida") == 1)
    .filter(col("memb_fl_data_fim_valida") == 1)
    .filter(col("memb_fl_periodo_valido") == 1)
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
    .partitionBy("bronze_dt_ingestao")
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