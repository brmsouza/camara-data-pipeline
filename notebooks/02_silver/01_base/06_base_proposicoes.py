# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 06_base_proposicoes
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates legislative proposition
# data from the Bronze layer.
#
# Context:
# This notebook transforms raw proposition payloads from bronze.proposicoes
# into a structured Silver Base table. The resulting dataset centralizes
# proposition metadata, legislative status, proposition lifecycle and
# parliamentary processing information required for downstream analytical
# layers and dimensional modeling.
#
# Responsibilities:
# - Parse raw CSV-like payload embedded in JSON structure
# - Apply schema standardization
# - Cast identifiers, dates and timestamps
# - Preserve proposition lifecycle and status relationships
# - Preserve legislative organization references
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Delta table for curated consumption
#
# Source:
# bronze.proposicoes
#
# Target:
# silver_base.proposicoes
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Partitioned by proposition year
# - Source for proposition analytics and legislative tracking
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
    from_csv,
    regexp_replace,
    expr,
)

from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.proposicoes"
TARGET_TABLE = "silver_base.proposicoes"

PIPELINE_NAME = "silver_base_proposicoes"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer="silver",
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

#print(f"Records read from Bronze: {records_read}")

# COMMAND ----------

df_map = (
    df_bronze
    .withColumn(
        "payload_map",
        from_json(col("raw_payload"), MapType(StringType(), StringType()))
    )
    .withColumn(
        "payload_data",
        expr("""
            element_at(
                map_values(
                    map_filter(
                        payload_map,
                        (k, v) -> k NOT IN ('_source_file', 'ano_referencia')
                    )
                ),
                1
            )
        """)
    )
)

# COMMAND ----------

proposicoes_csv_schema = """
id STRING,
uri STRING,
siglaTipo STRING,
numero STRING,
ano STRING,
codTipo STRING,
descricaoTipo STRING,
ementa STRING,
ementaDetalhada STRING,
keywords STRING,
dataApresentacao STRING,
uriOrgaoNumerador STRING,
uriPropAnterior STRING,
uriPropPrincipal STRING,
uriPropPosterior STRING,
urlInteiroTeor STRING,
urnFinal STRING,
ultimoStatus_dataHora STRING,
ultimoStatus_sequencia STRING,
ultimoStatus_uriRelator STRING,
ultimoStatus_idOrgao STRING,
ultimoStatus_siglaOrgao STRING,
ultimoStatus_uriOrgao STRING,
ultimoStatus_regime STRING,
ultimoStatus_descricaoTramitacao STRING,
ultimoStatus_idTipoTramitacao STRING,
ultimoStatus_descricaoSituacao STRING,
ultimoStatus_idSituacao STRING,
ultimoStatus_despacho STRING,
ultimoStatus_apreciacao STRING,
ultimoStatus_url STRING
"""

df_parsed = (
    df_map
    .withColumn(
        "csv_data",
        from_csv(
            col("payload_data"),
            proposicoes_csv_schema,
            {
                "sep": ";",
                "quote": '"',
                "escape": '"',
                "header": "false"
            }
        )
    )
)

# COMMAND ----------

df = (
    df_parsed
    .select(
        col("csv_data.id").try_cast("long").alias("prop_id_proposicao"),

        trim(col("csv_data.uri")).alias("prop_tx_uri"),

        upper(trim(col("csv_data.siglaTipo"))).alias("prop_sg_tipo"),

        col("csv_data.numero").try_cast("long").alias("prop_nr_numero"),

        col("csv_data.ano").try_cast("int").alias("prop_nr_ano"),

        col("csv_data.codTipo").try_cast("int").alias("prop_cd_tipo"),

        trim(col("csv_data.descricaoTipo")).alias("prop_tx_descricao_tipo"),

        trim(col("csv_data.ementa")).alias("prop_tx_ementa"),

        trim(col("csv_data.ementaDetalhada")).alias("prop_tx_ementa_detalhada"),

        trim(col("csv_data.keywords")).alias("prop_tx_keywords"),

        col("csv_data.dataApresentacao")
            .try_cast("timestamp")
            .alias("prop_ts_apresentacao"),

        trim(col("csv_data.uriOrgaoNumerador"))
            .alias("org_tx_uri_numerador"),

        trim(col("csv_data.uriPropAnterior"))
            .alias("prop_tx_uri_anterior"),

        trim(col("csv_data.uriPropPrincipal"))
            .alias("prop_tx_uri_principal"),

        trim(col("csv_data.uriPropPosterior"))
            .alias("prop_tx_uri_posterior"),

        trim(col("csv_data.urlInteiroTeor"))
            .alias("prop_tx_url_inteiro_teor"),

        trim(col("csv_data.urnFinal"))
            .alias("prop_tx_urn_final"),

        col("csv_data.ultimoStatus_dataHora")
            .try_cast("timestamp")
            .alias("status_ts_data_hora"),

        col("csv_data.ultimoStatus_sequencia")
            .try_cast("int")
            .alias("status_nr_sequencia"),

        trim(col("csv_data.ultimoStatus_uriRelator"))
            .alias("status_tx_uri_relator"),

        col("csv_data.ultimoStatus_idOrgao")
            .try_cast("long")
            .alias("status_id_orgao"),

        upper(trim(col("csv_data.ultimoStatus_siglaOrgao")))
            .alias("status_sg_orgao"),

        trim(col("csv_data.ultimoStatus_uriOrgao"))
            .alias("status_tx_uri_orgao"),

        trim(col("csv_data.ultimoStatus_regime"))
            .alias("status_tx_regime"),

        trim(col("csv_data.ultimoStatus_descricaoTramitacao"))
            .alias("status_tx_descricao_tramitacao"),

        col("csv_data.ultimoStatus_idTipoTramitacao")
            .try_cast("int")
            .alias("status_id_tipo_tramitacao"),

        trim(col("csv_data.ultimoStatus_descricaoSituacao"))
            .alias("status_tx_descricao_situacao"),

        col("csv_data.ultimoStatus_idSituacao")
            .try_cast("int")
            .alias("status_id_situacao"),

        trim(col("csv_data.ultimoStatus_despacho"))
            .alias("status_tx_despacho"),

        trim(col("csv_data.ultimoStatus_apreciacao"))
            .alias("status_tx_apreciacao"),

        trim(col("csv_data.ultimoStatus_url"))
            .alias("status_tx_url"),

        # ---------------------------------------------------
        # Bronze lineage / traceability
        # ---------------------------------------------------

        col("ingestion_timestamp")
            .alias("bronze_ts_ingestao"),

        col("ingestion_date")
            .alias("bronze_dt_ingestao"),

        col("source_endpoint")
            .alias("bronze_tx_endpoint"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_id_origem"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_tx_source_file"),

        col("batch_id")
            .alias("bronze_id_batch"),

        col("record_hash")
            .alias("bronze_tx_record_hash"),

        # ---------------------------------------------------
        # Silver metadata
        # ---------------------------------------------------

        current_timestamp()
            .alias("silver_ts_processamento")
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("prop_id_proposicao")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_valid = (
    df_dedup
    .filter(col("prop_id_proposicao").isNotNull())
    .filter(col("prop_nr_ano").isNotNull())
)

records_written = df_valid.count()

#print(f"Records valid for Silver Base: {records_written}")

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("prop_nr_ano")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

#display(spark.table(TARGET_TABLE).limit(50))

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer="silver",
    level="INFO",
    event_name="job_finished",
    message=f"source={PIPELINE_NAME} | finished successfully | records_read={records_read} | records_written={records_written}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
    finished_at=datetime.now(),
)

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")