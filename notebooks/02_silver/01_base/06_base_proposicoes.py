# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Base Layer — Legislative Propositions Standardization
# MAGIC
# MAGIC **Notebook:** `06_base_proposicoes`
# MAGIC
# MAGIC Parses, structures, types, deduplicates and validates legislative proposition
# MAGIC data from the Bronze layer.
# MAGIC
# MAGIC This notebook transforms raw proposition payloads from `bronze.proposicoes`
# MAGIC into a structured Silver Base table. The resulting dataset centralizes
# MAGIC proposition metadata, legislative status, proposition lifecycle and
# MAGIC parliamentary processing information required for downstream analytical
# MAGIC layers and dimensional modeling.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Parse raw CSV-like payloads embedded in JSON structures
# MAGIC - Apply schema standardization
# MAGIC - Cast identifiers, dates and timestamps
# MAGIC - Preserve proposition lifecycle and status relationships
# MAGIC - Preserve legislative organization references
# MAGIC - Preserve lineage and traceability columns
# MAGIC - Apply technical deduplication
# MAGIC - Persist Silver Base Delta table
# MAGIC - Validate technical date quality
# MAGIC - Validate proposition lifecycle consistency
# MAGIC
# MAGIC **Source of truth:** `bronze.proposicoes`  
# MAGIC
# MAGIC **Target:** `silver_base.proposicoes`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake format
# MAGIC - Partitioned by proposition year
# MAGIC - Source for proposition analytics and legislative tracking

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
    initcap,
    count,
    when,
    lit,
)

from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.proposicoes"
TARGET_TABLE = "silver_base.proposicoes"

PIPELINE_NAME = "silver_base_proposicoes"
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
        from_json(col("raw_payload"), MapType(StringType(), StringType()))
    )
)

# COMMAND ----------

from pyspark.sql.functions import year

status_ts_data_hora = (
    col("payload_map.ultimoStatus_dataHora")
    .try_cast("timestamp")
)

prop_ts_apresentacao = (
    col("payload_map.dataApresentacao")
    .try_cast("timestamp")
)

df_standardized = (
    df_map
    .select(
        col("payload_map.id")
            .try_cast("long")
            .alias("prop_id_proposicao"),

        trim(col("payload_map.uri"))
            .alias("prop_tx_uri"),

        upper(trim(col("payload_map.siglaTipo")))
            .alias("prop_sg_tipo"),

        col("payload_map.numero")
            .try_cast("long")
            .alias("prop_nr_numero"),

        # ---------------------------------------------------
        # Official proposition year from Câmara API
        # ---------------------------------------------------

        col("payload_map.ano")
            .try_cast("int")
            .alias("prop_nr_ano"),

        # ---------------------------------------------------
        # Technical/analytical year derived from
        # presentation timestamp
        # ---------------------------------------------------

        year(prop_ts_apresentacao)
            .alias("prop_nr_ano_apresentacao"),

        col("payload_map.codTipo")
            .try_cast("int")
            .alias("prop_cd_tipo"),

        initcap(trim(col("payload_map.descricaoTipo")))
            .alias("prop_tx_descricao_tipo"),

        initcap(trim(col("payload_map.ementa")))
            .alias("prop_tx_ementa"),

        initcap(trim(col("payload_map.ementaDetalhada")))
            .alias("prop_tx_ementa_detalhada"),

        trim(col("payload_map.keywords"))
            .alias("prop_tx_keywords"),

        prop_ts_apresentacao
            .alias("prop_ts_apresentacao"),

        when(
            prop_ts_apresentacao.isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("prop_fl_data_apresentacao_valida"),

        trim(col("payload_map.uriOrgaoNumerador"))
            .alias("org_tx_uri_numerador"),

        trim(col("payload_map.uriPropAnterior"))
            .alias("prop_tx_uri_anterior"),

        trim(col("payload_map.uriPropPrincipal"))
            .alias("prop_tx_uri_principal"),

        trim(col("payload_map.uriPropPosterior"))
            .alias("prop_tx_uri_posterior"),

        trim(col("payload_map.urlInteiroTeor"))
            .alias("prop_tx_url_inteiro_teor"),

        trim(col("payload_map.urnFinal"))
            .alias("prop_tx_urn_final"),

        status_ts_data_hora
            .alias("status_ts_data_hora"),

        when(
            status_ts_data_hora.isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("status_fl_data_hora_valida"),

        # ---------------------------------------------------
        # Compare only dates to avoid false negatives caused
        # by different timestamp granularities
        # ---------------------------------------------------

        when(
            status_ts_data_hora.isNull()
            |
            (
                status_ts_data_hora.cast("date")
                >=
                prop_ts_apresentacao.cast("date")
            ),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("status_fl_periodo_valido"),

        col("payload_map.ultimoStatus_sequencia")
            .try_cast("int")
            .alias("status_nr_sequencia"),

        trim(col("payload_map.ultimoStatus_uriRelator"))
            .alias("status_tx_uri_relator"),

        col("payload_map.ultimoStatus_idOrgao")
            .try_cast("long")
            .alias("status_id_orgao"),

        upper(trim(col("payload_map.ultimoStatus_siglaOrgao")))
            .alias("status_sg_orgao"),

        trim(col("payload_map.ultimoStatus_uriOrgao"))
            .alias("status_tx_uri_orgao"),

        initcap(trim(col("payload_map.ultimoStatus_regime")))
            .alias("status_tx_regime"),

        initcap(trim(col("payload_map.ultimoStatus_descricaoTramitacao")))
            .alias("status_tx_descricao_tramitacao"),

        col("payload_map.ultimoStatus_idTipoTramitacao")
            .try_cast("int")
            .alias("status_id_tipo_tramitacao"),

        initcap(trim(col("payload_map.ultimoStatus_descricaoSituacao")))
            .alias("status_tx_descricao_situacao"),

        col("payload_map.ultimoStatus_idSituacao")
            .try_cast("int")
            .alias("status_id_situacao"),

        trim(col("payload_map.ultimoStatus_despacho"))
            .alias("status_tx_despacho"),

        initcap(trim(col("payload_map.ultimoStatus_apreciacao")))
            .alias("status_tx_apreciacao"),

        trim(col("payload_map.ultimoStatus_url"))
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

        col("source_id")
            .alias("bronze_id_origem"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_tx_source_file"),

        col("payload_map")
            .getItem("ano_referencia")
            .cast("int")
            .alias("bronze_nr_ano_referencia"),

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
    df_standardized
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

df_discarded = (
    df_dedup
    .filter(
        col("prop_id_proposicao").isNull()
        |
        col("prop_nr_ano_apresentacao").isNull()
        |
        (col("prop_fl_data_apresentacao_valida") != 1)
        |
        (col("status_fl_periodo_valido") != 1)
    )
    .withColumn(
        "rejection_reason",
        when(
            col("prop_id_proposicao").isNull(),
            lit("prop_id_proposicao_is_null")
        )
        .when(
            col("prop_nr_ano_apresentacao").isNull(),
            lit("prop_nr_ano_apresentacao_is_null")
        )
        .when(
            col("prop_fl_data_apresentacao_valida") != 1,
            lit("prop_ts_apresentacao_invalid")
        )
        .when(
            col("status_fl_periodo_valido") != 1,
            lit("status_period_invalid")
        )
        .otherwise(lit("unknown"))
    )
)

df_valid = (
    df_dedup
    .filter(col("prop_id_proposicao").isNotNull())
    .filter(col("prop_nr_ano_apresentacao").isNotNull())
    .filter(col("prop_fl_data_apresentacao_valida") == 1)
    .filter(col("status_fl_periodo_valido") == 1)
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
    .partitionBy("prop_nr_ano_apresentacao")
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