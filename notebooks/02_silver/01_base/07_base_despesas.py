# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 03_base_despesas
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Parses, structures, types, deduplicates and validates CEAP expenses data
# from the Bronze layer.
#
# Context:
# This notebook transforms raw CSV-based payloads stored in bronze.despesas
# into a structured Silver Base table. This dataset will support the future
# construction of gold.ft_despesas_ceap, gold.dm_fornecedor and CEAP analytics.
#
# Responsibilities:
# - Parse raw CSV payload stored as JSON
# - Standardize expense fields
# - Cast dates and monetary values
# - Normalize supplier and CNPJ/CPF fields
# - Preserve lineage and traceability columns
# - Apply technical deduplication
# - Persist Silver Base Delta table
# - Validate technical CPF/CNPJ quality
# - Validate technical date quality
#
# Source:
# bronze.despesas
#
# Target:
# silver_base.despesas
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Critical source for CEAP analytics and anomaly detection
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table silver_base.despesas;

# COMMAND ----------

import uuid
from datetime import datetime

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    current_timestamp,
    row_number,
    count,
    from_json,
    regexp_replace,
    to_date,
    to_timestamp,
    sha2,
    concat_ws,
    initcap,
    length,
    when,
    lit,
    max,
    coalesce,
)

from pyspark.sql.types import MapType, StringType
from pyspark.sql.window import Window

# COMMAND ----------

SOURCE_TABLE = "bronze.despesas"
TARGET_TABLE = "silver_base.despesas"

PIPELINE_NAME = "silver_base_despesas"
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

df_standardized = (
    df_map
    .select(
        initcap(trim(col("payload_map.txNomeParlamentar")))
            .alias("desp_tx_nome_parlamentar"),

        regexp_replace(col("payload_map.cpf"), "[^0-9]", "")
            .alias("dept_nr_cpf"),

        when(
            length(regexp_replace(col("payload_map.cpf"), "[^0-9]", "")) == 11,
            lit(1)
        )
        .otherwise(lit(0))
        .alias("dept_fl_cpf_valido"),

        col("payload_map.nuDeputadoId")
            .cast("long")
            .alias("dept_id_deputado"),

        col("payload_map.ideCadastro")
            .cast("long")
            .alias("dept_id_cadastro"),

        trim(col("payload_map.nuCarteiraParlamentar"))
            .alias("dept_nr_carteira_parlamentar"),

        col("payload_map.nuLegislatura")
            .cast("int")
            .alias("leg_nr_ano_inicio"),

        upper(trim(col("payload_map.sgUF")))
            .alias("uf_sg_uf"),

        upper(trim(col("payload_map.sgPartido")))
            .alias("part_sg_partido"),

        col("payload_map.codLegislatura")
            .cast("int")
            .alias("leg_id_legislatura"),

        col("payload_map.numSubCota")
            .cast("int")
            .alias("desp_cd_subcota"),

        initcap(trim(col("payload_map.txtDescricao")))
            .alias("desp_tx_descricao"),

        col("payload_map.numEspecificacaoSubCota")
            .cast("int")
            .alias("desp_cd_especificacao_subcota"),

        initcap(trim(col("payload_map.txtDescricaoEspecificacao")))
            .alias("desp_tx_descricao_especificacao"),

        initcap(trim(col("payload_map.txtFornecedor")))
            .alias("forn_tx_nome"),

        regexp_replace(col("payload_map.txtCNPJCPF"), "[^0-9]", "")
            .alias("forn_nr_cnpj_cpf"),

        when(
            length(regexp_replace(col("payload_map.txtCNPJCPF"), "[^0-9]", "")) == 11,
            lit("CPF")
        )
        .when(
            length(regexp_replace(col("payload_map.txtCNPJCPF"), "[^0-9]", "")) == 14,
            lit("CNPJ")
        )
        .otherwise(lit("NA"))
        .alias("forn_tx_tipo_documento"),

        when(
            length(regexp_replace(col("payload_map.txtCNPJCPF"), "[^0-9]", "")).isin(11, 14),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("forn_fl_documento_valido"),

        trim(col("payload_map.txtNumero"))
            .alias("desp_nr_documento"),

        col("payload_map.indTipoDocumento")
            .cast("int")
            .alias("desp_cd_tipo_documento"),

        col("payload_map.ideDocumento")
            .cast("long")
            .alias("desp_id_documento"),

        trim(col("payload_map.urlDocumento"))
            .alias("desp_tx_url_documento"),

        to_date(to_timestamp(col("payload_map.datEmissao")))
            .alias("desp_dt_emissao"),

        when(
            to_date(to_timestamp(col("payload_map.datEmissao"))).isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("desp_fl_data_emissao_valida"),

        regexp_replace(col("payload_map.vlrDocumento"), ",", ".")
            .cast("decimal(18,2)")
            .alias("desp_vl_documento"),

        regexp_replace(col("payload_map.vlrGlosa"), ",", ".")
            .cast("decimal(18,2)")
            .alias("desp_vl_glosa"),

        regexp_replace(col("payload_map.vlrLiquido"), ",", ".")
            .cast("decimal(18,2)")
            .alias("desp_vl_liquido"),

        regexp_replace(col("payload_map.vlrRestituicao"), ",", ".")
            .cast("decimal(18,2)")
            .alias("desp_vl_restituicao"),

        col("payload_map.numMes")
            .cast("int")
            .alias("desp_nr_mes"),

        col("payload_map.numAno")
            .cast("int")
            .alias("desp_nr_ano"),

        col("payload_map.numParcela")
            .cast("int")
            .alias("desp_nr_parcela"),

        trim(col("payload_map.txtPassageiro"))
            .alias("desp_tx_passageiro"),

        trim(col("payload_map.txtTrecho"))
            .alias("desp_tx_trecho"),

        col("payload_map.numLote")
            .cast("long")
            .alias("desp_nr_lote"),

        col("payload_map.numRessarcimento")
            .cast("long")
            .alias("desp_nr_ressarcimento"),

        to_date(to_timestamp(col("payload_map.datPagamentoRestituicao")))
            .alias("desp_dt_pagamento_restituicao"),

        when(
            col("payload_map.datPagamentoRestituicao").isNull() |
            to_date(to_timestamp(col("payload_map.datPagamentoRestituicao"))).isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("desp_fl_data_restituicao_valida"),

        col("ingestion_timestamp").alias("bronze_ts_ingestao"),
        col("ingestion_date").alias("bronze_dt_ingestao"),
        col("source_endpoint").alias("bronze_tx_endpoint"),
        col("source_id").alias("bronze_id_origem"),

        col("payload_map")
            .getItem("_source_file")
            .alias("bronze_tx_source_file"),

        col("payload_map")
            .getItem("ano_referencia")
            .cast("int")
            .alias("bronze_nr_ano_referencia"),

        col("batch_id").alias("bronze_id_batch"),
        col("record_hash").alias("bronze_tx_record_hash"),

        current_timestamp().alias("silver_ts_processamento")
    )
)

# COMMAND ----------

df_deputado_ref = (
    df_standardized
    .filter(col("dept_id_cadastro").isNotNull())
    .filter(col("dept_id_deputado").isNotNull())
    .groupBy("dept_id_cadastro")
    .agg(
        max("dept_id_deputado").alias("ref_dept_id_deputado")
    )
    .withColumnRenamed("dept_id_cadastro", "ref_dept_id_cadastro")
)

df_standardized = (
    df_standardized.alias("desp")
    .join(
        df_deputado_ref.alias("ref"),
        col("desp.dept_id_cadastro") == col("ref.ref_dept_id_cadastro"),
        "left"
    )
    .select(
        *[
            col(f"desp.{c}")
            for c in df_standardized.columns
            if c != "dept_id_deputado"
        ],
        coalesce(
            col("desp.dept_id_deputado"),
            col("ref.ref_dept_id_deputado")
        ).alias("dept_id_deputado")
    )
)

# COMMAND ----------

df_keyed = (
    df_standardized
    .withColumn(
        "desp_tx_dedup_key",
        sha2(
            concat_ws(
                "||",
                col("dept_id_deputado").cast("string"),
                col("desp_id_documento").cast("string"),
                col("desp_nr_lote").cast("string"),
                col("desp_nr_ano").cast("string"),
                col("desp_nr_mes").cast("string"),
                col("desp_vl_liquido").cast("string"),
                col("forn_nr_cnpj_cpf"),
                col("bronze_tx_record_hash")
            ),
            256
        )
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("desp_tx_dedup_key")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df_keyed
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

invalid_null_key = (
    df_dedup
    .filter(col("desp_tx_dedup_key").isNull())
    .count()
)

invalid_null_year = (
    df_dedup
    .filter(col("desp_nr_ano").isNull())
    .count()
)

invalid_null_value = (
    df_dedup
    .filter(col("desp_vl_liquido").isNull())
    .count()
)

duplicated_keys = (
    df_dedup
    .groupBy("desp_tx_dedup_key")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if invalid_null_key > 0:
    raise Exception(
        f"Data quality error: {invalid_null_key} records without dedup key."
    )

if duplicated_keys > 0:
    raise Exception(
        f"Data quality error: {duplicated_keys} duplicated expense keys."
    )

df_discarded = (
    df_dedup
    .filter(
        col("desp_tx_dedup_key").isNull()
        |
        col("desp_nr_ano").isNull()
        |
        col("desp_vl_liquido").isNull()
    )
)


df_valid = (
    df_dedup
    .filter(col("desp_tx_dedup_key").isNotNull())
    .filter(col("desp_nr_ano").isNotNull())
    .filter(col("desp_vl_liquido").isNotNull())
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
    .partitionBy("desp_nr_ano")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=f"source={SOURCE_TABLE} | finished successfully | records_read={records_read} | records_written={records_written} | records_discarded={records_discarded}",
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