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
    from_csv,
    regexp_replace,
    to_date,
    to_timestamp,
    sha2,
    concat_ws,
    expr,
    initcap,
    length,
    when,
    lit,
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

despesas_csv_schema = """
txNomeParlamentar STRING,
cpf STRING,
ideCadastro STRING,
nuCarteiraParlamentar STRING,
nuLegislatura STRING,
sgUF STRING,
sgPartido STRING,
codLegislatura STRING,
numSubCota STRING,
txtDescricao STRING,
numEspecificacaoSubCota STRING,
txtDescricaoEspecificacao STRING,
txtFornecedor STRING,
txtCNPJCPF STRING,
txtNumero STRING,
indTipoDocumento STRING,
datEmissao STRING,
vlrDocumento STRING,
vlrGlosa STRING,
vlrLiquido STRING,
numMes STRING,
numAno STRING,
numParcela STRING,
txtPassageiro STRING,
txtTrecho STRING,
numLote STRING,
numRessarcimento STRING,
datPagamentoRestituicao STRING,
vlrRestituicao STRING,
nuDeputadoId STRING,
ideDocumento STRING,
urlDocumento STRING
"""

df_parsed = (
    df_map
    .withColumn(
        "csv_data",
        from_csv(
            col("payload_data"),
            despesas_csv_schema,
            {
                "sep": ";",
                "quote": '"',
                "escape": '"',
                "header": "false"
            }
        )
    )
)

df_standardized = (
    df_parsed
    .select(
        initcap(trim(col("csv_data.txNomeParlamentar"))).alias("desp_tx_nome_parlamentar"),

        regexp_replace(col("csv_data.cpf"), "[^0-9]", "")
            .alias("dept_nr_cpf"),
        when(
            length(
                regexp_replace(col("csv_data.cpf"), "[^0-9]", "")
            ) == 11,
            lit(1)
        )
        .otherwise(lit(0))
        .alias("dept_fl_cpf_valido"),
        col("csv_data.ideCadastro")
            .cast("long")
            .alias("dept_id_cadastro"),

        trim(col("csv_data.nuCarteiraParlamentar"))
            .alias("dept_nr_carteira_parlamentar"),

        col("csv_data.nuLegislatura")
            .cast("int")
            .alias("leg_nr_legislatura"),

        upper(trim(col("csv_data.sgUF")))
            .alias("uf_sg_uf"),

        upper(trim(col("csv_data.sgPartido")))
            .alias("part_sg_partido"),

        col("csv_data.codLegislatura")
            .cast("int")
            .alias("leg_cd_legislatura"),

        col("csv_data.numSubCota")
            .cast("int")
            .alias("desp_cd_subcota"),

        initcap(trim(col("csv_data.txtDescricao")))
            .alias("desp_tx_descricao"),

        col("csv_data.numEspecificacaoSubCota")
            .cast("int")
            .alias("desp_cd_especificacao_subcota"),

        initcap(trim(col("csv_data.txtDescricaoEspecificacao")))
            .alias("desp_tx_descricao_especificacao"),

        initcap(trim(col("csv_data.txtFornecedor")))
            .alias("forn_tx_nome"),

        regexp_replace(
            col("csv_data.txtCNPJCPF"),
            "[^0-9]",
            ""
        ).alias("forn_nr_cnpj_cpf"),

        when(
            length(
                regexp_replace(
                    col("csv_data.txtCNPJCPF"),
                    "[^0-9]",
                    ""
                )
            ) == 11,
            lit("CPF")
        )
        .when(
            length(
                regexp_replace(
                    col("csv_data.txtCNPJCPF"),
                    "[^0-9]",
                    ""
                )
            ) == 14,
            lit("CNPJ")
        )
        .otherwise(lit("NA"))
        .alias("forn_tx_tipo_documento"),

        when(
            length(
                regexp_replace(
                    col("csv_data.txtCNPJCPF"),
                    "[^0-9]",
                    ""
                )
            ).isin(11, 14),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("forn_fl_documento_valido"),        

        trim(col("csv_data.txtNumero"))
            .alias("desp_nr_documento"),

        col("csv_data.indTipoDocumento")
            .cast("int")
            .alias("desp_cd_tipo_documento"),

        to_date(
            to_timestamp(col("csv_data.datEmissao"))
        ).alias("desp_dt_emissao"),

        when(
            to_date(
                to_timestamp(col("csv_data.datEmissao"))
            ).isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("desp_fl_data_emissao_valida"),

        regexp_replace(
            col("csv_data.vlrDocumento"),
            ",",
            "."
        ).cast("decimal(18,2)")
            .alias("desp_vl_documento"),

        regexp_replace(
            col("csv_data.vlrGlosa"),
            ",",
            "."
        ).cast("decimal(18,2)")
            .alias("desp_vl_glosa"),

        regexp_replace(
            col("csv_data.vlrLiquido"),
            ",",
            "."
        ).cast("decimal(18,2)")
            .alias("desp_vl_liquido"),

        col("csv_data.numMes")
            .cast("int")
            .alias("desp_nr_mes"),

        col("csv_data.numAno")
            .cast("int")
            .alias("desp_nr_ano"),

        col("csv_data.numParcela")
            .cast("int")
            .alias("desp_nr_parcela"),

        trim(col("csv_data.txtPassageiro"))
            .alias("desp_tx_passageiro"),

        trim(col("csv_data.txtTrecho"))
            .alias("desp_tx_trecho"),

        col("csv_data.numLote")
            .cast("long")
            .alias("desp_nr_lote"),

        col("csv_data.numRessarcimento")
            .cast("long")
            .alias("desp_nr_ressarcimento"),

        to_date(
            to_timestamp(col("csv_data.datPagamentoRestituicao"))
        ).alias("desp_dt_pagamento_restituicao"),
        
        when(
            col("csv_data.datPagamentoRestituicao").isNull()
            | to_date(
                to_timestamp(col("csv_data.datPagamentoRestituicao"))
            ).isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("desp_fl_data_restituicao_valida"),

        regexp_replace(
            col("csv_data.vlrRestituicao"),
            ",",
            "."
        ).cast("decimal(18,2)")
            .alias("desp_vl_restituicao"),

        col("csv_data.nuDeputadoId")
            .cast("long")
            .alias("dept_id_deputado"),

        col("csv_data.ideDocumento")
            .cast("long")
            .alias("desp_id_documento"),

        trim(col("csv_data.urlDocumento"))
            .alias("desp_tx_url_documento"),

        # ----------------------------------------------------------------------
        # Bronze lineage / traceability
        # ----------------------------------------------------------------------

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

        # ----------------------------------------------------------------------
        # Silver processing
        # ----------------------------------------------------------------------

        current_timestamp()
            .alias("silver_ts_processamento")
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

invalid_null_key = df_dedup.filter(col("desp_tx_dedup_key").isNull()).count()
invalid_null_year = df_dedup.filter(col("desp_nr_ano").isNull()).count()
invalid_null_value = df_dedup.filter(col("desp_vl_liquido").isNull()).count()

duplicated_keys = (
    df_dedup
    .groupBy("desp_tx_dedup_key")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if invalid_null_key > 0:
    raise Exception(f"Data quality error: {invalid_null_key} records without dedup key.")

if duplicated_keys > 0:
    raise Exception(f"Data quality error: {duplicated_keys} duplicated expense keys.")


# COMMAND ----------

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