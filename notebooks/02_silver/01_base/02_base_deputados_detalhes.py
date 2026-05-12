# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 02_base_deputados_detalhes
# Layer: Silver Base
# Author: Bruno Souza
#
# Description:
# Performs parsing, standardization, typing, deduplication and quality validation
# for deputies detail data from the Bronze layer.
#
# Context:
# This notebook transforms raw ingestion data from bronze.deputados_detalhes into
# a structured and validated Silver Base table. The resulting dataset complements
# silver_base.deputados and supports the future construction of gold.dm_deputado.
#
# Responsibilities:
# - Parse raw JSON payload
# - Apply schema standardization
# - Cast and normalize fields
# - Remove invalid records
# - Perform technical deduplication
# - Preserve lineage and traceability columns
# - Persist Silver Base Delta table
# - Validate technical CPF quality
# - Validate technical email quality
# - Validate technical telephone quality
# - Validate technical date quality
#
# Source:
# bronze.deputados_detalhes
#
# Target:
# silver_base.deputados_detalhes
#
# Notes:
# - Idempotent execution
# - Delta Lake format
# - Ready for Silver Curated consumption
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
    to_date,
    lower,
    initcap,
    regexp_replace,
    length,
    when,
    lit,
)

from pyspark.sql.window import Window

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
)

# COMMAND ----------

SOURCE_TABLE = "bronze.deputados_detalhes"
TARGET_TABLE = "silver_base.deputados_detalhes"

PIPELINE_NAME = "silver_base_deputados_detalhes"
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

ultimo_status_schema = StructType([
    StructField("id", LongType(), True),
    StructField("nome", StringType(), True),
    StructField("siglaPartido", StringType(), True),
    StructField("siglaUf", StringType(), True),
    StructField("idLegislatura", LongType(), True),
    StructField("urlFoto", StringType(), True),
    StructField("email", StringType(), True),
    StructField("data", StringType(), True),
    StructField("nomeEleitoral", StringType(), True),
    StructField("gabinete", StructType([
        StructField("nome", StringType(), True),
        StructField("predio", StringType(), True),
        StructField("sala", StringType(), True),
        StructField("andar", StringType(), True),
        StructField("telefone", StringType(), True),
        StructField("email", StringType(), True),
    ]), True),
    StructField("situacao", StringType(), True),
    StructField("condicaoEleitoral", StringType(), True),
    StructField("descricaoStatus", StringType(), True),
])

deputados_detalhes_schema = StructType([
    StructField("id", LongType(), True),
    StructField("uri", StringType(), True),
    StructField("nomeCivil", StringType(), True),
    StructField("ultimoStatus", ultimo_status_schema, True),
    StructField("cpf", StringType(), True),
    StructField("sexo", StringType(), True),
    StructField("urlWebsite", StringType(), True),
    StructField("redeSocial", ArrayType(StringType()), True),
    StructField("dataNascimento", StringType(), True),
    StructField("dataFalecimento", StringType(), True),
    StructField("ufNascimento", StringType(), True),
    StructField("municipioNascimento", StringType(), True),
    StructField("escolaridade", StringType(), True),
])

# COMMAND ----------

df_parsed = (
    df_bronze
    .withColumn(
        "json_data",
        from_json(col("raw_payload"), deputados_detalhes_schema)
    )
)

df_standardized = (
    df_parsed
    .select(
        col("json_data.id")
            .alias("dept_id_deputado"),

        initcap(trim(col("json_data.nomeCivil")))
            .alias("dept_tx_nome_civil"),

        upper(trim(col("json_data.sexo")))
            .alias("dept_sg_sexo"),

        trim(col("json_data.urlWebsite"))
            .alias("dept_tx_url_website"),

        col("json_data.redeSocial")
            .alias("dept_arr_rede_social"),

        to_date(col("json_data.dataNascimento"))
            .alias("dept_dt_nascimento"),

        to_date(col("json_data.dataFalecimento"))
            .alias("dept_dt_falecimento"),

        when(
            to_date(col("json_data.dataNascimento")).isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("dept_fl_data_nascimento_valida"),

        when(
            col("json_data.dataFalecimento").isNull()
            | to_date(col("json_data.dataFalecimento")).isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("dept_fl_data_falecimento_valida"),

        upper(trim(col("json_data.ufNascimento")))
            .alias("uf_sg_nascimento"),

        initcap(trim(col("json_data.municipioNascimento")))
            .alias("dept_tx_municipio_nascimento"),

        initcap(trim(col("json_data.escolaridade")))
            .alias("dept_tx_escolaridade"),

        trim(col("json_data.uri"))
            .alias("dept_tx_uri"),

        regexp_replace(
            trim(col("json_data.cpf")),
            "[^0-9]",
            ""
        ).alias("dept_nr_cpf"),

        when(
            length(
                regexp_replace(
                    trim(col("json_data.cpf")),
                    "[^0-9]",
                    ""
                )
            ) == 11,
            lit(1)
        )
        .otherwise(lit(0))
        .alias("dept_fl_cpf_valido"),

        col("json_data.ultimoStatus.id")
            .alias("dept_id_ultimo_status"),

        initcap(trim(col("json_data.ultimoStatus.nome")))
            .alias("dept_tx_nome_parlamentar"),

        initcap(trim(col("json_data.ultimoStatus.nomeEleitoral")))
            .alias("dept_tx_nome_eleitoral"),

        upper(trim(col("json_data.ultimoStatus.siglaPartido")))
            .alias("part_sg_partido"),

        upper(trim(col("json_data.ultimoStatus.siglaUf")))
            .alias("uf_sg_uf"),

        col("json_data.ultimoStatus.idLegislatura")
            .alias("leg_id_legislatura"),

        lower(trim(col("json_data.ultimoStatus.email")))
            .alias("dept_tx_email"),

        when(
            lower(trim(col("json_data.ultimoStatus.email"))).rlike(
                "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
            ),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("dept_fl_email_valido"),

        to_date(col("json_data.ultimoStatus.data"))
            .alias("dept_dt_ultimo_status"),

        when(
            to_date(col("json_data.ultimoStatus.data")).isNotNull(),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("dept_fl_data_status_valida"),

        initcap(trim(col("json_data.ultimoStatus.situacao")))
            .alias("dept_tx_situacao"),

        initcap(trim(col("json_data.ultimoStatus.condicaoEleitoral")))
            .alias("dept_tx_condicao_eleitoral"),

        trim(col("json_data.ultimoStatus.descricaoStatus"))
            .alias("dept_tx_descricao_status"),

        initcap(trim(col("json_data.ultimoStatus.gabinete.nome")))
            .alias("gab_tx_nome"),

        trim(col("json_data.ultimoStatus.gabinete.predio"))
            .alias("gab_tx_predio"),

        trim(col("json_data.ultimoStatus.gabinete.sala"))
            .alias("gab_tx_sala"),

        trim(col("json_data.ultimoStatus.gabinete.andar"))
            .alias("gab_tx_andar"),

        regexp_replace(
            trim(col("json_data.ultimoStatus.gabinete.telefone")),
            "[^0-9]",
            ""
        ).alias("gab_tx_telefone"),

        when(
            length(
                regexp_replace(
                    trim(col("json_data.ultimoStatus.gabinete.telefone")),
                    "[^0-9]",
                    ""
                )
            ).between(8, 13),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("gab_fl_telefone_valido"),

        lower(trim(col("json_data.ultimoStatus.gabinete.email")))
            .alias("gab_tx_email"),

        when(
            lower(trim(col("json_data.ultimoStatus.gabinete.email"))).rlike(
                "^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"
            ),
            lit(1)
        )
        .otherwise(lit(0))
        .alias("gab_fl_email_valido"),

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
    .partitionBy("dept_id_deputado")
    .orderBy(col("bronze_ts_ingestao").desc_nulls_last())
)

df_dedup = (
    df_standardized 
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)

# COMMAND ----------

invalid_null_id = df_dedup.filter(col("dept_id_deputado").isNull()).count()

duplicated_ids = (
    df_dedup
    .groupBy("dept_id_deputado")
    .agg(count("*").alias("qt_registros"))
    .filter(col("qt_registros") > 1)
    .count()
)

if invalid_null_id > 0:
    raise Exception(f"Data quality error: {invalid_null_id} records without deputy ID.")

if duplicated_ids > 0:
    raise Exception(f"Data quality error: {duplicated_ids} duplicated deputy IDs.")

# COMMAND ----------

df_valid = df_dedup.filter(col("dept_id_deputado").isNotNull())

records_written = df_valid.count()

records_discarded = records_read - records_written

# COMMAND ----------

(
    df_valid.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
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