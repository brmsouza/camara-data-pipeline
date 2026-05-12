# Databricks notebook source
# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 14_curated_fornecedores
# Layer: Silver Curated
# Author: Bruno Souza
#
# Description:
# Builds the curated supplier dataset enriched with public CNPJ validation data.
#
# Context:
# This notebook reads supplier records from Silver Base, validates CNPJ suppliers
# using a public CNPJ API utility, and creates analytical flags for supplier
# registration status and potential suspicious documents.
#
# Grain:
# One row per supplier document.
#
# Source:
# silver_base.fornecedores
#
# Target:
# silver_curated.fornecedores
# ------------------------------------------------------------------------------


# COMMAND ----------

# MAGIC %run ../../90_common/table_logger

# COMMAND ----------

# MAGIC %run ../../90_common/cnpj_utils

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime
import uuid
import time

# COMMAND ----------

LAYER = "silver_curated"
PIPELINE_NAME = "silver_curated_fornecedores"

SOURCE_TABLE = "silver_base.fornecedores"
TARGET_TABLE = "silver_curated.fornecedores"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

REQUEST_SLEEP_SECONDS = 1.2


# COMMAND ----------

df_source = spark.table(SOURCE_TABLE)

records_read = df_source.count()

# COMMAND ----------

MAX_CNPJS_TO_VALIDATE = 50000

df_ceap_supplier_usage = (
    spark.table("silver_curated.despesas")
    .withColumn(
        "forn_nr_documento_limpo",
        F.regexp_replace(F.col("forn_nr_cnpj_cpf").cast("string"), "[^0-9]", "")
    )
    .groupBy("forn_nr_documento_limpo")
    .agg(
        F.count("*").alias("qt_despesas"),
        F.sum("desp_vl_liquido").alias("vl_total_liquido")
    )
)

df_cnpjs = (
    df_source
    .filter(F.col("forn_tx_tipo_documento") == "CNPJ")
    .join(
        df_ceap_supplier_usage,
        on="forn_nr_documento_limpo",
        how="left"
    )
    .fillna({"qt_despesas": 0, "vl_total_liquido": 0})
    .orderBy(
        F.col("vl_total_liquido").desc(),
        F.col("qt_despesas").desc()
    )
    .limit(MAX_CNPJS_TO_VALIDATE)
    .select("forn_nr_documento_limpo")
    .distinct()
)

cnpjs = [
    row["forn_nr_documento_limpo"]
    for row in df_cnpjs.collect()
]

# COMMAND ----------

cnpj_results = []

for idx, cnpj in enumerate(cnpjs, start=1):

    result = fetch_cnpj_data(cnpj)

    result["forn_nr_documento_limpo"] = cnpj

    cnpj_results.append(result)

    if idx % 500 == 0:
        print(f"Processed {idx}/{len(cnpjs)} CNPJs")

    time.sleep(REQUEST_SLEEP_SECONDS)

print(f"CNPJ API validation finished: {len(cnpj_results)} records")

# COMMAND ----------

cnpj_schema = T.StructType([
    T.StructField("forn_nr_documento_limpo", T.StringType(), False),
    T.StructField("cnpj_consulta_status", T.StringType(), True),
    T.StructField("cnpj_situacao_cadastral", T.StringType(), True),
    T.StructField("cnpj_razao_social", T.StringType(), True),
    T.StructField("cnpj_nome_fantasia", T.StringType(), True),
    T.StructField("cnpj_cnae_principal", T.StringType(), True),
    T.StructField("cnpj_uf", T.StringType(), True),
    T.StructField("cnpj_municipio", T.StringType(), True),
    T.StructField("cnpj_porte", T.StringType(), True),
    T.StructField("cnpj_capital_social", T.StringType(), True),
    T.StructField("cnpj_api_http_status", T.IntegerType(), True),
    T.StructField("cnpj_api_error", T.StringType(), True),
])

df_cnpj_api = spark.createDataFrame(
    cnpj_results,
    schema=cnpj_schema
)

# COMMAND ----------

df_curated = (
    df_source.alias("forn")
    .join(
        df_cnpj_api.alias("api"),
        F.col("forn.forn_nr_documento_limpo") == F.col("api.forn_nr_documento_limpo"),
        "left"
    )
    .select(
        F.col("forn.forn_tx_nome"),
        F.col("forn.forn_nr_documento_original_limpo"),
        F.col("forn.forn_nr_documento_limpo"),
        F.col("forn.forn_tx_tipo_documento"),
        F.col("forn.forn_fl_documento_repetido"),
        F.col("forn.forn_fl_documento_valido_formato"),
        F.col("forn.forn_tx_dedup_key"),

        F.col("forn.bronze_ts_ingestao"),
        F.col("forn.bronze_dt_ingestao"),
        F.col("forn.bronze_tx_endpoint"),
        F.col("forn.bronze_id_origem"),
        F.col("forn.bronze_id_batch"),
        F.col("forn.bronze_tx_record_hash"),

        F.col("forn.silver_base_ts_processamento"),
        F.col("forn.silver_base_id_batch"),

        F.col("api.cnpj_consulta_status").alias("api_tx_status_consulta_cnpj"),
        F.col("api.cnpj_api_http_status").alias("api_cd_http_status_cnpj"),
        F.col("api.cnpj_api_error").alias("api_tx_erro_consulta_cnpj"),
        F.col("api.cnpj_razao_social").alias("api_tx_razao_social_receita"),
        F.col("api.cnpj_nome_fantasia").alias("api_tx_nome_fantasia_receita"),
        F.col("api.cnpj_situacao_cadastral").alias("api_tx_situacao_cadastral"),
        F.col("api.cnpj_cnae_principal").alias("api_tx_cnae_principal"),
        F.col("api.cnpj_uf").alias("api_sg_uf_receita"),
        F.col("api.cnpj_municipio").alias("api_tx_municipio_receita"),
        F.col("api.cnpj_porte").alias("api_tx_porte_empresa"),
        F.col("api.cnpj_capital_social").alias("api_vl_capital_social")
    )
    .withColumn(
        "forn_tx_status_consulta_cnpj",
        F.when(
            F.col("forn_tx_tipo_documento") == "CNPJ",
            F.coalesce(
                F.col("api_tx_status_consulta_cnpj"),
                F.lit("NOT_VALIDATED")
            )
        ).otherwise(F.lit("NOT_APPLICABLE"))
    )
    .withColumn("forn_cd_http_status_cnpj", F.col("api_cd_http_status_cnpj"))
    .withColumn("forn_tx_erro_consulta_cnpj", F.col("api_tx_erro_consulta_cnpj"))
    .withColumn("forn_tx_razao_social_receita", F.col("api_tx_razao_social_receita"))
    .withColumn("forn_tx_nome_fantasia_receita", F.col("api_tx_nome_fantasia_receita"))
    .withColumn("forn_tx_situacao_cadastral", F.col("api_tx_situacao_cadastral"))
    .withColumn("forn_tx_cnae_principal", F.col("api_tx_cnae_principal"))
    .withColumn("forn_sg_uf_receita", F.col("api_sg_uf_receita"))
    .withColumn("forn_tx_municipio_receita", F.col("api_tx_municipio_receita"))
    .withColumn("forn_tx_porte_empresa", F.col("api_tx_porte_empresa"))
    .withColumn("forn_vl_capital_social", F.col("api_vl_capital_social").cast("double"))
    .withColumn(
        "forn_fl_cnpj_encontrado",
        F.when(F.col("forn_tx_status_consulta_cnpj") == "FOUND", F.lit(1))
         .otherwise(F.lit(0))
    )
    .withColumn(
        "forn_fl_cnpj_ativo",
        F.when(
            F.upper(F.coalesce(F.col("forn_tx_situacao_cadastral"), F.lit(""))) == "ATIVA",
            F.lit(1)
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "forn_fl_cnpj_suspeito",
        F.when(F.col("forn_tx_tipo_documento") != "CNPJ", F.lit(0))
         .when(F.col("forn_tx_status_consulta_cnpj") == "NOT_VALIDATED", F.lit(0))
         .when(
             F.col("forn_tx_status_consulta_cnpj").isin(
                 "INVALID_FORMAT",
                 "NOT_FOUND",
                 "ERROR"
             ),
             F.lit(1)
         )
         .when(
             F.upper(F.coalesce(F.col("forn_tx_situacao_cadastral"), F.lit("")))
             .isin("BAIXADA", "INAPTA", "SUSPENSA", "NULA"),
             F.lit(1)
         )
         .otherwise(F.lit(0))
    )
    .withColumn(
        "forn_tx_motivo_cnpj_suspeito",
        F.when(
            F.col("forn_tx_tipo_documento") != "CNPJ",
            F.lit("Documento não consultado por não ser CNPJ")
        )
        .when(
            F.col("forn_tx_status_consulta_cnpj") == "NOT_VALIDATED",
            F.lit("CNPJ não priorizado para validação API")
        )
        .when(
            F.col("forn_tx_status_consulta_cnpj") == "INVALID_FORMAT",
            F.lit("CNPJ com formato inválido")
        )
        .when(
            F.col("forn_tx_status_consulta_cnpj") == "NOT_FOUND",
            F.lit("CNPJ não encontrado na API pública")
        )
        .when(
            F.col("forn_tx_status_consulta_cnpj") == "ERROR",
            F.lit("Erro na consulta pública de CNPJ")
        )
        .when(
            F.upper(F.coalesce(F.col("forn_tx_situacao_cadastral"), F.lit("")))
            .isin("BAIXADA", "INAPTA", "SUSPENSA", "NULA"),
            F.concat(
                F.lit("Situação cadastral não ativa: "),
                F.col("forn_tx_situacao_cadastral")
            )
        )
        .otherwise(F.lit("Sem indício cadastral crítico"))
    )
    .drop(
        "api_tx_status_consulta_cnpj",
        "api_cd_http_status_cnpj",
        "api_tx_erro_consulta_cnpj",
        "api_tx_razao_social_receita",
        "api_tx_nome_fantasia_receita",
        "api_tx_situacao_cadastral",
        "api_tx_cnae_principal",
        "api_sg_uf_receita",
        "api_tx_municipio_receita",
        "api_tx_porte_empresa",
        "api_vl_capital_social"
    )
    .withColumn("silver_curated_ts_processamento", F.current_timestamp())
    .withColumn("silver_curated_id_batch", F.lit(batch_id))
)

# COMMAND ----------

records_written = df_curated.count()
records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Silver Curated validation failed: source silver_base.fornecedores has no records."
    )

if records_written == 0:
    raise Exception(
        "Silver Curated validation failed: silver_curated.fornecedores has no records."
    )

duplicated_documents = (
    df_curated
    .groupBy("forn_nr_documento_limpo")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicated_documents > 0:
    raise Exception(
        f"Silver Curated validation failed: duplicated supplier documents found = {duplicated_documents}"
    )

# COMMAND ----------

(
    df_curated
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
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