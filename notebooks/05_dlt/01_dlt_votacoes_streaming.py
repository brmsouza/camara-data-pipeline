# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 01_dlt_votacoes_streaming
# Layer: DLT / Lakeflow
#
# Description:
# Declarative pipeline for voting micro-batch data.
# Transforms Bronze Stream into Silver and Gold streaming tables.
#
# Flow:
# bronze_stream.votacoes_raw
#   -> silver_stream_votacoes_validas
#   -> gold_stream_votacoes_alertas
#
# Important:
# This notebook must NOT be executed manually from a standard Databricks notebook
# cluster.
#
# It must be executed only through a Databricks Lakeflow / Delta Live Tables
# pipeline, because the dlt module is available only in the DLT runtime context.
#
# Execution:
# Jobs & Pipelines
#   -> dlt_votacoes_streaming
#   -> Run / Start
# ------------------------------------------------------------------------------

import dlt
from pyspark.sql import functions as F

# Silver Stream
@dlt.table(
    name="silver_stream_votacoes_validas",
    comment="Validated voting records from Bronze Stream."
)
@dlt.expect_or_drop(
    "vota_id_votacao_not_null",
    "vota_id_votacao IS NOT NULL"
)
@dlt.expect_or_drop(
    "bronze_payload_not_null",
    "bronze_tx_payload IS NOT NULL"
)
@dlt.expect_or_drop(
    "bronze_ingestion_ts_not_null",
    "bronze_ts_ingestao IS NOT NULL"
)
@dlt.expect_or_drop(
    "bronze_record_hash_not_null",
    "bronze_tx_record_hash IS NOT NULL"
)
def silver_stream_votacoes_validas():

    return (
        spark.readStream.table("bronze_stream.votacoes_raw")

        .select(
            F.col("vota_id_votacao"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.data"
            ).alias("vota_dt_votacao"),

            F.to_timestamp(
                F.get_json_object(
                    "bronze_tx_payload",
                    "$.dataHoraRegistro"
                )
            ).alias("vota_ts_registro"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.siglaOrgao"
            ).alias("vota_sg_orgao"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.uriOrgao"
            ).alias("vota_uri_orgao"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.uriEvento"
            ).alias("vota_uri_evento"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.proposicaoObjeto"
            ).alias("prop_tx_objeto"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.uriProposicaoObjeto"
            ).alias("prop_uri_objeto"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.descricao"
            ).alias("vota_tx_descricao"),

            F.get_json_object(
                "bronze_tx_payload",
                "$.aprovacao"
            ).cast("int").alias("vota_fl_aprovacao"),

            F.col("bronze_tx_endpoint"),
            F.col("bronze_id_batch"),
            F.col("bronze_ts_ingestao"),
            F.col("bronze_dt_ingestao"),

            # Lineage / replay metadata
            F.col("bronze_tx_payload"),
            F.col("bronze_tx_record_hash")
        )

        .dropDuplicates(["vota_id_votacao"])
    )

# Gold Stream
@dlt.table(
    name="gold_stream_votacoes_alertas",
    comment="Voting alert classification from validated voting records."
)
@dlt.expect_or_drop(
    "vota_id_votacao_not_null",
    "vota_id_votacao IS NOT NULL"
)
@dlt.expect_or_drop(
    "vota_ts_registro_not_null",
    "vota_ts_registro IS NOT NULL"
)
def gold_stream_votacoes_alertas():

    df = dlt.read_stream("silver_stream_votacoes_validas")

    return (
        df
        .withColumn(
            "alert_tx_urgencia",
            F.when(
                F.upper(
                    F.coalesce(
                        F.col("vota_tx_descricao"),
                        F.lit("")
                    )
                ).contains("URGENTE"),
                F.lit("ALTA")
            )
            .when(
                F.upper(
                    F.coalesce(
                        F.col("prop_tx_objeto"),
                        F.lit("")
                    )
                ).contains("REQ"),
                F.lit("MEDIA")
            )
            .when(
                F.col("vota_fl_aprovacao") == 1,
                F.lit("MEDIA")
            )
            .otherwise(
                F.lit("BAIXA")
            )
        )

        .withColumn(
            "alert_fl_notificar",
            F.when(
                F.col("alert_tx_urgencia").isin("ALTA", "MEDIA"),
                F.lit(1)
            )
            .otherwise(
                F.lit(0)
            )
        )

        .withColumn(
            "alert_tx_motivo",
            F.when(
                F.col("alert_tx_urgencia") == "ALTA",
                F.lit("Descrição contém termo de urgência.")
            )
            .when(
                F.col("alert_tx_urgencia") == "MEDIA",
                F.lit("Votação aprovada ou associada a requerimento.")
            )
            .otherwise(
                F.lit("Votação classificada como baixa urgência.")
            )
        )

        .withColumn(
            "gold_ts_processamento",
            F.current_timestamp()
        )
    )