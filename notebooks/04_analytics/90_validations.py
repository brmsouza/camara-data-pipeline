# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 90_validations
# Layer: Gold Analytics
# Author: Bruno Souza
# ------------------------------------------------------------------------------

# Views / analytical objects validated in this notebook

# - CEAP analytics
# - Fronts analytics
# - Events analytics
# - Voting analytics
# - Engagement analytics
# - Decisive participation percentages

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

views_to_validate = [
    "gold.vw_despesas_ceap_analitica",
    "gold.vw_ranking_despesas_deputado_mensal",
    "gold.vw_despesas_deputado_segmento",
    "gold.vw_gastos_segmentados",
    "gold.vw_partidos_despesas_segmento",
    "gold.vw_perfil_gasto_partido",
    "gold.vw_anomalias_ceap_zscore",
    "gold.vw_top_10_gastos_partido_mensal",

    "gold.vw_frentes_membros_analitica",
    "gold.vw_frentes_diversidade_partidaria",
    "gold.vw_deputados_mais_frentes",
    "gold.vw_sobreposicao_frentes",
    "gold.vw_evolucao_frentes_legislatura",
    "gold.vw_especializacao_tematica",
    "gold.vw_partidos_especializacao_tematica",

    "gold.vw_eventos_analitica",
    "gold.vw_eventos_futuros",
    "gold.vw_densidade_eventos_semanal",
    "gold.vw_semanas_sem_atividade",

    "gold.vw_votacoes_analitica",
    "gold.vw_votos_deputados_analitica",
    "gold.vw_orientacoes_bancada_analitica",
    "gold.vw_fidelidade_partidaria",
    "gold.vw_partidos_fidelidade_votacao",
    "gold.vw_partidos_votos_distribuicao",
    "gold.vw_alinhamento_frente_vs_partido",

    "gold.vw_atividade_parlamentar_analitica",
    "gold.vw_indice_eficiencia_parlamentar",
    "gold.vw_indice_transparencia",
    "gold.vw_ausencias_votacoes_criticas",
    "gold.vw_ranking_ausencias_criticas",
    "gold.vw_score_engajamento_parlamentar",

    "gold.vw_perfil_parlamentar",
    "gold.vw_partidos_analitica",
    "gold.vw_partidos_perfil",
    "gold.vw_dashboard_partidos",
    "gold.vw_analise_ineficiencia_parlamentar",
]

# COMMAND ----------

from pyspark.sql import types as T

validation_schema = T.StructType([
    T.StructField("view_name", T.StringType(), False),
    T.StructField("validation_status", T.StringType(), False),
    T.StructField("records", T.LongType(), True),
    T.StructField("error_message", T.StringType(), True),
])

validation_results = []

for view_name in views_to_validate:
    try:
        df_view = spark.table(view_name)
        records = df_view.count()

        validation_results.append(
            {
                "view_name": view_name,
                "validation_status": "OK",
                "records": int(records),
                "error_message": "",
            }
        )

    except Exception as e:
        validation_results.append(
            {
                "view_name": view_name,
                "validation_status": "ERROR",
                "records": None,
                "error_message": str(e)[:1000],
            }
        )

df_validation_results = spark.createDataFrame(
    validation_results,
    schema=validation_schema
)


# COMMAND ----------

failed_views = (
    df_validation_results
    .filter(F.col("validation_status") == "ERROR")
    .count()
)

if failed_views > 0:
    raise Exception(
        f"Analytics validation failed: {failed_views} views returned errors."
    )

# COMMAND ----------

empty_views = (
    df_validation_results
    .filter(
        (F.col("validation_status") == "OK")
        & (F.col("records") == 0)
    )
    .count()
)

if empty_views > 0:
    raise Exception(
        f"Analytics validation failed: {empty_views} views returned zero records."
    )

df_ceap = spark.table("gold.vw_despesas_ceap_analitica")

df_anomalias = spark.table("gold.vw_anomalias_ceap_zscore")

invalid_active_flags = (
    df_ceap
    .filter(
        F.col("forn_fl_cnpj_ativo").isNotNull()
        & (~F.col("forn_fl_cnpj_ativo").isin(0, 1))
    )
    .count()
)

if invalid_active_flags > 0:
    raise Exception(
        f"CEAP validation failed: {invalid_active_flags} records with invalid active CNPJ flag."
    )    

# COMMAND ----------

invalid_anomaly_classification = (
    df_anomalias
    .filter(
        ~F.col("desp_tx_classificacao_anomalia").isin(
            "Anomalia crítica: outlier financeiro com fornecedor suspeito",
            "Possível anomalia com fornecedor suspeito",
            "Fornecedor com CNPJ suspeito",
            "Anomalia financeira extrema",
            "Possível anomalia financeira",
            "Comportamento esperado"
        )
    )
    .count()
)

if invalid_anomaly_classification > 0:
    raise Exception(
        f"CEAP anomaly validation failed: {invalid_anomaly_classification} invalid anomaly classifications."
    )

# COMMAND ----------

df_ceap = spark.table("gold.vw_despesas_ceap_analitica")

null_ceap_critical_keys = (
    df_ceap
    .filter(
        F.col("sk_resp_ceap").isNull()
        | F.col("sk_desp_tipo").isNull()
    )
    .count()
)

if null_ceap_critical_keys > 0:
    raise Exception(
        f"CEAP validation failed: {null_ceap_critical_keys} records with null critical keys."
    )

null_sk_data_emissao = (
    df_ceap
    .filter(F.col("sk_data_emissao").isNull())
    .count()
)

print(f"CEAP validation warning: null sk_data_emissao records = {null_sk_data_emissao}")

# COMMAND ----------

negative_ceap_values = (
    df_ceap
    .filter(F.col("desp_vl_liquido") < 0)
    .count()
)

print(f"CEAP validation warning: negative net expense records = {negative_ceap_values}")

invalid_negative_flag = (
    df_ceap
    .filter(
        (F.col("desp_vl_liquido") < 0)
        & (F.col("desp_fl_valor_negativo") != 1)
    )
    .count()
)

if invalid_negative_flag > 0:
    raise Exception(
        f"CEAP validation failed: {invalid_negative_flag} negative expense records without negative-value flag."
    )

# COMMAND ----------

df_anomalias = spark.table("gold.vw_anomalias_ceap_zscore")

invalid_zscore = (
    df_anomalias
    .filter(
        F.col("desp_nr_zscore").isNotNull()
        & (
            F.isnan(F.col("desp_nr_zscore"))
        )
    )
    .count()
)

if invalid_zscore > 0:
    raise Exception(
        f"CEAP anomaly validation failed: {invalid_zscore} invalid z-score values."
    )

# COMMAND ----------

df_frentes_div = spark.table("gold.vw_frentes_diversidade_partidaria")

invalid_hhi = (
    df_frentes_div
    .filter(
        (F.col("indice_hhi_partidario") < 0)
        | (F.col("indice_hhi_partidario") > 1)
    )
    .count()
)

if invalid_hhi > 0:
    raise Exception(
        f"Fronts validation failed: {invalid_hhi} HHI values outside expected range [0,1]."
    )

# COMMAND ----------

df_frentes_membros = spark.table("gold.vw_frentes_membros_analitica")

null_front_membership_keys = (
    df_frentes_membros
    .filter(
        F.col("frente_id_frente").isNull()
        | F.col("dept_id_deputado").isNull()
        | F.col("leg_id_legislatura").isNull()
    )
    .count()
)

if null_front_membership_keys > 0:
    raise Exception(
        f"Front membership validation failed: {null_front_membership_keys} records with null critical business keys."
    )

# COMMAND ----------

df_votos = spark.table("gold.vw_votos_deputados_analitica")

invalid_presence = (
    df_votos
    .filter(
        (F.col("qt_presenca") < 0)
        | (F.col("qt_presenca") > 1)
    )
    .count()
)

if invalid_presence > 0:
    raise Exception(
        f"Voting validation failed: {invalid_presence} records with invalid presence flag."
    )

# COMMAND ----------

df_fidelidade = spark.table("gold.vw_fidelidade_partidaria")

invalid_fidelity = (
    df_fidelidade
    .filter(
        (F.col("pc_participacao_decisiva") < 0)
        | (F.col("pc_participacao_decisiva") > 100)
    )
    .count()
)

if invalid_fidelity > 0:
    raise Exception(
        f"Voting validation failed: {invalid_fidelity} participation percentages outside expected range [0,100]."
    )

# COMMAND ----------

df_engajamento = spark.table("gold.vw_score_engajamento_parlamentar")

invalid_engagement_score = (
    df_engajamento
    .filter(F.col("score_engajamento") < 0)
    .count()
)

if invalid_engagement_score > 0:
    raise Exception(
        f"Engagement validation failed: {invalid_engagement_score} negative engagement scores."
    )

# COMMAND ----------

df_eficiencia = spark.table("gold.vw_indice_eficiencia_parlamentar")

invalid_efficiency = (
    df_eficiencia
    .filter(
        F.col("indice_eficiencia_parlamentar").isNotNull()
        & F.isnan(F.col("indice_eficiencia_parlamentar"))
    )
    .count()
)

if invalid_efficiency > 0:
    raise Exception(
        f"Efficiency validation failed: {invalid_efficiency} invalid efficiency index values."
    )

# COMMAND ----------

df_ceap = spark.table("gold.vw_despesas_ceap_analitica")

invalid_cnpj_flags = (
    df_ceap
    .filter(
        F.col("forn_fl_cnpj_suspeito").isNotNull()
        & (~F.col("forn_fl_cnpj_suspeito").isin(0, 1))
    )
    .count()
)

if invalid_cnpj_flags > 0:
    raise Exception(
        f"CEAP validation failed: {invalid_cnpj_flags} records with invalid CNPJ suspicious flag."
    )

# COMMAND ----------

df_anomalias = spark.table("gold.vw_anomalias_ceap_zscore")

invalid_anomaly_cnpj_flags = (
    df_anomalias
    .filter(
        F.col("forn_fl_cnpj_suspeito").isNotNull()
        & (~F.col("forn_fl_cnpj_suspeito").isin(0, 1))
    )
    .count()
)

if invalid_anomaly_cnpj_flags > 0:
    raise Exception(
        f"CEAP anomaly validation failed: {invalid_anomaly_cnpj_flags} records with invalid CNPJ suspicious flag."
    )

# COMMAND ----------

invalid_cnpj_status = (
    df_ceap
    .filter(
        F.col("forn_tx_tipo_documento") == "CNPJ"
    )
    .filter(
        ~F.col("forn_tx_status_consulta_cnpj").isin(
            "FOUND",
            "NOT_FOUND",
            "ERROR",
            "INVALID_FORMAT",
            "NOT_VALIDATED"
        )
    )
    .count()
)

if invalid_cnpj_status > 0:
    raise Exception(
        f"CEAP validation failed: {invalid_cnpj_status} CNPJ records with invalid consultation status."
    )

# COMMAND ----------

print("Gold Analytics validation completed successfully.")