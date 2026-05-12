# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 03_build_gold_eventos_analytics
# Layer: Gold Analytics
# Author: Bruno Souza
# ------------------------------------------------------------------------------

# COMMAND ----------

# Views / analytical objects included in this notebook

# - gold.vw_eventos_analitica
# - gold.vw_eventos_futuros
# - gold.vw_densidade_eventos_semanal
# - gold.vw_semanas_sem_atividade

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_eventos_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_eventos_analitica
-- Layer: Gold
--
-- Description:
-- Analytical parliamentary events view combining event fact records with
-- organization and date dimensions.
--
-- Grain:
-- One row per parliamentary event.
--
-- Source:
-- gold.ft_presenca_eventos
-- -----------------------------------------------------------------------------

SELECT
    ft.sk_evt,
    ft.sk_org,
    ft.sk_data_inicio,
    ft.sk_data_fim,

    ft.evt_id_evento,

    d.data_dt_data AS evt_dt_inicio,
    d.data_nr_ano AS evt_nr_ano,
    d.data_nr_mes AS evt_nr_mes,
    d.data_nr_semana_ano AS evt_nr_semana_ano,
    d.data_tx_ano_mes,

    ft.evt_dt_fim,

    org.org_id_orgao,
    org.org_sg_orgao,
    org.org_tx_nome,
    org.org_tx_tipo_curado,

    ft.evt_tx_uri,
    ft.evt_ts_inicio,
    ft.evt_ts_fim,

    ft.evt_tx_descricao,
    ft.evt_tx_tipo,
    ft.evt_tx_situacao,
    ft.evt_tx_tipo_curado,
    ft.evt_tx_situacao_curada,

    ft.evt_fl_inicio_valido,
    ft.evt_fl_fim_valido,
    ft.evt_fl_periodo_valido,

    ft.evt_fl_sessao,
    ft.evt_fl_audiencia_publica,
    ft.evt_fl_reuniao,
    ft.evt_fl_encerrado,
    ft.evt_fl_cancelado,
    ft.evt_fl_possui_registro,
    ft.evt_tx_url_registro,

    ft.evt_tx_local_interno,
    ft.evt_tx_predio,
    ft.evt_tx_sala,
    ft.evt_tx_andar,
    ft.evt_tx_local_externo,
    ft.evt_tx_tipo_local,
    ft.evt_qt_orgaos,

    ft.org_id_orgao_principal,
    ft.org_sg_orgao_principal,
    ft.org_tx_nome_principal,
    ft.org_tx_tipo_principal,
    ft.org_tx_siglas_relacionadas,

    ft.bronze_ts_ingestao,
    ft.bronze_dt_ingestao,
    ft.bronze_tx_endpoint,
    ft.bronze_id_origem,
    ft.bronze_id_batch,
    ft.bronze_tx_record_hash,

    ft.gold_ts_processamento,
    ft.gold_id_batch

FROM gold.ft_presenca_eventos ft

LEFT JOIN gold.dm_data d
    ON ft.sk_data_inicio = d.sk_data

LEFT JOIN gold.dm_orgao org
    ON ft.sk_org = org.sk_org
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_eventos_futuros AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_eventos_futuros
-- Layer: Gold
--
-- Description:
-- Upcoming legislative events.
--
-- Grain:
-- One row per future event.
--
-- Source:
-- gold.vw_eventos_analitica
-- -----------------------------------------------------------------------------

SELECT DISTINCT
    evt_id_evento,
    evt_tx_descricao,
    evt_tx_situacao,
    evt_tx_tipo,
    evt_tx_situacao_curada,
    evt_tx_tipo_curado,

    evt_tx_local_interno,
    evt_tx_local_externo,
    evt_tx_tipo_local,

    evt_dt_inicio,
    evt_dt_fim,

    evt_nr_ano,
    evt_nr_mes,
    evt_nr_semana_ano,
    data_tx_ano_mes,

    org_id_orgao,
    org_sg_orgao,
    org_tx_nome,
    org_tx_tipo_curado

FROM gold.vw_eventos_analitica

WHERE evt_dt_inicio >= CURRENT_DATE()
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_densidade_eventos_semanal AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_densidade_eventos_semanal
-- Layer: Gold
--
-- Description:
-- Weekly parliamentary event density analysis.
--
-- Grain:
-- One row per year and calendar week.
--
-- Source:
-- gold.vw_eventos_analitica
-- -----------------------------------------------------------------------------

SELECT
    evt_nr_ano,
    evt_nr_semana_ano,

    COUNT(DISTINCT evt_id_evento) AS qt_eventos,

    COUNT(DISTINCT org_id_orgao) AS qt_orgaos,

    SUM(
        CASE
            WHEN evt_fl_audiencia_publica = 1 THEN 1
            ELSE 0
        END
    ) AS qt_audiencias_publicas,

    SUM(
        CASE
            WHEN evt_fl_reuniao = 1 THEN 1
            ELSE 0
        END
    ) AS qt_reunioes,

    SUM(
        CASE
            WHEN evt_fl_sessao = 1 THEN 1
            ELSE 0
        END
    ) AS qt_sessoes

FROM gold.vw_eventos_analitica

GROUP BY
    evt_nr_ano,
    evt_nr_semana_ano
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_semanas_sem_atividade AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_semanas_sem_atividade
-- Layer: Gold
--
-- Description:
-- Identifies weeks with low or no legislative event activity.
--
-- Grain:
-- One row per year and calendar week.
--
-- Source:
-- gold.vw_densidade_eventos_semanal
-- -----------------------------------------------------------------------------

SELECT
    *,

    CASE
        WHEN qt_eventos = 0
            THEN 'Sem atividade'

        WHEN qt_eventos <= 5
            THEN 'Baixa atividade'

        WHEN qt_eventos <= 15
            THEN 'Atividade moderada'

        ELSE 'Alta atividade'
    END AS evento_tx_nivel_atividade

FROM gold.vw_densidade_eventos_semanal
""")