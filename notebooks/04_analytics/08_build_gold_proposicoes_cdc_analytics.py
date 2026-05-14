# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics Layer — Proposition CDC and Legislative Lifecycle Intelligence
# MAGIC
# MAGIC **Notebook:** `08_build_gold_proposicoes_cdc_analytics`
# MAGIC
# MAGIC Builds advanced analytical views and Parliamentary Intelligence products
# MAGIC related to proposition CDC historization and legislative lifecycle analysis
# MAGIC in the Gold Analytics layer.
# MAGIC
# MAGIC This notebook consolidates analytical datasets derived from CDC/SCD Type 2
# MAGIC historical proposition tramitacao records, enabling reconstruction of
# MAGIC legislative lifecycle events, proposition status evolution and temporal
# MAGIC analysis of parliamentary processing workflows.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read historical proposition CDC and SCD Type 2 datasets
# MAGIC - Build analytical proposition historization views
# MAGIC - Create legislative lifecycle and temporal processing analytics
# MAGIC - Create proposition tramitacao duration indicators
# MAGIC - Create proposition status transition monitoring analytics
# MAGIC - Create proposition progression and alerting analytical views
# MAGIC - Support legislative workflow and parliamentary process intelligence
# MAGIC - Preserve analytical lineage and traceability metadata
# MAGIC - Persist Gold CDC analytical views and marts
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - `silver_cdc.proposicoes_tramitacoes_scd2`
# MAGIC - `silver_cdc.proposicoes_tramitacoes_base`
# MAGIC - `gold.dm_proposicao`
# MAGIC - `gold.dm_orgao`
# MAGIC - `gold.dm_data`
# MAGIC
# MAGIC ## Views / Analytical Objects
# MAGIC
# MAGIC - `gold_cdc.vw_proposicoes_tramitacao_historica`
# MAGIC - `gold_cdc.vw_tempo_tramitacao_proposicoes`
# MAGIC - `gold_cdc.vw_alertas_tramitacao_proposicoes`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake analytical layer
# MAGIC - Supports CDC/SCD Type 2 legislative lifecycle analytics
# MAGIC - Supports proposition historization and parliamentary workflow intelligence

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold_cdc.vw_proposicoes_tramitacao_historica AS
-- -----------------------------------------------------------------------------
-- View: gold_cdc.vw_proposicoes_tramitacao_historica
-- Layer: Gold CDC
--
-- Description:
-- Complete historical CDC/SCD Type 2 reconstruction view for proposicoes
-- tramitacao events.
--
-- Grain:
-- One row per proposicao tramitacao version.
--
-- Source:
-- silver_cdc.proposicoes_tramitacoes_scd2
-- -----------------------------------------------------------------------------

SELECT
    prop_id_proposicao,
    tram_id_evento,

    tram_ts_tramitacao,
    tram_dt_tramitacao,

    tram_tx_sigla_orgao,
    tram_tx_regime,

    tram_tx_descricao_tramitacao,
    tram_tx_descricao_situacao,
    tram_tx_despacho,

    cdc_payload_hash,

    valid_from,
    valid_to,
    is_current,

    scd_ts_processamento

FROM silver_cdc.proposicoes_tramitacoes_scd2
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold_cdc.vw_tempo_tramitacao_proposicoes AS
-- -----------------------------------------------------------------------------
-- View: gold_cdc.vw_tempo_tramitacao_proposicoes
-- Layer: Gold CDC
--
-- Description:
-- Analytical processing time metrics for proposicoes based on historical
-- tramitacao events.
--
-- Grain:
-- One row per proposicao.
--
-- Source:
-- gold_cdc.vw_proposicoes_tramitacao_historica
-- -----------------------------------------------------------------------------

WITH resumo AS (

    SELECT
        prop_id_proposicao,

        COUNT(*) AS prop_qt_tramitacoes,

        MIN(tram_ts_tramitacao)
            AS prop_ts_primeira_tramitacao,

        MAX(tram_ts_tramitacao)
            AS prop_ts_ultima_tramitacao,

        DATEDIFF(
            MAX(tram_ts_tramitacao),
            MIN(tram_ts_tramitacao)
        ) AS prop_qt_dias_tramitacao,

        ROUND(
            CASE
                WHEN COUNT(*) <= 1 THEN 0

                ELSE DATEDIFF(
                    MAX(tram_ts_tramitacao),
                    MIN(tram_ts_tramitacao)
                ) / (COUNT(*) - 1)
            END,
            2
        ) AS prop_qt_media_dias_por_evento

    FROM gold_cdc.vw_proposicoes_tramitacao_historica

    GROUP BY
        prop_id_proposicao
),

estado_atual AS (

    SELECT *
    FROM (

        SELECT
            prop_id_proposicao,

            tram_tx_sigla_orgao
                AS prop_sg_orgao_atual,

            tram_tx_regime
                AS prop_tx_regime_atual,

            tram_tx_descricao_situacao
                AS prop_tx_situacao_atual,

            tram_tx_descricao_tramitacao
                AS prop_tx_tramitacao_atual,

            tram_tx_despacho
                AS prop_tx_despacho_atual,

            tram_ts_tramitacao
                AS prop_ts_ultima_movimentacao,

            ROW_NUMBER() OVER (
                PARTITION BY prop_id_proposicao
                ORDER BY tram_ts_tramitacao DESC
            ) AS rn

        FROM gold_cdc.vw_proposicoes_tramitacao_historica
    )

    WHERE rn = 1
)

SELECT
    r.prop_id_proposicao,

    r.prop_qt_tramitacoes,

    r.prop_ts_primeira_tramitacao,
    r.prop_ts_ultima_tramitacao,

    r.prop_qt_dias_tramitacao,
    r.prop_qt_media_dias_por_evento,

    e.prop_sg_orgao_atual,
    e.prop_tx_regime_atual,

    e.prop_tx_situacao_atual,
    e.prop_tx_tramitacao_atual,
    e.prop_tx_despacho_atual,

    e.prop_ts_ultima_movimentacao,

    CASE
        WHEN UPPER(
            COALESCE(e.prop_tx_situacao_atual, '')
        ) LIKE '%ARQUIV%'
            THEN 'ARQUIVADA'

        WHEN UPPER(
            COALESCE(e.prop_tx_tramitacao_atual, '')
        ) LIKE '%PLEN%'
            THEN 'PLENARIO'

        ELSE 'EM_TRAMITACAO'
    END AS prop_tx_status_analitico

FROM resumo r

LEFT JOIN estado_atual e
    ON r.prop_id_proposicao = e.prop_id_proposicao
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold_cdc.vw_alertas_tramitacao_proposicoes AS
-- -----------------------------------------------------------------------------
-- View: gold_cdc.vw_alertas_tramitacao_proposicoes
-- Layer: Gold CDC
--
-- Description:
-- Automatic proposicao movement alert classification view.
--
-- Grain:
-- One row per proposicao tramitacao event.
--
-- Source:
-- gold_cdc.vw_proposicoes_tramitacao_historica
-- -----------------------------------------------------------------------------

SELECT
    prop_id_proposicao,
    tram_id_evento,

    tram_ts_tramitacao,
    tram_dt_tramitacao,

    tram_tx_sigla_orgao,

    tram_tx_descricao_tramitacao,
    tram_tx_descricao_situacao,
    tram_tx_despacho,

    CASE
        WHEN UPPER(
            COALESCE(tram_tx_descricao_situacao, '')
        ) LIKE '%PLEN%'
            THEN 'AVANCO_PLENARIO'

        WHEN UPPER(
            COALESCE(tram_tx_descricao_situacao, '')
        ) LIKE '%ARQUIV%'
            THEN 'ARQUIVAMENTO'

        ELSE 'OUTRA_MOVIMENTACAO'
    END AS alert_tx_tipo,

    CASE
        WHEN UPPER(
            COALESCE(tram_tx_descricao_situacao, '')
        ) LIKE '%PLEN%'
            THEN 'Proposição avançou para Plenário.'

        WHEN UPPER(
            COALESCE(tram_tx_descricao_situacao, '')
        ) LIKE '%ARQUIV%'
            THEN 'Proposição foi arquivada.'

        ELSE 'Movimentação registrada.'
    END AS alert_tx_mensagem,

    CASE
        WHEN UPPER(
            COALESCE(tram_tx_descricao_situacao, '')
        ) LIKE '%PLEN%'
            THEN 1

        WHEN UPPER(
            COALESCE(tram_tx_descricao_situacao, '')
        ) LIKE '%ARQUIV%'
            THEN 1

        ELSE 0
    END AS alert_fl_notificar

FROM gold_cdc.vw_proposicoes_tramitacao_historica
""")