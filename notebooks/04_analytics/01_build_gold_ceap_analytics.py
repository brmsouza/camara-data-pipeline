# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics Layer — CEAP Parliamentary Expense Intelligence
# MAGIC
# MAGIC **Notebook:** `01_build_gold_ceap_analytics`
# MAGIC
# MAGIC Builds advanced analytical views and Parliamentary Intelligence products
# MAGIC related to CEAP parliamentary expenses in the Gold Analytics layer.
# MAGIC
# MAGIC This notebook consolidates analytical datasets derived from Gold fact and
# MAGIC dimension tables related to parliamentary expenses, suppliers, political
# MAGIC parties, expenditure segmentation and anomaly detection. The resulting
# MAGIC analytical products support transparency analysis, supplier intelligence,
# MAGIC financial behavior analysis, political expenditure profiling and spending
# MAGIC monitoring across deputies and parties.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read Gold CEAP fact and dimension tables
# MAGIC - Build analytical expenditure aggregation views
# MAGIC - Create supplier intelligence and anomaly detection indicators
# MAGIC - Create deputy and political party spending segmentation analytics
# MAGIC - Create monthly expenditure rankings and comparative views
# MAGIC - Support parliamentary transparency and accountability analysis
# MAGIC - Support political expenditure intelligence and behavioral analysis
# MAGIC - Preserve analytical lineage and traceability metadata
# MAGIC - Persist Gold analytical views and marts
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - `gold.ft_despesas_ceap`
# MAGIC - `gold.dm_deputado`
# MAGIC - `gold.dm_fornecedor`
# MAGIC - `gold.dm_partido`
# MAGIC - `gold.dm_tipo_despesa`
# MAGIC - `gold.dm_data`
# MAGIC - `gold.dm_uf`
# MAGIC
# MAGIC ## Views / Analytical Objects
# MAGIC
# MAGIC - `gold.vw_despesas_ceap_analitica`
# MAGIC - `gold.vw_ranking_despesas_deputado_mensal`
# MAGIC - `gold.vw_despesas_deputado_segmento`
# MAGIC - `gold.vw_gastos_segmentados`
# MAGIC - `gold.vw_partidos_despesas_segmento`
# MAGIC - `gold.vw_perfil_gasto_partido`
# MAGIC - `gold.vw_anomalias_ceap_zscore`
# MAGIC - `gold.vw_top_10_gastos_partido_mensal`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake analytical layer
# MAGIC - Supports Parliamentary Intelligence and CEAP analytics
# MAGIC - Supports anomaly detection and expenditure segmentation analysis

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_despesas_ceap_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_despesas_ceap_analitica
-- Layer: Gold
--
-- Description:
-- Analytical CEAP expense view combining the CEAP expense fact table with
-- conformed Gold dimensions, including supplier CNPJ validation flags.
-- This view preserves the operational CEAP granularity from the fact table:
-- the same document identifier may appear more than once due to installments,
-- complementary reimbursements or administrative breakdowns.
-- Records with desp_id_documento = 0 are preserved, but should be interpreted
-- as non-identifiable document records for document-level uniqueness analysis.
--
-- Grain:
-- One row per CEAP expense record from gold.ft_despesas_ceap.
--
-- Sources:
-- gold.ft_despesas_ceap
-- gold.dm_responsavel_ceap
-- gold.dm_data
-- gold.dm_deputado
-- gold.dm_partido
-- gold.dm_legislatura
-- gold.dm_uf
-- gold.dm_fornecedor
-- gold.dm_tipo_despesa
-- -----------------------------------------------------------------------------

SELECT
    ft.sk_resp_ceap,
    ft.sk_dept,
    ft.sk_part,
    ft.sk_leg,
    ft.sk_forn,
    ft.sk_desp_tipo,
    ft.sk_uf,
    ft.sk_data_emissao,

    d.data_dt_data AS desp_dt_emissao,
    d.data_nr_ano AS desp_nr_ano,
    d.data_nr_mes AS desp_nr_mes,
    d.data_tx_ano_mes,

    resp.resp_tx_tipo_responsavel,
    resp.resp_tx_nome_responsavel,
    resp.id_deputado,
    resp.id_cadastro_ceap,
    resp.id_deputado_ceap,
    resp.resp_nr_cpf,

    dept.dept_tx_nome_parlamentar,
    dept.dept_tx_status_mandato_curado,

    part.part_sg_partido,
    leg.leg_id_legislatura,
    leg.leg_nr_ano_eleicao,
    uf.uf_sg_uf,

    forn.forn_nr_cnpj_cpf,
    forn.forn_tx_nome,
    forn.forn_tx_tipo_documento,
    forn.forn_fl_documento_valido,
    forn.forn_fl_documento_repetido,

    forn.forn_tx_status_consulta_cnpj,
    forn.forn_cd_http_status_cnpj,
    forn.forn_tx_erro_consulta_cnpj,

    forn.forn_fl_cnpj_encontrado,
    forn.forn_fl_cnpj_ativo,
    forn.forn_fl_cnpj_suspeito,
    forn.forn_tx_motivo_cnpj_suspeito,

    forn.forn_tx_razao_social_receita,
    forn.forn_tx_nome_fantasia_receita,
    forn.forn_tx_situacao_cadastral,
    forn.forn_tx_cnae_principal,
    forn.forn_sg_uf_receita,
    forn.forn_tx_municipio_receita,
    forn.forn_tx_porte_empresa,
    forn.forn_vl_capital_social,

    tipo.desp_cd_subcota,
    tipo.desp_tx_segmento_despesa,
    tipo.desp_tx_tipo_despesa,
    tipo.desp_cd_especificacao_subcota,
    tipo.desp_tx_especificacao,

    ft.desp_id_documento,

    CASE
        WHEN ft.desp_id_documento = 0 THEN 1
        ELSE 0
    END AS desp_fl_documento_nao_identificado,

    ft.desp_nr_documento,
    ft.desp_cd_tipo_documento,
    ft.desp_tx_url_documento,
    ft.desp_dt_emissao AS desp_dt_emissao_original,
    ft.desp_nr_parcela,

    ft.desp_vl_documento,
    ft.desp_vl_glosa,
    ft.desp_vl_liquido,
    ft.desp_vl_restituicao,

    ft.desp_fl_possui_glosa,
    ft.desp_fl_possui_restituicao,
    ft.desp_fl_valor_negativo,
    ft.desp_fl_possui_documento_url,

    ft.desp_tx_passageiro,
    ft.desp_tx_trecho,
    ft.desp_nr_lote,
    ft.desp_nr_ressarcimento,
    ft.desp_dt_pagamento_restituicao,
    ft.desp_tx_dedup_key,

    ft.bronze_id_origem,
    ft.bronze_tx_source_file,
    ft.bronze_nr_ano_referencia,
    ft.bronze_id_batch,
    ft.bronze_tx_record_hash,

    ft.gold_ts_processamento,
    ft.gold_id_batch

FROM gold.ft_despesas_ceap ft

LEFT JOIN gold.dm_responsavel_ceap resp
    ON ft.sk_resp_ceap = resp.sk_resp_ceap

LEFT JOIN gold.dm_data d
    ON ft.sk_data_emissao = d.sk_data

LEFT JOIN gold.dm_deputado dept
    ON ft.sk_dept = dept.sk_dept

LEFT JOIN gold.dm_partido part
    ON ft.sk_part = part.sk_part

LEFT JOIN gold.dm_legislatura leg
    ON ft.sk_leg = leg.sk_leg

LEFT JOIN gold.dm_uf uf
    ON ft.sk_uf = uf.sk_uf

LEFT JOIN gold.dm_fornecedor forn
    ON ft.sk_forn = forn.sk_forn

LEFT JOIN gold.dm_tipo_despesa tipo
    ON ft.sk_desp_tipo = tipo.sk_desp_tipo
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_ranking_despesas_deputado_mensal AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_ranking_despesas_deputado_mensal
-- Layer: Gold
--
-- Description:
-- Monthly CEAP expense ranking by deputy.
--
-- Grain:
-- One row per month and deputy.
--
-- Source:
-- gold.vw_despesas_ceap_analitica
-- -----------------------------------------------------------------------------

SELECT
    data_tx_ano_mes,
    desp_nr_ano,
    desp_nr_mes,

    sk_resp_ceap,
    id_deputado,
    resp_tx_nome_responsavel AS dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf,

    COUNT(DISTINCT desp_id_documento) AS qt_documentos,
    SUM(desp_vl_documento) AS vl_total_documento,
    SUM(desp_vl_glosa) AS vl_total_glosa,
    SUM(desp_vl_liquido) AS vl_total_liquido,
    SUM(desp_vl_restituicao) AS vl_total_restituicao,
    AVG(desp_vl_liquido) AS vl_medio_liquido,

    SUM(desp_fl_possui_glosa) AS qt_despesas_com_glosa,
    SUM(desp_fl_possui_restituicao) AS qt_despesas_com_restituicao

FROM gold.vw_despesas_ceap_analitica

WHERE resp_tx_tipo_responsavel = 'DEPUTADO'

GROUP BY
    data_tx_ano_mes,
    desp_nr_ano,
    desp_nr_mes,
    sk_resp_ceap,
    id_deputado,
    resp_tx_nome_responsavel,
    part_sg_partido,
    uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_despesas_deputado_segmento AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_despesas_deputado_segmento
-- Layer: Gold
--
-- Description:
-- Analytical CEAP expense view aggregated by deputy, expense segment and month.
--
-- Grain:
-- One row per deputy, expense segment and month.
--
-- Source:
-- gold.vw_despesas_ceap_analitica
-- -----------------------------------------------------------------------------

SELECT
    sk_resp_ceap,
    id_deputado,
    resp_tx_nome_responsavel AS dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf,
    desp_tx_segmento_despesa,
    data_tx_ano_mes,

    COUNT(DISTINCT desp_id_documento) AS qt_documentos,
    COUNT(DISTINCT sk_forn) AS qt_fornecedores,
    SUM(desp_vl_liquido) AS vl_total_liquido,
    AVG(desp_vl_liquido) AS vl_medio_liquido,

    ROUND(
        SUM(desp_vl_liquido)
        / NULLIF(
            SUM(SUM(desp_vl_liquido)) OVER (
                PARTITION BY id_deputado, data_tx_ano_mes
            ),
            0
        ) * 100,
        2
    ) AS perc_participacao_segmento

FROM gold.vw_despesas_ceap_analitica

WHERE resp_tx_tipo_responsavel = 'DEPUTADO'

GROUP BY
    sk_resp_ceap,
    id_deputado,
    resp_tx_nome_responsavel,
    part_sg_partido,
    uf_sg_uf,
    desp_tx_segmento_despesa,
    data_tx_ano_mes
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_gastos_segmentados AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_gastos_segmentados
-- Layer: Gold
--
-- Description:
-- Analytical CEAP expense view grouped by deputy, month and expense segment.
--
-- Grain:
-- One row per deputy, month, expense segment and expense type.
--
-- Source:
-- gold.vw_despesas_ceap_analitica
-- -----------------------------------------------------------------------------

SELECT
    sk_resp_ceap,
    id_deputado,
    resp_tx_nome_responsavel AS dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf,

    desp_nr_ano AS data_nr_ano,
    desp_nr_mes AS data_nr_mes,
    data_tx_ano_mes,

    desp_tx_segmento_despesa,
    desp_tx_tipo_despesa,

    COUNT(DISTINCT desp_id_documento) AS qt_documentos,
    COUNT(DISTINCT sk_forn) AS qt_fornecedores,

    SUM(desp_vl_documento) AS desp_vl_total_documento,
    SUM(desp_vl_glosa) AS desp_vl_total_glosa,
    SUM(desp_vl_liquido) AS desp_vl_total_liquido,
    SUM(desp_vl_restituicao) AS desp_vl_total_restituicao,

    AVG(desp_vl_liquido) AS desp_vl_medio_liquido,

    ROUND(
        SUM(desp_vl_liquido)
        / NULLIF(
            SUM(SUM(desp_vl_liquido)) OVER (
                PARTITION BY sk_resp_ceap, data_tx_ano_mes
            ),
            0
        ) * 100,
        2
    ) AS desp_pc_participacao_segmento

FROM gold.vw_despesas_ceap_analitica
WHERE resp_tx_tipo_responsavel = 'DEPUTADO'

GROUP BY
    sk_resp_ceap,
    id_deputado,
    resp_tx_nome_responsavel,
    part_sg_partido,
    uf_sg_uf,
    desp_nr_ano,
    desp_nr_mes,
    data_tx_ano_mes,
    desp_tx_segmento_despesa,
    desp_tx_tipo_despesa
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_partidos_despesas_segmento AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_partidos_despesas_segmento
-- Layer: Gold
--
-- Description:
-- Analytical party expense view aggregated by expense segment and month.
--
-- Grain:
-- One row per party, month, expense segment and expense type.
--
-- Source:
-- gold.vw_despesas_ceap_analitica
-- -----------------------------------------------------------------------------

SELECT
    part_sg_partido,

    desp_tx_segmento_despesa,
    desp_tx_tipo_despesa,

    desp_nr_ano AS data_nr_ano,
    data_tx_ano_mes,

    COUNT(DISTINCT sk_resp_ceap) AS part_qt_responsaveis,
    COUNT(DISTINCT sk_dept) AS part_qt_deputados,
    COUNT(DISTINCT desp_id_documento) AS part_qt_documentos,
    COUNT(DISTINCT sk_forn) AS part_qt_fornecedores,

    SUM(desp_vl_documento) AS part_vl_total_documento,
    SUM(desp_vl_glosa) AS part_vl_total_glosa,
    SUM(desp_vl_liquido) AS part_vl_total_liquido,

    AVG(desp_vl_liquido) AS part_vl_medio_liquido,

    ROUND(
        SUM(desp_vl_liquido)
        / NULLIF(
            SUM(SUM(desp_vl_liquido)) OVER (
                PARTITION BY part_sg_partido, data_tx_ano_mes
            ),
            0
        ) * 100,
        2
    ) AS part_pc_participacao_segmento

FROM gold.vw_despesas_ceap_analitica

WHERE resp_tx_tipo_responsavel = 'DEPUTADO'

GROUP BY
    part_sg_partido,
    desp_tx_segmento_despesa,
    desp_tx_tipo_despesa,
    desp_nr_ano,
    data_tx_ano_mes
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_perfil_gasto_partido AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_perfil_gasto_partido
-- Layer: Gold
--
-- Description:
-- Analytical party expense profile by CEAP expense segment.
--
-- Grain:
-- One row per political party and expense segment.
--
-- Source:
-- gold.vw_despesas_ceap_analitica
-- -----------------------------------------------------------------------------

WITH base AS (

    SELECT
        part_sg_partido,
        desp_tx_segmento_despesa,

        COUNT(DISTINCT sk_resp_ceap) AS part_qt_responsaveis,
        COUNT(DISTINCT sk_dept) AS part_qt_deputados,
        COUNT(DISTINCT desp_id_documento) AS part_qt_documentos,
        COUNT(DISTINCT sk_forn) AS part_qt_fornecedores,

        SUM(desp_vl_liquido) AS part_vl_total_liquido,
        AVG(desp_vl_liquido) AS part_vl_medio_liquido,
        MAX(desp_vl_liquido) AS part_vl_max_liquido

    FROM gold.vw_despesas_ceap_analitica

    WHERE resp_tx_tipo_responsavel = 'DEPUTADO'

    GROUP BY
        part_sg_partido,
        desp_tx_segmento_despesa
),

total_partido AS (

    SELECT
        part_sg_partido,
        SUM(part_vl_total_liquido) AS part_vl_total_geral

    FROM base

    GROUP BY
        part_sg_partido
),

perfil AS (

    SELECT
        b.*,
        t.part_vl_total_geral,

        ROUND(
            b.part_vl_total_liquido
            / NULLIF(t.part_vl_total_geral, 0) * 100,
            2
        ) AS part_pc_segmento,

        ROW_NUMBER() OVER (
            PARTITION BY b.part_sg_partido
            ORDER BY b.part_vl_total_liquido DESC
        ) AS part_nr_ranking_segmento

    FROM base b

    LEFT JOIN total_partido t
        ON b.part_sg_partido = t.part_sg_partido
)

SELECT
    part_sg_partido,
    desp_tx_segmento_despesa,

    part_qt_responsaveis,
    part_qt_deputados,
    part_qt_documentos,
    part_qt_fornecedores,

    part_vl_total_liquido,
    part_vl_medio_liquido,
    part_vl_max_liquido,
    part_vl_total_geral,
    part_pc_segmento,
    part_nr_ranking_segmento,

    CASE
        WHEN part_nr_ranking_segmento = 1
            THEN 'Principal categoria de gasto'

        WHEN part_nr_ranking_segmento <= 3
            THEN 'Categoria relevante'

        ELSE 'Categoria complementar'
    END AS part_tx_relevancia_segmento

FROM perfil
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_anomalias_ceap_zscore AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_anomalias_ceap_zscore
-- Layer: Gold
--
-- Description:
-- Analytical anomaly detection view based on CEAP expense behavior using
-- Z-Score statistical deviation by expense category and UF.
-- The model combines financial outlier behavior with supplier validation
-- indicators in order to support analytical prioritization and exploratory
-- investigation workflows.
--
-- This model is intended for analytical and monitoring purposes only and
-- should not be interpreted as proof of fraud, misconduct or legal
-- irregularity.
--
-- Grain:
-- One row per deputy/category/UF/supplier-validation analytical aggregation.
--
-- Sources:
-- gold.vw_despesas_ceap_analitica
-- -----------------------------------------------------------------------------

WITH base AS (

    SELECT
        sk_resp_ceap,

        id_deputado,
        dept_tx_nome_parlamentar,

        part_sg_partido,
        uf_sg_uf,

        desp_tx_segmento_despesa,
        desp_tx_tipo_despesa,

        forn_fl_cnpj_suspeito,
        forn_tx_motivo_cnpj_suspeito,
        forn_tx_status_consulta_cnpj,
        forn_tx_situacao_cadastral,

        COUNT(DISTINCT forn_nr_cnpj_cpf) AS qt_fornecedores,
        COUNT(DISTINCT desp_id_documento) AS qt_documentos,

        SUM(desp_vl_liquido) AS desp_vl_total_liquido

    FROM gold.vw_despesas_ceap_analitica

    WHERE desp_id_documento <> 0

    GROUP BY
        sk_resp_ceap,
        id_deputado,
        dept_tx_nome_parlamentar,
        part_sg_partido,
        uf_sg_uf,
        desp_tx_segmento_despesa,
        desp_tx_tipo_despesa,
        forn_fl_cnpj_suspeito,
        forn_tx_motivo_cnpj_suspeito,
        forn_tx_status_consulta_cnpj,
        forn_tx_situacao_cadastral
),

estatisticas AS (

    SELECT
        desp_tx_tipo_despesa,
        uf_sg_uf,

        AVG(desp_vl_total_liquido) AS vl_media_categoria_uf,
        STDDEV(desp_vl_total_liquido) AS vl_desvio_categoria_uf

    FROM base

    GROUP BY
        desp_tx_tipo_despesa,
        uf_sg_uf
),

zscore AS (

    SELECT
        b.*,

        e.vl_media_categoria_uf,
        e.vl_desvio_categoria_uf,

        ROUND(
            (
                b.desp_vl_total_liquido
                - e.vl_media_categoria_uf
            )
            / NULLIF(e.vl_desvio_categoria_uf, 0),
            4
        ) AS desp_nr_zscore

    FROM base b

    LEFT JOIN estatisticas e
        ON b.desp_tx_tipo_despesa = e.desp_tx_tipo_despesa
       AND b.uf_sg_uf = e.uf_sg_uf
)

SELECT
    *,

    CASE
        WHEN desp_nr_zscore >= 3
             AND forn_fl_cnpj_suspeito = 1
            THEN 'Anomalia crítica: outlier financeiro com fornecedor suspeito'

        WHEN desp_nr_zscore >= 3
            THEN 'Anomalia financeira extrema'

        WHEN desp_nr_zscore >= 2
             AND forn_fl_cnpj_suspeito = 1
            THEN 'Possível anomalia com fornecedor suspeito'

        WHEN desp_nr_zscore >= 2
            THEN 'Possível anomalia financeira'

        WHEN forn_fl_cnpj_suspeito = 1
            THEN 'Fornecedor com CNPJ suspeito'

        ELSE 'Comportamento esperado'
    END AS desp_tx_classificacao_anomalia

FROM zscore
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_top_10_gastos_partido_mensal AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_top_10_gastos_partido_mensal
-- Layer: Gold
--
-- Description:
-- Monthly ranking of highest CEAP expenses by political party.
--
-- Grain:
-- One row per deputy, month and party.
--
-- Source:
-- gold.vw_despesas_ceap_analitica
-- -----------------------------------------------------------------------------

WITH base AS (

    SELECT
        part_sg_partido,

        desp_nr_ano,
        desp_nr_mes,
        data_tx_ano_mes,

        sk_resp_ceap,
        id_deputado,
        resp_tx_nome_responsavel AS dept_tx_nome_parlamentar,

        SUM(desp_vl_liquido) AS desp_vl_total_liquido

    FROM gold.vw_despesas_ceap_analitica

    WHERE resp_tx_tipo_responsavel = 'DEPUTADO'

    GROUP BY
        part_sg_partido,
        desp_nr_ano,
        desp_nr_mes,
        data_tx_ano_mes,
        sk_resp_ceap,
        id_deputado,
        resp_tx_nome_responsavel
),

ranking AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY part_sg_partido, data_tx_ano_mes
            ORDER BY desp_vl_total_liquido DESC
        ) AS rn

    FROM base
)

SELECT
    *
FROM ranking
WHERE rn <= 10
""")