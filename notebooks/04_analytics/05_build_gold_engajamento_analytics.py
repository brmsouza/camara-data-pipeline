# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics Layer — Parliamentary Engagement Intelligence
# MAGIC
# MAGIC **Notebook:** `05_build_gold_engajamento_analytics`
# MAGIC
# MAGIC Builds advanced analytical views and Parliamentary Intelligence products
# MAGIC related to parliamentary engagement, transparency and legislative activity
# MAGIC in the Gold Analytics layer.
# MAGIC
# MAGIC This notebook consolidates analytical datasets derived from Gold fact and
# MAGIC dimension tables related to parliamentary activity, voting participation,
# MAGIC event attendance, transparency indicators and engagement behavior.
# MAGIC The resulting analytical products support engagement scoring, parliamentary
# MAGIC efficiency analysis, transparency monitoring and absenteeism intelligence.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read Gold parliamentary activity fact and dimension tables
# MAGIC - Build analytical parliamentary engagement views
# MAGIC - Create engagement scoring and efficiency indicators
# MAGIC - Create transparency and accountability metrics
# MAGIC - Create absenteeism and critical voting absence analytics
# MAGIC - Support parliamentary behavior and institutional performance analysis
# MAGIC - Support political accountability and engagement monitoring
# MAGIC - Preserve analytical lineage and traceability metadata
# MAGIC - Persist Gold analytical views and marts
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - `gold.ft_atividade_parlamentar`
# MAGIC - `gold.ft_votos`
# MAGIC - `gold.ft_presenca_eventos`
# MAGIC - `gold.ft_votacoes`
# MAGIC - `gold.dm_deputado`
# MAGIC - `gold.dm_partido`
# MAGIC - `gold.dm_legislatura`
# MAGIC - `gold.dm_data`
# MAGIC - `gold.dm_evento`
# MAGIC
# MAGIC ## Views / Analytical Objects
# MAGIC
# MAGIC - `gold.vw_atividade_parlamentar_analitica`
# MAGIC - `gold.vw_indice_eficiencia_parlamentar`
# MAGIC - `gold.vw_indice_transparencia`
# MAGIC - `gold.vw_ausencias_votacoes_criticas`
# MAGIC - `gold.vw_ranking_ausencias_criticas`
# MAGIC - `gold.vw_score_engajamento_parlamentar`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake analytical layer
# MAGIC - Supports Parliamentary Intelligence and engagement analytics
# MAGIC - Supports transparency, efficiency and absenteeism analysis

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_atividade_parlamentar_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_atividade_parlamentar_analitica
-- Layer: Gold
--
-- Description:
-- Consolidated parliamentary activity analytical view combining expenses,
-- voting participation and parliamentary front indicators.
--
-- Grain:
-- One row per deputy.
--
-- Sources:
-- gold.dm_deputado
-- gold.vw_despesas_ceap_analitica
-- gold.vw_votos_deputados_analitica
-- gold.vw_frentes_membros_analitica
-- -----------------------------------------------------------------------------

WITH deputados AS (

    SELECT
        dept.id_deputado,
        dept.sk_dept,
        dept.dept_tx_nome_parlamentar,
        part.part_sg_partido,
        uf.uf_sg_uf

    FROM gold.dm_deputado dept

    LEFT JOIN gold.dm_partido part
        ON dept.part_sg_partido = part.part_sg_partido

    LEFT JOIN gold.dm_uf uf
        ON dept.uf_sg_uf = uf.uf_sg_uf
),

despesas AS (

    SELECT
        id_deputado,

        COUNT(DISTINCT desp_id_documento) AS qt_despesas,
        SUM(desp_vl_liquido) AS vl_total_liquido

    FROM gold.vw_despesas_ceap_analitica

    WHERE resp_tx_tipo_responsavel = 'DEPUTADO'

    GROUP BY
        id_deputado
),

votos AS (

    SELECT
        id_deputado,

        COUNT(DISTINCT vot_id_votacao) AS qt_votacoes,

        SUM(
            CASE
                WHEN vot_tx_voto_curado IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS qt_presencas_votacoes,

        SUM(vot_fl_sim) AS qt_votos_sim,
        SUM(vot_fl_nao) AS qt_votos_nao,
        SUM(vot_fl_abstencao) AS qt_votos_abstencao,
        SUM(vot_fl_obstrucao) AS qt_votos_obstrucao

    FROM gold.vw_votos_deputados_analitica

    GROUP BY
        id_deputado
),

frentes AS (

    SELECT
        dept_id_deputado AS id_deputado,

        COUNT(DISTINCT frente_id_frente) AS qt_frentes,
        MAX(memb_fl_coordenador) AS fl_coordenador_frente,
        MAX(memb_fl_presidente) AS fl_presidente_frente,
        MAX(memb_fl_vice) AS fl_vice_frente

    FROM gold.vw_frentes_membros_analitica

    GROUP BY
        dept_id_deputado
)

SELECT
    dept.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    dept.part_sg_partido,
    dept.uf_sg_uf,

    COALESCE(d.qt_despesas, 0) AS qt_despesas,
    COALESCE(d.vl_total_liquido, 0) AS vl_total_liquido,

    COALESCE(v.qt_votacoes, 0) AS qt_votacoes,
    COALESCE(v.qt_presencas_votacoes, 0) AS qt_presencas_votacoes,
    COALESCE(v.qt_votos_sim, 0) AS qt_votos_sim,
    COALESCE(v.qt_votos_nao, 0) AS qt_votos_nao,
    COALESCE(v.qt_votos_abstencao, 0) AS qt_votos_abstencao,
    COALESCE(v.qt_votos_obstrucao, 0) AS qt_votos_obstrucao,

    COALESCE(f.qt_frentes, 0) AS qt_frentes,
    COALESCE(f.fl_coordenador_frente, 0) AS fl_coordenador_frente,
    COALESCE(f.fl_presidente_frente, 0) AS fl_presidente_frente,
    COALESCE(f.fl_vice_frente, 0) AS fl_vice_frente

FROM deputados dept

LEFT JOIN despesas d
    ON dept.id_deputado = d.id_deputado

LEFT JOIN votos v
    ON dept.id_deputado = v.id_deputado

LEFT JOIN frentes f
    ON dept.id_deputado = f.id_deputado
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_indice_eficiencia_parlamentar AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_indice_eficiencia_parlamentar
-- Layer: Gold
--
-- Description:
-- Composite parliamentary activity indicator normalized by CEAP expense volume.
-- This metric acts as a proxy for activity intensity relative to declared
-- parliamentary reimbursement expenses and should not be interpreted as an
-- absolute measure of legislative performance or institutional effectiveness.
--
-- Grain:
-- One row per deputy.
--
-- Source:
-- gold.vw_atividade_parlamentar_analitica
-- -----------------------------------------------------------------------------

SELECT
    *,

    (
        qt_votacoes
        + (qt_frentes * 10)
        + (fl_presidente_frente * 20)
        + (fl_coordenador_frente * 15)
        + (fl_vice_frente * 10)
    ) AS parlamentar_score_atividade,

    ROUND(
        (
            qt_votacoes
            + (qt_frentes * 10)
            + (fl_presidente_frente * 20)
            + (fl_coordenador_frente * 15)
            + (fl_vice_frente * 10)
        )
        / NULLIF(vl_total_liquido, 0) * 1000,
        4
    ) AS indice_eficiencia_parlamentar

FROM gold.vw_atividade_parlamentar_analitica
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_indice_transparencia AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_indice_transparencia
-- Layer: Gold
--
-- Description:
-- Transparency indicator based on expense documentation and reimbursement.
--
-- Grain:
-- One row per deputy.
--
-- Sources:
-- gold.vw_despesas_ceap_analitica
-- gold.dm_deputado
-- gold.dm_partido
-- gold.dm_uf
-- -----------------------------------------------------------------------------

WITH base AS (

    SELECT
        id_deputado,

        COUNT(DISTINCT desp_id_documento) AS qt_documentos,

        SUM(desp_fl_possui_documento_url) AS qt_documentos_comprovados,
        SUM(desp_fl_possui_glosa) AS qt_glosas,
        SUM(desp_fl_possui_restituicao) AS qt_restituicoes,

        ROUND(
            SUM(desp_fl_possui_documento_url)
            / NULLIF(
                COUNT(DISTINCT desp_id_documento),
                0
            ) * 100,
            2
        ) AS pc_documentacao

    FROM gold.vw_despesas_ceap_analitica

    GROUP BY
        id_deputado
)

SELECT
    base.id_deputado,

    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,

    base.qt_documentos,
    base.qt_documentos_comprovados,
    base.qt_glosas,
    base.qt_restituicoes,
    base.pc_documentacao

FROM base

LEFT JOIN gold.dm_deputado dept
    ON base.id_deputado = dept.id_deputado

LEFT JOIN gold.dm_partido part
    ON dept.part_sg_partido = part.part_sg_partido

LEFT JOIN gold.dm_uf uf
    ON dept.uf_sg_uf = uf.uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_ausencias_votacoes_criticas AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_ausencias_votacoes_criticas
-- Layer: Gold
--
-- Description:
-- Identifies deputy absences in important parliamentary voting sessions.
--
-- Grain:
-- One row per deputy.
--
-- Source:
-- gold.vw_votos_deputados_analitica
-- -----------------------------------------------------------------------------

SELECT
    id_deputado,
    dept_tx_nome_parlamentar,

    part_sg_partido,
    uf_sg_uf,

    COUNT(DISTINCT vot_id_votacao) AS qt_votacoes,

    SUM(
        CASE
            WHEN vot_fl_presenca = 0 THEN 1
            ELSE 0
        END
    ) AS qt_ausencias,

    ROUND(
        SUM(
            CASE
                WHEN vot_fl_presenca = 0 THEN 1
                ELSE 0
            END
        )
        / NULLIF(
            COUNT(DISTINCT vot_id_votacao),
            0
        ) * 100,
        2
    ) AS pc_ausencia

FROM gold.vw_votos_deputados_analitica

GROUP BY
    id_deputado,
    dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_ranking_ausencias_criticas AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_ranking_ausencias_criticas
-- Layer: Gold
--
-- Description:
-- Ranking of deputies with the highest absence rates in voting sessions.
--
-- Grain:
-- One row per deputy.
--
-- Source:
-- gold.vw_ausencias_votacoes_criticas
-- -----------------------------------------------------------------------------

SELECT
    *,

    ROW_NUMBER() OVER (
        ORDER BY pc_ausencia DESC
    ) AS ranking_ausencias

FROM gold.vw_ausencias_votacoes_criticas
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_score_engajamento_parlamentar AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_score_engajamento_parlamentar
-- Layer: Gold
--
-- Description:
-- Composite parliamentary engagement score combining voting activity,
-- voting presence, decisive voting participation and parliamentary front activity.
-- CEAP expenses are intentionally not used as a positive engagement component
-- to avoid rewarding higher reimbursement volume.
--
-- Grain:
-- One row per deputy.
--
-- Sources:
-- gold.vw_atividade_parlamentar_analitica
-- gold.vw_fidelidade_partidaria
-- -----------------------------------------------------------------------------

WITH base AS (

    SELECT
        ativ.sk_dept,
        ativ.id_deputado,
        ativ.dept_tx_nome_parlamentar,

        ativ.part_sg_partido,
        ativ.uf_sg_uf,

        ativ.qt_votacoes,
        ativ.qt_presencas_votacoes,
        ativ.qt_frentes,

        fid.pc_participacao_decisiva

    FROM gold.vw_atividade_parlamentar_analitica ativ

    LEFT JOIN gold.vw_fidelidade_partidaria fid
        ON ativ.id_deputado = fid.id_deputado
),

maximos AS (

    SELECT
        MAX(qt_votacoes) AS max_qt_votacoes,
        MAX(qt_presencas_votacoes) AS max_qt_presencas_votacoes,
        MAX(qt_frentes) AS max_qt_frentes

    FROM base
),

scores AS (

    SELECT
        b.*,

        ROUND(
            COALESCE(b.qt_votacoes, 0)
            / NULLIF(m.max_qt_votacoes, 0),
            4
        ) AS score_votacoes,

        ROUND(
            COALESCE(b.qt_presencas_votacoes, 0)
            / NULLIF(m.max_qt_presencas_votacoes, 0),
            4
        ) AS score_presenca_votacoes,

        ROUND(
            COALESCE(b.pc_participacao_decisiva, 0) / 100,
            4
        ) AS score_participacao_decisiva,

        ROUND(
            COALESCE(b.qt_frentes, 0)
            / NULLIF(m.max_qt_frentes, 0),
            4
        ) AS score_frentes

    FROM base b

    CROSS JOIN maximos m
),

final AS (

    SELECT
        *,

        ROUND(
            (
                score_votacoes * 0.35
                + score_presenca_votacoes * 0.30
                + score_participacao_decisiva * 0.20
                + score_frentes * 0.15
            ) * 100,
            2
        ) AS score_engajamento

    FROM scores
)

SELECT
    *,

    CASE
        WHEN score_engajamento >= 75
            THEN 'Alto engajamento'

        WHEN score_engajamento >= 40
            THEN 'Médio engajamento'

        ELSE 'Baixo engajamento'
    END AS tx_faixa_engajamento

FROM final
""")