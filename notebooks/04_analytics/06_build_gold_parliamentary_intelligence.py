# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics Layer — Parliamentary Intelligence Executive Analytics
# MAGIC
# MAGIC **Notebook:** `06_build_gold_parliamentary_intelligence`
# MAGIC
# MAGIC Builds executive-level Parliamentary Intelligence analytical views and
# MAGIC strategic political analytics products in the Gold Analytics layer.
# MAGIC
# MAGIC This notebook consolidates analytical datasets derived from Gold fact and
# MAGIC dimension tables related to parliamentary behavior, political parties,
# MAGIC engagement, efficiency, transparency and institutional performance.
# MAGIC The resulting analytical products support executive dashboards, political
# MAGIC profiling, parliamentary segmentation and strategic legislative intelligence.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read Gold parliamentary fact and dimension tables
# MAGIC - Build executive-level Parliamentary Intelligence analytical views
# MAGIC - Create parliamentary behavioral and political profile analytics
# MAGIC - Create political party segmentation and institutional analysis
# MAGIC - Create executive dashboard aggregation views
# MAGIC - Create parliamentary inefficiency and performance indicators
# MAGIC - Support strategic legislative intelligence analysis
# MAGIC - Support political behavior and institutional performance monitoring
# MAGIC - Preserve analytical lineage and traceability metadata
# MAGIC - Persist Gold analytical views and marts
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - `gold.ft_atividade_parlamentar`
# MAGIC - `gold.ft_despesas_ceap`
# MAGIC - `gold.ft_votos`
# MAGIC - `gold.ft_presenca_eventos`
# MAGIC - `gold.dm_deputado`
# MAGIC - `gold.dm_partido`
# MAGIC - `gold.dm_legislatura`
# MAGIC - `gold.dm_uf`
# MAGIC - `gold.dm_data`
# MAGIC
# MAGIC ## Views / Analytical Objects
# MAGIC
# MAGIC - `gold.vw_perfil_parlamentar`
# MAGIC - `gold.vw_partidos_analitica`
# MAGIC - `gold.vw_partidos_perfil`
# MAGIC - `gold.vw_dashboard_partidos`
# MAGIC - `gold.vw_analise_ineficiencia_parlamentar`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake analytical layer
# MAGIC - Supports executive Parliamentary Intelligence analytics
# MAGIC - Supports political segmentation and institutional performance analysis

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_perfil_parlamentar AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_perfil_parlamentar
-- Layer: Gold
--
-- Description:
-- Consolidated parliamentary profile combining engagement, transparency,
-- voting participation and parliamentary front activity indicators.
--
-- Grain:
-- One row per deputy.
--
-- Sources:
-- gold.vw_atividade_parlamentar_analitica
-- gold.vw_indice_eficiencia_parlamentar
-- gold.vw_indice_transparencia
-- gold.vw_fidelidade_partidaria
-- gold.vw_score_engajamento_parlamentar
-- -----------------------------------------------------------------------------

SELECT
    ativ.sk_dept,
    ativ.id_deputado,
    ativ.dept_tx_nome_parlamentar,

    ativ.part_sg_partido,
    ativ.uf_sg_uf,

    ativ.qt_despesas,
    ativ.vl_total_liquido,

    ativ.qt_votacoes,
    ativ.qt_presencas_votacoes,

    ativ.qt_votos_sim,
    ativ.qt_votos_nao,
    ativ.qt_votos_abstencao,
    ativ.qt_votos_obstrucao,

    ativ.qt_frentes,

    ativ.fl_presidente_frente,
    ativ.fl_coordenador_frente,
    ativ.fl_vice_frente,

    ef.parlamentar_score_atividade,
    ef.indice_eficiencia_parlamentar,

    tr.qt_documentos,
    tr.qt_documentos_comprovados,
    tr.qt_glosas,
    tr.qt_restituicoes,
    tr.pc_documentacao,

    fid.qt_votacoes AS qt_votacoes_fidelidade,
    fid.qt_presencas_votacao,
    fid.pc_participacao_decisiva,
    fid.tx_faixa_fidelidade_partidaria,

    score.score_engajamento,
    CASE
        WHEN score.score_engajamento >= 75
            THEN 'Alto engajamento parlamentar'

        WHEN score.score_engajamento >= 40
            THEN 'Médio engajamento parlamentar'

        ELSE 'Baixo engajamento parlamentar'
    END AS tx_classificacao_engajamento

FROM gold.vw_atividade_parlamentar_analitica ativ

LEFT JOIN gold.vw_indice_eficiencia_parlamentar ef
    ON ativ.id_deputado = ef.id_deputado

LEFT JOIN gold.vw_indice_transparencia tr
    ON ativ.id_deputado = tr.id_deputado

LEFT JOIN gold.vw_fidelidade_partidaria fid
    ON ativ.id_deputado = fid.id_deputado

LEFT JOIN gold.vw_score_engajamento_parlamentar score
    ON ativ.id_deputado = score.id_deputado
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_partidos_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_partidos_analitica
-- Layer: Gold
--
-- Description:
-- Consolidated analytical political party view combining parliamentary
-- engagement, transparency and efficiency indicators.
--
-- Grain:
-- One row per political party.
--
-- Source:
-- gold.vw_perfil_parlamentar
-- -----------------------------------------------------------------------------

SELECT
    part_sg_partido,

    COUNT(DISTINCT id_deputado) AS qt_deputados,

    SUM(qt_despesas) AS qt_despesas,
    SUM(vl_total_liquido) AS vl_total_liquido,

    SUM(qt_votacoes) AS qt_votacoes,
    SUM(qt_frentes) AS qt_frentes,

    AVG(indice_eficiencia_parlamentar) AS vl_media_eficiencia,
    AVG(pc_documentacao) AS pc_media_documentacao,
    AVG(pc_participacao_decisiva) AS pc_media_participacao_decisiva,

    AVG(score_engajamento) AS score_medio_engajamento,

    SUM(fl_presidente_frente) AS qt_presidentes_frente,
    SUM(fl_coordenador_frente) AS qt_coordenadores_frente,
    SUM(fl_vice_frente) AS qt_vice_frente

FROM gold.vw_perfil_parlamentar

GROUP BY
    part_sg_partido
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_partidos_perfil AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_partidos_perfil
-- Layer: Gold
--
-- Description:
-- Political party analytical profile classification.
--
-- Grain:
-- One row per political party.
--
-- Source:
-- gold.vw_partidos_analitica
-- -----------------------------------------------------------------------------

SELECT
    *,

    CASE
        WHEN score_medio_engajamento >=75
            THEN 'Partido altamente engajado'

        WHEN score_medio_engajamento >= 40
            THEN 'Partido moderadamente engajado'

        ELSE 'Partido pouco engajado'
    END AS tx_classificacao_engajamento,

    CASE
        WHEN pc_media_participacao_decisiva >= 80
            THEN 'Alta participação decisiva'

        WHEN pc_media_participacao_decisiva >= 50
            THEN 'Participação moderada'

        ELSE 'Baixa participação decisiva'
    END AS tx_classificacao_participacao

FROM gold.vw_partidos_analitica
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_dashboard_partidos AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_dashboard_partidos
-- Layer: Gold
--
-- Description:
-- Executive dashboard analytical view for political parties.
--
-- Grain:
-- One row per political party.
--
-- Source:
-- gold.vw_partidos_perfil
-- -----------------------------------------------------------------------------

SELECT
    part_sg_partido,

    qt_deputados,
    qt_despesas,
    vl_total_liquido,

    qt_votacoes,
    qt_frentes,

    qt_presidentes_frente,
    qt_coordenadores_frente,
    qt_vice_frente,

    ROUND(vl_media_eficiencia, 2) AS vl_media_eficiencia,
    ROUND(pc_media_documentacao, 2) AS pc_media_documentacao,
    ROUND(pc_media_participacao_decisiva, 2) AS pc_media_participacao_decisiva,
    ROUND(score_medio_engajamento, 2) AS score_medio_engajamento,

    tx_classificacao_engajamento,
    tx_classificacao_participacao

FROM gold.vw_partidos_perfil
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_analise_ineficiencia_parlamentar AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_analise_ineficiencia_parlamentar
-- Layer: Gold
--
-- Description:
-- Identifies deputies with high CEAP expense volume and comparatively low
-- engagement score. This view acts as an analytical proxy for potential
-- activity-to-expense imbalance and should not be interpreted as a formal
-- audit conclusion or misconduct indicator.
--
-- Grain:
-- One row per deputy.
--
-- Source:
-- gold.vw_perfil_parlamentar
-- -----------------------------------------------------------------------------

SELECT
    *,

    CASE
        WHEN vl_total_liquido >= 100000
             AND score_engajamento < 75
            THEN 'Alto desequilíbrio atividade-despesa'

        WHEN vl_total_liquido >= 50000
             AND score_engajamento < 40
            THEN 'Desequilíbrio moderado atividade-despesa'

        ELSE 'Comportamento esperado'
    END AS tx_classificacao_ineficiencia

FROM gold.vw_perfil_parlamentar
""")