# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 02_build_gold_frentes_analytics
# Layer: Gold Analytics
# Author: Bruno Souza
# ------------------------------------------------------------------------------

# COMMAND ----------

# Views / analytical objects included in this notebook

# - gold.vw_frentes_membros_analitica
# - gold.vw_frentes_diversidade_partidaria
# - gold.vw_deputados_mais_frentes
# - gold.vw_sobreposicao_frentes
# - gold.vw_evolucao_frentes_legislatura
# - gold.vw_especializacao_tematica
# - gold.vw_partidos_especializacao_tematica

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_frentes_membros_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_frentes_membros_analitica
-- Layer: Gold
--
-- Description:
-- Analytical parliamentary front membership view integrating front members,
-- parties, legislature and thematic indicators.
--
-- Grain:
-- One row per deputy membership in a parliamentary front.
--
-- Source:
-- gold.ft_frentes_membros
-- -----------------------------------------------------------------------------

SELECT
    sk_frente,
    sk_dept,
    sk_part,
    sk_uf,
    sk_leg,

    frente_id_frente,
    dept_id_deputado,
    leg_id_legislatura,

    frente_tx_titulo,

    dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf,

    memb_cd_titulo,
    memb_tx_titulo,

    memb_fl_ativo,
    memb_fl_coordenador,
    memb_fl_presidente,
    memb_fl_vice,
    memb_fl_membro,

    frente_fl_tema_saude,
    frente_fl_tema_educacao,
    frente_fl_tema_seguranca,
    frente_fl_tema_agro,
    frente_fl_tema_mulher,
    frente_fl_tema_meio_ambiente,

    qt_membro_frente,
    qt_membro_ativo,
    qt_coordenador,
    qt_presidente,

    bronze_id_batch,
    gold_id_batch,
    gold_ts_processamento

FROM gold.ft_frentes_membros
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_frentes_diversidade_partidaria AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_frentes_diversidade_partidaria
-- Layer: Gold
--
-- Description:
-- Analytical parliamentary front diversity view using Herfindahl-Hirschman
-- Index (HHI) for party concentration analysis.
--
-- Grain:
-- One row per parliamentary front.
--
-- Source:
-- gold.vw_frentes_membros_analitica
-- -----------------------------------------------------------------------------

WITH base AS (

    SELECT
        frente_id_frente,
        frente_tx_titulo,
        part_sg_partido,

        COUNT(DISTINCT dept_id_deputado) AS qt_deputados_partido

    FROM gold.vw_frentes_membros_analitica

    GROUP BY
        frente_id_frente,
        frente_tx_titulo,
        part_sg_partido
),

totais AS (

    SELECT
        frente_id_frente,
        COUNT(DISTINCT dept_id_deputado) AS qt_total_deputados,
        COUNT(DISTINCT part_sg_partido) AS qt_total_partidos

    FROM gold.vw_frentes_membros_analitica

    GROUP BY
        frente_id_frente
),

participacao AS (

    SELECT
        b.*,
        t.qt_total_deputados,
        t.qt_total_partidos,

        (
            b.qt_deputados_partido
            / t.qt_total_deputados
        ) AS pc_participacao_partido

    FROM base b

    LEFT JOIN totais t
        ON b.frente_id_frente = t.frente_id_frente
),

hhi AS (

    SELECT
        frente_id_frente,
        MAX(frente_tx_titulo) AS frente_tx_titulo,

        MAX(qt_total_deputados) AS qt_total_deputados,
        MAX(qt_total_partidos) AS qt_total_partidos,

        ROUND(
            SUM(POWER(pc_participacao_partido, 2)),
            4
        ) AS indice_hhi_partidario

    FROM participacao

    GROUP BY
        frente_id_frente
)

SELECT
    *,

    CASE
        WHEN indice_hhi_partidario <= 0.15
            THEN 'Alta diversidade partidária'

        WHEN indice_hhi_partidario <= 0.25
            THEN 'Média diversidade partidária'

        ELSE 'Baixa diversidade partidária'
    END AS frente_tx_classificacao_diversidade

FROM hhi
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_deputados_mais_frentes AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_deputados_mais_frentes
-- Layer: Gold
--
-- Description:
-- Ranking of deputies participating in the largest number of parliamentary
-- fronts.
--
-- Grain:
-- One row per deputy.
--
-- Source:
-- gold.vw_frentes_membros_analitica
-- -----------------------------------------------------------------------------

SELECT
    dept_id_deputado,
    dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf,

    COUNT(DISTINCT frente_id_frente) AS qt_frentes,

    SUM(memb_fl_coordenador) AS qt_coordenacoes,
    SUM(memb_fl_presidente) AS qt_presidencias,

    ROW_NUMBER() OVER (
        ORDER BY COUNT(DISTINCT frente_id_frente) DESC
    ) AS ranking_frentes

FROM gold.vw_frentes_membros_analitica

GROUP BY
    dept_id_deputado,
    dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_sobreposicao_frentes AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_sobreposicao_frentes
-- Layer: Gold
--
-- Description:
-- Measures overlap of deputies between parliamentary fronts.
--
-- Grain:
-- One row per pair of parliamentary fronts.
--
-- Source:
-- gold.vw_frentes_membros_analitica
-- -----------------------------------------------------------------------------

WITH pares AS (

    SELECT
        a.frente_id_frente AS frente_id_origem,
        a.frente_tx_titulo AS frente_tx_origem,

        b.frente_id_frente AS frente_id_destino,
        b.frente_tx_titulo AS frente_tx_destino,

        COUNT(DISTINCT a.dept_id_deputado) AS qt_deputados_comuns

    FROM gold.vw_frentes_membros_analitica a

    INNER JOIN gold.vw_frentes_membros_analitica b
        ON a.dept_id_deputado = b.dept_id_deputado
       AND a.frente_id_frente < b.frente_id_frente

    GROUP BY
        a.frente_id_frente,
        a.frente_tx_titulo,
        b.frente_id_frente,
        b.frente_tx_titulo
)

SELECT
    *,

    ROW_NUMBER() OVER (
        ORDER BY qt_deputados_comuns DESC
    ) AS ranking_sobreposicao

FROM pares
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_evolucao_frentes_legislatura AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_evolucao_frentes_legislatura
-- Layer: Gold
--
-- Description:
-- Evolution of parliamentary fronts and memberships by legislature.
--
-- Grain:
-- One row per legislature.
--
-- Source:
-- gold.vw_frentes_membros_analitica
-- -----------------------------------------------------------------------------

SELECT
    leg_id_legislatura,

    COUNT(DISTINCT frente_id_frente) AS qt_frentes,
    COUNT(DISTINCT dept_id_deputado) AS qt_deputados,
    COUNT(DISTINCT part_sg_partido) AS qt_partidos,

    SUM(qt_membro_frente) AS qt_total_membros,
    SUM(qt_coordenador) AS qt_total_coordenadores,
    SUM(qt_presidente) AS qt_total_presidentes

FROM gold.vw_frentes_membros_analitica

GROUP BY
    leg_id_legislatura
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_especializacao_tematica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_especializacao_tematica
-- Layer: Gold
--
-- Description:
-- Thematic specialization profile of deputies based on parliamentary fronts.
--
-- Grain:
-- One row per deputy.
--
-- Source:
-- gold.vw_frentes_membros_analitica
-- -----------------------------------------------------------------------------

SELECT
    dept_id_deputado,
    dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf,

    SUM(frente_fl_tema_saude) AS qt_frentes_saude,
    SUM(frente_fl_tema_educacao) AS qt_frentes_educacao,
    SUM(frente_fl_tema_seguranca) AS qt_frentes_seguranca,
    SUM(frente_fl_tema_agro) AS qt_frentes_agro,
    SUM(frente_fl_tema_mulher) AS qt_frentes_mulher,
    SUM(frente_fl_tema_meio_ambiente) AS qt_frentes_meio_ambiente,

    COUNT(DISTINCT frente_id_frente) AS qt_total_frentes

FROM gold.vw_frentes_membros_analitica

GROUP BY
    dept_id_deputado,
    dept_tx_nome_parlamentar,
    part_sg_partido,
    uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_partidos_especializacao_tematica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_partidos_especializacao_tematica
-- Layer: Gold
--
-- Description:
-- Thematic specialization profile aggregated by political party.
--
-- Grain:
-- One row per political party.
--
-- Source:
-- gold.vw_frentes_membros_analitica
-- -----------------------------------------------------------------------------

SELECT
    part_sg_partido,

    COUNT(DISTINCT dept_id_deputado) AS qt_deputados,
    COUNT(DISTINCT frente_id_frente) AS qt_frentes,

    SUM(frente_fl_tema_saude) AS qt_frentes_saude,
    SUM(frente_fl_tema_educacao) AS qt_frentes_educacao,
    SUM(frente_fl_tema_seguranca) AS qt_frentes_seguranca,
    SUM(frente_fl_tema_agro) AS qt_frentes_agro,
    SUM(frente_fl_tema_mulher) AS qt_frentes_mulher,
    SUM(frente_fl_tema_meio_ambiente) AS qt_frentes_meio_ambiente

FROM gold.vw_frentes_membros_analitica

GROUP BY
    part_sg_partido
""")