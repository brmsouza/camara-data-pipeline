# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics Layer — Parliamentary Voting Intelligence
# MAGIC
# MAGIC **Notebook:** `04_build_gold_votacoes_analytics`
# MAGIC
# MAGIC Builds advanced analytical views and Parliamentary Intelligence products
# MAGIC related to parliamentary voting behavior in the Gold Analytics layer.
# MAGIC
# MAGIC This notebook consolidates analytical datasets derived from Gold fact and
# MAGIC dimension tables related to voting sessions, deputy votes, bancada
# MAGIC orientations, political party alignment and parliamentary behavior.
# MAGIC The resulting analytical products support voting intelligence, party cohesion
# MAGIC analysis, political alignment monitoring and legislative behavior analytics.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read Gold voting fact and dimension tables
# MAGIC - Build analytical voting aggregation views
# MAGIC - Create deputy voting behavior analytics
# MAGIC - Create bancada orientation and political alignment indicators
# MAGIC - Create party fidelity and cohesion metrics
# MAGIC - Create vote distribution and alignment comparison analytics
# MAGIC - Support parliamentary behavior and legislative intelligence analysis
# MAGIC - Preserve analytical lineage and traceability metadata
# MAGIC - Persist Gold analytical views and marts
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - `gold.ft_votacoes`
# MAGIC - `gold.ft_votos`
# MAGIC - `gold.ft_orientacoes_bancada`
# MAGIC - `gold.ft_frentes_membros`
# MAGIC - `gold.dm_deputado`
# MAGIC - `gold.dm_partido`
# MAGIC - `gold.dm_bancada`
# MAGIC - `gold.dm_frente`
# MAGIC - `gold.dm_legislatura`
# MAGIC - `gold.dm_data`
# MAGIC
# MAGIC ## Views / Analytical Objects
# MAGIC
# MAGIC - `gold.vw_votacoes_analitica`
# MAGIC - `gold.vw_votos_deputados_analitica`
# MAGIC - `gold.vw_orientacoes_bancada_analitica`
# MAGIC - `gold.vw_fidelidade_partidaria`
# MAGIC - `gold.vw_partidos_fidelidade_votacao`
# MAGIC - `gold.vw_partidos_votos_distribuicao`
# MAGIC - `gold.vw_alinhamento_frente_vs_partido`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake analytical layer
# MAGIC - Supports Parliamentary Intelligence and voting behavior analytics
# MAGIC - Supports political alignment and party discipline analysis

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_votacoes_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_votacoes_analitica
-- Layer: Gold
--
-- Description:
-- Analytical voting summary view combining voting fact records with proposition,
-- organization, event and date dimensions.
--
-- Grain:
-- One row per voting event.
--
-- Source:
-- gold.ft_votacoes
-- -----------------------------------------------------------------------------

SELECT
    ft.vot_id_votacao,
    ft.vot_tx_uri,

    d.data_dt_data AS vot_dt_votacao,
    d.data_nr_ano AS vot_nr_ano,
    d.data_nr_mes AS vot_nr_mes,
    d.data_tx_ano_mes,

    prop.prop_id_proposicao,
    prop.prop_sg_tipo,
    prop.prop_nr_numero,
    prop.prop_nr_ano,
    prop.prop_tx_ementa,
    prop.prop_tx_status_curado,
    prop.prop_fl_tramitando,
    prop.prop_fl_aprovada,
    prop.prop_fl_rejeitada,

    org.org_id_orgao,
    org.org_sg_orgao,
    org.org_tx_nome,
    org.org_tx_tipo_curado,

    evt.evt_id_evento,
    evt.evt_tx_tipo_curado,
    evt.evt_tx_situacao_curada,

    ft.vot_tx_descricao,
    ft.vot_tx_status_aprovacao,
    ft.vot_tx_resultado_curado,
    ft.vot_fl_aprovada,
    ft.vot_fl_rejeitada,
    ft.vot_qt_sim,
    ft.vot_qt_nao,
    ft.vot_qt_outros,
    ft.vot_qt_total,
    ft.vot_fl_possui_votos_contabilizados,

    ft.bronze_tx_endpoint,
    ft.bronze_id_origem,
    ft.bronze_id_batch,
    ft.bronze_tx_record_hash,
    ft.bronze_ts_ingestao,
    ft.bronze_dt_ingestao,
    ft.gold_ts_processamento,
    ft.gold_id_batch

FROM gold.ft_votacoes ft

LEFT JOIN gold.dm_data d
    ON ft.sk_data_votacao = d.sk_data

LEFT JOIN gold.dm_proposicao prop
    ON ft.sk_prop = prop.sk_prop

LEFT JOIN gold.dm_orgao org
    ON ft.sk_org = org.sk_org

LEFT JOIN gold.dm_evento evt
    ON ft.sk_evt = evt.sk_evt
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_votos_deputados_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_votos_deputados_analitica
-- Layer: Gold
--
-- Description:
-- Deputy-level analytical voting behavior view.
--
-- Grain:
-- One row per deputy vote.
--
-- Sources:
-- gold.ft_votos
-- gold.vw_votacoes_analitica
-- -----------------------------------------------------------------------------

SELECT
    ft.vot_id_votacao,

    vot.prop_id_proposicao,
    vot.prop_sg_tipo,
    vot.prop_nr_numero,
    vot.prop_nr_ano,
    vot.prop_tx_ementa,

    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,

    part.part_sg_partido,
    uf.uf_sg_uf,
    leg.leg_id_legislatura,

    d.data_dt_data AS vot_dt_votacao,
    d.data_tx_ano_mes,

    ft.vot_tx_voto,
    ft.vot_tx_voto_curado,

    ft.vot_fl_sim,
    ft.vot_fl_nao,
    ft.vot_fl_abstencao,
    ft.vot_fl_obstrucao,

    CASE
        WHEN ft.vot_tx_voto_curado IS NOT NULL THEN 1
        ELSE 0
    END AS vot_fl_presenca,

    1 AS qt_voto,

    CASE
        WHEN ft.vot_tx_voto_curado IS NOT NULL THEN 1
        ELSE 0
    END AS qt_presenca,

    ft.gold_ts_processamento,
    ft.gold_id_batch

FROM gold.ft_votos ft

LEFT JOIN gold.dm_deputado dept
    ON ft.sk_dept = dept.sk_dept

LEFT JOIN gold.dm_partido part
    ON ft.sk_part = part.sk_part

LEFT JOIN gold.dm_uf uf
    ON ft.sk_uf = uf.sk_uf

LEFT JOIN gold.dm_legislatura leg
    ON ft.sk_leg = leg.sk_leg

LEFT JOIN gold.dm_data d
    ON ft.sk_data_voto = d.sk_data

LEFT JOIN gold.vw_votacoes_analitica vot
    ON ft.vot_id_votacao = vot.vot_id_votacao
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_orientacoes_bancada_analitica AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_orientacoes_bancada_analitica
-- Layer: Gold
--
-- Description:
-- Analytical bancada voting orientation view combining orientation facts with
-- bancada and organization dimensions.
--
-- Grain:
-- One row per bancada orientation recommendation in a voting event.
--
-- Source:
-- gold.ft_orientacoes_bancada
-- -----------------------------------------------------------------------------

SELECT
    ft.vot_id_votacao,
    ft.vot_tx_dedup_key,

    ft.sk_banc,
    ft.sk_org,

    banc.banc_tx_bancada_curada,
    banc.banc_tx_tipo_bancada,
    banc.banc_tx_uri,

    org.org_sg_orgao,
    org.org_tx_nome,
    org.org_tx_tipo_curado,

    ft.vot_tx_orientacao,
    ft.vot_tx_orientacao_curada,
    ft.vot_tx_descricao_resultado,

    ft.vot_fl_orientacao_sim,
    ft.vot_fl_orientacao_nao,
    ft.vot_fl_orientacao_liberado,
    ft.vot_fl_orientacao_obstrucao,
    ft.vot_fl_orientacao_abstencao,

    ft.bronze_nr_ano_referencia,
    ft.bronze_ts_ingestao,
    ft.bronze_dt_ingestao,
    ft.bronze_tx_endpoint,
    ft.bronze_id_origem,
    ft.bronze_tx_source_file,
    ft.bronze_id_batch,
    ft.bronze_tx_record_hash,

    ft.gold_ts_processamento,
    ft.gold_id_batch

FROM gold.ft_orientacoes_bancada ft

LEFT JOIN gold.dm_bancada banc
    ON ft.sk_banc = banc.sk_banc

LEFT JOIN gold.dm_orgao org
    ON ft.sk_org = org.sk_org
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_fidelidade_partidaria AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_fidelidade_partidaria
-- Layer: Gold
--
-- Description:
-- Deputy-level voting participation and decisive voting behavior indicator.
-- This view currently acts as a proxy for parliamentary voting engagement.
-- Full party loyalty analysis requires explicit comparison between deputy
-- votes and bancada/party orientation recommendations.
--
-- Grain:
-- One row per deputy.
--
-- Sources:
-- gold.vw_votos_deputados_analitica
-- gold.dm_deputado
-- gold.dm_partido
-- gold.dm_uf
-- -----------------------------------------------------------------------------

WITH base AS (

    SELECT
        id_deputado,

        COUNT(DISTINCT vot_id_votacao) AS qt_votacoes,

        SUM(
            CASE
                WHEN vot_tx_voto_curado IS NOT NULL THEN 1
                ELSE 0
            END
        ) AS qt_presencas_votacao,

        SUM(vot_fl_sim) AS qt_votos_sim,
        SUM(vot_fl_nao) AS qt_votos_nao,
        SUM(vot_fl_abstencao) AS qt_abstencoes,
        SUM(vot_fl_obstrucao) AS qt_obstrucoes,

        ROUND(
            SUM(
                CASE
                    WHEN vot_fl_sim = 1
                         OR vot_fl_nao = 1
                        THEN 1
                    ELSE 0
                END
            )
            / NULLIF(
                SUM(
                    CASE
                        WHEN vot_tx_voto_curado IS NOT NULL THEN 1
                        ELSE 0
                    END
                ),
                0
            ) * 100,
            2
        ) AS pc_participacao_decisiva

    FROM gold.vw_votos_deputados_analitica

    GROUP BY
        id_deputado
)

SELECT
    base.id_deputado,

    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,

    base.qt_votacoes,
    base.qt_presencas_votacao,

    base.qt_votos_sim,
    base.qt_votos_nao,
    base.qt_abstencoes,
    base.qt_obstrucoes,

    base.pc_participacao_decisiva,

    CASE
        WHEN base.pc_participacao_decisiva >= 80
            THEN 'Alta participação em votações'

        WHEN base.pc_participacao_decisiva >= 50
            THEN 'Participação moderada em votações'

        ELSE 'Baixa participação em votações'
    END AS tx_faixa_fidelidade_partidaria

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
CREATE OR REPLACE VIEW gold.vw_partidos_fidelidade_votacao AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_partidos_fidelidade_votacao
-- Layer: Gold
--
-- Description:
-- Party-level voting participation indicators based on deputy voting behavior.
--
-- Grain:
-- One row per political party.
--
-- Source:
-- gold.vw_votos_deputados_analitica
-- -----------------------------------------------------------------------------

SELECT
    part_sg_partido,

    COUNT(DISTINCT id_deputado) AS qt_deputados,
    COUNT(DISTINCT vot_id_votacao) AS qt_votacoes,

    SUM(
        CASE
            WHEN vot_tx_voto_curado IS NOT NULL THEN 1
            ELSE 0
        END
    ) AS qt_presencas_votacao,

    SUM(vot_fl_sim) AS qt_votos_sim,
    SUM(vot_fl_nao) AS qt_votos_nao,
    SUM(vot_fl_abstencao) AS qt_abstencoes,
    SUM(vot_fl_obstrucao) AS qt_obstrucoes,

    ROUND(
        SUM(
            CASE
                WHEN vot_fl_sim = 1 OR vot_fl_nao = 1 THEN 1
                ELSE 0
            END
        )
        / NULLIF(
            SUM(
                CASE
                    WHEN vot_tx_voto_curado IS NOT NULL THEN 1
                    ELSE 0
                END
            ),
            0
        ) * 100,
        2
    ) AS pc_participacao_decisiva

FROM gold.vw_votos_deputados_analitica

GROUP BY
    part_sg_partido
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_partidos_votos_distribuicao AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_partidos_votos_distribuicao
-- Layer: Gold
--
-- Description:
-- Distribution of voting behavior by political party.
--
-- Grain:
-- One row per party and vote type.
--
-- Source:
-- gold.ft_votos
-- -----------------------------------------------------------------------------

SELECT
    part_sg_partido,
    vot_tx_voto,

    COUNT(*) AS qt_votos,

    ROUND(
        COUNT(*)
        / NULLIF(
            SUM(COUNT(*)) OVER (
                PARTITION BY part_sg_partido
            ),
            0
        ) * 100,
        2
    ) AS pc_distribuicao_voto

FROM gold.ft_votos

GROUP BY
    part_sg_partido,
    vot_tx_voto
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_alinhamento_frente_vs_partido AS
-- -----------------------------------------------------------------------------
-- View: gold.vw_alinhamento_frente_vs_partido
-- Layer: Gold
--
-- Description:
-- Measures predominant voting behavior within parliamentary fronts based on
-- majority vote tendency. This metric reflects directional convergence and
-- should not be interpreted as full ideological unanimity among members.
--
-- Grain:
-- One row per parliamentary front, party and voting event.
--
-- Sources:
-- gold.vw_frentes_membros_analitica
-- gold.vw_votos_deputados_analitica
-- -----------------------------------------------------------------------------

WITH votos_frente AS (

    SELECT
        f.frente_id_frente,
        f.frente_tx_titulo,
        f.part_sg_partido,

        v.vot_id_votacao,

        COUNT(DISTINCT f.dept_id_deputado) AS qt_deputados_frente,

        SUM(v.vot_fl_sim) AS qt_votos_sim,
        SUM(v.vot_fl_nao) AS qt_votos_nao,
        SUM(v.vot_fl_abstencao) AS qt_abstencoes,
        SUM(v.vot_fl_obstrucao) AS qt_obstrucoes,

        COUNT(*) AS qt_votos_validos

    FROM gold.vw_frentes_membros_analitica f

    INNER JOIN gold.vw_votos_deputados_analitica v
        ON f.dept_id_deputado = v.id_deputado

    WHERE v.vot_tx_voto_curado IS NOT NULL

    GROUP BY
        f.frente_id_frente,
        f.frente_tx_titulo,
        f.part_sg_partido,
        v.vot_id_votacao
),

base AS (

    SELECT
        frente_id_frente,
        frente_tx_titulo,
        part_sg_partido,

        COUNT(DISTINCT vot_id_votacao) AS qt_votacoes_analisadas,
        MAX(qt_deputados_frente) AS qt_deputados_frente,

        SUM(qt_votos_sim) AS qt_votos_sim,
        SUM(qt_votos_nao) AS qt_votos_nao,
        SUM(qt_abstencoes) AS qt_abstencoes,
        SUM(qt_obstrucoes) AS qt_obstrucoes,
        SUM(qt_votos_validos) AS qt_votos_validos,

        ROUND(
            SUM(GREATEST(qt_votos_sim, qt_votos_nao))
            / NULLIF(SUM(qt_votos_validos), 0) * 100,
            2
        ) AS pc_convergencia_voto

    FROM votos_frente

    GROUP BY
        frente_id_frente,
        frente_tx_titulo,
        part_sg_partido
)

SELECT
    *,

    CASE
        WHEN pc_convergencia_voto >= 80
            THEN 'Alta convergência'

        WHEN pc_convergencia_voto >= 60
            THEN 'Convergência moderada'

        ELSE 'Baixa convergência'
    END AS tx_classificacao_convergencia

FROM base
""")