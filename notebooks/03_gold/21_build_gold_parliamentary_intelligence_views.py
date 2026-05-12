# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 21_build_gold_parliamentary_intelligence_views
# Layer: Gold
# Author: Bruno Souza
#
# Description:
# Builds analytical intelligence views for parliamentary behavior profiling.
#
# Context:
# This notebook creates Gold analytical views that enrich the Star Schema with
# business-oriented metrics such as spending segmentation, party alignment,
# transparency, efficiency, thematic specialization and parliamentary profile.
#
# Target:
# gold analytical views
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "gold"
PIPELINE_NAME = "gold_build_parliamentary_intelligence_views"
TARGET_TABLE = "gold.parliamentary_intelligence_views"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_gastos_segmentados AS
SELECT
    dept.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,
    d.data_nr_ano,
    d.data_nr_mes,
    d.data_tx_ano_mes,

    tipo.desp_tx_segmento_despesa,
    tipo.desp_tx_tipo_despesa,

    COUNT(DISTINCT ft.desp_id_documento) AS qt_documentos,
    COUNT(DISTINCT ft.sk_forn) AS qt_fornecedores,

    SUM(ft.desp_vl_documento) AS desp_vl_total_documento,
    SUM(ft.desp_vl_glosa) AS desp_vl_total_glosa,
    SUM(ft.desp_vl_liquido) AS desp_vl_total_liquido,
    SUM(ft.desp_vl_restituicao) AS desp_vl_total_restituicao,

    AVG(ft.desp_vl_liquido) AS desp_vl_medio_liquido,

    ROUND(
        SUM(ft.desp_vl_liquido)
        / SUM(SUM(ft.desp_vl_liquido)) OVER (
            PARTITION BY dept.sk_dept, d.data_tx_ano_mes
        ) * 100,
        2
    ) AS desp_pc_participacao_segmento

FROM gold.ft_despesas_ceap ft
LEFT JOIN gold.dm_deputado dept
    ON ft.sk_dept = dept.sk_dept
LEFT JOIN gold.dm_partido part
    ON ft.sk_part = part.sk_part
LEFT JOIN gold.dm_uf uf
    ON ft.sk_uf = uf.sk_uf
LEFT JOIN gold.dm_data d
    ON ft.sk_data_emissao = d.sk_data
LEFT JOIN gold.dm_tipo_despesa tipo
    ON ft.sk_desp_tipo = tipo.sk_desp_tipo
GROUP BY
    dept.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,
    d.data_nr_ano,
    d.data_nr_mes,
    d.data_tx_ano_mes,
    tipo.desp_tx_segmento_despesa,
    tipo.desp_tx_tipo_despesa
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_fidelidade_partidaria AS
SELECT
    voto.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,

    COUNT(*) AS vot_qt_total,
    SUM(
        CASE
            WHEN voto.vot_tx_voto_curado = ori.vot_tx_orientacao_curada
            THEN 1 ELSE 0
        END
    ) AS vot_qt_alinhado_bancada,

    ROUND(
        SUM(
            CASE
                WHEN voto.vot_tx_voto_curado = ori.vot_tx_orientacao_curada
                THEN 1 ELSE 0
            END
        ) / COUNT(*) * 100,
        2
    ) AS vot_pc_alinhamento_bancada,

    CASE
        WHEN ROUND(
            SUM(
                CASE
                    WHEN voto.vot_tx_voto_curado = ori.vot_tx_orientacao_curada
                    THEN 1 ELSE 0
                END
            ) / COUNT(*) * 100,
            2
        ) >= 80 THEN 'Alta fidelidade partidária'

        WHEN ROUND(
            SUM(
                CASE
                    WHEN voto.vot_tx_voto_curado = ori.vot_tx_orientacao_curada
                    THEN 1 ELSE 0
                END
            ) / COUNT(*) * 100,
            2
        ) >= 50 THEN 'Média fidelidade partidária'

        ELSE 'Baixa fidelidade partidária'
    END AS vot_tx_faixa_fidelidade_partidaria

FROM gold.ft_votos voto
LEFT JOIN gold.ft_orientacoes_bancada ori
    ON voto.vot_id_votacao = ori.vot_id_votacao
   AND voto.part_sg_partido = ori.banc_tx_sigla_bancada
LEFT JOIN gold.dm_deputado dept
    ON voto.sk_dept = dept.sk_dept
LEFT JOIN gold.dm_partido part
    ON voto.sk_part = part.sk_part
LEFT JOIN gold.dm_uf uf
    ON voto.sk_uf = uf.sk_uf
GROUP BY
    voto.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_indice_transparencia AS
SELECT
    ft.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,

    COUNT(*) AS desp_qt_total,
    SUM(ft.desp_fl_possui_documento_url) AS desp_qt_com_documento_url,
    SUM(ft.desp_fl_possui_glosa) AS desp_qt_com_glosa,
    SUM(ft.desp_fl_possui_restituicao) AS desp_qt_com_restituicao,

    ROUND(
        SUM(ft.desp_fl_possui_documento_url) / COUNT(*) * 100,
        2
    ) AS desp_pc_documentacao,

    ROUND(
        (
            SUM(ft.desp_fl_possui_documento_url) / COUNT(*) * 70
        )
        -
        (
            SUM(ft.desp_fl_possui_glosa) / COUNT(*) * 15
        )
        -
        (
            SUM(ft.desp_fl_possui_restituicao) / COUNT(*) * 15
        ),
        2
    ) AS parlamentar_score_transparencia,

    CASE
        WHEN ROUND(
            (
                SUM(ft.desp_fl_possui_documento_url) / COUNT(*) * 70
            )
            -
            (
                SUM(ft.desp_fl_possui_glosa) / COUNT(*) * 15
            )
            -
            (
                SUM(ft.desp_fl_possui_restituicao) / COUNT(*) * 15
            ),
            2
        ) >= 60 THEN 'Alta transparência'
        WHEN ROUND(
            (
                SUM(ft.desp_fl_possui_documento_url) / COUNT(*) * 70
            )
            -
            (
                SUM(ft.desp_fl_possui_glosa) / COUNT(*) * 15
            )
            -
            (
                SUM(ft.desp_fl_possui_restituicao) / COUNT(*) * 15
            ),
            2
        ) >= 30 THEN 'Média transparência'
        ELSE 'Baixa transparência'
    END AS parlamentar_tx_faixa_transparencia

FROM gold.ft_despesas_ceap ft
LEFT JOIN gold.dm_deputado dept
    ON ft.sk_dept = dept.sk_dept
LEFT JOIN gold.dm_partido part
    ON ft.sk_part = part.sk_part
LEFT JOIN gold.dm_uf uf
    ON ft.sk_uf = uf.sk_uf
GROUP BY
    ft.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_indice_eficiencia_parlamentar AS
SELECT
    ativ.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,

    ativ.qt_votos,
    ativ.qt_frentes,
    ativ.qt_despesas,
    ativ.vl_total_liquido,

    (
        ativ.qt_votos
        + (ativ.qt_frentes * 10)
        + (ativ.fl_presidente_frente * 20)
        + (ativ.fl_coordenador_frente * 15)
        + (ativ.fl_vice_frente * 10)
    ) AS parlamentar_score_atividade,

    ROUND(
        (
            ativ.qt_votos
            + (ativ.qt_frentes * 10)
            + (ativ.fl_presidente_frente * 20)
            + (ativ.fl_coordenador_frente * 15)
            + (ativ.fl_vice_frente * 10)
        )
        / NULLIF(ativ.vl_total_liquido, 0) * 1000,
        4
    ) AS parlamentar_score_eficiencia_por_mil_reais,

    CASE
        WHEN ROUND(
            (
                ativ.qt_votos
                + (ativ.qt_frentes * 10)
                + (ativ.fl_presidente_frente * 20)
                + (ativ.fl_coordenador_frente * 15)
                + (ativ.fl_vice_frente * 10)
            )
            / NULLIF(ativ.vl_total_liquido, 0) * 1000,
            4
        ) >= 1 THEN 'Alta eficiência'

        WHEN ROUND(
            (
                ativ.qt_votos
                + (ativ.qt_frentes * 10)
                + (ativ.fl_presidente_frente * 20)
                + (ativ.fl_coordenador_frente * 15)
                + (ativ.fl_vice_frente * 10)
            )
            / NULLIF(ativ.vl_total_liquido, 0) * 1000,
            4
        ) >= 0.3 THEN 'Média eficiência'

        ELSE 'Baixa eficiência'
    END AS parlamentar_tx_faixa_eficiencia

FROM gold.ft_atividade_parlamentar ativ
LEFT JOIN gold.dm_deputado dept
    ON ativ.sk_dept = dept.sk_dept
LEFT JOIN gold.dm_partido part
    ON ativ.sk_part = part.sk_part
LEFT JOIN gold.dm_uf uf
    ON ativ.sk_uf = uf.sk_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_especializacao_tematica AS
SELECT
    fm.dept_id_deputado,
    dept.sk_dept,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf,

    SUM(fr.frente_fl_tema_saude) AS frente_qt_tema_saude,
    SUM(fr.frente_fl_tema_educacao) AS frente_qt_tema_educacao,
    SUM(fr.frente_fl_tema_seguranca) AS frente_qt_tema_seguranca,
    SUM(fr.frente_fl_tema_agro) AS frente_qt_tema_agro,
    SUM(fr.frente_fl_tema_mulher) AS frente_qt_tema_mulher,
    SUM(fr.frente_fl_tema_meio_ambiente) AS frente_qt_tema_meio_ambiente,

    CASE
        WHEN SUM(fr.frente_fl_tema_agro) >= 2 THEN 'Agro'
        WHEN SUM(fr.frente_fl_tema_seguranca) >= 2 THEN 'Segurança'
        WHEN SUM(fr.frente_fl_tema_meio_ambiente) >= 2 THEN 'Meio ambiente'
        WHEN SUM(fr.frente_fl_tema_mulher) >= 2 THEN 'Direitos das mulheres'
        WHEN SUM(fr.frente_fl_tema_educacao) >= 2 THEN 'Educação'
        WHEN SUM(fr.frente_fl_tema_saude) >= 2 THEN 'Saúde'
        ELSE 'Generalista'
    END AS parlamentar_tx_especializacao_tematica

FROM silver_curated.frentes_membros fm
LEFT JOIN gold.dm_deputado dept
    ON fm.dept_id_deputado = dept.id_deputado
LEFT JOIN gold.dm_partido part
    ON dept.part_sg_partido = part.part_sg_partido
LEFT JOIN gold.dm_uf uf
    ON dept.uf_sg_uf = uf.uf_sg_uf
LEFT JOIN gold.dm_frente fr
    ON fm.frente_id_frente = fr.frente_id_frente
GROUP BY
    fm.dept_id_deputado,
    dept.sk_dept,
    dept.dept_tx_nome_parlamentar,
    part.part_sg_partido,
    uf.uf_sg_uf
""")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW gold.vw_perfil_parlamentar AS
SELECT
    dept.sk_dept,
    dept.id_deputado,
    dept.dept_tx_nome_parlamentar,
    dept.dept_tx_status_mandato_curado,
    part.part_sg_partido,
    uf.uf_sg_uf,

    COALESCE(fid.vot_qt_total, 0) AS vot_qt_total,
    COALESCE(fid.vot_pc_alinhamento_bancada, 0) AS vot_pc_alinhamento_bancada,
    COALESCE(fid.vot_tx_faixa_fidelidade_partidaria, 'Sem dados') AS vot_tx_faixa_fidelidade_partidaria,

    COALESCE(transp.parlamentar_score_transparencia, 0) AS parlamentar_score_transparencia,
    COALESCE(transp.parlamentar_tx_faixa_transparencia, 'Sem dados') AS parlamentar_tx_faixa_transparencia,

    COALESCE(efi.parlamentar_score_atividade, 0) AS parlamentar_score_atividade,
    COALESCE(efi.parlamentar_score_eficiencia_por_mil_reais, 0) AS parlamentar_score_eficiencia_por_mil_reais,
    COALESCE(efi.parlamentar_tx_faixa_eficiencia, 'Sem dados') AS parlamentar_tx_faixa_eficiencia,

    COALESCE(tema.parlamentar_tx_especializacao_tematica, 'Generalista') AS parlamentar_tx_especializacao_tematica,

    CASE
        WHEN COALESCE(tema.parlamentar_tx_especializacao_tematica, 'Generalista') IN ('Agro', 'Segurança')
            THEN 'Perfil conservador temático'

        WHEN COALESCE(tema.parlamentar_tx_especializacao_tematica, 'Generalista') IN ('Meio ambiente', 'Direitos das mulheres')
            THEN 'Perfil progressista temático'

        WHEN COALESCE(fid.vot_pc_alinhamento_bancada, 0) >= 80
            THEN 'Perfil partidariamente alinhado'

        WHEN COALESCE(fid.vot_pc_alinhamento_bancada, 0) < 40
            AND COALESCE(fid.vot_qt_total, 0) > 0
            THEN 'Perfil independente'

        ELSE 'Perfil moderado/institucional'
    END AS parlamentar_tx_perfil_comportamental

FROM gold.dm_deputado dept
LEFT JOIN gold.dm_partido part
    ON dept.part_sg_partido = part.part_sg_partido
LEFT JOIN gold.dm_uf uf
    ON dept.uf_sg_uf = uf.uf_sg_uf
LEFT JOIN gold.vw_fidelidade_partidaria fid
    ON dept.sk_dept = fid.sk_dept
LEFT JOIN gold.vw_indice_transparencia transp
    ON dept.sk_dept = transp.sk_dept
LEFT JOIN gold.vw_indice_eficiencia_parlamentar efi
    ON dept.sk_dept = efi.sk_dept
LEFT JOIN gold.vw_especializacao_tematica tema
    ON dept.sk_dept = tema.sk_dept
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_partidos_analitica AS
# MAGIC SELECT
# MAGIC     part.part_sg_partido,
# MAGIC
# MAGIC     COUNT(DISTINCT dept.sk_dept) AS part_qt_deputados,
# MAGIC
# MAGIC     COALESCE(SUM(ativ.qt_despesas), 0) AS part_qt_despesas,
# MAGIC     COALESCE(SUM(ativ.vl_total_liquido), 0) AS part_vl_total_liquido,
# MAGIC     ROUND(
# MAGIC         COALESCE(SUM(ativ.vl_total_liquido), 0)
# MAGIC         / NULLIF(COUNT(DISTINCT dept.sk_dept), 0),
# MAGIC         2
# MAGIC     ) AS part_vl_medio_por_deputado,
# MAGIC
# MAGIC     COALESCE(SUM(ativ.qt_votos), 0) AS part_qt_votos,
# MAGIC     COALESCE(SUM(ativ.qt_votos_sim), 0) AS part_qt_votos_sim,
# MAGIC     COALESCE(SUM(ativ.qt_votos_nao), 0) AS part_qt_votos_nao,
# MAGIC     COALESCE(SUM(ativ.qt_votos_abstencao), 0) AS part_qt_votos_abstencao,
# MAGIC     COALESCE(SUM(ativ.qt_votos_obstrucao), 0) AS part_qt_votos_obstrucao,
# MAGIC
# MAGIC     COALESCE(SUM(ativ.qt_frentes), 0) AS part_qt_frentes,
# MAGIC
# MAGIC     ROUND(AVG(fid.vot_pc_alinhamento_bancada), 2) AS part_pc_medio_fidelidade,
# MAGIC
# MAGIC     ROUND(AVG(transp.parlamentar_score_transparencia), 2) AS part_score_medio_transparencia,
# MAGIC
# MAGIC     ROUND(AVG(efi.parlamentar_score_eficiencia_por_mil_reais), 4) AS part_score_medio_eficiencia
# MAGIC
# MAGIC FROM gold.dm_partido part
# MAGIC LEFT JOIN gold.dm_deputado dept
# MAGIC     ON part.part_sg_partido = dept.part_sg_partido
# MAGIC LEFT JOIN gold.ft_atividade_parlamentar ativ
# MAGIC     ON dept.sk_dept = ativ.sk_dept
# MAGIC LEFT JOIN gold.vw_fidelidade_partidaria fid
# MAGIC     ON dept.sk_dept = fid.sk_dept
# MAGIC LEFT JOIN gold.vw_indice_transparencia transp
# MAGIC     ON dept.sk_dept = transp.sk_dept
# MAGIC LEFT JOIN gold.vw_indice_eficiencia_parlamentar efi
# MAGIC     ON dept.sk_dept = efi.sk_dept
# MAGIC GROUP BY
# MAGIC     part.part_sg_partido;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_partidos_despesas_segmento AS
# MAGIC SELECT
# MAGIC     part.part_sg_partido,
# MAGIC     tipo.desp_tx_segmento_despesa,
# MAGIC     tipo.desp_tx_tipo_despesa,
# MAGIC     data.data_nr_ano,
# MAGIC     data.data_tx_ano_mes,
# MAGIC
# MAGIC     COUNT(DISTINCT ft.sk_dept) AS part_qt_deputados,
# MAGIC     COUNT(DISTINCT ft.desp_id_documento) AS part_qt_documentos,
# MAGIC     COUNT(DISTINCT ft.sk_forn) AS part_qt_fornecedores,
# MAGIC
# MAGIC     SUM(ft.desp_vl_documento) AS part_vl_total_documento,
# MAGIC     SUM(ft.desp_vl_glosa) AS part_vl_total_glosa,
# MAGIC     SUM(ft.desp_vl_liquido) AS part_vl_total_liquido,
# MAGIC     AVG(ft.desp_vl_liquido) AS part_vl_medio_liquido,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(ft.desp_vl_liquido)
# MAGIC         / SUM(SUM(ft.desp_vl_liquido)) OVER (
# MAGIC             PARTITION BY part.part_sg_partido, data.data_tx_ano_mes
# MAGIC         ) * 100,
# MAGIC         2
# MAGIC     ) AS part_pc_participacao_segmento
# MAGIC
# MAGIC FROM gold.ft_despesas_ceap ft
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON ft.sk_part = part.sk_part
# MAGIC LEFT JOIN gold.dm_tipo_despesa tipo
# MAGIC     ON ft.sk_desp_tipo = tipo.sk_desp_tipo
# MAGIC LEFT JOIN gold.dm_data data
# MAGIC     ON ft.sk_data_emissao = data.sk_data
# MAGIC GROUP BY
# MAGIC     part.part_sg_partido,
# MAGIC     tipo.desp_tx_segmento_despesa,
# MAGIC     tipo.desp_tx_tipo_despesa,
# MAGIC     data.data_nr_ano,
# MAGIC     data.data_tx_ano_mes;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_partidos_fidelidade_votacao AS
# MAGIC SELECT
# MAGIC     part.part_sg_partido,
# MAGIC
# MAGIC     COUNT(DISTINCT fid.sk_dept) AS part_qt_deputados_com_voto,
# MAGIC     SUM(fid.vot_qt_total) AS part_qt_votos,
# MAGIC     SUM(fid.vot_qt_alinhado_bancada) AS part_qt_votos_alinhados,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(fid.vot_qt_alinhado_bancada)
# MAGIC         / NULLIF(SUM(fid.vot_qt_total), 0) * 100,
# MAGIC         2
# MAGIC     ) AS part_pc_fidelidade_partidaria,
# MAGIC
# MAGIC     AVG(fid.vot_pc_alinhamento_bancada) AS part_pc_media_fidelidade_deputados,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN ROUND(
# MAGIC             SUM(fid.vot_qt_alinhado_bancada)
# MAGIC             / NULLIF(SUM(fid.vot_qt_total), 0) * 100,
# MAGIC             2
# MAGIC         ) >= 80 THEN 'Alta disciplina partidária'
# MAGIC
# MAGIC         WHEN ROUND(
# MAGIC             SUM(fid.vot_qt_alinhado_bancada)
# MAGIC             / NULLIF(SUM(fid.vot_qt_total), 0) * 100,
# MAGIC             2
# MAGIC         ) >= 50 THEN 'Média disciplina partidária'
# MAGIC
# MAGIC         ELSE 'Baixa disciplina partidária'
# MAGIC     END AS part_tx_faixa_disciplina_partidaria
# MAGIC
# MAGIC FROM gold.dm_partido part
# MAGIC LEFT JOIN gold.vw_fidelidade_partidaria fid
# MAGIC     ON part.part_sg_partido = fid.part_sg_partido
# MAGIC GROUP BY
# MAGIC     part.part_sg_partido;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_partidos_votos_distribuicao AS
# MAGIC SELECT
# MAGIC     part.part_sg_partido,
# MAGIC
# MAGIC     COUNT(*) AS part_qt_votos,
# MAGIC     SUM(ft.vot_fl_sim) AS part_qt_votos_sim,
# MAGIC     SUM(ft.vot_fl_nao) AS part_qt_votos_nao,
# MAGIC     SUM(ft.vot_fl_abstencao) AS part_qt_votos_abstencao,
# MAGIC     SUM(ft.vot_fl_obstrucao) AS part_qt_votos_obstrucao,
# MAGIC
# MAGIC     ROUND(SUM(ft.vot_fl_sim) / NULLIF(COUNT(*), 0) * 100, 2) AS part_pc_votos_sim,
# MAGIC     ROUND(SUM(ft.vot_fl_nao) / NULLIF(COUNT(*), 0) * 100, 2) AS part_pc_votos_nao,
# MAGIC     ROUND(SUM(ft.vot_fl_abstencao) / NULLIF(COUNT(*), 0) * 100, 2) AS part_pc_abstencao,
# MAGIC     ROUND(SUM(ft.vot_fl_obstrucao) / NULLIF(COUNT(*), 0) * 100, 2) AS part_pc_obstrucao
# MAGIC
# MAGIC FROM gold.ft_votos ft
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON ft.sk_part = part.sk_part
# MAGIC GROUP BY
# MAGIC     part.part_sg_partido;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_partidos_especializacao_tematica AS
# MAGIC SELECT
# MAGIC     part.part_sg_partido,
# MAGIC
# MAGIC     COUNT(DISTINCT esp.sk_dept) AS part_qt_deputados_com_frentes,
# MAGIC
# MAGIC     SUM(esp.frente_qt_tema_saude) AS part_qt_tema_saude,
# MAGIC     SUM(esp.frente_qt_tema_educacao) AS part_qt_tema_educacao,
# MAGIC     SUM(esp.frente_qt_tema_seguranca) AS part_qt_tema_seguranca,
# MAGIC     SUM(esp.frente_qt_tema_agro) AS part_qt_tema_agro,
# MAGIC     SUM(esp.frente_qt_tema_mulher) AS part_qt_tema_mulher,
# MAGIC     SUM(esp.frente_qt_tema_meio_ambiente) AS part_qt_tema_meio_ambiente,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN SUM(esp.frente_qt_tema_agro) >= GREATEST(
# MAGIC             SUM(esp.frente_qt_tema_saude),
# MAGIC             SUM(esp.frente_qt_tema_educacao),
# MAGIC             SUM(esp.frente_qt_tema_seguranca),
# MAGIC             SUM(esp.frente_qt_tema_mulher),
# MAGIC             SUM(esp.frente_qt_tema_meio_ambiente)
# MAGIC         ) THEN 'Agro'
# MAGIC
# MAGIC         WHEN SUM(esp.frente_qt_tema_seguranca) >= GREATEST(
# MAGIC             SUM(esp.frente_qt_tema_saude),
# MAGIC             SUM(esp.frente_qt_tema_educacao),
# MAGIC             SUM(esp.frente_qt_tema_agro),
# MAGIC             SUM(esp.frente_qt_tema_mulher),
# MAGIC             SUM(esp.frente_qt_tema_meio_ambiente)
# MAGIC         ) THEN 'Segurança'
# MAGIC
# MAGIC         WHEN SUM(esp.frente_qt_tema_meio_ambiente) >= GREATEST(
# MAGIC             SUM(esp.frente_qt_tema_saude),
# MAGIC             SUM(esp.frente_qt_tema_educacao),
# MAGIC             SUM(esp.frente_qt_tema_seguranca),
# MAGIC             SUM(esp.frente_qt_tema_agro),
# MAGIC             SUM(esp.frente_qt_tema_mulher)
# MAGIC         ) THEN 'Meio ambiente'
# MAGIC
# MAGIC         WHEN SUM(esp.frente_qt_tema_mulher) >= GREATEST(
# MAGIC             SUM(esp.frente_qt_tema_saude),
# MAGIC             SUM(esp.frente_qt_tema_educacao),
# MAGIC             SUM(esp.frente_qt_tema_seguranca),
# MAGIC             SUM(esp.frente_qt_tema_agro),
# MAGIC             SUM(esp.frente_qt_tema_meio_ambiente)
# MAGIC         ) THEN 'Direitos das mulheres'
# MAGIC
# MAGIC         WHEN SUM(esp.frente_qt_tema_educacao) >= GREATEST(
# MAGIC             SUM(esp.frente_qt_tema_saude),
# MAGIC             SUM(esp.frente_qt_tema_seguranca),
# MAGIC             SUM(esp.frente_qt_tema_agro),
# MAGIC             SUM(esp.frente_qt_tema_mulher),
# MAGIC             SUM(esp.frente_qt_tema_meio_ambiente)
# MAGIC         ) THEN 'Educação'
# MAGIC
# MAGIC         WHEN SUM(esp.frente_qt_tema_saude) >= GREATEST(
# MAGIC             SUM(esp.frente_qt_tema_educacao),
# MAGIC             SUM(esp.frente_qt_tema_seguranca),
# MAGIC             SUM(esp.frente_qt_tema_agro),
# MAGIC             SUM(esp.frente_qt_tema_mulher),
# MAGIC             SUM(esp.frente_qt_tema_meio_ambiente)
# MAGIC         ) THEN 'Saúde'
# MAGIC
# MAGIC         ELSE 'Generalista'
# MAGIC     END AS part_tx_especializacao_tematica
# MAGIC
# MAGIC FROM gold.dm_partido part
# MAGIC LEFT JOIN gold.vw_especializacao_tematica esp
# MAGIC     ON part.part_sg_partido = esp.part_sg_partido
# MAGIC GROUP BY
# MAGIC     part.part_sg_partido;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_partidos_perfil AS
# MAGIC SELECT
# MAGIC     ana.part_sg_partido,
# MAGIC
# MAGIC     ana.part_qt_deputados,
# MAGIC     ana.part_vl_total_liquido,
# MAGIC     ana.part_vl_medio_por_deputado,
# MAGIC     ana.part_qt_votos,
# MAGIC     ana.part_qt_frentes,
# MAGIC     ana.part_pc_medio_fidelidade,
# MAGIC     ana.part_score_medio_transparencia,
# MAGIC     ana.part_score_medio_eficiencia,
# MAGIC
# MAGIC     fid.part_pc_fidelidade_partidaria,
# MAGIC     fid.part_tx_faixa_disciplina_partidaria,
# MAGIC
# MAGIC     vot.part_pc_votos_sim,
# MAGIC     vot.part_pc_votos_nao,
# MAGIC     vot.part_pc_abstencao,
# MAGIC     vot.part_pc_obstrucao,
# MAGIC
# MAGIC     tema.part_tx_especializacao_tematica,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN tema.part_tx_especializacao_tematica IN ('Agro', 'Segurança')
# MAGIC             THEN 'Perfil conservador temático'
# MAGIC
# MAGIC         WHEN tema.part_tx_especializacao_tematica IN ('Meio ambiente', 'Direitos das mulheres')
# MAGIC             THEN 'Perfil progressista temático'
# MAGIC
# MAGIC         WHEN fid.part_pc_fidelidade_partidaria >= 80
# MAGIC             THEN 'Perfil altamente disciplinado'
# MAGIC
# MAGIC         WHEN fid.part_pc_fidelidade_partidaria < 50
# MAGIC             THEN 'Perfil heterogêneo/independente'
# MAGIC
# MAGIC         ELSE 'Perfil institucional/moderado'
# MAGIC     END AS part_tx_perfil_comportamental
# MAGIC
# MAGIC FROM gold.vw_partidos_analitica ana
# MAGIC LEFT JOIN gold.vw_partidos_fidelidade_votacao fid
# MAGIC     ON ana.part_sg_partido = fid.part_sg_partido
# MAGIC LEFT JOIN gold.vw_partidos_votos_distribuicao vot
# MAGIC     ON ana.part_sg_partido = vot.part_sg_partido
# MAGIC LEFT JOIN gold.vw_partidos_especializacao_tematica tema
# MAGIC     ON ana.part_sg_partido = tema.part_sg_partido;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_perfil_gasto_partido AS
# MAGIC WITH base AS (
# MAGIC     SELECT
# MAGIC         part.part_sg_partido,
# MAGIC         tipo.desp_tx_segmento_despesa,
# MAGIC
# MAGIC         COUNT(DISTINCT ft.sk_dept) AS part_qt_deputados,
# MAGIC         COUNT(DISTINCT ft.desp_id_documento) AS part_qt_documentos,
# MAGIC         COUNT(DISTINCT ft.sk_forn) AS part_qt_fornecedores,
# MAGIC
# MAGIC         SUM(ft.desp_vl_liquido) AS part_vl_total_liquido,
# MAGIC         AVG(ft.desp_vl_liquido) AS part_vl_medio_liquido,
# MAGIC         MAX(ft.desp_vl_liquido) AS part_vl_max_liquido
# MAGIC     FROM gold.ft_despesas_ceap ft
# MAGIC     LEFT JOIN gold.dm_partido part
# MAGIC         ON ft.sk_part = part.sk_part
# MAGIC     LEFT JOIN gold.dm_tipo_despesa tipo
# MAGIC         ON ft.sk_desp_tipo = tipo.sk_desp_tipo
# MAGIC     GROUP BY
# MAGIC         part.part_sg_partido,
# MAGIC         tipo.desp_tx_segmento_despesa
# MAGIC ),
# MAGIC
# MAGIC total_partido AS (
# MAGIC     SELECT
# MAGIC         part_sg_partido,
# MAGIC         SUM(part_vl_total_liquido) AS part_vl_total_geral
# MAGIC     FROM base
# MAGIC     GROUP BY part_sg_partido
# MAGIC ),
# MAGIC
# MAGIC perfil AS (
# MAGIC     SELECT
# MAGIC         b.*,
# MAGIC         t.part_vl_total_geral,
# MAGIC
# MAGIC         ROUND(
# MAGIC             b.part_vl_total_liquido / NULLIF(t.part_vl_total_geral, 0) * 100,
# MAGIC             2
# MAGIC         ) AS part_pc_segmento,
# MAGIC
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY b.part_sg_partido
# MAGIC             ORDER BY b.part_vl_total_liquido DESC
# MAGIC         ) AS part_nr_ranking_segmento
# MAGIC     FROM base b
# MAGIC     LEFT JOIN total_partido t
# MAGIC         ON b.part_sg_partido = t.part_sg_partido
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     part_sg_partido,
# MAGIC     desp_tx_segmento_despesa,
# MAGIC     part_qt_deputados,
# MAGIC     part_qt_documentos,
# MAGIC     part_qt_fornecedores,
# MAGIC     part_vl_total_liquido,
# MAGIC     part_vl_medio_liquido,
# MAGIC     part_vl_max_liquido,
# MAGIC     part_vl_total_geral,
# MAGIC     part_pc_segmento,
# MAGIC     part_nr_ranking_segmento,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN part_nr_ranking_segmento = 1 THEN 'Principal categoria de gasto'
# MAGIC         WHEN part_nr_ranking_segmento <= 3 THEN 'Categoria relevante'
# MAGIC         ELSE 'Categoria complementar'
# MAGIC     END AS part_tx_relevancia_segmento
# MAGIC
# MAGIC FROM perfil;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_dashboard_partidos AS
# MAGIC WITH principal_segmento AS (
# MAGIC     SELECT
# MAGIC         part_sg_partido,
# MAGIC         desp_tx_segmento_despesa,
# MAGIC         part_pc_segmento,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY part_sg_partido
# MAGIC             ORDER BY part_pc_segmento DESC
# MAGIC         ) AS rn
# MAGIC     FROM gold.vw_perfil_gasto_partido
# MAGIC ),
# MAGIC
# MAGIC concentracao AS (
# MAGIC     SELECT
# MAGIC         part_sg_partido,
# MAGIC
# MAGIC         MAX(part_pc_segmento) AS part_pc_maior_segmento,
# MAGIC
# MAGIC         COUNT(
# MAGIC             CASE
# MAGIC                 WHEN part_pc_segmento >= 20 THEN 1
# MAGIC             END
# MAGIC         ) AS part_qt_segmentos_relevantes
# MAGIC
# MAGIC     FROM gold.vw_perfil_gasto_partido
# MAGIC     GROUP BY part_sg_partido
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     perfil.part_sg_partido,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Estrutura partidária
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     perfil.part_qt_deputados,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Gastos parlamentares
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     ROUND(perfil.part_vl_total_liquido, 2) AS part_vl_total_liquido,
# MAGIC     ROUND(perfil.part_vl_medio_por_deputado, 2) AS part_vl_medio_por_deputado,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Fidelidade partidária
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     ROUND(perfil.part_pc_medio_fidelidade, 2) AS part_pc_medio_fidelidade,
# MAGIC     perfil.part_pc_fidelidade_partidaria,
# MAGIC     perfil.part_tx_faixa_disciplina_partidaria,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Eficiência e transparência
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     ROUND(perfil.part_score_medio_eficiencia, 4) AS part_score_medio_eficiencia,
# MAGIC     ROUND(perfil.part_score_medio_transparencia, 2) AS part_score_medio_transparencia,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Especialização temática
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     perfil.part_tx_especializacao_tematica,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Perfil comportamental
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     perfil.part_tx_perfil_comportamental,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Perfil de gasto dominante
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     seg.desp_tx_segmento_despesa AS part_tx_segmento_dominante,
# MAGIC     seg.part_pc_segmento AS part_pc_segmento_dominante,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Índice de concentração financeira
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     conc.part_pc_maior_segmento,
# MAGIC     conc.part_qt_segmentos_relevantes,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN conc.part_pc_maior_segmento >= 60
# MAGIC             THEN 'Alta concentração financeira'
# MAGIC
# MAGIC         WHEN conc.part_pc_maior_segmento >= 40
# MAGIC             THEN 'Média concentração financeira'
# MAGIC
# MAGIC         ELSE 'Baixa concentração financeira'
# MAGIC     END AS part_tx_faixa_concentracao_financeira,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN conc.part_qt_segmentos_relevantes <= 2
# MAGIC             THEN 'Perfil financeiro concentrado'
# MAGIC
# MAGIC         WHEN conc.part_qt_segmentos_relevantes <= 4
# MAGIC             THEN 'Perfil financeiro moderadamente diversificado'
# MAGIC
# MAGIC         ELSE 'Perfil financeiro diversificado'
# MAGIC     END AS part_tx_perfil_financeiro,
# MAGIC
# MAGIC     -- ---------------------------------------------
# MAGIC     -- Eficiência financeira parlamentar
# MAGIC     -- ---------------------------------------------
# MAGIC
# MAGIC     ROUND(
# MAGIC         perfil.part_vl_total_liquido
# MAGIC         / NULLIF(perfil.part_qt_votos, 0),
# MAGIC         2
# MAGIC     ) AS part_vl_custo_por_voto,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN ROUND(
# MAGIC             perfil.part_vl_total_liquido
# MAGIC             / NULLIF(perfil.part_qt_votos, 0),
# MAGIC             2
# MAGIC         ) <= 50
# MAGIC             THEN 'Alta eficiência financeira'
# MAGIC
# MAGIC         WHEN ROUND(
# MAGIC             perfil.part_vl_total_liquido
# MAGIC             / NULLIF(perfil.part_qt_votos, 0),
# MAGIC             2
# MAGIC         ) <= 150
# MAGIC             THEN 'Média eficiência financeira'
# MAGIC
# MAGIC         ELSE 'Baixa eficiência financeira'
# MAGIC     END AS part_tx_faixa_eficiencia_financeira
# MAGIC
# MAGIC FROM gold.vw_partidos_perfil perfil
# MAGIC
# MAGIC LEFT JOIN principal_segmento seg
# MAGIC     ON perfil.part_sg_partido = seg.part_sg_partido
# MAGIC    AND seg.rn = 1
# MAGIC
# MAGIC LEFT JOIN concentracao conc
# MAGIC     ON perfil.part_sg_partido = conc.part_sg_partido;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_analise_ineficiencia_parlamentar AS
# MAGIC
# MAGIC WITH gastos_criticos AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         ft.sk_dept,
# MAGIC
# MAGIC         SUM(
# MAGIC             CASE
# MAGIC                 WHEN tipo.desp_tx_segmento_despesa IN (
# MAGIC                     'Viagens',
# MAGIC                     'Hotéis',
# MAGIC                     'Locomoção',
# MAGIC                     'Combustível'
# MAGIC                 )
# MAGIC                 THEN ft.desp_vl_liquido
# MAGIC                 ELSE 0
# MAGIC             END
# MAGIC         ) AS desp_vl_gastos_criticos,
# MAGIC
# MAGIC         SUM(ft.desp_vl_liquido) AS desp_vl_total,
# MAGIC
# MAGIC         ROUND(
# MAGIC             SUM(
# MAGIC                 CASE
# MAGIC                     WHEN tipo.desp_tx_segmento_despesa IN (
# MAGIC                         'Viagens',
# MAGIC                         'Hotéis',
# MAGIC                         'Locomoção',
# MAGIC                         'Combustível'
# MAGIC                     )
# MAGIC                     THEN ft.desp_vl_liquido
# MAGIC                     ELSE 0
# MAGIC                 END
# MAGIC             )
# MAGIC             /
# MAGIC             NULLIF(SUM(ft.desp_vl_liquido), 0) * 100,
# MAGIC             2
# MAGIC         ) AS desp_pc_gastos_criticos
# MAGIC
# MAGIC     FROM gold.ft_despesas_ceap ft
# MAGIC     LEFT JOIN gold.dm_tipo_despesa tipo
# MAGIC         ON ft.sk_desp_tipo = tipo.sk_desp_tipo
# MAGIC     GROUP BY
# MAGIC         ft.sk_dept
# MAGIC ),
# MAGIC
# MAGIC presenca_eventos AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         sk_org,
# MAGIC         COUNT(*) AS evt_qt_eventos
# MAGIC     FROM gold.ft_presenca_eventos
# MAGIC     GROUP BY sk_org
# MAGIC ),
# MAGIC
# MAGIC atividade AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         sk_dept,
# MAGIC
# MAGIC         qt_votos,
# MAGIC         qt_frentes,
# MAGIC
# MAGIC         (
# MAGIC             qt_votos
# MAGIC             + (qt_frentes * 10)
# MAGIC             + (fl_presidente_frente * 20)
# MAGIC             + (fl_coordenador_frente * 15)
# MAGIC             + (fl_vice_frente * 10)
# MAGIC         ) AS parlamentar_score_atividade
# MAGIC
# MAGIC     FROM gold.ft_atividade_parlamentar
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     part.part_sg_partido,
# MAGIC     uf.uf_sg_uf,
# MAGIC
# MAGIC     ROUND(g.desp_vl_gastos_criticos, 2) AS desp_vl_gastos_criticos,
# MAGIC     ROUND(g.desp_vl_total, 2) AS desp_vl_total,
# MAGIC     ROUND(g.desp_pc_gastos_criticos, 2) AS desp_pc_gastos_criticos,
# MAGIC
# MAGIC     COALESCE(a.qt_votos, 0) AS qt_votos,
# MAGIC     COALESCE(a.qt_frentes, 0) AS qt_frentes,
# MAGIC     COALESCE(a.parlamentar_score_atividade, 0) AS parlamentar_score_atividade,
# MAGIC
# MAGIC     COALESCE(e.parlamentar_score_eficiencia_por_mil_reais, 0)
# MAGIC         AS parlamentar_score_eficiencia,
# MAGIC
# MAGIC     COALESCE(t.parlamentar_score_transparencia, 0)
# MAGIC         AS parlamentar_score_transparencia,
# MAGIC
# MAGIC     CASE
# MAGIC
# MAGIC         WHEN
# MAGIC             g.desp_pc_gastos_criticos >= 50
# MAGIC             AND COALESCE(a.parlamentar_score_atividade, 0) <= 100
# MAGIC             AND COALESCE(e.parlamentar_score_eficiencia_por_mil_reais, 0) < 0.3
# MAGIC
# MAGIC         THEN 'Alta ineficiência parlamentar'
# MAGIC
# MAGIC         WHEN
# MAGIC             g.desp_pc_gastos_criticos >= 35
# MAGIC             AND COALESCE(a.parlamentar_score_atividade, 0) <= 200
# MAGIC
# MAGIC         THEN 'Média ineficiência parlamentar'
# MAGIC
# MAGIC         ELSE 'Baixa ineficiência parlamentar'
# MAGIC
# MAGIC     END AS parlamentar_tx_faixa_ineficiencia
# MAGIC
# MAGIC FROM gastos_criticos g
# MAGIC
# MAGIC LEFT JOIN gold.dm_deputado dept
# MAGIC     ON g.sk_dept = dept.sk_dept
# MAGIC
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON dept.part_sg_partido = part.part_sg_partido
# MAGIC
# MAGIC LEFT JOIN gold.dm_uf uf
# MAGIC     ON dept.uf_sg_uf = uf.uf_sg_uf
# MAGIC
# MAGIC LEFT JOIN atividade a
# MAGIC     ON g.sk_dept = a.sk_dept
# MAGIC
# MAGIC LEFT JOIN gold.vw_indice_eficiencia_parlamentar e
# MAGIC     ON g.sk_dept = e.sk_dept
# MAGIC
# MAGIC LEFT JOIN gold.vw_indice_transparencia t
# MAGIC     ON g.sk_dept = t.sk_dept;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_ausencias_votacoes_criticas AS
# MAGIC
# MAGIC WITH votacoes_criticas AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         vot_id_votacao,
# MAGIC         vot_qt_sim,
# MAGIC         vot_qt_nao,
# MAGIC         vot_qt_total,
# MAGIC         vot_tx_resultado_curado,
# MAGIC
# MAGIC         ABS(vot_qt_sim - vot_qt_nao) AS vot_nr_diferenca
# MAGIC
# MAGIC     FROM gold.ft_votacoes
# MAGIC
# MAGIC     WHERE ABS(vot_qt_sim - vot_qt_nao) <= 5
# MAGIC ),
# MAGIC
# MAGIC base AS (
# MAGIC
# MAGIC     SELECT
# MAGIC         vc.vot_id_votacao,
# MAGIC         vc.vot_qt_sim,
# MAGIC         vc.vot_qt_nao,
# MAGIC         vc.vot_qt_total,
# MAGIC         vc.vot_nr_diferenca,
# MAGIC         vc.vot_tx_resultado_curado,
# MAGIC
# MAGIC         dept.id_deputado,
# MAGIC         dept.dept_tx_nome_parlamentar,
# MAGIC         part.part_sg_partido,
# MAGIC         uf.uf_sg_uf,
# MAGIC
# MAGIC         voto.vot_tx_voto_curado,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN voto.vot_tx_voto_curado IS NULL
# MAGIC                 THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS vot_fl_ausente,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN voto.vot_fl_abstencao = 1
# MAGIC                 THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS vot_fl_abstencao,
# MAGIC
# MAGIC         CASE
# MAGIC             WHEN voto.vot_fl_obstrucao = 1
# MAGIC                 THEN 1
# MAGIC             ELSE 0
# MAGIC         END AS vot_fl_obstrucao
# MAGIC
# MAGIC     FROM votacoes_criticas vc
# MAGIC
# MAGIC     LEFT JOIN gold.ft_votos voto
# MAGIC         ON vc.vot_id_votacao = voto.vot_id_votacao
# MAGIC
# MAGIC     LEFT JOIN gold.dm_deputado dept
# MAGIC         ON voto.sk_dept = dept.sk_dept
# MAGIC
# MAGIC     LEFT JOIN gold.dm_partido part
# MAGIC         ON voto.sk_part = part.sk_part
# MAGIC
# MAGIC     LEFT JOIN gold.dm_uf uf
# MAGIC         ON voto.sk_uf = uf.sk_uf
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     *,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN vot_fl_ausente = 1
# MAGIC             THEN 'Ausente em votação crítica'
# MAGIC
# MAGIC         WHEN vot_fl_abstencao = 1
# MAGIC             THEN 'Abstenção em votação crítica'
# MAGIC
# MAGIC         WHEN vot_fl_obstrucao = 1
# MAGIC             THEN 'Obstrução em votação crítica'
# MAGIC
# MAGIC         ELSE 'Participação efetiva'
# MAGIC     END AS parlamentar_tx_comportamento_critico
# MAGIC
# MAGIC FROM base;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_ranking_ausencias_criticas AS
# MAGIC SELECT
# MAGIC     part_sg_partido,
# MAGIC     id_deputado,
# MAGIC     dept_tx_nome_parlamentar,
# MAGIC
# MAGIC     COUNT(*) AS qt_votacoes_criticas,
# MAGIC
# MAGIC     SUM(vot_fl_ausente) AS qt_ausencias_criticas,
# MAGIC     SUM(vot_fl_abstencao) AS qt_abstencoes_criticas,
# MAGIC     SUM(vot_fl_obstrucao) AS qt_obstrucoes_criticas
# MAGIC
# MAGIC FROM gold.vw_ausencias_votacoes_criticas
# MAGIC
# MAGIC GROUP BY
# MAGIC     part_sg_partido,
# MAGIC     id_deputado,
# MAGIC     dept_tx_nome_parlamentar;