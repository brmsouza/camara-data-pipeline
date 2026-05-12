# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_despesas_ceap_analitica AS
# MAGIC SELECT
# MAGIC     ft.sk_dept,
# MAGIC     ft.sk_part,
# MAGIC     ft.sk_leg,
# MAGIC     ft.sk_forn,
# MAGIC     ft.sk_desp_tipo,
# MAGIC     ft.sk_uf,
# MAGIC     ft.sk_data_emissao,
# MAGIC
# MAGIC     d.data_dt_data AS desp_dt_emissao,
# MAGIC     d.data_nr_ano AS desp_nr_ano,
# MAGIC     d.data_nr_mes AS desp_nr_mes,
# MAGIC     d.data_tx_ano_mes,
# MAGIC
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     dept.dept_tx_status_mandato_curado,
# MAGIC
# MAGIC     part.part_sg_partido,
# MAGIC     leg.leg_id_legislatura,
# MAGIC     leg.leg_nr_ano_eleicao,
# MAGIC     uf.uf_sg_uf,
# MAGIC
# MAGIC     forn.forn_nr_cnpj_cpf,
# MAGIC     forn.forn_tx_nome,
# MAGIC     forn.forn_tx_tipo_documento,
# MAGIC
# MAGIC     tipo.desp_cd_subcota,
# MAGIC     tipo.desp_tx_tipo_despesa,
# MAGIC     tipo.desp_cd_especificacao_subcota,
# MAGIC     tipo.desp_tx_especificacao,
# MAGIC
# MAGIC     ft.desp_id_documento,
# MAGIC     ft.desp_nr_documento,
# MAGIC     ft.desp_vl_documento,
# MAGIC     ft.desp_vl_glosa,
# MAGIC     ft.desp_vl_liquido,
# MAGIC     ft.desp_vl_restituicao,
# MAGIC     ft.desp_fl_possui_glosa,
# MAGIC     ft.desp_fl_possui_restituicao,
# MAGIC     ft.desp_fl_valor_negativo,
# MAGIC     ft.desp_fl_possui_documento_url
# MAGIC
# MAGIC FROM gold.ft_despesas_ceap ft
# MAGIC LEFT JOIN gold.dm_data d
# MAGIC     ON ft.sk_data_emissao = d.sk_data
# MAGIC LEFT JOIN gold.dm_deputado dept
# MAGIC     ON ft.sk_dept = dept.sk_dept
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON ft.sk_part = part.sk_part
# MAGIC LEFT JOIN gold.dm_legislatura leg
# MAGIC     ON ft.sk_leg = leg.sk_leg
# MAGIC LEFT JOIN gold.dm_uf uf
# MAGIC     ON ft.sk_uf = uf.sk_uf
# MAGIC LEFT JOIN gold.dm_fornecedor forn
# MAGIC     ON ft.sk_forn = forn.sk_forn
# MAGIC LEFT JOIN gold.dm_tipo_despesa tipo
# MAGIC     ON ft.sk_desp_tipo = tipo.sk_desp_tipo;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_ranking_despesas_deputado_mensal AS
# MAGIC SELECT
# MAGIC     data_tx_ano_mes,
# MAGIC     desp_nr_ano,
# MAGIC     desp_nr_mes,
# MAGIC     id_deputado,
# MAGIC     dept_tx_nome_parlamentar,
# MAGIC     part_sg_partido,
# MAGIC     uf_sg_uf,
# MAGIC
# MAGIC     COUNT(DISTINCT desp_id_documento) AS qt_documentos,
# MAGIC     SUM(desp_vl_documento) AS vl_total_documento,
# MAGIC     SUM(desp_vl_glosa) AS vl_total_glosa,
# MAGIC     SUM(desp_vl_liquido) AS vl_total_liquido,
# MAGIC     SUM(desp_vl_restituicao) AS vl_total_restituicao,
# MAGIC     AVG(desp_vl_liquido) AS vl_medio_liquido,
# MAGIC
# MAGIC     SUM(desp_fl_possui_glosa) AS qt_despesas_com_glosa,
# MAGIC     SUM(desp_fl_possui_restituicao) AS qt_despesas_com_restituicao
# MAGIC
# MAGIC FROM gold.vw_despesas_ceap_analitica
# MAGIC GROUP BY
# MAGIC     data_tx_ano_mes,
# MAGIC     desp_nr_ano,
# MAGIC     desp_nr_mes,
# MAGIC     id_deputado,
# MAGIC     dept_tx_nome_parlamentar,
# MAGIC     part_sg_partido,
# MAGIC     uf_sg_uf;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_votacoes_analitica AS
# MAGIC SELECT
# MAGIC     ft.vot_id_votacao,
# MAGIC     ft.vot_tx_uri,
# MAGIC
# MAGIC     d.data_dt_data AS vot_dt_votacao,
# MAGIC     d.data_nr_ano AS vot_nr_ano,
# MAGIC     d.data_nr_mes AS vot_nr_mes,
# MAGIC     d.data_tx_ano_mes,
# MAGIC
# MAGIC     prop.prop_id_proposicao,
# MAGIC     prop.prop_sg_tipo,
# MAGIC     prop.prop_nr_numero,
# MAGIC     prop.prop_nr_ano,
# MAGIC     prop.prop_tx_ementa,
# MAGIC     prop.prop_tx_status_curado,
# MAGIC     prop.prop_fl_tramitando,
# MAGIC     prop.prop_fl_aprovada,
# MAGIC     prop.prop_fl_rejeitada,
# MAGIC
# MAGIC     org.org_id_orgao,
# MAGIC     org.org_sg_orgao,
# MAGIC     org.org_tx_nome,
# MAGIC     org.org_tx_tipo_curado,
# MAGIC
# MAGIC     evt.evt_id_evento,
# MAGIC     evt.evt_tx_tipo_curado,
# MAGIC     evt.evt_tx_situacao_curada,
# MAGIC
# MAGIC     ft.vot_tx_descricao,
# MAGIC     ft.vot_tx_status_aprovacao,
# MAGIC     ft.vot_tx_resultado_curado,
# MAGIC     ft.vot_fl_aprovada,
# MAGIC     ft.vot_fl_rejeitada,
# MAGIC     ft.vot_qt_sim,
# MAGIC     ft.vot_qt_nao,
# MAGIC     ft.vot_qt_outros,
# MAGIC     ft.vot_qt_total,
# MAGIC     ft.vot_fl_possui_votos_contabilizados
# MAGIC
# MAGIC FROM gold.ft_votacoes ft
# MAGIC LEFT JOIN gold.dm_data d
# MAGIC     ON ft.sk_data_votacao = d.sk_data
# MAGIC LEFT JOIN gold.dm_proposicao prop
# MAGIC     ON ft.sk_prop = prop.sk_prop
# MAGIC LEFT JOIN gold.dm_orgao org
# MAGIC     ON ft.sk_org = org.sk_org
# MAGIC LEFT JOIN gold.dm_evento evt
# MAGIC     ON ft.sk_evt = evt.sk_evt;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_votos_deputados_analitica AS
# MAGIC SELECT
# MAGIC     ft.vot_id_votacao,
# MAGIC
# MAGIC     d.data_dt_data AS vot_dt_voto,
# MAGIC     d.data_nr_ano AS vot_nr_ano,
# MAGIC     d.data_nr_mes AS vot_nr_mes,
# MAGIC     d.data_tx_ano_mes,
# MAGIC
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     dept.dept_tx_status_mandato_curado,
# MAGIC
# MAGIC     part.part_sg_partido,
# MAGIC     leg.leg_id_legislatura,
# MAGIC     uf.uf_sg_uf,
# MAGIC
# MAGIC     ft.vot_tx_voto,
# MAGIC     ft.vot_tx_voto_curado,
# MAGIC     ft.vot_fl_sim,
# MAGIC     ft.vot_fl_nao,
# MAGIC     ft.vot_fl_abstencao,
# MAGIC     ft.vot_fl_obstrucao,
# MAGIC
# MAGIC     vot.prop_id_proposicao,
# MAGIC     vot.prop_sg_tipo,
# MAGIC     vot.prop_nr_numero,
# MAGIC     vot.prop_nr_ano,
# MAGIC     vot.prop_tx_ementa,
# MAGIC     vot.org_sg_orgao,
# MAGIC     vot.vot_tx_resultado_curado
# MAGIC
# MAGIC FROM gold.ft_votos ft
# MAGIC LEFT JOIN gold.dm_data d
# MAGIC     ON ft.sk_data_voto = d.sk_data
# MAGIC LEFT JOIN gold.dm_deputado dept
# MAGIC     ON ft.sk_dept = dept.sk_dept
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON ft.sk_part = part.sk_part
# MAGIC LEFT JOIN gold.dm_legislatura leg
# MAGIC     ON ft.sk_leg = leg.sk_leg
# MAGIC LEFT JOIN gold.dm_uf uf
# MAGIC     ON ft.sk_uf = uf.sk_uf
# MAGIC LEFT JOIN gold.vw_votacoes_analitica vot
# MAGIC     ON ft.vot_id_votacao = vot.vot_id_votacao;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_orientacoes_bancada_analitica AS
# MAGIC SELECT
# MAGIC     ft.vot_id_votacao,
# MAGIC
# MAGIC     banc.banc_tx_sigla_bancada,
# MAGIC     banc.banc_tx_uri,
# MAGIC
# MAGIC     org.org_sg_orgao,
# MAGIC     org.org_tx_nome,
# MAGIC     org.org_tx_tipo_curado,
# MAGIC
# MAGIC     ft.vot_tx_orientacao,
# MAGIC     ft.vot_tx_orientacao_curada,
# MAGIC     ft.vot_tx_descricao_resultado,
# MAGIC
# MAGIC     ft.vot_fl_orientacao_sim,
# MAGIC     ft.vot_fl_orientacao_nao,
# MAGIC     ft.vot_fl_orientacao_liberado,
# MAGIC     ft.vot_fl_orientacao_obstrucao,
# MAGIC     ft.vot_fl_orientacao_abstencao,
# MAGIC
# MAGIC     ft.bronze_nr_ano_referencia
# MAGIC
# MAGIC FROM gold.ft_orientacoes_bancada ft
# MAGIC LEFT JOIN gold.dm_bancada banc
# MAGIC     ON ft.sk_banc = banc.sk_banc
# MAGIC LEFT JOIN gold.dm_orgao org
# MAGIC     ON ft.sk_org = org.sk_org;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_eventos_analitica AS
# MAGIC SELECT
# MAGIC     ft.sk_evt,
# MAGIC     ft.evt_id_evento,
# MAGIC
# MAGIC     d.data_dt_data AS evt_dt_inicio,
# MAGIC     d.data_nr_ano AS evt_nr_ano,
# MAGIC     d.data_nr_mes AS evt_nr_mes,
# MAGIC     d.data_tx_ano_mes,
# MAGIC
# MAGIC     org.org_id_orgao,
# MAGIC     org.org_sg_orgao,
# MAGIC     org.org_tx_nome,
# MAGIC     org.org_tx_tipo_curado,
# MAGIC
# MAGIC     ft.evt_tx_descricao,
# MAGIC     ft.evt_tx_tipo,
# MAGIC     ft.evt_tx_situacao,
# MAGIC     ft.evt_tx_tipo_curado,
# MAGIC     ft.evt_tx_situacao_curada,
# MAGIC
# MAGIC     ft.evt_fl_sessao,
# MAGIC     ft.evt_fl_audiencia_publica,
# MAGIC     ft.evt_fl_reuniao,
# MAGIC     ft.evt_fl_encerrado,
# MAGIC     ft.evt_fl_cancelado,
# MAGIC     ft.evt_fl_possui_registro,
# MAGIC
# MAGIC     ft.evt_tx_local_interno,
# MAGIC     ft.evt_tx_predio,
# MAGIC     ft.evt_tx_sala,
# MAGIC     ft.evt_tx_andar,
# MAGIC     ft.evt_tx_local_externo,
# MAGIC     ft.evt_tx_tipo_local,
# MAGIC     ft.evt_qt_orgaos
# MAGIC
# MAGIC FROM gold.ft_presenca_eventos ft
# MAGIC LEFT JOIN gold.dm_data d
# MAGIC     ON ft.sk_data_inicio = d.sk_data
# MAGIC LEFT JOIN gold.dm_orgao org
# MAGIC     ON ft.sk_org = org.sk_org;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_atividade_parlamentar_analitica AS
# MAGIC SELECT
# MAGIC     ft.sk_dept,
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     dept.dept_tx_status_mandato_curado,
# MAGIC
# MAGIC     part.part_sg_partido,
# MAGIC     leg.leg_id_legislatura,
# MAGIC     leg.leg_nr_ano_eleicao,
# MAGIC     uf.uf_sg_uf,
# MAGIC
# MAGIC     ft.qt_despesas,
# MAGIC     ft.vl_total_documento,
# MAGIC     ft.vl_total_glosa,
# MAGIC     ft.vl_total_liquido,
# MAGIC     ft.vl_total_restituicao,
# MAGIC     ft.fl_possui_glosa,
# MAGIC     ft.fl_possui_restituicao,
# MAGIC
# MAGIC     ft.qt_votos,
# MAGIC     ft.qt_votos_sim,
# MAGIC     ft.qt_votos_nao,
# MAGIC     ft.qt_votos_abstencao,
# MAGIC     ft.qt_votos_obstrucao,
# MAGIC
# MAGIC     ft.qt_frentes,
# MAGIC     ft.fl_coordenador_frente,
# MAGIC     ft.fl_presidente_frente,
# MAGIC     ft.fl_vice_frente,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN ft.qt_votos >= 100 AND ft.qt_frentes >= 3 THEN 'Alta atividade'
# MAGIC         WHEN ft.qt_votos >= 50 OR ft.qt_frentes >= 1 THEN 'Média atividade'
# MAGIC         ELSE 'Baixa atividade'
# MAGIC     END AS parlamentar_tx_faixa_atividade
# MAGIC
# MAGIC FROM gold.ft_atividade_parlamentar ft
# MAGIC LEFT JOIN gold.dm_deputado dept
# MAGIC     ON ft.sk_dept = dept.sk_dept
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON ft.sk_part = part.sk_part
# MAGIC LEFT JOIN gold.dm_legislatura leg
# MAGIC     ON ft.sk_leg = leg.sk_leg
# MAGIC LEFT JOIN gold.dm_uf uf
# MAGIC     ON ft.sk_uf = uf.sk_uf;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_ranking_despesas_segmento AS
# MAGIC SELECT
# MAGIC     tipo.desp_tx_segmento_despesa,
# MAGIC     tipo.desp_tx_tipo_despesa,
# MAGIC     ft.desp_nr_ano,
# MAGIC     ft.desp_nr_mes,
# MAGIC     d.data_tx_ano_mes,
# MAGIC
# MAGIC     COUNT(DISTINCT ft.desp_id_documento) AS qt_documentos,
# MAGIC     COUNT(DISTINCT ft.sk_dept) AS qt_deputados,
# MAGIC     COUNT(DISTINCT ft.sk_forn) AS qt_fornecedores,
# MAGIC
# MAGIC     SUM(ft.desp_vl_documento) AS vl_total_documento,
# MAGIC     SUM(ft.desp_vl_glosa) AS vl_total_glosa,
# MAGIC     SUM(ft.desp_vl_liquido) AS vl_total_liquido,
# MAGIC     SUM(ft.desp_vl_restituicao) AS vl_total_restituicao,
# MAGIC
# MAGIC     AVG(ft.desp_vl_liquido) AS vl_medio_liquido,
# MAGIC     MAX(ft.desp_vl_liquido) AS vl_max_liquido
# MAGIC
# MAGIC FROM gold.ft_despesas_ceap ft
# MAGIC LEFT JOIN gold.dm_tipo_despesa tipo
# MAGIC     ON ft.sk_desp_tipo = tipo.sk_desp_tipo
# MAGIC LEFT JOIN gold.dm_data d
# MAGIC     ON ft.sk_data_emissao = d.sk_data
# MAGIC GROUP BY
# MAGIC     tipo.desp_tx_segmento_despesa,
# MAGIC     tipo.desp_tx_tipo_despesa,
# MAGIC     ft.desp_nr_ano,
# MAGIC     ft.desp_nr_mes,
# MAGIC     d.data_tx_ano_mes;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_despesas_deputado_segmento AS
# MAGIC SELECT
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     part.part_sg_partido,
# MAGIC     uf.uf_sg_uf,
# MAGIC     tipo.desp_tx_segmento_despesa,
# MAGIC     d.data_tx_ano_mes,
# MAGIC
# MAGIC     COUNT(DISTINCT ft.desp_id_documento) AS qt_documentos,
# MAGIC     COUNT(DISTINCT ft.sk_forn) AS qt_fornecedores,
# MAGIC     SUM(ft.desp_vl_liquido) AS vl_total_liquido,
# MAGIC     AVG(ft.desp_vl_liquido) AS vl_medio_liquido,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(ft.desp_vl_liquido)
# MAGIC         / SUM(SUM(ft.desp_vl_liquido)) OVER (
# MAGIC             PARTITION BY dept.id_deputado, d.data_tx_ano_mes
# MAGIC         ) * 100,
# MAGIC         2
# MAGIC     ) AS perc_participacao_segmento
# MAGIC
# MAGIC FROM gold.ft_despesas_ceap ft
# MAGIC LEFT JOIN gold.dm_deputado dept
# MAGIC     ON ft.sk_dept = dept.sk_dept
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON ft.sk_part = part.sk_part
# MAGIC LEFT JOIN gold.dm_uf uf
# MAGIC     ON ft.sk_uf = uf.sk_uf
# MAGIC LEFT JOIN gold.dm_tipo_despesa tipo
# MAGIC     ON ft.sk_desp_tipo = tipo.sk_desp_tipo
# MAGIC LEFT JOIN gold.dm_data d
# MAGIC     ON ft.sk_data_emissao = d.sk_data
# MAGIC GROUP BY
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     part.part_sg_partido,
# MAGIC     uf.uf_sg_uf,
# MAGIC     tipo.desp_tx_segmento_despesa,
# MAGIC     d.data_tx_ano_mes;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vw_perfil_parlamentar AS
# MAGIC WITH base AS (
# MAGIC
# MAGIC SELECT
# MAGIC     dept.sk_dept,
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     part.part_sg_partido,
# MAGIC     uf.uf_sg_uf,
# MAGIC
# MAGIC     SUM(voto.vot_fl_sim) AS qt_votos_sim,
# MAGIC     SUM(voto.vot_fl_nao) AS qt_votos_nao,
# MAGIC     SUM(voto.vot_fl_abstencao) AS qt_abstencoes,
# MAGIC     SUM(voto.vot_fl_obstrucao) AS qt_obstrucoes,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN ori.vot_tx_orientacao_curada = voto.vot_tx_voto_curado
# MAGIC             THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS qt_alinhado_bancada,
# MAGIC
# MAGIC     COUNT(*) AS qt_total_votos,
# MAGIC
# MAGIC     MAX(ativ.fl_presidente_frente) AS fl_presidente_frente,
# MAGIC     MAX(ativ.fl_coordenador_frente) AS fl_coordenador_frente,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN frente.frente_fl_tema_agro = 1 THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS score_agro,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN frente.frente_fl_tema_meio_ambiente = 1 THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS score_meio_ambiente,
# MAGIC
# MAGIC     SUM(
# MAGIC         CASE
# MAGIC             WHEN frente.frente_fl_tema_mulher = 1 THEN 1
# MAGIC             ELSE 0
# MAGIC         END
# MAGIC     ) AS score_mulher
# MAGIC
# MAGIC FROM gold.ft_votos voto
# MAGIC
# MAGIC LEFT JOIN gold.dm_deputado dept
# MAGIC     ON voto.sk_dept = dept.sk_dept
# MAGIC
# MAGIC LEFT JOIN gold.dm_partido part
# MAGIC     ON voto.sk_part = part.sk_part
# MAGIC
# MAGIC LEFT JOIN gold.dm_uf uf
# MAGIC     ON voto.sk_uf = uf.sk_uf
# MAGIC
# MAGIC LEFT JOIN gold.ft_orientacoes_bancada ori
# MAGIC     ON voto.vot_id_votacao = ori.vot_id_votacao
# MAGIC
# MAGIC LEFT JOIN gold.ft_atividade_parlamentar ativ
# MAGIC     ON voto.sk_dept = ativ.sk_dept
# MAGIC
# MAGIC LEFT JOIN gold.dm_frente frente
# MAGIC     ON ativ.sk_dept = frente.sk_frente
# MAGIC
# MAGIC GROUP BY
# MAGIC     dept.sk_dept,
# MAGIC     dept.id_deputado,
# MAGIC     dept.dept_tx_nome_parlamentar,
# MAGIC     part.part_sg_partido,
# MAGIC     uf.uf_sg_uf
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC     *,
# MAGIC
# MAGIC     ROUND(
# MAGIC         qt_alinhado_bancada / qt_total_votos * 100,
# MAGIC         2
# MAGIC     ) AS perc_alinhamento_bancada,
# MAGIC
# MAGIC     CASE
# MAGIC         WHEN score_agro >= 3
# MAGIC             AND score_meio_ambiente = 0
# MAGIC             THEN 'Conservador'
# MAGIC
# MAGIC         WHEN score_meio_ambiente >= 2
# MAGIC             OR score_mulher >= 2
# MAGIC             THEN 'Progressista'
# MAGIC
# MAGIC         WHEN qt_obstrucoes >= 50
# MAGIC             THEN 'Oposicionista'
# MAGIC
# MAGIC         WHEN qt_alinhado_bancada >= qt_total_votos * 0.8
# MAGIC             THEN 'Governista'
# MAGIC
# MAGIC         ELSE 'Moderado'
# MAGIC     END AS parlamentar_tx_perfil
# MAGIC
# MAGIC FROM base;