# Databricks notebook source
# MAGIC %sql
# MAGIC -- FKs nulas nas facts
# MAGIC SELECT 'ft_despesas_ceap' AS tabela, COUNT(*) AS qt_registros
# MAGIC FROM gold.ft_despesas_ceap
# MAGIC WHERE sk_dept IS NULL OR sk_data_emissao IS NULL;
# MAGIC
# MAGIC SELECT 'ft_votos' AS tabela, COUNT(*) AS qt_registros
# MAGIC FROM gold.ft_votos
# MAGIC WHERE sk_dept IS NULL OR sk_data_voto IS NULL;
# MAGIC
# MAGIC SELECT 'ft_votacoes' AS tabela, COUNT(*) AS qt_registros
# MAGIC FROM gold.ft_votacoes
# MAGIC WHERE sk_data_votacao IS NULL;
# MAGIC
# MAGIC SELECT 'ft_presenca_eventos' AS tabela, COUNT(*) AS qt_registros
# MAGIC FROM gold.ft_presenca_eventos
# MAGIC WHERE sk_evt IS NULL OR sk_data_inicio IS NULL;
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE VIEW gold.vw_quality_gold_model AS
# MAGIC SELECT 'ft_despesas_ceap' AS tabela, COUNT(*) AS qt_registros FROM gold.ft_despesas_ceap
# MAGIC UNION ALL
# MAGIC SELECT 'ft_votos', COUNT(*) FROM gold.ft_votos
# MAGIC UNION ALL
# MAGIC SELECT 'ft_votacoes', COUNT(*) FROM gold.ft_votacoes
# MAGIC UNION ALL
# MAGIC SELECT 'ft_orientacoes_bancada', COUNT(*) FROM gold.ft_orientacoes_bancada
# MAGIC UNION ALL
# MAGIC SELECT 'ft_presenca_eventos', COUNT(*) FROM gold.ft_presenca_eventos
# MAGIC UNION ALL
# MAGIC SELECT 'ft_atividade_parlamentar', COUNT(*) FROM gold.ft_atividade_parlamentar;