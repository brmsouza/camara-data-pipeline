# Databricks notebook source
# MAGIC %sql
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Notebook: 04_create_cdc_scd2_objects
# MAGIC -- Layer: Setup
# MAGIC --
# MAGIC -- Description:
# MAGIC -- Creates control, Bronze, Silver and Analytics objects required for the
# MAGIC -- proposicoes tramitacoes CDC / SCD Type 2 pipeline.
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS control;
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze_cdc;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_cdc;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold_cdc;
# MAGIC CREATE SCHEMA IF NOT EXISTS monitoring;
# MAGIC
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- CDC offset / control table
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS control.proposicoes_tramitacoes_cdc_control (
# MAGIC     pipeline_name STRING,
# MAGIC     last_processed_proposicao_id BIGINT,
# MAGIC     last_processed_ts TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC MERGE INTO control.proposicoes_tramitacoes_cdc_control AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         'proposicoes_tramitacoes_cdc' AS pipeline_name,
# MAGIC         0 AS last_processed_proposicao_id,
# MAGIC         TIMESTAMP('1900-01-01 00:00:00') AS last_processed_ts,
# MAGIC         current_timestamp() AS updated_at
# MAGIC ) AS source
# MAGIC ON target.pipeline_name = source.pipeline_name
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         pipeline_name,
# MAGIC         last_processed_proposicao_id,
# MAGIC         last_processed_ts,
# MAGIC         updated_at
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.pipeline_name,
# MAGIC         source.last_processed_proposicao_id,
# MAGIC         source.last_processed_ts,
# MAGIC         source.updated_at
# MAGIC     );
# MAGIC
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Bronze CDC raw table
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS bronze_cdc.proposicoes_tramitacoes_raw (
# MAGIC     prop_id_proposicao BIGINT,
# MAGIC     tram_id_evento STRING,
# MAGIC     bronze_tx_endpoint STRING,
# MAGIC     bronze_id_batch STRING,
# MAGIC     bronze_ts_ingestao TIMESTAMP,
# MAGIC     bronze_dt_ingestao DATE,
# MAGIC     bronze_tx_payload STRING,
# MAGIC     bronze_tx_payload_hash STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Silver CDC normalized table
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_cdc.proposicoes_tramitacoes_base (
# MAGIC     prop_id_proposicao BIGINT,
# MAGIC     tram_id_evento STRING,
# MAGIC     tram_ts_tramitacao TIMESTAMP,
# MAGIC     tram_dt_tramitacao DATE,
# MAGIC     tram_tx_sequencia STRING,
# MAGIC     tram_tx_sigla_orgao STRING,
# MAGIC     tram_tx_uri_orgao STRING,
# MAGIC     tram_tx_regime STRING,
# MAGIC     tram_tx_descricao_tramitacao STRING,
# MAGIC     tram_tx_descricao_situacao STRING,
# MAGIC     tram_tx_despacho STRING,
# MAGIC     tram_tx_url STRING,
# MAGIC     cdc_payload_hash STRING,
# MAGIC     bronze_id_batch STRING,
# MAGIC     bronze_ts_ingestao TIMESTAMP,
# MAGIC     silver_ts_processamento TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Silver SCD Type 2 table
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS silver_cdc.proposicoes_tramitacoes_scd2 (
# MAGIC     prop_id_proposicao BIGINT,
# MAGIC     tram_id_evento STRING,
# MAGIC     tram_ts_tramitacao TIMESTAMP,
# MAGIC     tram_dt_tramitacao DATE,
# MAGIC     tram_tx_sigla_orgao STRING,
# MAGIC     tram_tx_regime STRING,
# MAGIC     tram_tx_descricao_tramitacao STRING,
# MAGIC     tram_tx_descricao_situacao STRING,
# MAGIC     tram_tx_despacho STRING,
# MAGIC     cdc_payload_hash STRING,
# MAGIC     valid_from TIMESTAMP,
# MAGIC     valid_to TIMESTAMP,
# MAGIC     is_current BOOLEAN,
# MAGIC     scd_ts_processamento TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Gold alerts table
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS gold_cdc.proposicoes_tramitacoes_alertas (
# MAGIC     prop_id_proposicao BIGINT,
# MAGIC     tram_id_evento STRING,
# MAGIC     tram_ts_tramitacao TIMESTAMP,
# MAGIC     alert_tx_tipo STRING,
# MAGIC     alert_tx_mensagem STRING,
# MAGIC     alert_fl_notificar INT,
# MAGIC     cdc_payload_hash STRING,
# MAGIC     gold_ts_processamento TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;