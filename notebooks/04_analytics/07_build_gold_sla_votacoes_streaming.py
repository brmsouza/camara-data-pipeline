# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Analytics Layer — Streaming SLA and Operational Monitoring
# MAGIC
# MAGIC **Notebook:** `07_sla_votacoes_streaming`
# MAGIC
# MAGIC Builds operational SLA monitoring views for the parliamentary voting
# MAGIC streaming pipeline in the Gold Analytics layer.
# MAGIC
# MAGIC This notebook consolidates operational and streaming execution metadata
# MAGIC related to the real-time voting ingestion pipeline, supporting monitoring
# MAGIC of end-to-end latency, execution health, ingestion volume and operational
# MAGIC quality indicators for streaming workloads.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Read streaming operational monitoring datasets
# MAGIC - Build SLA and streaming execution monitoring views
# MAGIC - Monitor end-to-end streaming latency indicators
# MAGIC - Monitor ingestion volume and execution health metrics
# MAGIC - Support operational observability for streaming workloads
# MAGIC - Support replay and streaming incident analysis
# MAGIC - Preserve operational lineage and monitoring metadata
# MAGIC - Persist Gold operational monitoring views
# MAGIC - Register operational execution metrics
# MAGIC
# MAGIC ## Sources
# MAGIC
# MAGIC - `monitoring.pipeline_log`
# MAGIC - `bronze_stream.votacoes_raw`
# MAGIC - `silver_stream_votacoes_validas`
# MAGIC - `gold_stream_votacoes_alertas`
# MAGIC
# MAGIC ## Views / Analytical Objects
# MAGIC
# MAGIC - `monitoring.vw_sla_votacoes_streaming`
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Idempotent execution
# MAGIC - Delta Lake analytical monitoring layer
# MAGIC - Supports streaming observability and SLA monitoring
# MAGIC - Supports operational governance for real-time voting pipelines

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW monitoring.vw_sla_votacoes_streaming AS
-- -----------------------------------------------------------------------------
-- View: monitoring.vw_sla_votacoes_streaming
-- Layer: Monitoring / Analytics
--
-- Description:
-- SLA monitoring view for the real-time voting micro-batch pipeline.
-- Provides execution latency, volume, inferred discarded records and execution
-- status by batch.
--
-- Grain:
-- One row per pipeline batch execution.
--
-- Source:
-- monitoring.pipeline_log
-- -----------------------------------------------------------------------------

WITH execution_log AS (

    SELECT
        pipeline_name,
        batch_id,

        MIN(started_at) AS started_at,
        MAX(finished_at) AS finished_at,

        MAX(
            CASE
                WHEN event_name = 'job_finished'
                    THEN COALESCE(status, 'SUCCESS')
            END
        ) AS status,

        MAX(
            CASE
                WHEN event_name = 'job_finished'
                    THEN records_read
            END
        ) AS records_read,

        MAX(
            CASE
                WHEN event_name = 'job_finished'
                    THEN records_written
            END
        ) AS records_written,

        MAX(
            CASE
                WHEN event_name = 'job_failed'
                    THEN error_message
            END
        ) AS error_message

    FROM monitoring.pipeline_log

    WHERE pipeline_name = 'bronze_stream_votacoes_microbatch'

    GROUP BY
        pipeline_name,
        batch_id
)

SELECT
    pipeline_name,
    batch_id,

    started_at,
    finished_at,

    CASE
        WHEN finished_at IS NULL
            THEN NULL

        ELSE TIMESTAMPDIFF(
            SECOND,
            started_at,
            finished_at
        )
    END AS sla_latency_seconds,

    COALESCE(records_read, 0) AS records_read,
    COALESCE(records_written, 0) AS records_written,

    CASE
        WHEN records_read IS NULL
            THEN 0

        WHEN records_written IS NULL
            THEN records_read

        ELSE records_read - records_written
    END AS records_discarded,

    CASE
        WHEN records_read IS NULL
             OR records_read = 0
            THEN 0

        ELSE ROUND(
            (
                CASE
                    WHEN records_written IS NULL
                        THEN records_read

                    ELSE records_read - records_written
                END
                / records_read
            ) * 100,
            2
        )
    END AS sla_error_rate_pct,

    CASE
        WHEN status = 'SUCCESS'
             AND TIMESTAMPDIFF(SECOND, started_at, finished_at) <= 600
            THEN 'SLA_OK'

        WHEN status = 'SUCCESS'
             AND TIMESTAMPDIFF(SECOND, started_at, finished_at) > 600
            THEN 'SLA_WARNING'

        WHEN status = 'FAILED'
            THEN 'SLA_FAILED'

        ELSE 'SLA_UNKNOWN'
    END AS sla_status,

    status AS execution_status,
    error_message,

    current_timestamp() AS analytics_ts_processamento

FROM execution_log
""")