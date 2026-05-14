# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Stream Layer — Voting Micro-Batch Ingestion
# MAGIC
# MAGIC **Notebook:** `99_ingest_votacoes_microbatch`  
# MAGIC **Endpoint:** `/votacoes`
# MAGIC
# MAGIC Performs incremental micro-batch ingestion of parliamentary voting events
# MAGIC from the Câmara dos Deputados Open Data API.
# MAGIC
# MAGIC This notebook implements the streaming ingestion entry point for near real-time
# MAGIC voting analytics, using offset control based on voting identifiers to detect
# MAGIC new voting events and persist raw payloads into Bronze Stream Delta tables.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Perform incremental micro-batch ingestion of voting events
# MAGIC - Consume the `/votacoes` endpoint using offset-based control
# MAGIC - Detect new voting sessions using voting identifiers
# MAGIC - Preserve raw API payloads for replayability and auditing
# MAGIC - Persist streaming ingestion records into Bronze Stream Delta tables
# MAGIC - Support near real-time parliamentary voting analytics
# MAGIC - Register operational execution metrics and ingestion logs
# MAGIC - Support replay and recovery of streaming micro-batches
# MAGIC
# MAGIC ## Notes
# MAGIC
# MAGIC - Incremental micro-batch ingestion pipeline
# MAGIC - Uses offset control based on voting identifiers
# MAGIC - Persists raw payloads into Bronze Stream Delta tables
# MAGIC - Supports streaming replay and recovery workflows
# MAGIC - Data persisted in Delta Lake
# MAGIC - Execution logged in `monitoring.pipeline_log`
# MAGIC
# MAGIC **Target:** Bronze Stream voting ingestion tables

# COMMAND ----------

# MAGIC %run ../90_common/config

# COMMAND ----------

# MAGIC %run ../90_common/logger

# COMMAND ----------

# MAGIC %run ../90_common/api_client

# COMMAND ----------

# MAGIC %run ../90_common/bronze_writer

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

import uuid
import json
import hashlib
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    TimestampType,
    DateType
)


# COMMAND ----------


ENDPOINT = "/votacoes"
TABLE_NAME = "bronze_stream.votacoes_raw"
OFFSET_TABLE = "control.votacoes_stream_offset"
LOG_TABLE = "monitoring.pipeline_log"

PIPELINE_NAME = "bronze_stream_votacoes_microbatch"
LAYER = "bronze_stream"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
records_discarded = 0

# COMMAND ----------

def build_record_hash(payload: dict) -> str:
    payload_str = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()


def log_event(
    level,
    event_name,
    message,
    status=None,
    records_read=None,
    records_written=None,
    records_discarded=None,
    error_message=None,
    finished_at=None
):
    schema = StructType([
        StructField("pipeline_name", StringType(), True),
        StructField("batch_id", StringType(), True),
        StructField("layer", StringType(), True),
        StructField("level", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("message", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("target_table", StringType(), True),
        StructField("started_at", TimestampType(), True),
        StructField("finished_at", TimestampType(), True),
        StructField("status", StringType(), True),
        StructField("records_read", LongType(), True),
        StructField("records_written", LongType(), True),
        StructField("records_discarded", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("created_at", TimestampType(), True),
    ])

    log_df = spark.createDataFrame(
        [(
            PIPELINE_NAME,
            batch_id,
            LAYER,
            level,
            event_name,
            message,
            ENDPOINT,
            TABLE_NAME,
            started_at,
            finished_at,
            status,
            int(records_read) if records_read is not None else None,
            int(records_written) if records_written is not None else None,
            int(records_discarded) if records_discarded is not None else None,
            error_message,
            datetime.now()
        )],
        schema
    )

    log_df.write.mode("append").saveAsTable(LOG_TABLE)
try:
    log_event(
        level="INFO",
        event_name="job_started",
        message="Starting voting micro-batch ingestion."
    )


    offset_row = (
        spark.table(OFFSET_TABLE)
        .filter(F.col("endpoint") == ENDPOINT)
        .select("last_processed_id", "last_processed_ts")
        .limit(1)
        .collect()
    )

    if offset_row:
        last_processed_id = offset_row[0]["last_processed_id"]
        last_processed_ts = offset_row[0]["last_processed_ts"]
    else:
        last_processed_id = "0"
        last_processed_ts = datetime(1900, 1, 1)

    log_event(
        level="INFO",
        event_name="offset_loaded",
        message=f"Last processed ID: {last_processed_id} | Last processed TS: {last_processed_ts}"
    )

    payload = get_data(
        endpoint="/votacoes",
        params={
            "itens": 100,
            "ordenarPor": "dataHoraRegistro",
            "ordem": "ASC",
        },
        timeout=60,
        retries=5,
        sleep_seconds=5,
    )

    records = payload.get("dados", [])
    records_read = len(records)


    new_records = []

    for record in records:
        vota_id_votacao = str(record.get("id")) if record.get("id") is not None else None
        data_hora_registro = record.get("dataHoraRegistro")

        if not vota_id_votacao or not data_hora_registro:
            continue

        record_ts = datetime.fromisoformat(data_hora_registro)

        if record_ts > last_processed_ts:
            ingestion_ts = datetime.now()

            new_records.append({
                "vota_id_votacao": vota_id_votacao,
                "bronze_tx_endpoint": ENDPOINT,
                "bronze_id_batch": batch_id,
                "bronze_ts_ingestao": ingestion_ts,
                "bronze_dt_ingestao": ingestion_ts.date(),
                "bronze_tx_payload": json.dumps(record, ensure_ascii=False),
                "bronze_tx_record_hash": build_record_hash(record)
            })

    records_written = len(new_records)
    records_discarded = records_read - records_written

    if new_records:
        bronze_schema = StructType([
            StructField("vota_id_votacao", StringType(), True),
            StructField("bronze_tx_endpoint", StringType(), True),
            StructField("bronze_id_batch", StringType(), True),
            StructField("bronze_ts_ingestao", TimestampType(), True),
            StructField("bronze_dt_ingestao", DateType(), True),
            StructField("bronze_tx_payload", StringType(), True),
            StructField("bronze_tx_record_hash", StringType(), True),
        ])

        df_new = spark.createDataFrame(new_records, bronze_schema)

        df_new.write.mode("append").saveAsTable(TABLE_NAME)

        max_processed = (
            df_new
            .withColumn(
                "dataHoraRegistro",
                F.to_timestamp(
                    F.get_json_object(F.col("bronze_tx_payload"), "$.dataHoraRegistro")
                )
            )
            .orderBy(F.col("dataHoraRegistro").desc())
            .select("vota_id_votacao", "dataHoraRegistro")
            .limit(1)
            .collect()[0]
        )

        max_processed_id = max_processed["vota_id_votacao"]
        max_processed_ts = max_processed["dataHoraRegistro"]

        spark.sql(f"""
            MERGE INTO {OFFSET_TABLE} AS target
            USING (
                SELECT
                    '{ENDPOINT}' AS endpoint,
                    '{max_processed_id}' AS last_processed_id,
                    TIMESTAMP('{max_processed_ts}') AS last_processed_ts,
                    current_timestamp() AS updated_at
            ) AS source
            ON target.endpoint = source.endpoint
            WHEN MATCHED THEN
                UPDATE SET
                    target.last_processed_id = source.last_processed_id,
                    target.last_processed_ts = source.last_processed_ts,
                    target.updated_at = source.updated_at
            WHEN NOT MATCHED THEN
                INSERT (
                    endpoint,
                    last_processed_id,
                    last_processed_ts,
                    updated_at
                )
                VALUES (
                    source.endpoint,
                    source.last_processed_id,
                    source.last_processed_ts,
                    source.updated_at
                )
        """)

    log_event(
        level="INFO",
        event_name="job_finished",
        message="Voting micro-batch ingestion finished successfully.",
        status="SUCCESS",
        records_read=records_read,
        records_written=records_written,
        records_discarded=records_discarded,
        finished_at=datetime.now()
    )

except Exception as e:
    log_event(
        level="ERROR",
        event_name="job_failed",
        message="Voting micro-batch ingestion failed.",
        status="FAILED",
        records_read=records_read,
        records_written=records_written,
        records_discarded=records_discarded,
        error_message=str(e),
        finished_at=datetime.now()
    )

    raise e

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM bronze_stream.votacoes_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM control.votacoes_stream_offset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM monitoring.pipeline_log
# MAGIC WHERE pipeline_name = 'bronze_stream_votacoes_microbatch'
# MAGIC ORDER BY created_at DESC;