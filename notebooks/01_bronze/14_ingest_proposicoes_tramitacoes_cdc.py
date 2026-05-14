# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 14_ingest_proposicoes_tramitacoes_cdc
# Layer: Bronze CDC
#
# Description:
# Incremental ingestion of proposicoes tramitacoes for CDC/SCD Type 2 analysis.
# Consumes /proposicoes/{id}/tramitacoes and stores raw payload events with hash.
# ------------------------------------------------------------------------------


# COMMAND ----------

# MAGIC
# MAGIC %run ../90_common/config

# COMMAND ----------

# MAGIC %run ../90_common/logger

# COMMAND ----------

# MAGIC %run ../90_common/api_client

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
    LongType,
    StringType,
    TimestampType,
    DateType
)


# COMMAND ----------


PIPELINE_NAME = "bronze_cdc_proposicoes_tramitacoes"
LAYER = "bronze_cdc"

SOURCE_TABLE = "silver_base.proposicoes"
TARGET_TABLE = "bronze_cdc.proposicoes_tramitacoes_raw"
LOG_TABLE = "monitoring.pipeline_log"

ENDPOINT_TEMPLATE = "/proposicoes/{id}/tramitacoes"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
records_discarded = 0

# COMMAND ----------

def build_payload_hash(payload: dict) -> str:
    payload_str = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload_str.encode("utf-8")).hexdigest()


def log_event(
    level,
    event_name,
    message,
    endpoint=None,
    status=None,
    records_read=None,
    records_written=None,
    records_discarded=None,
    error_message=None,
    finished_at=None
):
    schema = """
        pipeline_name STRING,
        batch_id STRING,
        layer STRING,
        level STRING,
        event_name STRING,
        message STRING,
        endpoint STRING,
        target_table STRING,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        status STRING,
        records_read BIGINT,
        records_written BIGINT,
        records_discarded BIGINT,
        error_message STRING,
        created_at TIMESTAMP
    """

    log_df = spark.createDataFrame(
        [(
            PIPELINE_NAME,
            batch_id,
            LAYER,
            level,
            event_name,
            message,
            endpoint,
            TARGET_TABLE,
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


# ------------------------------------------------------------------------------
# Load proposicoes to process
# ------------------------------------------------------------------------------

try:
    log_event(
        level="INFO",
        event_name="job_started",
        message="Starting proposicoes tramitacoes CDC ingestion."
    )

    df_proposicoes = (
        spark.table(SOURCE_TABLE)
        .select(F.col("prop_id_proposicao").cast("long").alias("prop_id_proposicao"))
        .where(F.col("prop_id_proposicao").isNotNull())
        .dropDuplicates(["prop_id_proposicao"])
    )

    proposicoes = [
        row["prop_id_proposicao"]
        for row in df_proposicoes.limit(10).collect()
    ]

    records_read = len(proposicoes)

    log_event(
        level="INFO",
        event_name="proposicoes_loaded",
        message=f"Loaded {records_read} proposicoes for CDC tramitacoes ingestion.",
        records_read=records_read
    )

    # --------------------------------------------------------------------------
    # Consume API
    # --------------------------------------------------------------------------

    raw_records = []

    for prop_id in proposicoes:

        endpoint = ENDPOINT_TEMPLATE.format(id=prop_id)

        try:
            payload = get_data(
                endpoint=endpoint,
                params=None,
                timeout=60,
                retries=5,
                sleep_seconds=5
            )
            tramitacoes = payload.get("dados", [])

            for tramite in tramitacoes:

                tram_id_evento = (
                    str(tramite.get("sequencia"))
                    if tramite.get("sequencia") is not None
                    else None
                )

                ingestion_ts = datetime.now()

                raw_records.append({
                    "prop_id_proposicao": int(prop_id),
                    "tram_id_evento": tram_id_evento,
                    "bronze_tx_endpoint": endpoint,
                    "bronze_id_batch": batch_id,
                    "bronze_ts_ingestao": ingestion_ts,
                    "bronze_dt_ingestao": ingestion_ts.date(),
                    "bronze_tx_payload": json.dumps(tramite, ensure_ascii=False),
                    "bronze_tx_payload_hash": build_payload_hash(tramite)
                })

        except Exception as e:
            records_discarded += 1

            log_event(
                level="WARN",
                event_name="proposicao_tramitacoes_failed",
                message=f"Failed to ingest tramitacoes for proposicao {prop_id}.",
                endpoint=endpoint,
                error_message=str(e)
            )

    records_written = len(raw_records)

    # --------------------------------------------------------------------------
    # Write Bronze CDC
    # --------------------------------------------------------------------------

    if raw_records:

        schema = StructType([
            StructField("prop_id_proposicao", LongType(), True),
            StructField("tram_id_evento", StringType(), True),
            StructField("bronze_tx_endpoint", StringType(), True),
            StructField("bronze_id_batch", StringType(), True),
            StructField("bronze_ts_ingestao", TimestampType(), True),
            StructField("bronze_dt_ingestao", DateType(), True),
            StructField("bronze_tx_payload", StringType(), True),
            StructField("bronze_tx_payload_hash", StringType(), True),
        ])

        df_raw = spark.createDataFrame(raw_records, schema)

        df_raw.write.mode("append").saveAsTable(TARGET_TABLE)

    # --------------------------------------------------------------------------
    # Finish log
    # --------------------------------------------------------------------------

    log_event(
        level="INFO",
        event_name="job_finished",
        message="Proposicoes tramitacoes CDC ingestion finished successfully.",
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
        message="Proposicoes tramitacoes CDC ingestion failed.",
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
# MAGIC FROM monitoring.pipeline_log
# MAGIC WHERE pipeline_name = 'bronze_cdc_proposicoes_tramitacoes'
# MAGIC ORDER BY created_at DESC;