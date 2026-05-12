# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 13_ingest_legislaturas
# Layer: Bronze
# Author: Bruno Souza
#
# Description:
# Ingests legislature reference data from the Câmara dos Deputados Open Data API.
#
# Context:
# This notebook consumes the /legislaturas endpoint, which provides basic
# information about parliamentary legislature periods. The resulting Bronze
# table preserves the source payload with technical metadata for replay,
# auditability and downstream dimensional modeling.
#
# Responsibilities:
# - Call the /legislaturas endpoint
# - Extract legislature records from the API response
# - Persist raw records with Bronze lineage metadata
# - Register operational execution metrics
#
# Source:
# API Dados Abertos Câmara dos Deputados - /legislaturas
#
# Target:
# bronze.legislaturas
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/api_client

# COMMAND ----------

# MAGIC %run ../90_common/bronze_writer

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

import uuid
from datetime import datetime

# COMMAND ----------

LAYER = "bronze"
ENDPOINT = "/legislaturas"
PIPELINE_NAME = "bronze_ingest_legislaturas"
TARGET_TABLE = "bronze.legislaturas"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

records_read = 0
records_written = 0
records_discarded = 0

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={ENDPOINT} | started",
    endpoint=ENDPOINT,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

payload = get_data(
    endpoint=ENDPOINT,
    params={"itens": 100},
    timeout=30,
)

records = payload.get("dados", [])

records_read = len(records)

# COMMAND ----------

if records:
    df_bronze = build_bronze_dataframe(
        records=records,
        source_endpoint=ENDPOINT,
        source_id_field="id",
    )

    records_written = df_bronze.count()

    write_bronze_delta(
        df=df_bronze,
        table_name=TARGET_TABLE,
        mode="overwrite",
    )
else:
    records_written = 0

# COMMAND ----------

records_discarded = records_read - records_written

if records_read == 0:
    raise Exception(
        "Bronze validation failed: /legislaturas returned no records."
    )

if records_written == 0:
    raise Exception(
        "Bronze validation failed: bronze.legislaturas has no records written."
    )

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=f"source={ENDPOINT} | finished successfully | records_read={records_read} | records_written={records_written} | records_discarded={records_discarded}",
    endpoint=ENDPOINT,
    target_table=TARGET_TABLE,
    started_at=started_at,
    finished_at=datetime.now(),
)