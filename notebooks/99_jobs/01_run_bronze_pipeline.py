# Databricks notebook source
# MAGIC %md
# MAGIC # Orchestration Layer — Bronze Pipeline Execution
# MAGIC
# MAGIC **Notebook:** `01_run_bronze_pipeline`
# MAGIC
# MAGIC Executes the complete Bronze ingestion pipeline.
# MAGIC
# MAGIC This notebook orchestrates all Bronze ingestion notebooks in deterministic
# MAGIC execution order, prioritizing file-based ingestion notebooks when available.
# MAGIC It provides centralized orchestration control for the raw ingestion layer and
# MAGIC serves as the primary Bronze refresh entry point for Databricks Workflows.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Execute Bronze ingestion notebooks in deterministic order
# MAGIC - Prioritize `*_file` ingestion notebook versions when available
# MAGIC - Register orchestration execution metrics
# MAGIC - Register notebook-level execution status
# MAGIC - Stop execution on failure to avoid inconsistent downstream refreshes
# MAGIC - Provide operational visibility for Bronze refresh jobs
# MAGIC - Support centralized orchestration of the raw ingestion layer
# MAGIC
# MAGIC ## Source
# MAGIC
# MAGIC - `01_bronze` notebooks
# MAGIC
# MAGIC **Target:** Bronze layer tables

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from collections import OrderedDict
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "orchestration"
PIPELINE_NAME = "bronze_pipeline_orchestration"
SOURCE_TABLE = "01_bronze"
TARGET_TABLE = "bronze"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

BRONZE_PIPELINE = OrderedDict({

    "01_ingest_deputados":
        "/Workspace/camara-data-pipeline/01_bronze/01_ingest_deputados",

    "02_ingest_deputados_detalhes":
        "/Workspace/camara-data-pipeline/01_bronze/02_ingest_deputados_detalhes",

    "03_ingest_frentes":
        "/Workspace/camara-data-pipeline/01_bronze/03_ingest_frentes",

    "04_ingest_eventos":
        "/Workspace/camara-data-pipeline/01_bronze/04_ingest_eventos",

    "05_ingest_frentes_membros":
        "/Workspace/camara-data-pipeline/01_bronze/05_ingest_frentes_membros",

    "06b_ingest_proposicoes_file":
        "/Workspace/camara-data-pipeline/01_bronze/06b_ingest_proposicoes_file",

    "07b_ingest_despesas_file":
        "/Workspace/camara-data-pipeline/01_bronze/07b_ingest_despesas_file",

    "08_ingest_orgaos":
        "/Workspace/camara-data-pipeline/01_bronze/08_ingest_orgaos",

    "09b_ingest_orgaos_membros_file":
        "/Workspace/camara-data-pipeline/01_bronze/09b_ingest_orgaos_membros_file",

    "10b_ingest_votacoes_file":
        "/Workspace/camara-data-pipeline/01_bronze/10b_ingest_votacoes_file",

    "11b_ingest_votacoes_orientacoes_file":
        "/Workspace/camara-data-pipeline/01_bronze/11b_ingest_votacoes_orientacoes_file",

    "12b_ingest_votacoes_votos_file":
        "/Workspace/camara-data-pipeline/01_bronze/12b_ingest_votacoes_votos_file",

    "13_ingest_legislaturas":
        "/Workspace/camara-data-pipeline/01_bronze/13_ingest_legislaturas",

})

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={SOURCE_TABLE} | started | target_table={TARGET_TABLE} | notebooks={len(BRONZE_PIPELINE)}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

executed_notebooks = []
failed_notebooks = []

records_read = len(BRONZE_PIPELINE)
records_written = 0
records_discarded = 0

try:

    for notebook_name, notebook_path in BRONZE_PIPELINE.items():

        notebook_started_at = datetime.now()

        print("=" * 80)
        print(f"RUNNING: {notebook_name}")
        print(f"PATH: {notebook_path}")
        print("=" * 80)

        try:

            dbutils.notebook.run(
                notebook_path,
                timeout_seconds=0,
            )

            notebook_finished_at = datetime.now()
            duration_seconds = int(
                (notebook_finished_at - notebook_started_at).total_seconds()
            )

            executed_notebooks.append(notebook_name)
            records_written = len(executed_notebooks)

            log_pipeline_event(
                batch_id=batch_id,
                pipeline_name=PIPELINE_NAME,
                layer=LAYER,
                level="INFO",
                event_name="notebook_finished",
                message=(
                    f"notebook={notebook_name} | path={notebook_path} "
                    f"| finished successfully | duration_seconds={duration_seconds}"
                ),
                endpoint=notebook_path,
                target_table=TARGET_TABLE,
                records_read=1,
                records_written=1,
                started_at=notebook_started_at,
                finished_at=notebook_finished_at,
            )

            print(f"FINISHED: {notebook_name}")

        except Exception as notebook_error:

            notebook_finished_at = datetime.now()
            duration_seconds = int(
                (notebook_finished_at - notebook_started_at).total_seconds()
            )

            failed_notebooks.append(notebook_name)
            records_discarded = len(failed_notebooks)

            log_pipeline_event(
                batch_id=batch_id,
                pipeline_name=PIPELINE_NAME,
                layer=LAYER,
                level="ERROR",
                event_name="notebook_failed",
                message=(
                    f"notebook={notebook_name} | path={notebook_path} "
                    f"| failed | duration_seconds={duration_seconds}"
                ),
                endpoint=notebook_path,
                target_table=TARGET_TABLE,
                records_read=1,
                records_written=0,
                started_at=notebook_started_at,
                finished_at=notebook_finished_at,
                error_message=str(notebook_error),
            )

            raise notebook_error

except Exception as e:

    log_pipeline_event(
        batch_id=batch_id,
        pipeline_name=PIPELINE_NAME,
        layer=LAYER,
        level="ERROR",
        event_name="job_failed",
        message=(
            f"source={SOURCE_TABLE} | failed "
            f"| records_read={records_read} "
            f"| records_written={records_written} "
            f"| records_discarded={records_discarded} "
            f"| failed_notebooks={failed_notebooks}"
        ),
        endpoint=SOURCE_TABLE,
        target_table=TARGET_TABLE,
        records_read=records_read,
        records_written=records_written,
        started_at=started_at,
        finished_at=datetime.now(),
        error_message=str(e),
    )

    raise e

# COMMAND ----------

records_written = len(executed_notebooks)
records_discarded = len(failed_notebooks)

if records_read == 0:
    raise Exception(
        "Bronze orchestration validation failed: no Bronze notebooks configured."
    )

if records_written == 0:
    raise Exception(
        "Bronze orchestration validation failed: no Bronze notebooks executed."
    )

if records_discarded > 0:
    raise Exception(
        f"Bronze orchestration validation failed: failed notebooks found = {failed_notebooks}"
    )

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=(
        f"source={SOURCE_TABLE} | finished successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| records_discarded={records_discarded}"
    ),
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    records_read=records_read,
    records_written=records_written,
    started_at=started_at,
    finished_at=datetime.now(),
)

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_finished",
    message=(
        f"source={SOURCE_TABLE} | finished successfully "
        f"| records_read={records_read} "
        f"| records_written={records_written} "
        f"| records_discarded={records_discarded}"
    ),
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    records_read=records_read,
    records_written=records_written,
    started_at=started_at,
    finished_at=datetime.now(),
)