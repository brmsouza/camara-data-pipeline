# Databricks notebook source
# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 02_run_silver_base_pipeline
# Layer: Orchestration
# Author: Bruno Souza
#
# Description:
# Executes the complete Silver Base pipeline.
#
# Context:
# This notebook orchestrates all Silver Base notebooks in deterministic order.
# It provides centralized execution control for standardized base tables that
# feed Silver Curated and Gold layers.
#
# Responsibilities:
# - Execute Silver Base notebooks in deterministic order
# - Register orchestration execution metrics
# - Register notebook-level execution status
# - Stop execution on failure to avoid inconsistent downstream refreshes
# - Provide operational visibility for Silver Base refresh jobs
#
# Source:
# bronze layer tables
#
# Target:
# silver_base layer tables
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from collections import OrderedDict
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "orchestration"
PIPELINE_NAME = "silver_base_pipeline_orchestration"
SOURCE_TABLE = "bronze"
TARGET_TABLE = "silver_base"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

SILVER_BASE_PIPELINE = OrderedDict({

    "01_base_deputados":
        "/Workspace/camara-data-pipeline/02_silver/01_base/01_base_deputados",

    "02_base_deputados_detalhes":
        "/Workspace/camara-data-pipeline/02_silver/01_base/02_base_deputados_detalhes",

    "03_base_frentes":
        "/Workspace/camara-data-pipeline/02_silver/01_base/03_base_frentes",

    "04_base_eventos":
        "/Workspace/camara-data-pipeline/02_silver/01_base/04_base_eventos",

    "05_base_frentes_membros":
        "/Workspace/camara-data-pipeline/02_silver/01_base/05_base_frentes_membros",

    "06_base_proposicoes":
        "/Workspace/camara-data-pipeline/02_silver/01_base/06_base_proposicoes",

    "07_base_despesas":
        "/Workspace/camara-data-pipeline/02_silver/01_base/07_base_despesas",

    "08_base_orgaos":
        "/Workspace/camara-data-pipeline/02_silver/01_base/08_base_orgaos",

    "09_base_orgaos_membros":
        "/Workspace/camara-data-pipeline/02_silver/01_base/09_base_orgaos_membros",

    "10_base_votacoes":
        "/Workspace/camara-data-pipeline/02_silver/01_base/10_base_votacoes",

    "11_base_votacoes_orientacoes":
        "/Workspace/camara-data-pipeline/02_silver/01_base/11_base_votacoes_orientacoes",

    "12_base_votacoes_votos":
        "/Workspace/camara-data-pipeline/02_silver/01_base/12_base_votacoes_votos",

    "13_base_legislaturas":
        "/Workspace/camara-data-pipeline/02_silver/01_base/13_base_legislaturas",

    "14_base_fornecedores":
        "/Workspace/camara-data-pipeline/02_silver/01_base/14_base_fornecedores",

    "15_base_proposicoes_tramitacoes_cdc":
        "/Workspace/camara-data-pipeline/02_silver/01_base/15_base_proposicoes_tramitacoes_cdc",

})

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={SOURCE_TABLE} | started | target_table={TARGET_TABLE} | notebooks={len(SILVER_BASE_PIPELINE)}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

executed_notebooks = []
failed_notebooks = []

records_read = len(SILVER_BASE_PIPELINE)
records_written = 0
records_discarded = 0

try:

    for notebook_name, notebook_path in SILVER_BASE_PIPELINE.items():

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
        "Silver Base orchestration validation failed: no Silver Base notebooks configured."
    )

if records_written == 0:
    raise Exception(
        "Silver Base orchestration validation failed: no Silver Base notebooks executed."
    )

if records_discarded > 0:
    raise Exception(
        f"Silver Base orchestration validation failed: failed notebooks found = {failed_notebooks}"
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

print(f"Pipeline: {PIPELINE_NAME}")
print(f"Layer: {LAYER}")
print(f"Source: {SOURCE_TABLE}")
print(f"Target: {TARGET_TABLE}")
print(f"Records read: {records_read}")
print(f"Records written: {records_written}")
print(f"Records discarded: {records_discarded}")
print(f"Executed notebooks: {executed_notebooks}")
print(f"Failed notebooks: {failed_notebooks}")