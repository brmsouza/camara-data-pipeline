# Databricks notebook source
# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 03_run_silver_curated_pipeline
# Layer: Orchestration
# Author: Bruno Souza
#
# Description:
# Executes the complete Silver Curated pipeline.
#
# Context:
# This notebook orchestrates all Silver Curated notebooks in deterministic order.
# It provides centralized execution control for curated business-ready datasets
# that feed Gold analytical layers and dashboards.
#
# Responsibilities:
# - Execute Silver Curated notebooks in deterministic order
# - Register orchestration execution metrics
# - Register notebook-level execution status
# - Stop execution on failure to avoid inconsistent downstream refreshes
# - Provide operational visibility for Silver Curated refresh jobs
#
# Source:
# silver_base layer tables
#
# Target:
# silver_curated layer tables
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from collections import OrderedDict
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "orchestration"
PIPELINE_NAME = "silver_curated_pipeline_orchestration"
SOURCE_TABLE = "silver_base"
TARGET_TABLE = "silver_curated"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

SILVER_CURATED_PIPELINE = OrderedDict({

    "01_curated_deputados":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/01_curated_deputados",

    "03_curated_frentes":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/03_curated_frentes",

    "04_curated_eventos":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/04_curated_eventos",

    "05_curated_frentes_membros":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/05_curated_frentes_membros",

    "06_curated_proposicoes":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/06_curated_proposicoes",

    "07_curated_despesas":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/07_curated_despesas",

    "08_curated_orgaos":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/08_curated_orgaos",

    "09_curated_orgaos_membros":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/09_curated_orgaos_membros",

    "10_curated_votacoes":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/10_curated_votacoes",

    "11_curated_votacoes_orientacoes":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/11_curated_votacoes_orientacoes",

    "12_curated_votacoes_votos":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/12_curated_votacoes_votos",

    "13_curated_legislaturas":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/13_curated_legislaturas",

    "14_curated_fornecedores":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/14_curated_fornecedores",

    "15_curated_proposicoes_tramitacoes_scd":
        "/Workspace/camara-data-pipeline/02_silver/02_curated/15_curated_proposicoes_tramitacoes_scd",

})

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={SOURCE_TABLE} | started | target_table={TARGET_TABLE} | notebooks={len(SILVER_CURATED_PIPELINE)}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

executed_notebooks = []
failed_notebooks = []

records_read = len(SILVER_CURATED_PIPELINE)
records_written = 0
records_discarded = 0

try:

    for notebook_name, notebook_path in SILVER_CURATED_PIPELINE.items():

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
        "Silver Curated orchestration validation failed: no Silver Curated notebooks configured."
    )

if records_written == 0:
    raise Exception(
        "Silver Curated orchestration validation failed: no Silver Curated notebooks executed."
    )

if records_discarded > 0:
    raise Exception(
        f"Silver Curated orchestration validation failed: failed notebooks found = {failed_notebooks}"
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