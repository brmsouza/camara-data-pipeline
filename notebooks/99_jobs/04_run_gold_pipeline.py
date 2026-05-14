# Databricks notebook source
# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 04_run_gold_pipeline
# Layer: Orchestration
# Author: Bruno Souza
#
# Description:
# Executes the complete Gold dimensional and fact pipeline.
#
# Context:
# This notebook orchestrates all Gold build notebooks in deterministic order.
# It creates/refreshed Gold dimensions first, then fact tables and analytical
# relationship tables used by downstream analytics and dashboards.
#
# Responsibilities:
# - Execute Gold notebooks in deterministic dependency order
# - Create Gold schema before tables
# - Build dimensions before facts
# - Register orchestration execution metrics
# - Register notebook-level execution status
# - Stop execution on failure to avoid inconsistent downstream refreshes
# - Provide operational visibility for Gold refresh jobs
#
# Source:
# silver_curated layer tables
#
# Target:
# gold layer tables
# ------------------------------------------------------------------------------

# COMMAND ----------

# MAGIC %run ../90_common/table_logger

# COMMAND ----------

from collections import OrderedDict
from datetime import datetime
import uuid

# COMMAND ----------

LAYER = "orchestration"
PIPELINE_NAME = "gold_pipeline_orchestration"
SOURCE_TABLE = "silver_curated"
TARGET_TABLE = "gold"

batch_id = str(uuid.uuid4())
started_at = datetime.now()

# COMMAND ----------

GOLD_PIPELINE = OrderedDict({

    "00_create_gold_schema":
        "/Workspace/camara-data-pipeline/03_gold/00_create_gold_schema",

    "01_build_dm_tempo":
        "/Workspace/camara-data-pipeline/03_gold/01_build_dm_tempo",

    "02_build_dm_legislatura":
        "/Workspace/camara-data-pipeline/03_gold/02_build_dm_legislatura",

    "03_build_dm_partido":
        "/Workspace/camara-data-pipeline/03_gold/03_build_dm_partido",

    "04_build_dm_deputado":
        "/Workspace/camara-data-pipeline/03_gold/04_build_dm_deputado",

    "05_build_dm_proposicao":
        "/Workspace/camara-data-pipeline/03_gold/05_build_dm_proposicao",

    "06_build_dm_orgao":
        "/Workspace/camara-data-pipeline/03_gold/06_build_dm_orgao",

    "07_build_dm_gabinete":
        "/Workspace/camara-data-pipeline/03_gold/07_build_dm_gabinete",

    "08_build_dm_fornecedor":
        "/Workspace/camara-data-pipeline/03_gold/08_build_dm_fornecedor",

    "09_build_dm_evento":
        "/Workspace/camara-data-pipeline/03_gold/09_build_dm_evento",

    "10_build_dm_frente":
        "/Workspace/camara-data-pipeline/03_gold/10_build_dm_frente",

    "11_build_dm_uf":
        "/Workspace/camara-data-pipeline/03_gold/11_build_dm_uf",

    "12_build_dm_tipo_despesa":
        "/Workspace/camara-data-pipeline/03_gold/12_build_dm_tipo_despesa",

    "13_build_dm_bancada":
        "/Workspace/camara-data-pipeline/03_gold/13_build_dm_bancada",

    "14_build_dm_responsavel_ceap":
        "/Workspace/camara-data-pipeline/03_gold/14_build_dm_responsavel_ceap",

    "15_build_ft_despesas_ceap":
        "/Workspace/camara-data-pipeline/03_gold/15_build_ft_despesas_ceap",

    "16_build_ft_votacoes":
        "/Workspace/camara-data-pipeline/03_gold/16_build_ft_votacoes",

    "17_build_ft_votos":
        "/Workspace/camara-data-pipeline/03_gold/17_build_ft_votos",

    "18_build_ft_orientacoes_bancada":
        "/Workspace/camara-data-pipeline/03_gold/18_build_ft_orientacoes_bancada",

    "19_build_ft_atividade_parlamentar":
        "/Workspace/camara-data-pipeline/03_gold/19_build_ft_atividade_parlamentar",

    "20_build_ft_presenca_eventos":
        "/Workspace/camara-data-pipeline/03_gold/20_build_ft_presenca_eventos",

    "21_build_ft_frentes_membros":
        "/Workspace/camara-data-pipeline/03_gold/21_build_ft_frentes_membros",

})

# COMMAND ----------

log_pipeline_event(
    batch_id=batch_id,
    pipeline_name=PIPELINE_NAME,
    layer=LAYER,
    level="INFO",
    event_name="job_started",
    message=f"source={SOURCE_TABLE} | started | target_table={TARGET_TABLE} | notebooks={len(GOLD_PIPELINE)}",
    endpoint=SOURCE_TABLE,
    target_table=TARGET_TABLE,
    started_at=started_at,
)

# COMMAND ----------

executed_notebooks = []
failed_notebooks = []

records_read = len(GOLD_PIPELINE)
records_written = 0
records_discarded = 0

try:

    for notebook_name, notebook_path in GOLD_PIPELINE.items():

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
        "Gold orchestration validation failed: no Gold notebooks configured."
    )

if records_written == 0:
    raise Exception(
        "Gold orchestration validation failed: no Gold notebooks executed."
    )

if records_discarded > 0:
    raise Exception(
        f"Gold orchestration validation failed: failed notebooks found = {failed_notebooks}"
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