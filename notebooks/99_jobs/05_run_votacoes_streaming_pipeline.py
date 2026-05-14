# Databricks notebook source
# ------------------------------------------------------------------------------
# Notebook: 04_run_votacoes_streaming_pipeline
# Layer: Jobs / Orchestration
#
# Description:
# Executes the complete real-time voting streaming pipeline.
# ------------------------------------------------------------------------------

STREAMING_PIPELINE = [

    "/Workspace/camara-data-pipeline/01_bronze/99_ingest_votacoes_microbatch.py",

    "/Workspace/camara-data-pipeline/04_analytics/07_sla_votacoes_streaming.py",
]

for notebook_path in STREAMING_PIPELINE:

    print("=" * 80)
    print(f"RUNNING: {notebook_path}")
    print("=" * 80)

    dbutils.notebook.run(
        notebook_path,
        timeout_seconds=0,
    )

print("=" * 80)
print("STREAMING PIPELINE FINISHED")
print("=" * 80)