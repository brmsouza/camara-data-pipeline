# Databricks notebook source
# MAGIC %md
# MAGIC # Jobs / Orchestration Layer — Real-Time Voting Streaming Pipeline Execution
# MAGIC
# MAGIC **Notebook:** `04_run_votacoes_streaming_pipeline`
# MAGIC
# MAGIC Executes the complete real-time voting streaming pipeline.
# MAGIC
# MAGIC This notebook orchestrates the execution of the near real-time parliamentary
# MAGIC voting streaming workflow, coordinating ingestion, processing and monitoring
# MAGIC activities related to voting micro-batches and streaming analytical workloads.
# MAGIC
# MAGIC ## Responsibilities
# MAGIC
# MAGIC - Execute the real-time voting streaming pipeline
# MAGIC - Coordinate voting micro-batch ingestion workflows
# MAGIC - Trigger streaming Bronze, Silver and Gold processing stages
# MAGIC - Support near real-time parliamentary voting analytics
# MAGIC - Register orchestration and streaming execution metrics
# MAGIC - Provide operational visibility for streaming workloads
# MAGIC - Support replay and reprocessing of voting micro-batches
# MAGIC - Coordinate monitoring and SLA validation for streaming execution
# MAGIC
# MAGIC **Target:** Real-time voting streaming analytical pipeline

# COMMAND ----------

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