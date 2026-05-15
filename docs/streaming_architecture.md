# Streaming Architecture

🇺🇸 Technical document — Streaming, CDC, DLT and Operational Observability Architecture

---

# Objective

This document describes the streaming, micro-batch, CDC, SCD Type 2, Delta Live Tables, SLA monitoring, replay and operational observability architecture implemented in the `camara-data-pipeline` project.

The objective is to document how the project evolved beyond traditional batch processing within the Medallion architecture by incorporating modern Data Engineering patterns for incremental ingestion, operational monitoring and controlled reprocessing.

---

# Overview

The streaming architecture was created to address the optional challenge of near real-time parliamentary voting monitoring.

The solution combines:

* incremental micro-batch ingestion;
* voting offset control;
* raw payload preservation;
* deterministic record hash generation;
* batch_id traceability;
* Delta Live Tables;
* declarative quality expectations;
* Gold alert generation;
* SLA monitoring;
* replay and reprocessing strategy.

---

# Implemented Components

| Component | Status | Description |
|---|---|---|
| Voting micro-batch | Implemented | Incremental ingestion of new voting sessions |
| Offset control | Implemented | Last processed ID/timestamp tracking |
| Delta Live Tables | Implemented | Bronze → Silver → Gold pipeline with validations |
| Declarative expectations | Implemented | DLT quality validation rules |
| Gold alerts | Implemented | Urgency classification and notification flags |
| SLA monitoring | Implemented | Latency, volume and error monitoring |
| Replay/reprocessing | Partially Implemented | Strategy based on offsets, logs and raw payload |
| Observability | Implemented | Operational logs and batch traceability |

---

# Logical Architecture

```text
Brazilian Chamber API
        │
        │ /votacoes
        ▼
Bronze Streaming / Micro-batch
        │
        │ raw payload + lineage + hash
        ▼
Silver Streaming / DLT
        │
        │ validations + expectations
        ▼
Gold Streaming Alerts
        │
        │ urgency + notification flag
        ▼
Monitoring / SLA Dashboard
```

---

# Streaming Workflow

The streaming workflow is responsible for executing incremental parliamentary voting ingestion at recurring intervals.

## Responsible notebook

```text
notebooks/99_jobs/05_run_votacoes_streaming_pipeline.py
```

## Responsibilities

* orchestrate streaming pipeline execution;
* execute micro-batch ingestion;
* control operational dependencies;
* register execution logs;
* enable recurring Databricks Workflow execution;
* support replay and controlled reprocessing.

## Visual evidence


![Streaming Workflow](../assets/images/job_votacoes_streaming_microbatch.png)


---

# Micro-Batch Ingestion

The micro-batch ingestion continuously monitors new parliamentary voting sessions from the `/votacoes` API endpoint.

## Responsible notebook

```text
notebooks/01_bronze/99_ingest_votacoes_microbatch.py
```

## Source

```text
/votacoes
```

## Target table

```text
bronze_stream.votacoes_raw
```

---

# Delta Live Tables

The architecture also includes a Delta Live Tables pipeline to structure the Bronze → Silver → Gold flow with declarative quality enforcement.

## Responsible notebook

```text
notebooks/05_dlt/01_dlt_votacoes_streaming.py
```

## Visual evidence


![DLT Pipeline](assets/images/dlt_votacoes_streaming.png)

---

# SLA Monitoring

## Object

```text
monitoring.vw_sla_votacoes_streaming
```

## Monitored metrics

* end-to-end latency;
* processed records volume;
* error rate;
* execution duration;
* execution status;
* records read;
* records written;
* discarded records.

---

# CDC / SCD Type 2

In addition to voting streaming, the project implements CDC/SCD Type 2 for proposition processing history.

## Responsible notebooks

```text
notebooks/00_setup/04_create_cdc_scd2_objects.py
notebooks/01_bronze/14_ingest_proposicoes_tramitacoes_cdc.py
notebooks/02_silver/01_base/15_base_proposicoes_tramitacoes_cdc.py
notebooks/02_silver/02_curated/15_curated_proposicoes_tramitacoes_scd.py
notebooks/04_analytics/08_build_gold_proposicoes_cdc_analytics.py
```

---

# Conclusion

The streaming architecture implemented in the `camara-data-pipeline` project demonstrates a significant technical evolution compared to traditional batch pipelines.

The solution combines incremental ingestion, micro-batch processing, DLT, CDC/SCD2, operational observability, SLA monitoring, replay capabilities and governance-oriented architecture patterns.