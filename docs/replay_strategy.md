# Replay Strategy

🇺🇸 Technical document — Replay, Recovery and Reprocessing Strategy

---

# Overview

This document describes the replay, recovery and reprocessing strategy implemented in the `camara-data-pipeline` project.

The architecture was designed to support resilient and traceable data processing by preserving raw ingestion, lineage metadata and operational execution history across all Medallion layers.

The replay strategy guarantees:

* reproducibility;
* operational resiliency;
* controlled reprocessing;
* batch traceability;
* historical reconstruction;
* analytical consistency.

---

# Replay Philosophy

The project follows a replay-first architecture philosophy.

The Bronze layer preserves raw ingestion and metadata to allow downstream reconstruction whenever required.

## Main objectives

* rebuild downstream layers;
* recover from operational failures;
* validate transformations;
* support auditing;
* support CDC reconstruction;
* support streaming recovery.

---

# Replay Architecture

```text
Source APIs / Files
        │
        ▼
Bronze Raw Layer
(raw payloads + metadata)
        │
        │ replay source
        ▼
Silver Base
(parsing + quality)
        │
        ▼
Silver Curated
(curated entities)
        │
        ▼
Gold Layer
(dimensions + facts + views)
        │
        ▼
Analytics / Dashboards / Monitoring
```

---

# Replayable Bronze Layer

The Bronze layer is the foundation of replayability.

## Main characteristics

* raw payload preservation;
* deterministic ingestion;
* batch lineage;
* source metadata;
* ingestion timestamps;
* record hash generation.

---

# Bronze Metadata

## Main replay metadata

| Field | Purpose |
|---|---|
| bronze_id_batch | Batch traceability |
| bronze_ts_ingestao | Ingestion timestamp |
| bronze_tx_endpoint | Source endpoint |
| bronze_tx_record_hash | Deterministic record comparison |
| bronze_tx_source_file | Replay source file |
| bronze_nr_ano_referencia | Reference year |

---

# Replay Scenarios

## API Failure

### Scenario

Temporary API outage or timeout during ingestion.

### Strategy

* preserve already ingested records;
* rerun affected ingestion notebook;
* validate logs;
* reconstruct downstream layers if necessary.

---

## Invalid Transformation

### Scenario

Incorrect transformation logic identified in Silver or Gold layers.

### Strategy

* preserve Bronze unchanged;
* correct transformation logic;
* rerun Silver/Gold layers;
* validate reconstructed outputs.

---

## Deduplication Issue

### Scenario

Duplicate records identified after ingestion.

### Strategy

* validate deterministic hash logic;
* rerun deduplication layer;
* rebuild downstream marts.

---

## CDC Reconstruction

### Scenario

CDC inconsistency or missing historical version.

### Strategy

* replay Bronze CDC ingestion;
* regenerate payload comparison;
* rebuild SCD Type 2 history.

---

## Streaming Offset Failure

### Scenario

Incorrect streaming offset progression.

### Strategy

* reset offset control;
* replay micro-batch ingestion;
* validate replayed batches.

---

# Replay Levels

| Layer | Replay Capability |
|---|---|
| Bronze | Full replay source |
| Silver Base | Rebuildable from Bronze |
| Silver Curated | Rebuildable from Silver Base |
| Gold | Rebuildable from Curated |
| Analytics | Rebuildable from Gold |

---

# Batch Reconstruction

Every execution receives a unique batch identifier.

## Main goals

* execution traceability;
* replay auditing;
* operational debugging;
* SLA correlation.

## Batch flow

```text
batch_id
    │
    ▼
Bronze Raw Layer
    │
    ▼
Silver Base
    │
    ▼
Silver Curated
    │
    ▼
Gold Layer
    │
    ▼
Analytics / Monitoring
```

---

# Deterministic Processing

Replayability depends on deterministic processing patterns.

## Main principles

* deterministic transformations;
* stable deduplication;
* explicit validations;
* reproducible aggregations;
* replay-safe processing.

---

# Delta Lake Recovery

Delta Lake strengthens replay and recovery capabilities.

## Main benefits

* ACID transactions;
* versioned tables;
* schema evolution;
* historical consistency;
* rollback support;
* replay reconstruction.

---

# Streaming Replay Strategy

Streaming workloads also support replay.

## Main controls

* offset tracking;
* batch lineage;
* record hash;
* raw payload preservation;
* execution logging.

---

# Offset Control

The streaming pipeline tracks processed voting offsets.

## Main object

```text
control.votacoes_stream_offset
```

## Purpose

* avoid duplicate processing;
* support replay;
* allow controlled recovery;
* preserve execution continuity.

---

# CDC Replay Strategy

The project implements CDC/SCD Type 2 replay support.

## Main controls

| Field | Purpose |
|---|---|
| valid_from | Historical version start |
| valid_to | Historical version end |
| is_current | Current active version |
| cdc_payload_hash | Change detection |

---

# Reprocessing Workflow

## Standard recovery flow

```text
Identify issue
        │
        ▼
Validate logs and metrics
        │
        ▼
Determine affected layer
        │
        ▼
Execute source layer replay
        │
        ▼
Rebuild downstream layers
        │
        ▼
Validate tables and analytical outputs
```

---

# Operational Logging

Replayability depends on operational observability.

## Main log table

```text
monitoring.pipeline_log
```

---

# Logged Metrics

| Metric | Purpose |
|---|---|
| records_read | Validate ingestion volume |
| records_written | Validate persistence |
| records_discarded | Validate rejected records |
| execution_duration | SLA validation |
| status | Execution monitoring |
| batch_id | Replay correlation |

---

# SLA Recovery

The replay strategy also supports SLA recovery analysis.

## Examples

* delayed ingestion;
* failed streaming execution;
* incomplete batches;
* replayed workflows;
* long-running executions.

---

# Replay Best Practices

The project implements modern replay and recovery best practices.

## Main practices

* preserve raw ingestion;
* avoid destructive transformations;
* maintain lineage metadata;
* use deterministic processing;
* log operational metrics;
* isolate Medallion layers;
* support downstream reconstruction.

---

# Limitations

## API Dependency

Replay depends on public API availability.

---

## Historical Reconstruction

CDC historical quality depends on recurring executions and retained Bronze history.

---

## Streaming Latency

Replay timing depends on workflow scheduling frequency.

---

# Related Documents

| Document | Purpose |
|---|---|
| streaming_architecture.md | Streaming and DLT architecture |
| governance_and_lineage.md | Governance and lineage |
| parliamentary_intelligence.md | Analytical architecture |
| runbook.md | Incident procedures |

---

# Conclusion

The replay strategy implemented in `camara-data-pipeline` demonstrates enterprise-oriented resiliency and operational recovery practices.

The architecture combines replayable Bronze ingestion, deterministic transformations, Delta Lake recovery, CDC reconstruction and streaming offset recovery to support reliable and traceable Lakehouse processing.
