# Runbook

🇺🇸 Operational document — Incident Response, Recovery and Reprocessing Procedures

---

# Overview

This runbook documents operational procedures for the `camara-data-pipeline` project.

It provides guidance for investigating failures, recovering pipelines, replaying data, validating lineage and restoring analytical consistency across the Lakehouse layers.

---

# Scope

This runbook covers:

* API ingestion failures;
* schema changes;
* duplicated records;
* unexpected volumes;
* rejected records;
* lineage issues;
* Gold layer inconsistencies;
* Analytics failures;
* streaming offset issues;
* Delta Live Tables failures;
* SLA degradation.

---

# Operational Layers

```text
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

# General Incident Procedure

```text
Detect issue
        │
        ▼
Identify affected pipeline
        │
        ▼
Check monitoring logs
        │
        ▼
Validate source and target tables
        │
        ▼
Apply recovery action
        │
        ▼
Reprocess affected layer
        │
        ▼
Validate outputs
```

---

# Main Monitoring Table

```text
monitoring.pipeline_log
```

## Key fields

| Field | Purpose |
|---|---|
| batch_id | Execution traceability |
| pipeline_name | Pipeline identification |
| layer | Processing layer |
| event_name | Execution event |
| records_read | Input volume |
| records_written | Persisted volume |
| records_discarded | Rejected/discarded volume |
| started_at | Start timestamp |
| finished_at | End timestamp |
| status | Execution result |
| error_message | Failure details |

---

# Common Incidents

## API Failure

### Symptoms

* timeout;
* empty response;
* HTTP error;
* unexpected API payload;
* incomplete ingestion.

### Action

1. Check the API availability.
2. Review `monitoring.pipeline_log`.
3. Validate the affected endpoint.
4. Rerun the ingestion notebook.
5. Rebuild downstream layers if required.

### Recommended recovery

```text
Replay from Bronze if raw records exist.
Re-run Bronze ingestion if records were not persisted.
```

---

## Schema Change

### Symptoms

* missing columns;
* new fields in API response;
* parsing errors;
* Silver validation failure.

### Action

1. Compare current Bronze payload with previous payload.
2. Adjust Silver Base parsing logic.
3. Validate schema assumptions.
4. Reprocess Silver Base.
5. Reprocess Silver Curated, Gold and Analytics if needed.

---

## Duplicated Records

### Symptoms

* duplicated business keys;
* inflated metrics;
* unexpected count increase;
* duplicated fact rows.

### Action

1. Identify duplicated key.
2. Validate hash logic.
3. Review deduplication window.
4. Reprocess affected Silver/Gold layer.
5. Validate final counts.

---

## Unexpected Volume

### Symptoms

* records_read much higher or lower than expected;
* records_written = 0;
* excessive records_discarded;
* empty downstream table.

### Action

1. Review source parameters.
2. Validate API pagination.
3. Check filters by year/date/ID.
4. Review discarded records.
5. Reprocess affected layer.

---

## Rejected Records Above Expected

### Symptoms

* high `records_discarded`;
* validation errors;
* invalid IDs;
* invalid dates;
* null mandatory fields.

### Action

1. Check rejected records table when available.
2. Review rejection reason.
3. Validate source payload quality.
4. Adjust validation logic only if business rule changed.
5. Reprocess affected layer.

---

# Layer Recovery

## Bronze Recovery

Bronze should preserve raw ingestion and replay capability.

### Recovery actions

* rerun ingestion notebook;
* validate API endpoint;
* validate batch_id;
* validate record hash;
* validate source file if file-based ingestion.

---

## Silver Base Recovery

Silver Base can be rebuilt from Bronze.

### Recovery actions

* fix parsing or validation logic;
* rerun Silver Base notebook;
* validate records_read, records_written and records_discarded;
* validate rejected records.

---

## Silver Curated Recovery

Silver Curated can be rebuilt from Silver Base.

### Recovery actions

* validate business fallback rules;
* validate standardization logic;
* rerun curated notebook;
* validate final entity consistency.

---

## Gold Recovery

Gold should be rebuilt from Silver Curated.

### Recovery actions

* validate dimension keys;
* validate fact grain;
* validate surrogate key mapping;
* rebuild affected dimension or fact;
* re-run analytics if required.

---

## Analytics Recovery

Analytics should be rebuilt from Gold objects.

### Recovery actions

* validate Gold dependencies;
* validate analytical grain;
* rerun analytical notebook;
* compare KPIs with previous run.

---

# Streaming Incidents

## Offset Inconsistency

### Symptoms

* duplicated streaming records;
* missing voting sessions;
* offset lower or higher than expected.

### Action

1. Validate `control.votacoes_stream_offset`.
2. Check last processed voting ID/timestamp.
3. Reset offset if required.
4. Replay micro-batch.
5. Validate `bronze_stream.votacoes_raw`.

---

## DLT Failure

### Symptoms

* DLT pipeline failed;
* expectations dropping excessive records;
* Silver/Gold streaming tables not updated.

### Action

1. Validate Bronze streaming table.
2. Review DLT expectations.
3. Check invalid record patterns.
4. Restart DLT pipeline.
5. Validate Gold alert table.

---

## SLA Delay

### Symptoms

* high latency;
* delayed micro-batch;
* incomplete workflow execution.

### Action

1. Validate `monitoring.vw_sla_votacoes_streaming`.
2. Check workflow execution duration.
3. Validate API availability.
4. Replay delayed interval if necessary.

---

# CDC / SCD Type 2 Incidents

## Missing Historical Version

### Symptoms

* missing SCD2 row;
* incorrect `is_current`;
* incorrect `valid_from` / `valid_to`.

### Action

1. Validate CDC source ingestion.
2. Validate payload hash comparison.
3. Reprocess CDC base layer.
4. Rebuild SCD2 table.
5. Validate current version.

---

# Validation Checklist

Before closing an incident, validate:

* records_read;
* records_written;
* records_discarded;
* target table count;
* null keys;
* duplicated keys;
* lineage fields;
* Gold facts and dimensions;
* analytics outputs;
* monitoring logs.

---

# Related Documents

| Document | Purpose |
|---|---|
| `streaming_architecture.md` | Streaming and DLT architecture |
| `governance_and_lineage.md` | Governance and lineage |
| `replay_strategy.md` | Replay and recovery strategy |
| `notebooks_catalog.md` | Notebook responsibilities |

---

# Conclusion

This runbook provides operational recovery guidance for the `camara-data-pipeline` project.

The project was designed to support reliable reprocessing, traceability, replay and controlled incident recovery across the Lakehouse architecture.
