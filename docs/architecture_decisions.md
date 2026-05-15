# Architecture Decisions

🇺🇸 Technical document — Architecture and Modeling Decisions

---

# Overview

This document records the main architecture, modeling and engineering decisions adopted in the `camara-data-pipeline` project.

The objective is to make the project decisions explicit, reproducible and defensible from a Data Engineering perspective.

---

# Decision 1 — Databricks as Main Platform

## Decision

Use Databricks Free Edition as the primary development and execution platform.

## Rationale

Databricks provides native support for:

* Apache Spark;
* PySpark notebooks;
* Spark SQL;
* Delta Lake;
* Workflows;
* Delta Live Tables;
* Lakehouse architecture.

## Outcome

The project demonstrates modern Lakehouse engineering patterns in a realistic platform environment.

---

# Decision 2 — Medallion Architecture

## Decision

Adopt a Medallion Architecture with Bronze, Silver Base, Silver Curated, Gold and Analytics layers.

## Rationale

This separation provides:

* raw data preservation;
* progressive refinement;
* replayability;
* governance;
* analytical consistency;
* separation of technical and business logic.

---

# Decision 3 — Split Silver into Base and Curated

## Decision

Separate the Silver layer into:

```text
02_silver/01_base
02_silver/02_curated
```

## Rationale

Silver Base is responsible for technical standardization and validation.

Silver Curated is responsible for reusable business-oriented entities and light enrichment.

## Outcome

The architecture avoids mixing technical cleansing with business curation.

---

# Decision 4 — Keep Dimensions and Facts in Gold

## Decision

Create final dimensions and facts only in the Gold layer.

## Rationale

Silver should prepare reliable entities, but Gold should represent the final analytical model.

## Outcome

The model remains aligned with dimensional modeling best practices.

---

# Decision 5 — Use Star Schema

## Decision

Adopt Star Schema modeling in Gold.

## Rationale

Star Schema provides:

* analytical simplicity;
* reusable dimensions;
* independent facts;
* scalable BI consumption;
* clear analytical grain.

---

# Decision 6 — No Fact-to-Fact Relationships

## Decision

Avoid direct relationships between fact tables.

## Rationale

Fact tables should be analyzed through conformed dimensions.

## Outcome

The analytical model remains clean, scalable and easier to understand.

---

# Decision 7 — Preserve Bronze Lineage

## Decision

Preserve technical lineage metadata from Bronze into downstream layers when relevant.

## Main lineage fields

* `bronze_ts_ingestao`;
* `bronze_dt_ingestao`;
* `bronze_tx_endpoint`;
* `bronze_id_batch`;
* `bronze_tx_record_hash`;
* `bronze_tx_source_file`;
* `bronze_nr_ano_referencia`.

## Rationale

Lineage is required for traceability, replay and auditing.

---

# Decision 8 — Use Deterministic Hashes

## Decision

Generate deterministic record hashes.

## Rationale

Hashes support:

* deduplication;
* replay validation;
* CDC comparison;
* change detection;
* auditability.

---

# Decision 9 — Use Explicit Quality Validations

## Decision

Use explicit quality validation logic with `raise Exception` for critical failures.

## Rationale

Fail-fast behavior avoids silent analytical corruption.

## Outcome

Pipelines fail when critical assumptions are violated.

---

# Decision 10 — Track records_discarded

## Decision

Track discarded records as part of pipeline execution.

## Rationale

Discarded records are important for:

* data quality analysis;
* operational monitoring;
* debugging;
* governance.

---

# Decision 11 — Use Rejected Records When Applicable

## Decision

Persist rejected records when business or quality rules require investigation.

## Rationale

Rejected records should not disappear silently.

---

# Decision 12 — Implement CDC / SCD Type 2

## Decision

Implement CDC and SCD Type 2 for proposition processing events.

## Rationale

Proposition processing changes over time and requires historical tracking.

## Main fields

* `valid_from`;
* `valid_to`;
* `is_current`;
* `cdc_payload_hash`.

---

# Decision 13 — Implement Streaming Micro-Batch

## Decision

Implement voting monitoring through scheduled micro-batch ingestion.

## Rationale

Micro-batch provides near-real-time behavior while remaining simple and reliable in the project context.

---

# Decision 14 — Use Delta Live Tables

## Decision

Use Delta Live Tables for the streaming validation pipeline.

## Rationale

DLT provides:

* declarative expectations;
* quality enforcement;
* managed flow;
* streaming-oriented design.

---

# Decision 15 — Use Monitoring Tables

## Decision

Use monitoring tables such as:

```text
monitoring.pipeline_log
monitoring.vw_sla_votacoes_streaming
```

## Rationale

Operational observability is required for SLA monitoring, replay and incident investigation.

---

# Decision 16 — Keep README Executive

## Decision

Keep the README concise and move detailed technical documentation to `docs/`.

## Rationale

The README should be GitHub-friendly and recruiter-friendly, while technical depth should live in dedicated documentation files.

---

# Conclusion

The architecture decisions adopted in `camara-data-pipeline` were designed to demonstrate enterprise-style Data Engineering practices using Databricks, PySpark, Spark SQL, Delta Lake and Medallion Architecture.

The project prioritizes clarity, replayability, governance, dimensional modeling, observability and analytical value.
