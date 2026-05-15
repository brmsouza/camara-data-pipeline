# camara-data-pipeline — Documentation Index

🇺🇸 Central technical and architectural documentation index for the `camara-data-pipeline` project.

---

# Overview

This directory contains the technical, analytical and architectural documentation for the `camara-data-pipeline` project.

The documentation strategy follows enterprise-oriented Data Engineering documentation practices with separation between:

* executive project overview;
* operational architecture;
* governance and metadata;
* replay and resiliency strategies;
* streaming and CDC processing;
* analytical architecture;
* Parliamentary Intelligence products;
* notebook catalog;
* technical decisions;
* operational procedures.

---

# Principal Validation Documents

The following documents represent the primary technical validation artifacts for the project delivery and challenge adherence.

| Document | Description |
|---|---|
| `final_challenge_adherence_matrix.md` | Main technical validation and challenge adherence document |
| `README.md` | Executive project overview |

---

# Documentation Structure

| Document | Description |
|---|---|
| `streaming_architecture.md` | Streaming, CDC, DLT and SLA architecture |
| `governance_and_lineage.md` | Governance, lineage and observability |
| `replay_strategy.md` | Replay, recovery and reprocessing strategy |
| `parliamentary_intelligence.md` | Parliamentary analytics and intelligence layer |
| `gold_layer_enterprise_data_dictionary.md` | Enterprise Gold layer data dictionary |
| `analytical_data_products.md` | Analytical datasets and data products |
| `notebooks_catalog.md` | Notebook catalog and responsibilities |
| `architecture_decisions.md` | Architectural and modeling decisions |
| `runbook.md` | Operational incident procedures |

---

# Architecture Diagrams

| Diagram | Description |
|---|---|
| `assets/images/parliamentary_lakehouse_architecture.png` | Enterprise Medallion Lakehouse architecture |
| `assets/images/parliamentary_intelligence_gold_architecture.png` | Gold dimensional and analytical architecture |
| `assets/images/camara_data_platform_architecture.png` | End-to-end data platform architecture |
| `assets/images/job_votacoes_streaming_microbatch.png` | Streaming micro-batch workflow |
| `assets/images/dlt_votacoes_streaming.png` | DLT streaming pipeline architecture |

---

# Main Architectural Topics

## Lakehouse Architecture

The project implements a Medallion Lakehouse architecture using:

* Bronze;
* Silver Base;
* Silver Curated;
* Gold;
* Analytics.

The architecture separates technical processing responsibilities from analytical business abstractions, improving maintainability, governance and analytical scalability.

---

## Streaming and CDC

The platform also implements advanced processing capabilities including:

* streaming micro-batch ingestion;
* Delta Live Tables (DLT);
* CDC / SCD Type 2;
* SLA monitoring;
* workflow orchestration;
* replay and recovery strategies;
* offset-based incremental ingestion.

---

## Governance and Replay

The governance strategy includes:

* lineage preservation;
* batch tracking;
* replayability;
* deterministic processing;
* operational observability;
* structured logging;
* metadata validation;
* schema drift detection.

---

## Parliamentary Intelligence

The analytical layer includes:

* transparency indicators;
* parliamentary efficiency analytics;
* voting analytics;
* CEAP expense analytics;
* anomaly detection;
* engagement score;
* party intelligence;
* parliamentary front analytics;
* political alignment analytics.

---

# Repository Structure

```text
camara-data-pipeline/
│
├── README.md
│
├── docs/
│   ├── index.md
│   ├── final_challenge_adherence_matrix.md
│   ├── streaming_architecture.md
│   ├── governance_and_lineage.md
│   ├── replay_strategy.md
│   ├── parliamentary_intelligence.md
│   ├── gold_layer_enterprise_data_dictionary.md
│   ├── analytical_data_products.md
│   ├── notebooks_catalog.md
│   ├── architecture_decisions.md
│   └── runbook.md
│
├── assets/
│   └── images/
│
└── notebooks/
```

---

# Documentation Principles

The documentation strategy prioritizes:

* technical clarity;
* reproducibility;
* operational transparency;
* architectural explainability;
* governance visibility;
* replay traceability;
* analytical consistency;
* enterprise maintainability.

---

# Intended Audience

The documentation was designed for:

* Data Engineers;
* Analytics Engineers;
* technical recruiters;
* architecture reviewers;
* portfolio evaluation;
* technical interview discussions;
* educational study;
* enterprise architecture assessments.

---

# Conclusion

The documentation architecture implemented in `camara-data-pipeline` was designed to separate executive project presentation from deep technical documentation, following modern enterprise documentation standards for Data Engineering, Lakehouse platforms and analytical governance.