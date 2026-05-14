# Project Documentation

🇺🇸 Central documentation index for the `camara-data-pipeline` project.

---

# Overview

This directory contains the technical and architectural documentation for the `camara-data-pipeline` project.

The documentation strategy follows enterprise-oriented Data Engineering documentation practices with separation between:

* executive project overview;
* operational architecture;
* governance;
* replay strategy;
* analytical architecture;
* notebook catalog;
* technical decisions;
* operational procedures.

---

# Documentation Structure

| Document | Description |
|---|---|
| `README.md` | Executive project overview |
| `streaming_architecture.md` | Streaming, CDC, DLT and SLA architecture |
| `governance_and_lineage.md` | Governance, lineage and observability |
| `replay_strategy.md` | Replay, recovery and reprocessing strategy |
| `parliamentary_intelligence.md` | Parliamentary analytics and intelligence layer |
| `notebooks_catalog.md` | Notebook catalog and responsibilities |
| `architecture_decisions.md` | Architectural and modeling decisions |
| `challenge_matrix.md` | Challenge adherence matrix |
| `runbook.md` | Operational incident procedures |

---

# Main Architectural Topics

## Lakehouse Architecture

The project implements a Medallion Lakehouse architecture using:

* Bronze;
* Silver Base;
* Silver Curated;
* Gold;
* Analytics.

---

## Streaming and CDC

The platform also implements advanced processing features including:

* streaming micro-batch ingestion;
* Delta Live Tables (DLT);
* CDC / SCD Type 2;
* SLA monitoring;
* workflow orchestration;
* replay and recovery strategies.

---

## Governance and Replay

The governance strategy includes:

* lineage preservation;
* batch tracking;
* replayability;
* deterministic processing;
* operational observability;
* structured logging.

---

## Parliamentary Intelligence

The analytical layer includes:

* transparency indicators;
* parliamentary efficiency analytics;
* voting analytics;
* CEAP expense analytics;
* anomaly detection;
* engagement score;
* party intelligence.

---

# Repository Structure

```text
camara-data-pipeline/
│
├── README.md
│
├── docs/
│   ├── index.md
│   ├── streaming_architecture.md
│   ├── governance_and_lineage.md
│   ├── replay_strategy.md
│   ├── parliamentary_intelligence.md
│   ├── notebooks_catalog.md
│   ├── architecture_decisions.md
│   ├── challenge_matrix.md
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
* analytical consistency.

---

# Intended Audience

The documentation was designed for:

* Data Engineers;
* Analytics Engineers;
* technical recruiters;
* architecture reviewers;
* portfolio evaluation;
* technical interview discussions;
* educational study.

---

# Conclusion

The documentation architecture implemented in `camara-data-pipeline` was designed to separate executive project presentation from deep technical documentation, following modern enterprise documentation standards for Data Engineering and Lakehouse platforms.
