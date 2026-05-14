# Camara Data Pipeline — Parliamentary Lakehouse Platform

Enterprise-style Lakehouse Data Engineering platform fully implemented natively on Databricks for large-scale parliamentary analytics using PySpark, Delta Lake, Delta Live Tables (DLT), streaming micro-batch ingestion, CDC/SCD Type 2 processing, multi-endpoint REST APIs and Medallion architecture.

<p align="left">
  <img src="https://img.shields.io/badge/python-3.12+-3776AB?style=flat-square&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-Data%20Engineering-E25A1C?style=flat-square&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Databricks-Lakehouse%20Platform-FF3621?style=flat-square&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Delta%20Lake-ACID%20Tables-00ADD8?style=flat-square" />
  <img src="https://img.shields.io/badge/architecture-medallion-6A1B9A?style=flat-square" />
  <img src="https://img.shields.io/badge/streaming-DLT%20%2B%20CDC-00897B?style=flat-square" />
  <img src="https://img.shields.io/badge/analytics-parliamentary-2E7D32?style=flat-square" />
  <img src="https://img.shields.io/badge/release-v1.0.0-1976D2?style=flat-square" />
</p>

🇧🇷 Portuguese version: [README.pt-BR.md](README.pt-BR.md)

---

# Educational Purpose

This project was developed for educational, portfolio and technical study purposes.
The repository demonstrates enterprise-oriented Data Engineering concepts using public parliamentary datasets and modern Lakehouse architecture patterns.
No political affiliation, governmental endorsement or institutional relationship is implied.
All analytical indicators and intelligence layers were created exclusively for technical demonstration, analytical experimentation and engineering architecture studies.

---

# Why This Project Is Different

Unlike traditional portfolio ETL projects, this solution implements:

* multi-endpoint distributed REST ingestion;
* replayable Medallion Lakehouse architecture;
* deterministic lineage and batch tracking;
* CDC / SCD Type 2 historization;
* streaming micro-batch ingestion;
* Delta Live Tables (DLT);
* SLA monitoring and operational observability;
* supplier enrichment using public CNPJ datasets;
* parliamentary intelligence analytical marts;
* anomaly detection and behavioral analytics;
* governance-oriented replay and recovery strategy;
* enterprise-style technical documentation.

The project was designed to simulate real-world enterprise Data Engineering architecture patterns rather than isolated ETL scripts.

---

# Engineering Highlights

| Capability | Implementation |
|---|---|
| Multi-endpoint REST ingestion | Câmara dos Deputados Open Data API |
| Streaming micro-batch | Parliamentary voting ingestion |
| CDC / SCD Type 2 | Proposition processing historization |
| Delta Live Tables | Streaming Bronze → Silver → Gold pipeline |
| Replayability | Replayable Bronze ingestion and lineage |
| Governance | Batch tracking and deterministic hashes |
| SLA Monitoring | Streaming operational metrics |
| Supplier enrichment | Public Brazilian CNPJ datasets |
| Parliamentary intelligence | Analytical Gold marts |
| Anomaly detection | Z-score expenditure analysis |
| Workflow orchestration | Databricks Workflows |
| Observability | Monitoring tables and execution logs |

---

# Technologies Used

| Category | Technology |
|---|---|
| Platform | Databricks Free Edition |
| Languages | PySpark (Python) and Spark SQL |
| Processing Engine | Apache Spark |
| Storage | Delta Lake |
| Architecture | Medallion Lakehouse Architecture |
| Streaming | Delta Live Tables (DLT) and Micro-batch |
| APIs | REST APIs |
| Version Control | GitHub |
| Modeling | Star Schema / Dimensional Modeling |
| Observability | Operational monitoring tables |
| Governance | Replayable lineage architecture |

---

# Main Data Sources

## Câmara dos Deputados Open Data API

Official documentation:

https://dadosabertos.camara.leg.br/swagger/api.html

---

## Main Consumed Endpoints

| Domain | Endpoint |
|---|---|
| Deputies | `/deputados` |
| Deputy details | `/deputados/{id}` |
| Parliamentary expenses | `/deputados/{id}/despesas` |
| Parliamentary fronts | `/frentes` |
| Front members | `/frentes/{id}/membros` |
| Legislative events | `/eventos` |
| Propositions | `/proposicoes` |
| Proposition processing | `/proposicoes/{id}/tramitacoes` |
| Organizations | `/orgaos` |
| Organization members | `/orgaos/{id}/membros` |
| Voting sessions | `/votacoes` |
| Voting votes | `/votacoes/{id}/votos` |
| Voting orientations | `/votacoes/{id}/orientacoes` |
| Legislatures | `/legislaturas` |

---

## External Enrichment Sources

The project also integrates external public datasets for analytical enrichment.

### Supplier Enrichment

Public Brazilian CNPJ datasets are used to:

* validate suppliers;
* classify CPF/CNPJ entities;
* identify active/inactive suppliers;
* support anomaly detection;
* improve CEAP expenditure analytics.

This enrichment layer simulates real-world enterprise master data integration patterns.

---

# Overview

`camara-data-pipeline` is a modern Lakehouse Data Engineering platform designed to ingest, validate, curate and analytically model parliamentary open data from the Brazilian Chamber of Deputies ecosystem.

The architecture follows a progressive Medallion refinement strategy across:

* Bronze;
* Silver Base;
* Silver Curated;
* Gold;
* Analytics.

The platform combines:

* scalable API ingestion;
* replayable pipelines;
* governance and lineage;
* dimensional modeling;
* streaming ingestion;
* CDC / SCD Type 2;
* Delta Live Tables;
* operational observability;
* parliamentary intelligence analytics.

---

# Architecture

The platform follows a layered Lakehouse architecture with progressive data refinement and replayability.

```text
Bronze
    │
    ▼
Silver Base
    │
    ▼
Silver Curated
    │
    ▼
Gold
    │
    ▼
Analytics
```

---

## Architecture Principles

The architecture was designed around the following principles:

* replay-first ingestion;
* raw data preservation;
* deterministic processing;
* explicit validations;
* analytical scalability;
* governance-oriented lineage;
* operational observability;
* modular pipeline design.

---

## Architecture Diagram

![Architecture](assets/images/camadamedalhao_camaradeputados.png)

---

# Streaming, CDC and DLT

The project also implements advanced modern Data Engineering capabilities.

## Implemented Components

* streaming micro-batch ingestion;
* Delta Live Tables (DLT);
* CDC / SCD Type 2;
* proposition historization;
* workflow orchestration;
* SLA monitoring;
* replay and recovery strategy;
* operational observability;
* streaming lineage tracking.

---

## Workflow Orchestration

![Workflow](assets/images/job_camara_medallion_pipeline.png)

---

## Streaming Micro-Batch

![Streaming](assets/images/job_votacoes_streaming_microbatch.png)

---

## Delta Live Tables

![DLT](assets/images/dlt_votacoes_streaming.png)

---

Detailed streaming documentation is available in:


[streaming_architecture.md](docs/streaming_architecture.md) 


---

# Parliamentary Intelligence Analytics

The Gold and Analytics layers implement advanced parliamentary intelligence analytical marts.

## Main Analytical Domains

* parliamentary expenses analytics;
* voting analytics;
* transparency indicators;
* parliamentary efficiency indicators;
* supplier intelligence;
* political alignment analytics;
* parliamentary engagement score;
* anomaly detection;
* parliamentary front analytics;
* party analytical dashboards.

---

## Gold Dimensional Model

![Gold Model](assets/images/modelo_camaradeputados.png)

---

## Main Analytical Capabilities

| Capability | Description |
|---|---|
| CEAP Analytics | Parliamentary expense analysis |
| Supplier Intelligence | Supplier enrichment and validation |
| Transparency Index | Parliamentary transparency indicators |
| Efficiency Index | Parliamentary efficiency indicators |
| Voting Analytics | Voting behavior and party alignment |
| Party Intelligence | Political party analytical dashboards |
| Front Analytics | Parliamentary front concentration analysis |
| Engagement Score | Parliamentary participation metrics |
| Z-Score Analytics | Expenditure anomaly detection |

---

Detailed analytical documentation is available in:


[parliamentary_intelligence.md](docs/parliamentary_intelligence.md)

[gold_layer_enterprise_data_dictionary.md](docs/gold_layer_enterprise_data_dictionary.md)

---

# Governance, Replay and Observability

The architecture preserves governance, lineage and replayability across all processing layers.

## Main Implemented Concepts

* replayable Bronze ingestion;
* batch lineage tracking;
* deterministic record hashes;
* operational monitoring;
* rejected records handling;
* replay and recovery support;
* CDC historization;
* streaming offset tracking;
* SLA monitoring;
* operational logging.

---

## Governance Diagram

![Governance](assets/images/pilares_analiticos.png)

---

Detailed governance documentation is available in:


[governance_and_lineage.md](docs/governance_and_lineage.md)

[replay_strategy.md](docs/replay_strategy.md) 

[runbook.md](docs/runbook.md) 


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
│   ├── gold_layer_enterprise_data_dictionary.md
│   ├── parliamentary_intelligence.md
│   ├── notebooks_catalog.md
│   ├── architecture_decisions.md
│   ├── challenge_matrix.md
│   └── runbook.md
│
├── assets/
│   └── images/
│
├── notebooks/
│   ├── 00_setup/
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_gold/
│   ├── 04_analytics/
│   ├── 05_dlt/
│   ├── 90_common/
│   └── 99_jobs/
│
└── requirements.txt
```

---

# Documentation

Detailed technical documentation is available under:

```text
docs/
```

| Document | Description |
|---|---|
| [streaming_architecture.md](docs/streaming_architecture.md) | Streaming, CDC, DLT and SLA architecture |
| [governance_and_lineage.md](docs/governance_and_lineage.md) | Governance, lineage and observability |
| [replay_strategy.md](docs/replay_strategy.md) | Replay and recovery strategy |
| [parliamentary_intelligence.md](docs/parliamentary_intelligence.md) | Parliamentary analytics and intelligence |
| [gold_layer_enterprise_data_dictionary.md](docs/gold_layer_enterprise_data_dictionary.md) | Enterprise Gold layer dimensional data dictionary |
| [architecture_decisions.md](docs/architecture_decisions.md) | Architecture and modeling decisions |
| [challenge_matrix.md](docs/challenge_matrix.md) | Challenge adherence matrix |
| [runbook.md](docs/runbook.md) | Operational incident procedures |
| [notebooks_catalog.md](docs/notebooks_catalog.md) | Notebook catalog and responsibilities |

---

# Engineering Goals

The project was designed to demonstrate:

* enterprise-style Data Engineering architecture;
* scalable Lakehouse processing;
* replayability and resiliency;
* streaming and CDC patterns;
* dimensional modeling;
* operational observability;
* analytical governance;
* modern Databricks engineering workflows.

---
## Final Challenge Adherence Matrix

The project includes a complete adherence matrix mapping the Databricks final
challenge requirements to the implemented pipelines, analytical products,
Gold dimensional models, streaming architecture and Parliamentary Intelligence
capabilities.

### Documents

- [Final Challenge Adherence Matrix](docs/final_challenge_adherence_matrix.md)
- [Matriz de Aderência ao Desafio Final](docs/final_challenge_adherence_matrix.pt-BR.md)

### Covered Areas

- Medallion Architecture (Bronze / Silver / Gold)
- Gold Star Schema dimensional modeling
- CEAP parliamentary expense analytics
- Parliamentary fronts analytics
- Voting intelligence and party alignment
- Legislative events analytics
- Parliamentary engagement analytics
- CDC / SCD Type 2 historization
- Streaming pipelines with DLT / Lakeflow
- SLA monitoring and observability
- Metadata governance and validation
- Parliamentary Intelligence analytical products
---

# Author

Bruno Souza

Data Engineer focused on scalable analytical platforms, governance, Lakehouse architecture, dimensional modeling and modern Data Engineering practices.