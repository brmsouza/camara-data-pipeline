# camara-data-pipeline — Índice de Documentação

🇧🇷 Índice central de documentação técnica e arquitetural do projeto `camara-data-pipeline`.

---

# Visão Geral

Este diretório contém a documentação técnica, analítica e arquitetural do projeto `camara-data-pipeline`.

A estratégia documental segue práticas enterprise de documentação para Engenharia de Dados com separação entre:

* visão executiva do projeto;
* arquitetura operacional;
* governança e metadata;
* estratégias de replay e resiliência;
* processamento streaming e CDC;
* arquitetura analítica;
* produtos de Parliamentary Intelligence;
* catálogo de notebooks;
* decisões técnicas;
* procedimentos operacionais.

---

# Principais Documentos de Validação

Os documentos abaixo representam os principais artefatos técnicos de validação da entrega do projeto e aderência ao desafio.

| Documento | Descrição |
|---|---|
| `final_challenge_adherence_matrix.pt-BR.md` | Documento principal de validação técnica e aderência ao desafio |
| `README.pt-BR.md` | Visão executiva do projeto |

---

# Estrutura da Documentação

| Documento | Descrição |
|---|---|
| `streaming_architecture.pt-BR.md` | Arquitetura streaming, CDC, DLT e SLA |
| `streaming_sla_observability.pt-BR.md` | Monitoramento de SLA e observabilidade operacional |
| `governance_and_lineage.pt-BR.md` | Governança, lineage e observabilidade |
| `replay_strategy.pt-BR.md` | Estratégia de replay, recuperação e reprocessamento |
| `parliamentary_intelligence.pt-BR.md` | Camada analítica e inteligência parlamentar |
| `gold_layer_enterprise_data_dictionary.pt-BR.md` | Dicionário enterprise da camada Gold |
| `analytical_data_products.pt-BR.md` | Datasets analíticos e produtos de dados |
| `notebook_engineering_standards.pt-BR.md` | Padrões de engenharia e construção de notebooks |
| `notebooks_catalog.pt-BR.md` | Catálogo de notebooks e responsabilidades |
| `architecture_decisions.pt-BR.md` | Decisões arquiteturais e de modelagem |
| `runbook.pt-BR.md` | Procedimentos operacionais e recuperação de incidentes |

---

# Diagramas Arquiteturais

| Diagrama | Descrição |
|---|---|
| `assets/images/parliamentary_lakehouse_architecture.png` | Arquitetura enterprise Medallion Lakehouse |
| `assets/images/parliamentary_intelligence_gold_architecture.png` | Arquitetura dimensional e analítica Gold |
| `assets/images/camara_data_platform_architecture.png` | Arquitetura end-to-end da plataforma de dados |
| `assets/images/job_votacoes_streaming_microbatch.png` | Workflow streaming micro-batch |
| `assets/images/dlt_votacoes_streaming.png` | Arquitetura do pipeline streaming DLT |

---

# Principais Tópicos Arquiteturais

## Arquitetura Lakehouse

O projeto implementa uma arquitetura Medallion Lakehouse utilizando:

* Bronze;
* Silver Base;
* Silver Curated;
* Gold;
* Analytics.

A arquitetura separa responsabilidades técnicas de processamento das abstrações analíticas de negócio, melhorando manutenibilidade, governança e escalabilidade analítica.

---

## Streaming e CDC

A plataforma também implementa capacidades avançadas de processamento incluindo:

* ingestão streaming em micro-batch;
* Delta Live Tables (DLT);
* CDC / SCD Type 2;
* monitoramento de SLA;
* orquestração de workflows;
* estratégias de replay e recuperação;
* ingestão incremental baseada em offset.

---

## Governança e Replay

A estratégia de governança inclui:

* preservação de lineage;
* rastreamento de batches;
* replayabilidade;
* processamento determinístico;
* observabilidade operacional;
* logging estruturado;
* validação de metadata;
* detecção de schema drift.

---

## Parliamentary Intelligence

A camada analítica inclui:

* indicadores de transparência;
* analytics de eficiência parlamentar;
* analytics de votação;
* analytics de despesas CEAP;
* detecção de anomalias;
* score de engajamento;
* inteligência partidária;
* analytics de frentes parlamentares;
* analytics de alinhamento político.

---

# Estrutura do Repositório

```text
camara-data-pipeline/
│
├── README.pt-BR.md
│
├── docs/
│   ├── index.pt-BR.md
│   ├── final_challenge_adherence_matrix.pt-BR.md
│   ├── streaming_architecture.pt-BR.md
│   ├── streaming_sla_observability.pt-BR.md
│   ├── governance_and_lineage.pt-BR.md
│   ├── replay_strategy.pt-BR.md
│   ├── parliamentary_intelligence.pt-BR.md
│   ├── gold_layer_enterprise_data_dictionary.pt-BR.md
│   ├── analytical_data_products.pt-BR.md
│   ├── notebook_engineering_standards.pt-BR.md
│   ├── notebooks_catalog.pt-BR.md
│   ├── architecture_decisions.pt-BR.md
│   └── runbook.pt-BR.md
│
├── assets/
│   └── images/
│
└── notebooks/
```

---

# Princípios da Documentação

A estratégia documental prioriza:

* clareza técnica;
* reprodutibilidade;
* transparência operacional;
* explicabilidade arquitetural;
* visibilidade de governança;
* rastreabilidade de replay;
* consistência analítica;
* manutenibilidade enterprise.

---

# Público-Alvo

A documentação foi projetada para:

* Engenheiros de Dados;
* Analytics Engineers;
* recrutadores técnicos;
* revisores de arquitetura;
* avaliação de portfólio;
* discussões técnicas em entrevistas;
* estudo educacional;
* avaliações de arquitetura enterprise.

---

# Conclusão

A arquitetura documental implementada no `camara-data-pipeline` foi desenhada para separar a apresentação executiva do projeto da documentação técnica aprofundada, seguindo padrões modernos enterprise para Engenharia de Dados, plataformas Lakehouse e governança analítica.