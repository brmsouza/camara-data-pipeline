# camara-data-pipeline

🇺🇸 English version: [README.md](README.md)

<p align="left">
  <img src="https://img.shields.io/badge/python-3.12+-3776AB?style=flat-square&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/PySpark-Data%20Engineering-E25A1C?style=flat-square&logo=apachespark&logoColor=white" />
  <img src="https://img.shields.io/badge/Databricks-Lakehouse%20Platform-FF3621?style=flat-square&logo=databricks&logoColor=white" />
  <img src="https://img.shields.io/badge/Delta%20Lake-ACID%20Tables-00ADD8?style=flat-square" />
  <img src="https://img.shields.io/badge/architecture-medallion-6A1B9A?style=flat-square" />
  <img src="https://img.shields.io/badge/analytics-parliamentary-2E7D32?style=flat-square" />
  <img src="https://img.shields.io/badge/release-v1.0.0-1976D2?style=flat-square" />
</p>

Projeto completo de Engenharia de Dados Lakehouse desenvolvido no Databricks utilizando PySpark, Spark SQL e Delta Lake para analytics parlamentares, governança, linhagem, streaming e modelagem dimensional.

---

## Finalidade Educacional

Este projeto foi desenvolvido com fins educacionais, estudo técnico e composição de portfólio profissional de Engenharia de Dados.

A solução demonstra conceitos modernos de Data Engineering utilizando datasets públicos parlamentares e padrões enterprise de arquitetura Lakehouse.

Nenhuma afiliação política, vínculo institucional ou posicionamento governamental é representado neste repositório.

As análises e indicadores apresentados possuem finalidade exclusivamente técnica, analítica e educacional.

---

# Tecnologias Utilizadas

| Categoria | Tecnologia |
|---|---|
| Plataforma | Databricks Free Edition |
| Linguagens | PySpark (Python) e Spark SQL |
| Armazenamento | Delta Lake |
| Arquitetura | Medallion Architecture (Bronze, Silver e Gold) |
| Streaming | Delta Live Tables (DLT) e Micro-batch |
| Fonte de Dados | API Dados Abertos Câmara |
| Versionamento | GitHub |
| Engine de Processamento | Apache Spark |
| Modelagem Analítica | Star Schema / Modelagem Dimensional |

---

## Plataforma

### Databricks Free Edition

Página oficial:

https://www.databricks.com/blog/introducing-databricks-free-edition

O projeto foi desenvolvido utilizando recursos Lakehouse do Databricks incluindo:

* Delta Lake;
* Workflows;
* notebooks PySpark;
* Spark SQL;
* streaming micro-batch;
* Delta Live Tables (DLT);
* implementação Medallion Architecture.

---

## Linguagens

### PySpark (Python)

Utilizado para:

* pipelines de ingestão;
* transformações;
* validações de qualidade;
* processamento CDC;
* processamento streaming;
* orquestração;
* processamento analítico.

### Spark SQL

Utilizado para:

* views analíticas;
* marts Gold;
* modelagem dimensional;
* agregações;
* produtos analíticos;
* validações de qualidade.

---

## Fonte de Dados

### API Dados Abertos Câmara dos Deputados

Documentação oficial:

https://dadosabertos.camara.leg.br/swagger/api.html

A API fornece datasets públicos parlamentares incluindo:

* deputados;
* despesas;
* proposições;
* frentes parlamentares;
* eventos legislativos;
* órgãos;
* votações;
* orientações de voto;
* votos;
* legislaturas.

---

## Padrão Arquitetural

### Medallion Architecture

O projeto implementa arquitetura Lakehouse Medallion com refinamento progressivo dos dados:

| Camada | Responsabilidade |
|---|---|
| Bronze | Ingestão bruta e replayabilidade |
| Silver Base | Padronização técnica e validações |
| Silver Curated | Entidades reutilizáveis orientadas ao negócio |
| Gold | Modelagem dimensional e marts analíticos |
| Analytics | Inteligência parlamentar e produtos analíticos |

---

## Recursos Avançados Implementados

O projeto também implementa:

* CDC / SCD Type 2;
* Delta Live Tables (DLT);
* streaming micro-batch;
* replay/reprocessamento;
* workflow orchestration;
* SLA monitoring;
* observabilidade operacional;
* padrões de governança e lineage.

---

# Sumário

* [Visão Geral](#visão-geral)
* [Escopo do Desafio](#escopo-do-desafio)
* [Fontes de Dados e APIs](#fontes-de-dados-e-apis)
* [Arquitetura](#arquitetura)
* [Arquitetura Medallion](#arquitetura-medallion)
* [Arquitetura Streaming, CDC e DLT](#arquitetura-streaming-cdc-e-dlt)
* [Orquestração de Workflows](#orquestração-de-workflows)
* [Modelo Dimensional Gold](#modelo-dimensional-gold)
* [Governança, Resiliência e Analytics](#governança-resiliência-e-analytics)
* [Principais Analytics Entregues](#principais-analytics-entregues)
* [Streaming e Inteligência Parlamentar](#streaming-e-inteligência-parlamentar)
* [Stack Tecnológica](#stack-tecnológica)
* [Estrutura do Repositório](#estrutura-do-repositório)
* [Arquitetura da Documentação](#arquitetura-da-documentação)
* [Qualidade e Lineage](#qualidade-e-lineage)
* [Processamento Incremental e Replay](#processamento-incremental-e-replay)
* [Enriquecimento de Fornecedores e Detecção de Anomalias](#enriquecimento-de-fornecedores-e-detecção-de-anomalias)
* [Considerações e Limitações Analíticas](#considerações-e-limitações-analíticas)
* [Evoluções Futuras](#evoluções-futuras)
* [Documentação](#documentação)
* [Autor](#autor)

---

# Visão Geral

`camara-data-pipeline` é um projeto moderno de Engenharia de Dados Lakehouse desenvolvido para ingestão, validação, curadoria e modelagem analítica de dados públicos parlamentares da Câmara dos Deputados.

O projeto foi construído utilizando padrão Medallion Architecture com refinamento progressivo entre as camadas Bronze, Silver Base, Silver Curated, Gold e Analytics.

A solução possui foco em:

* pipelines escaláveis de ingestão;
* replayabilidade e resiliência;
* modelagem dimensional;
* governança e lineage;
* analytics parlamentares;
* produtos analíticos financeiros e políticos;
* CDC e historização SCD Type 2;
* streaming micro-batch;
* Delta Live Tables;
* workflow orchestration;
* SLA monitoring e observabilidade operacional.

---

# Escopo do Desafio

| Tema do Desafio | Status |
|---|---|
| Analytics CEAP | ✔ |
| Frentes parlamentares | ✔ |
| Eventos legislativos | ✔ |
| Análise de votações | ✔ |
| Engagement score | ✔ |
| Enriquecimento de fornecedores | ✔ |
| Detecção de anomalias com z-score | ✔ |
| Governança e lineage | ✔ |
| Replay e resiliência | ✔ |
| CDC / SCD Type 2 | Implementado Parcialmente |
| Streaming micro-batch | ✔ |
| Delta Live Tables (DLT) | ✔ |
| SLA monitoring | ✔ |
| Workflow orchestration | ✔ |
| Inteligência Parlamentar | ✔ |
| Analytics de CPI | Roadmap |

---

# Fontes de Dados e APIs

O projeto utiliza datasets públicos parlamentares e governamentais para construção de uma plataforma analítica Lakehouse focada em atividade legislativa brasileira.

---

## Principal Fonte de Dados

### API Dados Abertos Câmara dos Deputados

API oficial:

https://dadosabertos.camara.leg.br/swagger/api.html

Principais datasets consumidos:

| Dataset | Endpoint |
|---|---|
| Deputados | `/deputados` |
| Detalhes deputados | `/deputados/{id}` |
| Frentes parlamentares | `/frentes` |
| Membros frentes | `/frentes/{id}/membros` |
| Eventos legislativos | `/eventos` |
| Proposições | `/proposicoes` |
| Tramitações | `/proposicoes/{id}/tramitacoes` |
| Despesas CEAP | `/deputados/{id}/despesas` |
| Órgãos | `/orgaos` |
| Membros órgãos | `/orgaos/{id}/membros` |
| Votações | `/votacoes` |
| Orientações | `/votacoes/{id}/orientacoes` |
| Votos | `/votacoes/{id}/votos` |
| Legislaturas | `/legislaturas` |

---

## Fonte de Enriquecimento

### Receita Federal / Bases Públicas CNPJ

Utilizado para:

* enriquecimento de fornecedores;
* validação CPF/CNPJ;
* validação ativo/inativo;
* suporte à análise de anomalias;
* detecção de fornecedores suspeitos.

---

# Arquitetura

O projeto segue arquitetura Lakehouse em camadas com refinamento progressivo dos dados parlamentares.

* Bronze preserva ingestão bruta e replayabilidade;
* Silver Base executa tratamento técnico e validações;
* Silver Curated prepara entidades reutilizáveis;
* Gold materializa dimensões, fatos e marts analíticos;
* Analytics entrega produtos analíticos parlamentares.

![Architecture](assets/images/camadamedalhao_camaradeputados.png)

---

# Arquitetura Medallion

## Bronze

Camada de ingestão bruta responsável por preservar:

* respostas originais da API;
* metadados;
* lineage;
* histórico de replay;
* rastreabilidade de ingestão.

---

## Silver Base

Camada técnica responsável por:

* parsing;
* tipagem;
* validações estruturais;
* deduplicação;
* descarte de registros inválidos;
* flags técnicas de qualidade.

---

## Silver Curated

Camada reutilizável orientada ao negócio.

Principais características:

* regras leves de negócio;
* fallback logic;
* padronização textual;
* enriquecimento Receita Federal;
* entidades reutilizáveis.

---

## Gold

Camada dimensional analítica responsável por:

* dimensões;
* fatos;
* marts analíticos;
* views analíticas;
* inteligência parlamentar.

---

# Arquitetura Streaming, CDC e DLT

O projeto evoluiu além de uma arquitetura batch tradicional e atualmente também implementa:

* ingestão incremental micro-batch;
* pipelines CDC;
* historização SCD Type 2;
* Delta Live Tables (DLT);
* workflow orchestration;
* SLA monitoring;
* replay/reprocessamento;
* observabilidade operacional.

---

## Workflow Orchestration

![Workflow Orchestration](assets/images/job_camara_medallion_pipeline.PNG)

---

## Streaming Micro-Batch

![Streaming Microbatch](assets/images/job_votacoes_streaming_microbatch.PNG)

---

## Delta Live Tables

![DLT Pipeline](assets/images/dlt_votacoes_streaming.PNG)

---

# Modelo Dimensional Gold

A camada Gold segue abordagem Star Schema com fatos independentes e dimensões reutilizáveis.

![Gold Model](assets/images/modelo_camaradeputados.png)

---

# Governança, Resiliência e Analytics

O projeto implementa padrões de governança, resiliência e observabilidade analítica.

![Governance](assets/images/pilares_analiticos.png)

---

# Principais Analytics Entregues

## Analytics CEAP

* ranking de despesas parlamentares;
* análise categoria × UF;
* fornecedores suspeitos;
* detecção de anomalias z-score.

---

## Frentes Parlamentares

* análise HHI;
* análise de overlap;
* alinhamento político;
* análise de concentração.

---

## Analytics de Votação

* alinhamento partidário;
* alinhamento de frentes;
* análise de orientações;
* divergência de voto.

---

## Eventos Legislativos

* calendário legislativo;
* densidade semanal;
* períodos de inatividade;
* aproximação de presença.

---

# Streaming e Inteligência Parlamentar

O projeto também inclui produtos operacionais e analíticos avançados.

## Streaming Voting Alerts

* monitoramento micro-batch;
* classificação de urgência;
* geração de alertas;
* monitoramento SLA.

---

## CDC Proposition Analytics

* histórico de tramitações;
* versionamento SCD Type 2;
* análise temporal;
* analytics de tramitações.

---

## Parliamentary Intelligence

Views analíticas avançadas incluem:

* perfil parlamentar;
* perfil partidário;
* índice de transparência;
* índice de eficiência;
* especialização temática;
* comportamento financeiro partidário.

---

# Stack Tecnológica

## Tecnologias Principais

* Databricks
* PySpark
* Spark SQL
* Delta Lake
* Python
* GitHub
* Delta Live Tables

---

# Estrutura do Repositório

```text
camara-data-pipeline/
│
├── assets/
│   └── images/
│
├── docs/
│   ├── notebooks_catalog.md
│   ├── challenge_matrix.md
│   ├── architecture_decisions.md
│   ├── streaming_architecture.md
│   ├── runbook.md
│   └── pdf/
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
└── README.md
```

---

# Documentação

Documentações adicionais disponíveis em:

```text
docs/
```

Principais documentos:

| Documento | Descrição |
|---|---|
| notebooks_catalog.md | Catálogo completo de notebooks |
| challenge_matrix.md | Matriz de aderência |
| architecture_decisions.md | Decisões arquiteturais |
| streaming_architecture.md | Arquitetura streaming e CDC |
| runbook.md | Procedimentos operacionais |

---

# Autor

Bruno Souza

Engenheiro de Dados focado em plataformas analíticas escaláveis, governança, modelagem dimensional e arquiteturas modernas Lakehouse.