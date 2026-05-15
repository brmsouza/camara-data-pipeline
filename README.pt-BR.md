# Camara Data Pipeline — Plataforma Lakehouse Parlamentar

Plataforma Lakehouse de Engenharia de Dados em padrão enterprise totalmente implementada de forma nativa no Databricks para analytics parlamentares em larga escala utilizando PySpark, Delta Lake, Delta Live Tables (DLT), ingestão streaming em micro-batch, processamento CDC/SCD Type 2, APIs REST multi-endpoint e arquitetura Medallion.

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

🇺🇸 English version: [README.md](README.md)

---

# Finalidade Educacional

Este projeto foi desenvolvido para fins educacionais, estudo técnico e portfólio profissional.
O repositório demonstra conceitos modernos de Engenharia de Dados utilizando dados parlamentares públicos e padrões de arquitetura Lakehouse enterprise.
Não existe qualquer vínculo político, governamental ou institucional com a Câmara dos Deputados.
Todos os indicadores analíticos e camadas de inteligência presentes neste projeto possuem finalidade exclusivamente técnica, educacional e experimental.

---

# Por Que Este Projeto É Diferente

Diferente de projetos tradicionais de portfólio focados apenas em ETL, esta solução implementa:

* ingestão distribuída de múltiplos endpoints REST;
* arquitetura Lakehouse Medallion replayável;
* rastreabilidade determinística e controle de batch;
* historização CDC / SCD Type 2;
* ingestão streaming em micro-batch;
* Delta Live Tables (DLT);
* monitoramento de SLA e observabilidade operacional;
* enriquecimento de fornecedores utilizando datasets públicos de CNPJ;
* marts analíticos de inteligência parlamentar;
* detecção de anomalias e analytics comportamentais;
* estratégia de replay e recuperação orientada à governança;
* documentação técnica em padrão enterprise.

O projeto foi desenhado para simular padrões reais de Engenharia de Dados corporativa e não apenas pipelines ETL isolados.

---

# Destaques de Engenharia

| Capacidade | Implementação |
|---|---|
| Ingestão REST multi-endpoint | API Dados Abertos Câmara |
| Streaming micro-batch | Ingestão de votações parlamentares |
| CDC / SCD Type 2 | Historização de tramitações |
| Delta Live Tables | Pipeline streaming Bronze → Silver → Gold |
| Replayabilidade | Ingestão Bronze replayável |
| Governança | Controle de batch e hashes determinísticos |
| SLA Monitoring | Métricas operacionais streaming |
| Enriquecimento de fornecedores | Datasets públicos de CNPJ |
| Inteligência parlamentar | Marts analíticos Gold |
| Detecção de anomalias | Análise z-score |
| Orquestração | Databricks Workflows |
| Observabilidade | Tabelas de monitoramento e logs |

---

# Tecnologias Utilizadas

| Categoria | Tecnologia |
|---|---|
| Plataforma | Databricks Free Edition |
| Linguagens | PySpark (Python) e Spark SQL |
| Engine de Processamento | Apache Spark |
| Armazenamento | Delta Lake |
| Arquitetura | Medallion Lakehouse Architecture |
| Streaming | Delta Live Tables (DLT) e Micro-batch |
| APIs | APIs REST |
| Versionamento | GitHub |
| Modelagem | Star Schema / Modelagem Dimensional |
| Observabilidade | Tabelas operacionais de monitoramento |
| Governança | Arquitetura replayável com lineage |

---

# Principais Fontes de Dados

## API Dados Abertos Câmara dos Deputados

Documentação oficial:

https://dadosabertos.camara.leg.br/swagger/api.html

---

## Principais Endpoints Consumidos

| Domínio | Endpoint |
|---|---|
| Deputados | `/deputados` |
| Detalhes de deputados | `/deputados/{id}` |
| Despesas parlamentares | `/deputados/{id}/despesas` |
| Frentes parlamentares | `/frentes` |
| Membros de frentes | `/frentes/{id}/membros` |
| Eventos legislativos | `/eventos` |
| Proposições | `/proposicoes` |
| Tramitações | `/proposicoes/{id}/tramitacoes` |
| Órgãos | `/orgaos` |
| Membros de órgãos | `/orgaos/{id}/membros` |
| Votações | `/votacoes` |
| Votos | `/votacoes/{id}/votos` |
| Orientações partidárias | `/votacoes/{id}/orientacoes` |
| Legislaturas | `/legislaturas` |

---

## Fontes Externas de Enriquecimento

O projeto também integra datasets públicos externos para enriquecimento analítico.

### Enriquecimento de Fornecedores

Datasets públicos brasileiros de CNPJ são utilizados para:

* validação de fornecedores;
* classificação CPF/CNPJ;
* identificação de fornecedores ativos/inativos;
* apoio à detecção de anomalias;
* melhoria dos analytics financeiros CEAP.

Essa camada simula integrações reais de master data utilizadas em ambientes corporativos.

---

# Visão Geral

`camara-data-pipeline` é uma plataforma moderna de Engenharia de Dados Lakehouse desenvolvida para ingerir, validar, curar e modelar analiticamente dados parlamentares públicos do ecossistema da Câmara dos Deputados.

A arquitetura segue uma estratégia Medallion com refinamento progressivo entre:

* Bronze;
* Silver Base;
* Silver Curated;
* Gold;
* Analytics.

A plataforma combina:

* ingestão escalável via APIs;
* pipelines replayáveis;
* governança e lineage;
* modelagem dimensional;
* ingestão streaming;
* CDC / SCD Type 2;
* Delta Live Tables;
* observabilidade operacional;
* analytics de inteligência parlamentar.

---

# Arquitetura

A plataforma segue uma arquitetura Lakehouse em camadas com refinamento progressivo e replayabilidade.

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

## Princípios Arquiteturais

A arquitetura foi construída sobre os seguintes princípios:

* ingestão orientada a replay;
* preservação de dados brutos;
* processamento determinístico;
* validações explícitas;
* escalabilidade analítica;
* lineage orientado à governança;
* observabilidade operacional;
* pipelines modulares.

---

## Diagrama da Arquitetura

![Architecture](assets/images/camadamedalhao_camaradeputados.png)

---

# Streaming, CDC e DLT

O projeto também implementa capacidades modernas avançadas de Engenharia de Dados.

## Componentes Implementados

* ingestão streaming em micro-batch;
* Delta Live Tables (DLT);
* CDC / SCD Type 2;
* historização de tramitações;
* orquestração via workflows;
* monitoramento de SLA;
* estratégia de replay e recuperação;
* observabilidade operacional;
* lineage streaming.

---

## Orquestração

![Workflow](assets/images/job_camara_medallion_pipeline.png)

---

## Streaming Micro-Batch

![Streaming](assets/images/job_votacoes_streaming_microbatch.png)

---

## Delta Live Tables

![DLT](assets/images/dlt_votacoes_streaming.png)

---

Documentação detalhada disponível em:


[streaming_architecture.pt-BR.md](docs/streaming_architecture.pt-BR.md) 


---

# Analytics de Inteligência Parlamentar

As camadas Gold e Analytics implementam marts analíticos avançados de inteligência parlamentar.

## Principais Domínios Analíticos

* analytics de despesas parlamentares;
* analytics de votações;
* indicadores de transparência;
* indicadores de eficiência parlamentar;
* inteligência de fornecedores;
* analytics de alinhamento político;
* score de engajamento parlamentar;
* detecção de anomalias;
* analytics de frentes parlamentares;
* dashboards analíticos partidários.

---

## Modelo Dimensional Gold

![Gold Model](assets/images/modelo_camaradeputados.png)

---

## Principais Capacidades Analíticas

| Capacidade | Descrição |
|---|---|
| Analytics CEAP | Análise de despesas parlamentares |
| Inteligência de fornecedores | Enriquecimento e validação |
| Índice de transparência | Indicadores analíticos parlamentares |
| Índice de eficiência | Eficiência parlamentar |
| Analytics de votação | Comportamento e alinhamento político |
| Inteligência partidária | Dashboards analíticos |
| Analytics de frentes | Análise de concentração parlamentar |
| Score de engajamento | Métricas de participação |
| Analytics Z-Score | Detecção de anomalias financeiras |

---

Documentação analítica detalhada disponível em:


[parliamentary_intelligence.pt-BR.md](docs/parliamentary_intelligence.pt-BR.md)

[gold_layer_enterprise_data_dictionary.pt-BR.md](docs/gold_layer_enterprise_data_dictionary.pt-BR.md)

---

# Governança, Replay e Observabilidade

A arquitetura preserva governança, lineage e replayabilidade em todas as camadas.

## Principais Conceitos Implementados

* ingestão Bronze replayável;
* lineage por batch;
* hashes determinísticos;
* monitoramento operacional;
* tratamento de registros rejeitados;
* replay e recuperação;
* historização CDC;
* controle de offset streaming;
* monitoramento de SLA;
* logging operacional.

---

## Diagrama de Governança

![Governance](assets/images/pilares_analiticos.png)

---

Documentação detalhada disponível em:

[governance_and_lineage.pt-BR.md](docs/governance_and_lineage.pt-BR.md) 

[replay_strategy.pt-BR.md](docs/replay_strategy.pt-BR.md)

[runbook.pt-BR.md](docs/runbook.pt-BR.md)


---

# Estrutura do Repositório

```text
camara-data-pipeline/
│
├── README.pt-BRmd
│
├── docs/
│   ├── index.pt-BR.md
│   ├── streaming_architecture.pt-BR.md
│   ├── governance_and_lineage.pt-BR.md
│   ├── replay_strategy.pt-BR.md
│   ├── gold_layer_enterprise_data_dictionary.pt-BR.md
│   ├── parliamentary_intelligence.pt-BR.md
│   ├── notebooks_catalog.pt-BR.md
│   ├── architecture_decisions.pt-BR.md
│   ├── challenge_matrix.pt-BR.md
│   ├── analytical_data_products.pt-BR.md
│   ├── final_challenge_adherence_matrix.pt-BR.md
│   └── runbook.pt-BR.md
│
├── assets/
│   └── images/
│
├── data/
│   └── parliamentary_intelligence/
│       ├── ceap/
│       ├── frentes/
│       ├── eventos/
│       ├── votacoes/
│       ├── engajamento/
│       ├── partidos/
│       ├── cdc/
│       └── streaming/
│
├── notebooks/
│   ├── 00_setup/
│   ├── 01_bronze/
│   ├── 02_silver/
│   ├── 03_gold/
│   ├── 04_analytics/
│   ├── 05_dlt/
│   ├── 90_common/
│   ├── 93_admin/
│   └── 99_jobs/
│
└── requirements.txt
```
## Produtos Analíticos de Dados

O projeto possui uma estrutura dedicada em `data/` contendo exports analíticos
em CSV gerados a partir das views Gold e datasets de Parliamentary Intelligence.

Esses exports fornecem evidências analíticas reproduzíveis para:
- análises CEAP
- análises de frentes parlamentares
- análises de eventos legislativos
- inteligência de votações
- indicadores de engajamento parlamentar
- inteligência partidária
- histórico analítico CDC/SCD2
- monitoramento de SLA streaming

A documentação detalhada dos exports analíticos está disponível em:

[analytical_data_products.pt-BR.md](docs/analytical_data_products.pt-BR.md)

### Estrutura de Dados

```text
data/parliamentary_intelligence/
├── ceap/
├── frentes/
├── eventos/
├── votacoes/
├── engajamento/
├── partidos/
├── cdc/
└── streaming/
---

# Documentação

A documentação técnica detalhada está disponível em:

```text
docs/
```

| Documento | Descrição |
|---|---|
| [streaming_architecture.pt-BR.md](docs/streaming_architecture.pt-BR.md) | Streaming, CDC, DLT e SLA |
| [governance_and_lineage.pt-BR.md](docs/governance_and_lineage.pt-BR.md) | Governança, lineage e observabilidade |
| [replay_strategy.pt-BR.md](docs/replay_strategy.pt-BR.md) | Estratégia de replay e recuperação |
| [gold_layer_enterprise_data_dictionary.pt-BR.md](docs/gold_layer_enterprise_data_dictionary.pt-BR.md) | Dicionário de dados dimensionais da camada Enterprise Gold |
| [parliamentary_intelligence.pt-BR.md](docs/parliamentary_intelligence.pt-BR.md) | Analytics e inteligência parlamentar |
| [architecture_decisions.pt-BR.md](docs/architecture_decisions.pt-BR.md) | Decisões arquiteturais e modelagem |
| [challenge_matrix.pt-BR.md](docs/challenge_matrix.pt-BR.md) | Matriz de aderência ao desafio |
| [runbook.pt-BR.md](docs/runbook.pt-BR.md) | Procedimentos operacionais |
| [notebooks_catalog.pt-BR.md](docs/notebooks_catalog.pt-BR.md) | Catálogo de notebooks |

---

## Padrões de Construção de Notebooks

O projeto adota um modelo padronizado de construção de notebooks definindo:
- estrutura dos notebooks
- organização das células
- estrutura de logging
- registro de linhagem
- fluxo de validação
- fluxo de persistência Delta
- padrão de métricas operacionais
- tratamento de registros rejeitados
- estrutura de notebooks CDC/SCD2
- estrutura de notebooks streaming

A documentação detalhada de construção dos notebooks está disponível em:

- [notebook_engineering_standards.pt-BR.md](docs/notebook_engineering_standards.pt-BR.md)

---
# Objetivos de Engenharia

O projeto foi desenvolvido para demonstrar:

* arquitetura enterprise de Engenharia de Dados;
* processamento Lakehouse escalável;
* replayabilidade e resiliência;
* padrões CDC e streaming;
* modelagem dimensional;
* observabilidade operacional;
* governança analítica;
* workflows modernos em Databricks.

---
## Matriz de Aderência ao Desafio Final

O projeto inclui uma matriz completa de aderência mapeando os requisitos do
desafio final Databricks para os pipelines implementados, produtos analíticos,
modelagem dimensional Gold, arquitetura streaming e capacidades de
Parliamentary Intelligence.

### Documentos

- [Matriz de Aderência ao Desafio Final](docs/final_challenge_adherence_matrix.pt-BR.md)

### Áreas Cobertas

- Arquitetura Medalhão (Bronze / Silver / Gold)
- Modelagem dimensional Gold Star Schema
- Analytics de despesas parlamentares CEAP
- Analytics de frentes parlamentares
- Inteligência de votações e alinhamento partidário
- Analytics de eventos legislativos
- Analytics de engajamento parlamentar
- Historificação CDC / SCD Type 2
- Pipelines streaming com DLT / Lakeflow
- Monitoramento de SLA e observabilidade
- Governança e validação de metadata
- Produtos analíticos de Parliamentary Intelligence
---


# Autor

Bruno Souza

Engenheiro de Dados com foco em plataformas analíticas escaláveis, governança, arquitetura Lakehouse, modelagem dimensional e práticas modernas de Engenharia de Dados.