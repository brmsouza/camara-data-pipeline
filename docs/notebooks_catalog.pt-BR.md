# Catálogo de Notebooks

🇺🇸 Documento técnico — Catálogo Completo de Notebooks, Pipelines e Módulos Reutilizáveis

---

# Visão Geral

Este documento descreve o catálogo técnico completo do repositório `camara-data-pipeline`.

O objetivo é documentar a responsabilidade, localização, camada arquitetural, entradas, saídas e papel operacional de cada notebook, pipeline e módulo reutilizável do projeto.

O catálogo segue a arquitetura Medallion implementada no projeto:

```text
Bronze → Silver Base → Silver Curated → Gold → Analytics
```

Além dos workloads avançados:

* CDC / SCD Type 2;
* streaming micro-batch;
* Delta Live Tables;
* observabilidade operacional;
* monitoramento SLA;
* workflows Databricks.

---

# Fluxo Arquitetural

```text
00_setup
   ↓
01_bronze
   ↓
02_silver/01_base
   ↓
02_silver/02_curated
   ↓
03_gold
   ↓
04_analytics
```

Workloads avançados:

```text
00_setup + 01_bronze + 02_silver + 04_analytics + 05_dlt + 99_jobs
```

---

# Resumo por Camada

| Camada | Responsabilidade |
|---|---|
| `00_setup` | Criação de schemas, tabelas de controle, estruturas CDC/SCD2, streaming e monitoramento |
| `01_bronze` | Ingestão bruta replayável com lineage e preservação de payload |
| `02_silver/01_base` | Parsing técnico, padronização, validações e deduplicação |
| `02_silver/02_curated` | Entidades enriquecidas e prontas para analytics |
| `03_gold` | Modelagem dimensional Star Schema |
| `04_analytics` | Views analíticas, marts, SLA e inteligência parlamentar |
| `05_dlt` | Streaming declarativo via Delta Live Tables |
| `90_common` | Componentes reutilizáveis e utilitários |
| `99_jobs` | Orquestração via Databricks Workflows |

---

# Estrutura Geral de Diretórios

```text
notebooks/
├── 00_setup/
├── 01_bronze/
├── 02_silver/
│   ├── 01_base/
│   └── 02_curated/
├── 03_gold/
├── 04_analytics/
├── 05_dlt/
├── 90_common/
└── 99_jobs/
```

---

# Camada `00_setup`

Camada responsável pela preparação do ambiente.

## Principais responsabilidades

* criação de schemas;
* criação de tabelas de controle;
* estruturas CDC/SCD2;
* objetos streaming;
* monitoramento operacional;
* inicialização do ambiente.

## Principais notebooks

| Notebook | Objetivo |
|---|---|
| `00_create_schemas.py` | Criação de schemas |
| `01_create_control_tables.py` | Tabelas de controle operacional |
| `03_create_streaming_objects.py` | Objetos streaming |
| `04_create_cdc_scd2_objects.py` | Estruturas CDC/SCD2 |
| `90_admin_test_api_connection.py` | Teste de conectividade |
| `91_admin_reset_environment.py` | Reset operacional |
| `92_admin_check_quality_tables.py` | Validações administrativas |

---

# Camada `01_bronze`

Camada de ingestão bruta replayável.

## Principais responsabilidades

* ingestão via APIs REST;
* ingestão via arquivos CSV;
* preservação de payload bruto;
* geração de lineage;
* geração de hash determinístico;
* replayabilidade;
* logging operacional.

---

# APIs Utilizadas

O projeto utiliza principalmente a API Dados Abertos da Câmara dos Deputados.

## Principais endpoints

| Endpoint | Objetivo |
|---|---|
| `/deputados` | Deputados |
| `/proposicoes` | Proposições |
| `/votacoes` | Sessões de votação |
| `/eventos` | Eventos legislativos |
| `/orgaos` | Órgãos legislativos |
| `/frentes` | Frentes parlamentares |
| `/legislaturas` | Legislaturas |

---

# Principais Notebooks Bronze

| Notebook | Saída |
|---|---|
| `01_ingest_deputados.py` | `bronze.deputados` |
| `06_ingest_proposicoes.py` | `bronze.proposicoes` |
| `07_ingest_despesas.py` | `bronze.despesas` |
| `10_ingest_votacoes.py` | `bronze.votacoes` |
| `12_ingest_votacoes_votos.py` | `bronze.votacoes_votos` |
| `13_ingest_legislaturas.py` | `bronze.legislaturas` |

---

# Bronze CDC

O projeto implementa ingestão CDC para tramitações parlamentares.

## Notebook CDC

```text
14_ingest_proposicoes_tramitacoes_cdc.py
```

## Saída

```text
bronze_cdc.proposicoes_tramitacoes_raw
```

---

# Bronze Streaming

O projeto implementa ingestão streaming micro-batch.

## Notebook Streaming

```text
99_ingest_votacoes_microbatch.py
```

## Saída

```text
bronze_stream.votacoes_raw
```

---

# Camada `02_silver/01_base`

Camada de padronização técnica.

## Principais responsabilidades

* parsing;
* casting;
* normalização;
* deduplicação;
* validações técnicas;
* tipagem;
* validação CPF/CNPJ;
* validação de datas;
* tratamento de inconsistências.

---

# Principais Entidades Silver Base

| Entidade | Tabela |
|---|---|
| Deputados | `silver_base.deputados` |
| Proposições | `silver_base.proposicoes` |
| Despesas | `silver_base.despesas` |
| Votações | `silver_base.votacoes` |
| Eventos | `silver_base.eventos` |
| Órgãos | `silver_base.orgaos` |

---

# Validação de CNPJ

O projeto implementa enriquecimento e validação de fornecedores via API pública de CNPJ.

## Principais objetivos

* validação cadastral;
* detecção de inconsistências;
* enriquecimento analítico;
* classificação de risco;
* suporte à transparência parlamentar.

## Notebook relacionado

```text
14_base_fornecedores.py
```

---

# Camada `02_silver/02_curated`

Camada de entidades enriquecidas e analytics-ready.

## Principais responsabilidades

* enriquecimento de negócio;
* padronização analítica;
* fallback de atributos;
* classificação analítica;
* flags de negócio;
* entidades reutilizáveis.

---

# Principais Entidades Curated

| Entidade | Tabela |
|---|---|
| Deputados | `silver_curated.deputados` |
| Despesas | `silver_curated.despesas` |
| Votações | `silver_curated.votacoes` |
| Fornecedores | `silver_curated.fornecedores` |
| Legislaturas | `silver_curated.legislaturas` |

---

# CDC / SCD Type 2

O projeto implementa historização de tramitações parlamentares.

## Fluxo CDC

```text
Bronze CDC
    ↓
Silver CDC Base
    ↓
Silver CDC SCD2
```

## Principais notebooks

| Notebook | Objetivo |
|---|---|
| `14_ingest_proposicoes_tramitacoes_cdc.py` | CDC Bronze |
| `15_base_proposicoes_tramitacoes_cdc.py` | CDC Silver Base |
| `15_curated_proposicoes_tramitacoes_scd.py` | SCD Type 2 |

---

# Camada `03_gold`

Camada dimensional Star Schema.

## Principais responsabilidades

* dimensões conformadas;
* tabelas fato;
* surrogate keys;
* modelagem dimensional;
* marts reutilizáveis;
* analytics escaláveis.

---

# Principais Dimensões

| Dimensão | Tabela |
|---|---|
| Deputado | `gold.dm_deputado` |
| Partido | `gold.dm_partido` |
| Legislatura | `gold.dm_legislatura` |
| Proposição | `gold.dm_proposicao` |
| Fornecedor | `gold.dm_fornecedor` |
| Evento | `gold.dm_evento` |

---

# Principais Tabelas Fato

| Fato | Tabela |
|---|---|
| Despesas CEAP | `gold.ft_despesas_ceap` |
| Votações | `gold.ft_votacoes` |
| Votos | `gold.ft_votos` |
| Orientações | `gold.ft_orientacoes_bancada` |
| Atividade Parlamentar | `gold.ft_atividade_parlamentar` |

---

# Camada `04_analytics`

Camada responsável pelos produtos analíticos finais.

## Principais responsabilidades

* dashboards;
* views analíticas;
* inteligência parlamentar;
* monitoramento SLA;
* validações analíticas;
* indicadores políticos;
* analytics de transparência.

---

# Principais Notebooks Analytics

| Notebook | Objetivo |
|---|---|
| `01_build_gold_ceap_analytics.py` | Analytics CEAP |
| `04_build_gold_votacoes_analytics.py` | Analytics de votações |
| `05_build_gold_engajamento_analytics.py` | Analytics de engajamento |
| `06_build_gold_parliamentary_intelligence.py` | Inteligência parlamentar |
| `07_build_gold_sla_votacoes_streaming.py` | SLA streaming |
| `08_build_gold_proposicoes_cdc_analytics.py` | Analytics CDC |

---

# Camada `05_dlt`

Camada Delta Live Tables.

## Objetivo

Implementar pipeline streaming declarativo com validações automáticas.

## Notebook principal

```text
01_dlt_votacoes_streaming.py
```

## Principais capacidades

* expectations declarativas;
* pipelines streaming;
* validações automáticas;
* alertas Gold;
* monitoramento operacional.

---

# Camada `90_common`

Módulos reutilizáveis compartilhados entre notebooks.

## Principais módulos

| Módulo | Objetivo |
|---|---|
| `api_client.py` | Cliente reutilizável API |
| `bronze_writer.py` | Escrita Bronze |
| `cnpj_utils.py` | Utilidades CPF/CNPJ |
| `config.py` | Configuração global |
| `logger.py` | Logging padronizado |
| `pagination.py` | Paginação API |
| `table_logger.py` | Logging Delta |

---

# Camada `99_jobs`

Orquestração via Databricks Workflows.

## Principais responsabilidades

* execução por camada;
* controle de dependência;
* monitoramento operacional;
* replay controlado;
* agendamento.

---

# Principais Workflows

| Workflow | Objetivo |
|---|---|
| `01_run_bronze_pipeline.py` | Pipeline Bronze |
| `02_run_silver_base_pipeline.py` | Pipeline Silver Base |
| `03_run_silver_curated_pipeline.py` | Pipeline Silver Curated |
| `04_run_gold_pipeline.py` | Pipeline Gold |
| `05_run_votacoes_streaming_pipeline.py` | Pipeline Streaming |

---

# Observabilidade Operacional

O projeto implementa monitoramento operacional completo.

## Principais métricas

* records_read;
* records_written;
* records_discarded;
* execution_duration;
* SLA;
* batch lineage;
* replay tracking.

---

# Principais Características Técnicas

## Engenharia de Dados

* arquitetura Medallion;
* Delta Lake;
* Star Schema;
* CDC/SCD2;
* streaming micro-batch;
* replayabilidade;
* lineage;
* observabilidade.

---

# Tecnologias Utilizadas

| Tecnologia | Objetivo |
|---|---|
| Databricks | Plataforma Lakehouse |
| PySpark | Processamento distribuído |
| Spark SQL | Transformações SQL |
| Delta Lake | Armazenamento ACID |
| Delta Live Tables | Streaming declarativo |
| REST APIs | Ingestão de dados |
| Unity Catalog | Governança |

---

# Conclusão

O catálogo de notebooks do projeto `camara-data-pipeline` demonstra uma arquitetura moderna de Engenharia de Dados construída sobre princípios Lakehouse, processamento distribuído, modelagem dimensional, replayabilidade e observabilidade operacional.

A estrutura foi projetada para suportar pipelines batch e streaming, analytics parlamentares, governança de dados e workloads orientados a transparência pública.