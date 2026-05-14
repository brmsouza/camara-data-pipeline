# Matriz do Desafio

🇺🇸 Documento técnico — Matriz de Aderência ao Desafio

---

# Visão Geral

Este documento mapeia a implementação do projeto `camara-data-pipeline` em relação aos principais requisitos do desafio e aos requisitos avançados opcionais.

O objetivo é fornecer uma visão clara e defensável de como cada item do desafio é atendido através da arquitetura Lakehouse implementada, notebooks, camadas analíticas, padrões de governança e documentação.

---

# Resumo

| Área do Desafio | Status |
|---|---|
| Arquitetura Medallion | Implementado |
| Ingestão Bronze | Implementado |
| Padronização Silver Base | Implementado |
| Entidades Silver Curated | Implementado |
| Modelo dimensional Gold | Implementado |
| Analytics CEAP | Implementado |
| Analytics de frentes parlamentares | Implementado |
| Analytics de eventos legislativos | Implementado |
| Analytics de votações | Implementado |
| Governança e lineage | Implementado |
| Replay e resiliência | Implementado |
| CDC / SCD Type 2 | Parcialmente Implementado |
| Streaming micro-batch | Implementado |
| Delta Live Tables | Implementado |
| Monitoramento SLA | Implementado |
| Analytics ciclo CPI | Roadmap |

---

# Requisitos Principais de Arquitetura

| Requisito | Implementação no Projeto | Status |
|---|---|---|
| Camadas Bronze, Silver e Gold | Implementadas através de `01_bronze`, `02_silver`, `03_gold` e `04_analytics` | Implementado |
| Ingestão de dados via APIs públicas | Notebooks de ingestão da API Dados Abertos Câmara | Implementado |
| Controles de qualidade de dados | validações explícitas, registros rejeitados e `records_discarded` | Implementado |
| Rastreamento de lineage | Metadados Bronze, batch_id e hash de registros | Implementado |
| Suporte a reprocessamento | Bronze replayável e reconstrução em camadas | Implementado |
| Modelagem dimensional | Dimensões e fatos Gold | Implementado |

---

# Fontes de Dados

| Dataset | Endpoint / Fonte | Status |
|---|---|---|
| Deputados | `/deputados` | Implementado |
| Detalhes de deputados | `/deputados/{id}` | Implementado |
| Frentes parlamentares | `/frentes` | Implementado |
| Membros de frentes | `/frentes/{id}/membros` | Implementado |
| Eventos | `/eventos` | Implementado |
| Proposições | `/proposicoes` | Implementado |
| Tramitações de proposições | `/proposicoes/{id}/tramitacoes` | Implementado |
| Despesas | `/deputados/{id}/despesas` | Implementado |
| Órgãos | `/orgaos` | Implementado |
| Sessões de votação | `/votacoes` | Implementado |
| Votos | `/votacoes/{id}/votos` | Implementado |
| Legislaturas | `/legislaturas` | Implementado |
| Datasets públicos de CNPJ | Enriquecimento de fornecedores | Implementado |

---

# Requisitos Analíticos

## Analytics CEAP

| Requisito | Implementação | Status |
|---|---|---|
| Análise de despesas parlamentares | `ft_despesas_ceap` e views analíticas CEAP | Implementado |
| Análise de fornecedores | `dm_fornecedor` e enriquecimento de fornecedores | Implementado |
| Análise por categoria de despesa | Camada analítica Gold | Implementado |
| Detecção de anomalias | Classificação z-score | Implementado |

---

## Frentes Parlamentares

| Requisito | Implementação | Status |
|---|---|---|
| Analytics de membros de frentes | `ft_frentes_membros` | Implementado |
| Diversidade partidária | views analíticas e análise de concentração | Implementado |
| Sobreposição de participação | analytics de frentes | Implementado |

---

## Eventos Legislativos

| Requisito | Implementação | Status |
|---|---|---|
| Analytics de calendário de eventos | `dm_evento` e fatos de eventos | Implementado |
| Análise de participação | `ft_presenca_eventos` | Implementado |
| Análise de densidade semanal | views analíticas | Implementado |

---

## Analytics de Votação

| Requisito | Implementação | Status |
|---|---|---|
| Comportamento de votação | `ft_votos` | Implementado |
| Alinhamento partidário | views analíticas de votação | Implementado |
| Análise de orientações | `ft_orientacoes_bancada` | Implementado |
| Divergência de votação | lógica analítica | Implementado |

---

# Requisitos Avançados Opcionais

## CDC / SCD Type 2

| Requisito | Implementação | Status |
|---|---|---|
| Ingestão incremental CDC | ingestão CDC de tramitações | Implementado |
| Comparação de hash de payload | `cdc_payload_hash` | Implementado |
| Campos SCD2 | `valid_from`, `valid_to`, `is_current` | Implementado |
| Maturidade histórica completa | depende de execução recorrente e retenção | Parcialmente Implementado |

---

## Alertas Streaming de Votações

| Requisito | Implementação | Status |
|---|---|---|
| Micro-batch agendado | `05_run_votacoes_streaming_pipeline.py` | Implementado |
| Ingestão incremental `/votacoes` | notebook Bronze micro-batch | Implementado |
| Controle de offset | `control.votacoes_stream_offset` | Implementado |
| DLT Bronze → Silver → Gold | pipeline `05_dlt` | Implementado |
| Expectations DLT | regras declarativas de validação | Implementado |
| Alertas Gold | `gold_stream_votacoes_alertas` | Implementado |
| Dashboard SLA | `monitoring.vw_sla_votacoes_streaming` | Implementado |
| Estratégia de replay | baseada em offset e payload bruto | Parcialmente Implementado |

---

# Requisitos de Governança

| Requisito | Implementação | Status |
|---|---|---|
| Logging | `monitoring.pipeline_log` | Implementado |
| records_read | métricas de pipeline | Implementado |
| records_written | métricas de pipeline | Implementado |
| records_discarded | métricas de pipeline | Implementado |
| registros rejeitados | estratégia de qualidade Silver | Implementado |
| suporte a replay | reconstrução Bronze e Delta | Implementado |
| documentação operacional | runbook e docs | Implementado |

---

# Requisitos de Documentação

| Entregável | Implementação | Status |
|---|---|---|
| README | `README.md` | Implementado |
| README em português | `README.pt-BR.md` | Implementado |
| Catálogo de notebooks | `docs/notebooks_catalog.md` | Implementado |
| Arquitetura streaming | `docs/streaming_architecture.md` | Implementado |
| Documentação de governança | `docs/governance_and_lineage.md` | Implementado |
| Estratégia de replay | `docs/replay_strategy.md` | Implementado |
| Inteligência parlamentar | `docs/parliamentary_intelligence.md` | Implementado |
| Runbook | `docs/runbook.md` | Implementado |

---

# Itens de Roadmap

| Item | Motivo |
|---|---|
| Analytics completos do ciclo CPI | documentado como evolução analítica futura |
| Integrações externas de alertas | não requerido no escopo atual |
| Analytics preditivos | evolução analítica futura |
| NLP / speech analytics | enriquecimento futuro |

---

# Conclusão

O projeto `camara-data-pipeline` atende fortemente aos requisitos principais do desafio e implementa diversas capacidades avançadas opcionais de engenharia.

A solução demonstra uma arquitetura moderna de Engenharia de Dados utilizando camadas Medallion, Delta Lake, PySpark, modelagem dimensional, governança, replayabilidade, streaming micro-batch, DLT e analytics de inteligência parlamentar.