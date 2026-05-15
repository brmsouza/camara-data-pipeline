# Matriz de Aderência ao Desafio Final — camara-data-pipeline

Documento consolidando o mapeamento entre os requisitos do desafio final Databricks e os produtos analíticos, pipelines, tabelas, views e componentes técnicos implementados no projeto `camara-data-pipeline`.

---

## Visão Geral da Matriz

Esta matriz consolida a rastreabilidade entre os requisitos do Desafio Final Databricks e os componentes efetivamente implementados no projeto `camara-data-pipeline`.

O objetivo é facilitar a validação técnica da entrega, apresentando em um único documento:

- os requisitos do desafio;
- os notebooks e pipelines relacionados;
- as tabelas dimensionais e fatos da camada Gold;
- as views analíticas implementadas;
- os componentes de streaming, CDC/SCD2, governança e observabilidade;
- a documentação técnica complementar.

Dessa forma, a matriz funciona como um guia de rastreabilidade da solução, permitindo localizar rapidamente onde cada requisito foi atendido dentro da arquitetura do projeto.

---

## Diferenciais Enterprise

Além dos requisitos centrais do desafio, o projeto implementa capacidades adicionais de nível enterprise voltadas para escalabilidade, manutenibilidade, governança e maturidade analítica.

As capacidades enterprise implementadas incluem:

- Arquitetura Medalhão completa
- Separação entre Silver Base e Silver Curated
- Governança orientada a metadata
- Dicionário corporativo da camada Gold
- Histórico CDC/SCD2
- Compatibilidade com Delta Time Travel
- Pipelines streaming com DLT/Lakeflow
- Dashboards de monitoramento SLA
- Arquitetura de replay e reprocessamento
- Runbooks operacionais
- Lineage e observabilidade ponta a ponta
- Validação de metadata e detecção de schema drift
- Produtos analíticos avançados de Parliamentary Intelligence
- Pipelines analíticos de detecção de anomalias
- Views analíticas enterprise e camada semântica

---

## Estratégia da Camada Silver

A camada Silver foi intencionalmente dividida em:

- Silver Base
- Silver Curated

Essa decisão arquitetural melhora:

- escalabilidade analítica;
- reutilização de entidades;
- manutenibilidade;
- clareza de lineage;
- separação entre responsabilidades técnicas e de negócio;
- padronização de governança;
- abstração de negócio;
- consistência analítica entre os produtos Gold.

### Responsabilidades da Silver Base

A camada Silver Base concentra:

- parsing;
- tipagem;
- padronização;
- deduplicação;
- validações técnicas;
- normalização bruta de entidades de negócio;
- preparação para CDC;
- enforcement de qualidade técnica.

### Responsabilidades da Silver Curated

A camada Silver Curated concentra:

- entidades reutilizáveis de negócio;
- enriquecimento analítico;
- fallback rules;
- padronização semântica;
- datasets prontos para analytics;
- integrações com fontes externas;
- preparação para modelagem dimensional Gold.

---

## 1. Atlas das Frentes Parlamentares

| Requisito | View / Produto Analítico | Referência Técnica | Status |
|---|---|---|---|
| Tabela Gold de frentes parlamentares | `gold.vw_frentes_analitica` | `03_gold/` | ATENDIDO |
| Diversidade partidária (HHI) | `gold.vw_frentes_diversidade_hhi` | `04_analytics/` | ATENDIDO |
| Deputados em múltiplas frentes | `gold.vw_deputados_multiplas_frentes` | `04_analytics/` | ATENDIDO |
| Sobreposição de membros entre frentes | `gold.vw_frentes_sobreposicao_membros` | `04_analytics/` | ATENDIDO |
| Evolução das frentes por legislatura | `gold.vw_frentes_evolucao_legislatura` | `04_analytics/` | ATENDIDO |

---

## 2. Calendário Analítico de Eventos Legislativos

| Requisito | View / Produto Analítico | Referência Técnica | Status |
|---|---|---|---|
| Tabela Gold de eventos com dimensões de órgão, tipo e data | `gold.vw_eventos_analitica` | `03_gold/` | ATENDIDO |
| Taxa de presença por deputado e tipo de evento | `gold.vw_presenca_eventos_deputado` | `04_analytics/` | ATENDIDO |
| Comparativo de frequência antes/depois de períodos eleitorais | `gold.vw_eventos_frequencia_eleitoral` | `04_analytics/` | PARCIAL |
| Densidade semanal de eventos | `gold.vw_eventos_densidade_semanal` | `04_analytics/` | ATENDIDO |
| Eventos futuros agendados | `gold.vw_eventos_futuros` | `04_analytics/` | ATENDIDO |

---

## 3. Correlação entre Frentes Parlamentares e Votações

| Requisito | View / Produto Analítico | Referência Técnica | Status |
|---|---|---|---|
| Análise de alinhamento entre deputados da mesma frente | `gold.vw_frentes_votacoes_alinhamento` | `04_analytics/` | ATENDIDO |
| Comparativo entre alinhamento de frente versus partido | `gold.vw_alinhamento_frente_vs_partido` | `04_analytics/` | ATENDIDO |
| Análise de fidelidade partidária | `gold.vw_fidelidade_partidaria` | `04_analytics/` | ATENDIDO |
| Análise de divergência partidária | `gold.vw_divergencia_partidaria` | `04_analytics/` | ATENDIDO |
| Base analítica consolidada de votações | `gold.vw_votacoes_analitica` | `03_gold/` | ATENDIDO |

---

## 4. Inteligência Analítica de Gastos CEAP

| Requisito | View / Produto Analítico | Referência Técnica | Status |
|---|---|---|---|
| Ingestão incremental de `/deputados/{id}/despesas` com paginação | `01_bronze/07_ingest_despesas.py` | `01_bronze/` | ATENDIDO |
| Ingestão alternativa baseada em arquivos | `01_bronze/07b_ingest_despesas_file.py` | `01_bronze/` | ATENDIDO |
| Tabela fato de despesas parlamentares | `gold.ft_despesas_ceap` | `03_gold/` | ATENDIDO |
| Dimensões de deputado, fornecedor, categoria e data | `gold.dm_deputado`, `gold.dm_fornecedor`, `gold.dm_tipo_despesa`, `gold.dm_data` | `03_gold/` | ATENDIDO |
| Score de anomalia utilizando z-score por categoria × estado | `gold.vw_anomalias_ceap_zscore` | `04_analytics/` | ATENDIDO |
| Ranking de fornecedores com flags de CNPJ suspeito | `gold.vw_despesas_ceap_analitica` | `04_analytics/` | ATENDIDO |
| Análise mensal/top gastos por partido | `gold.vw_partidos_analitica` | `04_analytics/` | ATENDIDO |

---

## 5. Pipeline de Auditoria de CPIs

| Requisito | Resposta do Projeto | Status |
|---|---|---|
| Tabela dedicada de timeline de CPIs | Evolução futura documentada | ROADMAP |
| Análise de relacionamento CPI × proposições | Arquitetura preparada para evolução futura | ROADMAP |
| Análise de duração de CPIs | Não implementado no escopo atual | ROADMAP |
| Cruzamento entre convocados e entidades privadas | Dependente de fontes externas adicionais | ROADMAP |
| Comparativo de produtividade de CPIs | Evolução futura documentada | ROADMAP |

---

## 6. Monitoramento de Presença e Absenteísmo Parlamentar

| Requisito | View / Produto Analítico | Referência Técnica | Status |
|---|---|---|---|
| Correlação entre eventos e votações | `gold.vw_score_engajamento_parlamentar` | `04_analytics/` | ATENDIDO |
| Score composto de engajamento | `gold.vw_score_engajamento_parlamentar` | `04_analytics/` | ATENDIDO (escopo limitado) |
| Detecção de padrão de absenteísmo | `gold.vw_absenteismo_parlamentar` | `04_analytics/` | ATENDIDO |
| Série temporal de engajamento | `gold.vw_engajamento_temporal` | `04_analytics/` | PARCIAL |
| Relatório mensal de engajamento parlamentar | `gold.vw_engajamento_parlamentar_mensal` | `04_analytics/` | PARCIAL |

---

## 7. Arquitetura de Dados e Engenharia

| Requisito | Implementação no Projeto | Status |
|---|---|---|
| Arquitetura Medalhão | Bronze, Silver Base, Silver Curated e Gold | ATENDIDO |
| Camada Bronze | Ingestão raw com payloads, metadata e replayabilidade | ATENDIDO |
| Camada Silver Base | Parsing, tipagem, padronização, deduplicação e qualidade técnica | ATENDIDO |
| Camada Silver Curated | Entidades curadas prontas para analytics | ATENDIDO |
| Camada Gold | Dimensões, fatos e views analíticas | ATENDIDO |
| Delta Lake | Persistência Delta em todas as camadas | ATENDIDO |
| PySpark | Transformações e processamento nativo em PySpark | ATENDIDO |
| Orquestração | Notebooks `run_*_pipeline` | ATENDIDO |
| Observabilidade | `monitoring.pipeline_log` | ATENDIDO |
| Reprocessamento | Notebooks administrativos e execução idempotente | ATENDIDO |
| Lineage | Colunas de metadata `bronze_*`, `silver_*`, `gold_*` | ATENDIDO |
| Data Quality | Validações Bronze, Silver e Gold | ATENDIDO |

---

## 8. Modelagem Dimensional Gold

### 8.1 Dimensões

| Dimensão | Finalidade Analítica | Status |
|---|---|---|
| `gold.dm_data` | Dimensão calendário para análises temporais | ATENDIDO |
| `gold.dm_legislatura` | Dimensão de legislaturas parlamentares | ATENDIDO |
| `gold.dm_partido` | Dimensão de partidos políticos | ATENDIDO |
| `gold.dm_deputado` | Dimensão conformada de deputados | ATENDIDO |
| `gold.dm_proposicao` | Dimensão de proposições legislativas | ATENDIDO |
| `gold.dm_orgao` | Dimensão de órgãos legislativos | ATENDIDO |
| `gold.dm_gabinete` | Dimensão de gabinetes parlamentares | ATENDIDO |
| `gold.dm_fornecedor` | Dimensão de fornecedores CEAP | ATENDIDO |
| `gold.dm_evento` | Dimensão de eventos legislativos | ATENDIDO |
| `gold.dm_frente` | Dimensão de frentes parlamentares | ATENDIDO |
| `gold.dm_uf` | Dimensão de unidades federativas | ATENDIDO |
| `gold.dm_tipo_despesa` | Dimensão de tipos de despesa CEAP | ATENDIDO |
| `gold.dm_bancada` | Dimensão de bancadas parlamentares e blocos | ATENDIDO |
| `gold.dm_responsavel_ceap` | Dimensão de responsáveis CEAP | ATENDIDO |

### 8.2 Tabelas Fato

| Tabela Fato | Finalidade Analítica | Status |
|---|---|---|
| `gold.ft_despesas_ceap` | Fato de despesas parlamentares CEAP | ATENDIDO |
| `gold.ft_votacoes` | Fato de sessões de votação | ATENDIDO |
| `gold.ft_votos` | Fato de votos individuais dos deputados | ATENDIDO |
| `gold.ft_orientacoes_bancada` | Fato de orientações de bancadas parlamentares | ATENDIDO |
| `gold.ft_atividade_parlamentar` | Fato de engajamento e atividade parlamentar | ATENDIDO |
| `gold.ft_presenca_eventos` | Fato de presença em eventos legislativos | ATENDIDO |
| `gold.ft_frentes_membros` | Fato de composição das frentes parlamentares | ATENDIDO |

---

## 9. Desafios Opcionais

### 9.1 Pipeline Streaming de Votações em Tempo Real

| Requisito | Implementação no Projeto | Status |
|---|---|---|
| Job micro-batch agendado consumindo `/votacoes` a cada 10 minutos | `99_ingest_votacoes_microbatch` / `04_run_votacoes_streaming_pipeline` | ATENDIDO |
| Controle de offset por identificador de votação | Gerenciamento de offset Bronze Stream | ATENDIDO |
| Pipeline streaming DLT Bronze → Silver → Gold | `01_dlt_votacoes_streaming` | ATENDIDO |
| Expectativas declarativas de qualidade | DLT / Lakeflow expectations | ATENDIDO |
| Dashboard de monitoramento SLA | Dashboard de latência, volume e taxa de erro | ATENDIDO |
| Runbook de incidentes e replay | Estratégia documentada de replay e reprocessamento | ATENDIDO |

### 9.2 CDC de Tramitações com SCD Type 2

| Requisito | Implementação no Projeto | Status |
|---|---|---|
| Ingestão incremental de tramitações | `14_ingest_proposicoes_tramitacoes_cdc` | ATENDIDO |
| Chaves hash de payload para CDC | `bronze_cdc.proposicoes_tramitacoes_raw` | ATENDIDO |
| Tabela Silver SCD Type 2 | `silver_cdc.proposicoes_tramitacoes_scd2` | ATENDIDO |
| Campos `valid_from`, `valid_to`, `is_current` | Implementados na camada SCD2 | ATENDIDO |
| Suporte à reconstrução histórica | Compatível com Delta Time Travel | PARCIAL |
| Alertas para avanço/arquivamento de proposições | Roadmap analítico | ROADMAP |

---

## 10. Governança, Catálogo e Metadata

| Requisito / Capacidade | Implementação no Projeto | Status |
|---|---|---|
| Dicionário corporativo da camada Gold | `docs/gold_layer_enterprise_data_dictionary.md` | ATENDIDO |
| Comentários em tabelas e colunas | `99_apply_gold_comments.py` | ATENDIDO |
| Validação de metadata | `99_validate_gold_metadata.py` | ATENDIDO |
| Detecção de schema drift | Validação entre schema físico e definições de metadata | ATENDIDO |
| Padronização de headers dos notebooks | Headers Markdown orientados por camada | ATENDIDO |
| Catálogo de notebooks | `docs/notebooks_catalog.md` | ATENDIDO |
| Documentação técnica | Diretório `docs/` | ATENDIDO |
| Runbooks operacionais | Documentação operacional e streaming | ATENDIDO |

---

## Referências Arquiteturais

### Arquitetura Enterprise

- [Arquitetura Lakehouse Parlamentar](assets/images/parliamentary_lakehouse_architecture.png)
- [Arquitetura da Plataforma de Dados da Câmara](assets/images/camara_data_platform_architecture.png)

### Arquitetura Dimensional e Analítica Gold

- [Arquitetura Gold de Parliamentary Intelligence](assets/images/parliamentary_intelligence_gold_architecture.png)

### Arquitetura Streaming

- [Arquitetura Microbatch de Streaming de Votações](assets/images/job_votacoes_streaming_microbatch.png)
- [Pipeline Streaming DLT de Votações](assets/images/dlt_votacoes_streaming.png)

### Governança e Observabilidade

- [Dashboard de Observabilidade do Pipeline Legislativo](assets/images/figure_1_legislative_pipeline_observability_dashboard.png)
- [Dashboard de Monitoramento de Volume Legislativo](assets/images/figure_2_legislative_volume_monitoring.png)

---

## Status Geral das Entregas

| Área | Status |
|---|---|
| Requisitos Principais do Desafio | CONCLUÍDO |
| Arquitetura Medalhão | CONCLUÍDO |
| Pipelines Bronze/Silver/Gold | CONCLUÍDO |
| Modelagem Dimensional Gold | CONCLUÍDO |
| Parliamentary Intelligence | CONCLUÍDO |
| Inteligência Analítica CEAP | CONCLUÍDO |
| Desafio Opcional de Streaming | CONCLUÍDO |
| Desafio Opcional CDC/SCD2 | CONCLUÍDO |
| Governança e Metadata | CONCLUÍDO |
| Framework de Data Quality | CONCLUÍDO |
| Estratégia de Replay e Reprocessamento | CONCLUÍDO |
| Observabilidade e Monitoramento | CONCLUÍDO |
| Pipeline de Auditoria de CPIs | ROADMAP |

---

## Destaques Arquiteturais Enterprise

A arquitetura do projeto foi construída seguindo princípios modernos de Engenharia de Dados enterprise utilizando capacidades do Databricks Lakehouse.

Os principais destaques arquiteturais incluem:

- Arquitetura Medalhão escalável;
- Persistência Delta em todas as camadas;
- Pipelines replayable e idempotentes;
- Histórico CDC/SCD2;
- Arquitetura preparada para streaming;
- Pipelines declarativos DLT/Lakeflow;
- Governança orientada a metadata;
- Modelagem dimensional Gold enterprise;
- Observabilidade operacional;
- Camada semântica analítica;
- Produtos analíticos de Parliamentary Intelligence.

---

## Conclusão Executiva

O projeto `camara-data-pipeline` é uma plataforma analítica parlamentar construída sobre arquitetura moderna Databricks Lakehouse, desenvolvida para ingestão, processamento, governança e análise de dados da Câmara dos Deputados.

O projeto contempla:

- Arquitetura Medalhão escalável;
- Modelagem dimensional analítica;
- Historização CDC/SCD2;
- Pipelines streaming;
- Governança orientada a metadata;
- Estratégias de replay e reprocessamento;
- Observabilidade operacional;
- Validação de qualidade orientada a metadata;
- Produtos analíticos de Parliamentary Intelligence.

A implementação mantém foco em:

- manutenibilidade;
- replayabilidade;
- escalabilidade analítica;
- resiliência operacional;
- governança;
- documentação técnica;
- lineage ponta a ponta;
- consistência semântica.
