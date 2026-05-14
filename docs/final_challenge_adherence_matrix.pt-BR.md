# Matriz de Aderência ao Desafio Final — camara-data-pipeline

Documento consolidando o mapeamento entre os requisitos do desafio final Databricks e os produtos analíticos, pipelines, tabelas, views e componentes técnicos implementados no projeto `camara-data-pipeline`.

---

## 1. Atlas das Frentes Parlamentares

| Requisito | View / Produto Analítico | Status |
|---|---|---|
| Tabela gold de frentes | `gold.vw_frentes_analitica` | ATENDIDO |
| Diversidade partidária (HHI) | `gold.vw_frentes_diversidade_hhi` | ATENDIDO |
| Deputados em múltiplas frentes | `gold.vw_deputados_multiplas_frentes` | ATENDIDO |
| Sobreposição entre frentes | `gold.vw_frentes_sobreposicao_membros` | ATENDIDO |
| Evolução por legislatura | `gold.vw_frentes_evolucao_legislatura` | ATENDIDO |

---

## 2. Calendário Analítico de Eventos Legislativos

| Requisito | View / Produto Analítico | Status |
|---|---|---|
| Tabela gold de eventos com órgão, tipo e data | `gold.vw_eventos_analitica` | ATENDIDO |
| Taxa de presença por deputado e tipo de evento | `gold.vw_presenca_eventos_deputado` | ATENDIDO |
| Comparativo de frequência antes/depois de períodos eleitorais | `gold.vw_eventos_frequencia_eleitoral` | PARCIAL |
| Densidade de eventos por semana | `gold.vw_eventos_densidade_semanal` | ATENDIDO |
| Eventos futuros agendados | `gold.vw_eventos_futuros` | ATENDIDO |

---

## 3. Correlação entre Frentes e Votações

| Requisito | View / Produto Analítico | Status |
|---|---|---|
| Verificar alinhamento entre membros da mesma frente | `gold.vw_frentes_votacoes_alinhamento` | ATENDIDO |
| Comparar alinhamento frente × partido | `gold.vw_alinhamento_frente_vs_partido` | ATENDIDO |
| Análise de fidelidade partidária | `gold.vw_fidelidade_partidaria` | ATENDIDO |
| Divergência em relação ao partido | `gold.vw_divergencia_partidaria` | ATENDIDO |
| Base analítica de votações | `gold.vw_votacoes_analitica` | ATENDIDO |

---

## 4. Raio-X de Gastos da CEAP

| Requisito | View / Produto Analítico | Status |
|---|---|---|
| Ingestão de `/deputados/{id}/despesas` com paginação | `01_bronze/07_ingest_despesas.py` | ATENDIDO |
| Ingestão alternativa por arquivos | `01_bronze/07b_ingest_despesas_file.py` | ATENDIDO |
| Tabela fato de despesas | `gold.ft_despesas_ceap` | ATENDIDO |
| Dimensões de deputado, fornecedor, categoria, mês | `gold.dm_deputado`, `gold.dm_fornecedor`, `gold.dm_tipo_despesa`, `gold.dm_data` | ATENDIDO |
| Score de anomalia z-score por categoria × UF | `gold.vw_anomalias_ceap_zscore` | ATENDIDO |
| Ranking de fornecedores mais pagos com flags de CNPJ suspeito | `gold.vw_despesas_ceap_analitica` | ATENDIDO |
| Relatório mensal/top gastos por partido | `gold.vw_partidos_analitica` | ATENDIDO |

---

## 5. Pipeline de Auditoria de CPIs

| Requisito | Resposta do Projeto | Status |
|---|---|---|
| Tabela específica de CPIs com timeline | Evolução futura documentada | ROADMAP |
| Join CPI × proposições | Arquitetura preparada para extensão futura | ROADMAP |
| Análise de duração das CPIs | Não implementado no escopo atual | ROADMAP |
| Rede de convocados cruzada com entidades privadas | Dependente de fonte externa adicional | ROADMAP |
| Comparativo de produtividade das CPIs | Evolução futura documentada | ROADMAP |

---

## 6. Monitor de Presença e Absenteísmo Parlamentar

| Requisito | View / Produto Analítico | Status |
|---|---|---|
| Junção entre eventos e votações | `gold.vw_score_engajamento_parlamentar` | ATENDIDO |
| Score composto de engajamento | `gold.vw_score_engajamento_parlamentar` | PARCIAL |
| Detecção de padrão de ausência | `gold.vw_absenteismo_parlamentar` | ATENDIDO |
| Série temporal de engajamento | `gold.vw_engajamento_temporal` | PARCIAL |
| Relatório mensal por deputado | `gold.vw_engajamento_parlamentar_mensal` | PARCIAL |

---

## 7. Arquitetura de Dados e Engenharia

| Requisito | Implementação no Projeto | Status |
|---|---|---|
| Arquitetura medalhão | Bronze, Silver Base, Silver Curated e Gold | ATENDIDO |
| Camada Bronze | Ingestão raw com payload, metadados e replayabilidade | ATENDIDO |
| Camada Silver Base | Parsing, tipagem, padronização, deduplicação e qualidade técnica | ATENDIDO |
| Camada Silver Curated | Entidades de negócio curadas e analytics-ready | ATENDIDO |
| Camada Gold | Dimensões, fatos e views analíticas | ATENDIDO |
| Delta Lake | Persistência em tabelas Delta | ATENDIDO |
| PySpark | Processamento e transformações principais em PySpark | ATENDIDO |
| Orquestração | Notebooks `run_*_pipeline` por camada | ATENDIDO |
| Observabilidade | `monitoring.pipeline_log` | ATENDIDO |
| Reprocessamento | Notebooks Admin e execução idempotente | ATENDIDO |
| Lineage | Colunas `bronze_*`, `silver_*`, `gold_*` | ATENDIDO |
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
| `gold.dm_bancada` | Dimensão de bancadas e blocos | ATENDIDO |
| `gold.dm_responsavel_ceap` | Dimensão de responsáveis CEAP | ATENDIDO |

### 8.2 Fatos

| Fato | Finalidade Analítica | Status |
|---|---|---|
| `gold.ft_despesas_ceap` | Fato de despesas parlamentares CEAP | ATENDIDO |
| `gold.ft_votacoes` | Fato consolidado de votações | ATENDIDO |
| `gold.ft_votos` | Fato de votos individuais dos deputados | ATENDIDO |
| `gold.ft_orientacoes_bancada` | Fato de orientações de bancadas | ATENDIDO |
| `gold.ft_atividade_parlamentar` | Fato consolidado de atividade parlamentar | ATENDIDO |
| `gold.ft_presenca_eventos` | Fato de presença em eventos legislativos | ATENDIDO |
| `gold.ft_frentes_membros` | Fato de composição das frentes parlamentares | ATENDIDO |

---

## 9. Desafios Opcionais

### 9.1 Streaming de Votações com Alertas em Tempo Real

| Requisito | Implementação no Projeto | Status |
|---|---|---|
| Job agendado a cada 10 minutos consumindo `/votacoes` | `99_ingest_votacoes_microbatch` / `04_run_votacoes_streaming_pipeline` | ATENDIDO |
| Controle de offset por ID | Bronze Stream com offset por votação | ATENDIDO |
| Pipeline DLT Bronze → Prata → Ouro | `01_dlt_votacoes_streaming` | ATENDIDO |
| Expectativas de qualidade declarativas | DLT / Lakeflow expectations | ATENDIDO |
| SLA dashboard | Dashboard de latência, volume e erro | ATENDIDO |
| Runbook de incidentes | Estratégia documentada de replay e reprocessamento | ATENDIDO |

### 9.2 CDC de Tramitação com SCD Type 2

| Requisito | Implementação no Projeto | Status |
|---|---|---|
| Ingestão incremental de tramitações | `14_ingest_proposicoes_tramitacoes_cdc` | ATENDIDO |
| Chaves/hash de payload para CDC | `bronze_cdc.proposicoes_tramitacoes_raw` | ATENDIDO |
| Tabela Silver SCD Type 2 | `silver_cdc.proposicoes_tramitacoes_scd2` | ATENDIDO |
| Campos `valid_from`, `valid_to`, `is_current` | Implementados na camada SCD2 | ATENDIDO |
| Reconstrução histórica | Estrutura compatível com Delta Time Travel | PARCIAL |
| Alertas para avanço/arquivamento | Roadmap analítico | ROADMAP |

---

## 10. Governança, Catálogo e Metadata

| Requisito / Capacidade | Implementação no Projeto | Status |
|---|---|---|
| Dicionário Gold | `docs/gold_layer_enterprise_data_dictionary.md` | ATENDIDO |
| Comentários em tabelas e colunas | `99_apply_gold_comments.py` | ATENDIDO |
| Validação de metadata | `99_validate_gold_metadata.py` | ATENDIDO |
| Detecção de drift de schema | Validação entre schema físico e `metadata_comments` | ATENDIDO |
| Padronização de headers dos notebooks | Headers Markdown por camada e notebook | ATENDIDO |
| Catálogo de notebooks | `docs/notebooks_catalog.md` | ATENDIDO |
| Documentação técnica | Diretório `docs/` | ATENDIDO |
| Runbooks | Documentação operacional e streaming | ATENDIDO |

---

## 11. Conclusão Geral

O projeto `camara-data-pipeline` atende fortemente aos requisitos do desafio final, cobrindo ingestão, tratamento, modelagem, qualidade, governança, observabilidade, reprocessamento e analytics avançado sobre dados parlamentares da Câmara dos Deputados.

Além dos requisitos centrais, o projeto implementa componentes adicionais de maturidade enterprise, incluindo arquitetura medalhão completa, Gold Star Schema, metadata governance, CDC/SCD Type 2, pipeline streaming com DLT/Lakeflow, validações operacionais e produtos analíticos de Parliamentary Intelligence.

Os itens relacionados a CPIs permanecem documentados como evolução futura, pois dependem de escopo analítico e fontes complementares específicas.
