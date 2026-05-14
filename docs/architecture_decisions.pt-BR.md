# Decisões Arquiteturais

🇺🇸 English version: [architecture_decisions.md](architecture_decisions.md)

Documento técnico — Decisões de Arquitetura e Modelagem

---

# Visão Geral

Este documento registra as principais decisões de arquitetura, modelagem e engenharia adotadas no projeto `camara-data-pipeline`.

O objetivo é tornar as decisões do projeto explícitas, reproduzíveis e defensáveis sob a perspectiva de Engenharia de Dados.

---

# Decisão 1 — Databricks como Plataforma Principal

## Decisão

Utilizar Databricks Free Edition como principal plataforma de desenvolvimento e execução.

## Justificativa

O Databricks fornece suporte nativo para:

* Apache Spark;
* notebooks PySpark;
* Spark SQL;
* Delta Lake;
* Workflows;
* Delta Live Tables;
* arquitetura Lakehouse.

## Resultado

O projeto demonstra padrões modernos de engenharia Lakehouse em um ambiente próximo ao corporativo.

---

# Decisão 2 — Arquitetura Medallion

## Decisão

Adotar uma arquitetura Medallion com camadas Bronze, Silver Base, Silver Curated, Gold e Analytics.

## Justificativa

Essa separação proporciona:

* preservação de dados brutos;
* refinamento progressivo;
* replayabilidade;
* governança;
* consistência analítica;
* separação entre lógica técnica e lógica de negócio.

---

# Decisão 3 — Separar Silver em Base e Curated

## Decisão

Separar a camada Silver em:

```text
02_silver/01_base
02_silver/02_curated
```

## Justificativa

A Silver Base é responsável pela padronização e validação técnica.

A Silver Curated é responsável por entidades reutilizáveis orientadas ao negócio e enriquecimento leve.

---

# Decisão 4 — Dimensões e Fatos apenas na Gold

## Decisão

Criar dimensões e fatos finais apenas na camada Gold.

## Justificativa

A Silver prepara entidades confiáveis, enquanto a Gold representa o modelo analítico final.

---

# Decisão 5 — Utilizar Star Schema

## Decisão

Adotar modelagem Star Schema na Gold.

## Justificativa

Star Schema fornece:

* simplicidade analítica;
* dimensões reutilizáveis;
* fatos independentes;
* escalabilidade para BI;
* granularidade analítica clara.

---

# Decisão 6 — Evitar Relacionamento entre Fatos

## Decisão

Evitar relacionamentos diretos entre tabelas fato.

## Justificativa

Fatos devem ser analisados através de dimensões conformadas.

---

# Decisão 7 — Preservar Lineage da Bronze

## Decisão

Preservar metadados técnicos de lineage da Bronze nas camadas downstream quando relevante.

## Principais campos

* `bronze_ts_ingestao`;
* `bronze_dt_ingestao`;
* `bronze_tx_endpoint`;
* `bronze_id_batch`;
* `bronze_tx_record_hash`;
* `bronze_tx_source_file`;
* `bronze_nr_ano_referencia`.

---

# Decisão 8 — Utilizar Hashes Determinísticos

## Decisão

Gerar hashes determinísticos para os registros.

## Justificativa

Os hashes suportam:

* deduplicação;
* validação de replay;
* comparação CDC;
* detecção de mudanças;
* auditoria.

---

# Decisão 9 — Validações Explícitas de Qualidade

## Decisão

Utilizar validações explícitas com `raise Exception` para falhas críticas.

## Justificativa

A estratégia fail-fast evita corrupção analítica silenciosa.

---

# Decisão 10 — Controle de records_discarded

## Decisão

Controlar registros descartados como parte da execução dos pipelines.

## Justificativa

Registros descartados são importantes para:

* análise de qualidade;
* monitoramento operacional;
* debugging;
* governança.

---

# Decisão 11 — Persistir Registros Rejeitados

## Decisão

Persistir registros rejeitados quando regras de negócio ou qualidade exigirem investigação.

## Justificativa

Registros rejeitados não devem desaparecer silenciosamente.

---

# Decisão 12 — Implementar CDC / SCD Type 2

## Decisão

Implementar CDC e SCD Type 2 para tramitações de proposições.

## Justificativa

As tramitações mudam ao longo do tempo e exigem histórico.

## Principais campos

* `valid_from`;
* `valid_to`;
* `is_current`;
* `cdc_payload_hash`.

---

# Decisão 13 — Implementar Streaming Micro-Batch

## Decisão

Implementar monitoramento de votações através de ingestão micro-batch agendada.

## Justificativa

Micro-batch fornece comportamento near-real-time mantendo simplicidade e confiabilidade.

---

# Decisão 14 — Utilizar Delta Live Tables

## Decisão

Utilizar Delta Live Tables para o pipeline streaming de validação.

## Justificativa

DLT fornece:

* expectativas declarativas;
* enforcement de qualidade;
* fluxo gerenciado;
* arquitetura orientada a streaming.

---

# Decisão 15 — Utilizar Tabelas de Monitoramento

## Decisão

Utilizar tabelas de monitoramento como:

```text
monitoring.pipeline_log
monitoring.vw_sla_votacoes_streaming
```

## Justificativa

Observabilidade operacional é necessária para SLA, replay e investigação de incidentes.

---

# Decisão 16 — Manter README Executivo

## Decisão

Manter o README mais enxuto e mover documentação detalhada para `docs/`.

## Justificativa

O README deve ser amigável para GitHub e recrutadores, enquanto a profundidade técnica fica em documentos dedicados.

---

# Conclusão

As decisões arquiteturais adotadas no `camara-data-pipeline` foram desenhadas para demonstrar práticas enterprise de Engenharia de Dados utilizando Databricks, PySpark, Spark SQL, Delta Lake e arquitetura Medallion.

O projeto prioriza clareza, replayabilidade, governança, modelagem dimensional, observabilidade e valor analítico.
