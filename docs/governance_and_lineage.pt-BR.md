# Governança e Lineage

🇺🇸 Documento técnico — Governança, Lineage, Replay, Qualidade e Observabilidade Operacional

---

# Visão Geral

Este documento descreve as estratégias de governança, lineage, replay, validação, resiliência e observabilidade operacional implementadas no projeto `camara-data-pipeline`.

O projeto adota princípios de governança orientados ao padrão enterprise para garantir:

* rastreabilidade;
* reprodutibilidade;
* replayabilidade;
* resiliência operacional;
* qualidade de dados;
* confiabilidade analítica;
* observabilidade técnica.

---

# Estratégia de Governança

| Princípio | Descrição |
|---|---|
| Rastreabilidade | Rastrear origem dos dados e histórico de processamento |
| Replayabilidade | Permitir reconstrução e reprocessamento controlado |
| Qualidade de Dados | Evitar propagação inválida entre camadas |
| Observabilidade Operacional | Monitorar saúde da execução e comportamento de SLA |

---

# Metadados Bronze

## Principais campos de metadados

| Campo | Descrição |
|---|---|
| bronze_ts_ingestao | Timestamp de ingestão |
| bronze_dt_ingestao | Data de ingestão |
| bronze_tx_endpoint | Endpoint da API de origem |
| bronze_id_batch | Identificador do batch de execução |
| bronze_tx_record_hash | Hash determinístico do registro |
| bronze_tx_source_file | Arquivo de origem para replay |
| bronze_nr_ano_referencia | Ano de referência |

---

# Estratégia de Qualidade de Dados

O projeto implementa validações explícitas de qualidade.

## Princípios de validação

* validações fail-fast;
* regras determinísticas de validação;
* registros rejeitados explícitos;
* preservação de lineage;
* visibilidade operacional.

---

# Estratégia de Replay

A arquitetura foi desenhada para suportar replay e reprocessamento controlado.

## Princípios de replay

* preservar ingestão bruta;
* reconstruir camadas downstream;
* reprocessamento determinístico;
* rastreabilidade de replay;
* reconstrução em nível de batch.

---

# Governança Delta Lake

Delta Lake é utilizado para reforçar governança e replayabilidade.

## Principais benefícios

* transações ACID;
* versionamento;
* suporte a replay;
* evolução de schema;
* processamento incremental;
* reconstrução histórica.

---

# Governança Streaming

Workloads streaming também preservam metadados de governança.

## Principais controles

* rastreamento de offset;
* lineage por batch;
* hash de registros;
* preservação de payload;
* monitoramento SLA;
* logging de execução.

---

# Logging Operacional

## Principal tabela de logs

```text
monitoring.pipeline_log
```

---

# Monitoramento SLA

O projeto monitora métricas operacionais relacionadas a SLA.

## Exemplos

* latência de execução;
* duração do processamento;
* taxa de registros descartados;
* execução de replay;
* status de execução de workflows.

---

# Boas Práticas de Engenharia

O projeto implementa boas práticas modernas de Engenharia de Dados incluindo:

* Arquitetura Medallion;
* validações explícitas;
* preservação de lineage;
* processamento determinístico;
* replayabilidade;
* CDC/SCD2;
* governança streaming;
* expectations DLT;
* monitoramento operacional;
* logging estruturado.

---

# Conclusão

A arquitetura de governança e lineage implementada no `camara-data-pipeline` demonstra práticas de engenharia orientadas ao padrão enterprise com foco em resiliência operacional, replayabilidade, observabilidade e confiabilidade analítica.