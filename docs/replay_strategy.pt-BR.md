# Estratégia de Replay

🇺🇸 Documento técnico — Estratégia de Replay, Recuperação e Reprocessamento

---

# Visão Geral

Este documento descreve a estratégia de replay, recuperação e reprocessamento implementada no projeto `camara-data-pipeline`.

A arquitetura foi desenhada para suportar processamento de dados resiliente e rastreável através da preservação da ingestão bruta, metadados de lineage e histórico operacional de execução em todas as camadas Medallion.

A estratégia de replay garante:

* reprodutibilidade;
* resiliência operacional;
* reprocessamento controlado;
* rastreabilidade de batches;
* reconstrução histórica;
* consistência analítica.

---

# Filosofia de Replay

O projeto segue uma filosofia arquitetural replay-first.

A camada Bronze preserva ingestão bruta e metadados para permitir reconstrução downstream sempre que necessário.

## Principais objetivos

* reconstruir camadas downstream;
* recuperar falhas operacionais;
* validar transformações;
* suportar auditoria;
* suportar reconstrução CDC;
* suportar recuperação streaming.

---

# Arquitetura de Replay

```text
API de Origem
    │
    ▼
Camada Bronze Raw
    │
    │ origem de replay
    ▼
Silver Base
    │
    ▼
Silver Curated
    │
    ▼
Camada Gold
    │
    ▼
Analytics / Dashboards
```

---

# Camada Bronze Replayável

A camada Bronze é a fundação da replayabilidade.

## Principais características

* preservação de payload bruto;
* ingestão determinística;
* lineage por batch;
* metadados de origem;
* timestamps de ingestão;
* geração de hash de registros.

---

# Metadados Bronze

## Principais metadados de replay

| Campo | Objetivo |
|---|---|
| bronze_id_batch | Rastreabilidade de batch |
| bronze_ts_ingestao | Timestamp de ingestão |
| bronze_tx_endpoint | Endpoint de origem |
| bronze_tx_record_hash | Comparação determinística de registros |
| bronze_tx_source_file | Arquivo de origem para replay |
| bronze_nr_ano_referencia | Ano de referência |

---

# Cenários de Replay

## Falha de API

### Cenário

Indisponibilidade temporária da API ou timeout durante ingestão.

### Estratégia

* preservar registros já ingeridos;
* reexecutar notebook de ingestão afetado;
* validar logs;
* reconstruir camadas downstream se necessário.

---

## Transformação Inválida

### Cenário

Lógica incorreta de transformação identificada nas camadas Silver ou Gold.

### Estratégia

* preservar Bronze sem alteração;
* corrigir lógica de transformação;
* reexecutar camadas Silver/Gold;
* validar saídas reconstruídas.

---

## Problema de Deduplicação

### Cenário

Registros duplicados identificados após ingestão.

### Estratégia

* validar lógica determinística de hash;
* reexecutar camada de deduplicação;
* reconstruir marts downstream.

---

## Reconstrução CDC

### Cenário

Inconsistência CDC ou ausência de versão histórica.

### Estratégia

* executar replay da ingestão Bronze CDC;
* regenerar comparação de payload;
* reconstruir histórico SCD Type 2.

---

## Falha de Offset Streaming

### Cenário

Progressão incorreta do offset streaming.

### Estratégia

* resetar controle de offset;
* executar replay da ingestão micro-batch;
* validar batches reprocessados.

---

# Níveis de Replay

| Camada | Capacidade de Replay |
|---|---|
| Bronze | Origem completa de replay |
| Silver Base | Reconstruível a partir da Bronze |
| Silver Curated | Reconstruível a partir da Silver Base |
| Gold | Reconstruível a partir da Curated |
| Analytics | Reconstruível a partir da Gold |

---

# Reconstrução de Batch

Cada execução recebe um identificador único de batch.

## Principais objetivos

* rastreabilidade de execução;
* auditoria de replay;
* debugging operacional;
* correlação de SLA.

## Fluxo do batch

```text
batch_id
    │
    ▼
Bronze
    │
    ▼
Silver
    │
    ▼
Gold
    │
    ▼
Monitoring
```

---

# Processamento Determinístico

Replayabilidade depende de padrões determinísticos de processamento.

## Principais princípios

* transformações determinísticas;
* deduplicação estável;
* validações explícitas;
* agregações reproduzíveis;
* processamento seguro para replay.

---

# Recuperação Delta Lake

Delta Lake fortalece capacidades de replay e recuperação.

## Principais benefícios

* transações ACID;
* tabelas versionadas;
* evolução de schema;
* consistência histórica;
* suporte a rollback;
* reconstrução de replay.

---

# Estratégia de Replay Streaming

Workloads streaming também suportam replay.

## Principais controles

* rastreamento de offset;
* lineage por batch;
* hash de registros;
* preservação de payload bruto;
* logging de execução.

---

# Controle de Offset

O pipeline streaming controla offsets de votações processadas.

## Principal objeto

```text
control.votacoes_stream_offset
```

## Objetivo

* evitar processamento duplicado;
* suportar replay;
* permitir recuperação controlada;
* preservar continuidade de execução.

---

# Estratégia CDC Replay

O projeto implementa suporte a replay CDC/SCD Type 2.

## Principais controles

| Campo | Objetivo |
|---|---|
| valid_from | Início da versão histórica |
| valid_to | Fim da versão histórica |
| is_current | Versão ativa atual |
| cdc_payload_hash | Detecção de mudança |

---

# Workflow de Reprocessamento

## Fluxo padrão de recuperação

```text
Identificar problema
    │
    ▼
Validar logs
    │
    ▼
Determinar camada afetada
    │
    ▼
Executar replay da camada origem
    │
    ▼
Reconstruir camadas downstream
    │
    ▼
Validar saídas
```

---

# Logging Operacional

Replayabilidade depende de observabilidade operacional.

## Principal tabela de logs

```text
monitoring.pipeline_log
```

---

# Métricas Registradas

| Métrica | Objetivo |
|---|---|
| records_read | Validar volume de ingestão |
| records_written | Validar persistência |
| records_discarded | Validar registros rejeitados |
| execution_duration | Validação de SLA |
| status | Monitoramento de execução |
| batch_id | Correlação de replay |

---

# Recuperação SLA

A estratégia de replay também suporta análise de recuperação SLA.

## Exemplos

* ingestão atrasada;
* falha de execução streaming;
* batches incompletos;
* workflows reexecutados;
* execuções longas.

---

# Boas Práticas de Replay

O projeto implementa boas práticas modernas de replay e recuperação.

## Principais práticas

* preservar ingestão bruta;
* evitar transformações destrutivas;
* manter metadados de lineage;
* utilizar processamento determinístico;
* registrar métricas operacionais;
* isolar camadas Medallion;
* suportar reconstrução downstream.

---

# Limitações

## Dependência da API

Replay depende da disponibilidade da API pública.

---

## Reconstrução Histórica

A qualidade histórica CDC depende de execuções recorrentes e retenção do histórico Bronze.

---

## Latência Streaming

O tempo de replay depende da frequência de agendamento do workflow.

---

# Documentos Relacionados

| Documento | Objetivo |
|---|---|
| streaming_architecture.md | Arquitetura Streaming e DLT |
| governance_and_lineage.md | Governança e lineage |
| parliamentary_intelligence.md | Arquitetura analítica |
| runbook.md | Procedimentos de incidentes |

---

# Conclusão

A estratégia de replay implementada no `camara-data-pipeline` demonstra práticas de resiliência e recuperação operacional orientadas ao padrão enterprise.

A arquitetura combina ingestão Bronze replayável, transformações determinísticas, recuperação Delta Lake, reconstrução CDC e recuperação de offset streaming para suportar processamento Lakehouse confiável e rastreável.