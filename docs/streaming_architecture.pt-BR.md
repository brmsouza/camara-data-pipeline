# Arquitetura Streaming

🇺🇸 Documento técnico — Arquitetura de Streaming, CDC, DLT e Observabilidade Operacional

---

# Objetivo

Este documento descreve a arquitetura de streaming, micro-batch, CDC, SCD Type 2, Delta Live Tables, monitoramento de SLA, replay e observabilidade operacional implementada no projeto `camara-data-pipeline`.

O objetivo é documentar como o projeto evoluiu além do processamento batch tradicional dentro da arquitetura Medallion ao incorporar padrões modernos de Engenharia de Dados para ingestão incremental, monitoramento operacional e reprocessamento controlado.

---

# Visão Geral

A arquitetura streaming foi criada para atender ao desafio opcional de monitoramento quase em tempo real das votações parlamentares.

A solução combina:

* ingestão incremental micro-batch;
* controle de offset de votações;
* preservação de payload bruto;
* geração determinística de hash de registros;
* rastreabilidade por batch_id;
* Delta Live Tables;
* expectativas declarativas de qualidade;
* geração de alertas Gold;
* monitoramento de SLA;
* estratégia de replay e reprocessamento.

---

# Componentes Implementados

| Componente | Status | Descrição |
|---|---|---|
| Micro-batch de votações | Implementado | Ingestão incremental de novas sessões de votação |
| Controle de offset | Implementado | Controle do último ID/timestamp processado |
| Delta Live Tables | Implementado | Pipeline Bronze → Silver → Gold com validações |
| Expectativas declarativas | Implementado | Regras de validação de qualidade DLT |
| Alertas Gold | Implementado | Classificação de urgência e flags de notificação |
| Monitoramento de SLA | Implementado | Monitoramento de latência, volume e erros |
| Replay/reprocessamento | Parcialmente Implementado | Estratégia baseada em offsets, logs e payload bruto |
| Observabilidade | Implementado | Logs operacionais e rastreabilidade de batches |

---

# Arquitetura Lógica

```text
API da Câmara dos Deputados
        │
        │ /votacoes
        ▼
Bronze Streaming / Micro-batch
        │
        │ payload bruto + lineage + hash
        ▼
Silver Streaming / DLT
        │
        │ validações + expectations
        ▼
Gold Streaming Alerts
        │
        │ urgência + flag de notificação
        ▼
Monitoramento / Dashboard SLA
```

---

# Workflow Streaming

O workflow streaming é responsável por executar a ingestão incremental de votações parlamentares em intervalos recorrentes.

## Notebook responsável

```text
notebooks/99_jobs/05_run_votacoes_streaming_pipeline.py
```

## Responsabilidades

* orquestrar a execução do pipeline streaming;
* executar ingestão micro-batch;
* controlar dependências operacionais;
* registrar logs de execução;
* habilitar execução recorrente via Databricks Workflow;
* suportar replay e reprocessamento controlado.

## Evidência visual


![Streaming Workflow](../assets/images/job_votacoes_streaming_microbatch.png)


---

# Ingestão Micro-Batch

A ingestão micro-batch monitora continuamente novas sessões de votação parlamentar através do endpoint `/votacoes`.

## Notebook responsável

```text
notebooks/01_bronze/99_ingest_votacoes_microbatch.py
```

## Fonte

```text
/votacoes
```

## Tabela alvo

```text
bronze_stream.votacoes_raw
```

---

# Delta Live Tables

A arquitetura também inclui um pipeline Delta Live Tables para estruturar o fluxo Bronze → Silver → Gold com enforcement declarativo de qualidade.

## Notebook responsável

```text
notebooks/05_dlt/01_dlt_votacoes_streaming.py
```
cert---

# Monitoramento SLA

## Objeto

```text
monitoring.vw_sla_votacoes_streaming
```

## Métricas monitoradas

* latência end-to-end;
* volume de registros processados;
* taxa de erro;
* duração da execução;
* status da execução;
* records read;
* records written;
* discarded records.

---

# CDC / SCD Type 2

Além do streaming de votações, o projeto implementa CDC/SCD Type 2 para histórico de tramitações de proposições.

## Notebooks responsáveis

```text
notebooks/00_setup/04_create_cdc_scd2_objects.py
notebooks/01_bronze/14_ingest_proposicoes_tramitacoes_cdc.py
notebooks/02_silver/01_base/15_base_proposicoes_tramitacoes_cdc.py
notebooks/02_silver/02_curated/15_curated_proposicoes_tramitacoes_scd.py
notebooks/04_analytics/08_build_gold_proposicoes_cdc_analytics.py
```

---

# Conclusão

A arquitetura streaming implementada no projeto `camara-data-pipeline` demonstra uma evolução técnica significativa em comparação com pipelines batch tradicionais.

A solução combina ingestão incremental, processamento micro-batch, DLT, CDC/SCD2, observabilidade operacional, monitoramento SLA, capacidades de replay e padrões arquiteturais orientados à governança.
