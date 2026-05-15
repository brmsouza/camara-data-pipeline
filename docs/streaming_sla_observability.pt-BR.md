# Observabilidade de SLA em Streaming

🇧🇷 Documento técnico — Monitoramento em Tempo Real de Pipelines Legislativos e Observabilidade Operacional

---

# Visão Geral

Este documento descreve a arquitetura de observabilidade operacional implementada no projeto `camara-data-pipeline` para monitoramento de pipelines legislativos em streaming executados no Databricks.

A solução foi projetada para fornecer visibilidade operacional enterprise sobre pipelines legislativos de micro-batch, incluindo:

- monitoramento de SLA ponta a ponta;
- monitoramento de throughput;
- rastreamento de confiabilidade operacional;
- monitoramento de taxa de erro por execução;
- classificação de saúde operacional do pipeline;
- observabilidade operacional de streaming;
- dashboards executivos operacionais.

A implementação foi desenvolvida como parte do desafio opcional:

> "SLA dashboard: latência end-to-end, volume e taxa de erro por execução"

do desafio final Databricks/Tiller Engineering.

---

# Visão Geral do Dashboard

## Operational Health Monitor

![Figura 1 — Dashboard de Observabilidade do Pipeline Legislativo](../assets/images/figure_1_legislative_pipeline_observability_dashboard.png)

*Figura 1 — Dashboard enterprise de observabilidade operacional para pipelines legislativos em streaming, incluindo monitoramento de SLA, throughput, taxa de erro e classificação de saúde operacional.*

---

## Monitoramento de Volume Legislativo

![Figura 2 — Monitoramento de Volume Legislativo](../assets/images/figure_2_legislative_volume_monitoring.png)

*Figura 2 — Indicadores de throughput legislativo, volume de proposições e métricas contextuais operacionais utilizadas para suporte à observabilidade do workload de streaming.*

---

# Objetivos

A solução de observabilidade foi projetada com os seguintes objetivos:

- Monitorar latência ponta a ponta de streaming
- Acompanhar throughput operacional
- Medir taxa de erro das execuções
- Detectar degradação de SLA
- Fornecer visibilidade operacional dos pipelines
- Apoiar troubleshooting e análise de incidentes
- Permitir reporting executivo operacional
- Centralizar métricas de execução dos pipelines
- Aumentar maturidade de monitoramento de workloads streaming

---

# Contexto Arquitetural

A arquitetura de monitoramento está integrada à plataforma enterprise Medallion implementada no projeto.

```text
API
  ↓
Camada Bronze
  ↓
Camada Silver Base
  ↓
Camada Silver Curated
  ↓
Camada Gold
  ↓
Monitoramento e Observabilidade Streaming
  ↓
Dashboard Databricks SQL
```

---

# Arquitetura de Monitoramento Streaming

O fluxo de observabilidade operacional segue a arquitetura abaixo:

```text
Job Streaming Micro-Batch
        ↓
Logging de Execução do Pipeline
        ↓
Tabelas Delta de Monitoramento
        ↓
Agregação de Métricas Operacionais
        ↓
Datasets Databricks SQL
        ↓
Dashboard Operacional
```

---

# Estratégia de Monitoramento

A solução utiliza logging centralizado de execução através do helper reutilizável:

```python
log_pipeline_event()
```

Os eventos operacionais são persistidos em tabelas Delta de monitoramento para visibilidade analítica e operacional.

Cada execução de pipeline armazena:

- identificadores de batch;
- timestamps de execução;
- duração da execução;
- registros lidos;
- registros escritos;
- registros descartados;
- nível de execução;
- status operacional;
- metadados da execução.

---

# Tabela de Monitoramento

## Tabela

```text
monitoring.pipeline_log
```

## Principais Colunas

| Coluna | Descrição |
|---|---|
| `batch_id` | Identificador único da execução |
| `pipeline_name` | Nome do pipeline executado |
| `layer` | Camada Medallion |
| `started_at` | Timestamp de início |
| `finished_at` | Timestamp de término |
| `duration_seconds` | Duração total da execução |
| `records_read` | Total de registros consumidos |
| `records_written` | Total de registros escritos com sucesso |
| `records_discarded` | Total de registros rejeitados/descartados |
| `level` | Severidade operacional |
| `event_name` | Classificação do evento operacional |

---

# Dashboard

## Nome do Dashboard

```text
Operational Health Monitor
```

O dashboard foi projetado para fornecer observabilidade operacional enterprise para workloads legislativos em streaming.

A solução segue conceitos de observabilidade encontrados em:

- plataformas de monitoramento Databricks;
- dashboards operacionais Grafana;
- Azure Monitor;
- DataDog;
- sistemas enterprise de observabilidade streaming.

---

# Seções do Dashboard

## SEÇÃO 1 — SLA e Confiabilidade

Indicadores executivos operacionais focados em saúde do pipeline e conformidade de SLA.

### KPIs

- Avg Streaming SLA
- Pipeline Error Rate
- Success Rate
- Critical Alerts

### Gráficos

- SLA Performance Trend
- Health Status Distribution
- Error Rate Trend
- Throughput Trend

---

## SEÇÃO 2 — Throughput e Volume

Visibilidade operacional sobre volume legislativo em streaming.

### KPIs

- Total Propositions
- Active Propositions
- Streaming Throughput

---

## SEÇÃO 3 — Contexto Legislativo

Fornece visibilidade contextual sobre o status operacional legislativo.

### Gráficos

- Legislative Status Distribution

---

# Definição dos KPIs

## Avg Streaming SLA

Mede a latência média ponta a ponta dos micro-batches de streaming.

### SQL

```sql
SELECT
    ROUND(AVG(duration_seconds), 2) AS avg_streaming_sla
FROM monitoring.pipeline_log
```

### Significado Operacional

Representa a latência média de processamento do pipeline de streaming.

---

# Pipeline Error Rate

Mede o percentual de registros descartados em relação ao total processado.

### SQL

```sql
SELECT
    ROUND(
        (
            SUM(records_discarded)
            / NULLIF(SUM(records_read), 0)
        ) * 100,
        2
    ) AS pipeline_error_rate
FROM monitoring.pipeline_log
```

### Significado Operacional

Indica degradação de qualidade de dados ou falhas operacionais durante a execução.

---

# Success Rate

Mede o percentual de execuções bem-sucedidas do pipeline.

### SQL

```sql
SELECT
    ROUND(
        (
            SUM(
                CASE
                    WHEN level = 'INFO' THEN 1
                    ELSE 0
                END
            )
            / COUNT(*)
        ) * 100,
        2
    ) AS success_rate
FROM monitoring.pipeline_log
```

### Significado Operacional

Representa confiabilidade operacional e estabilidade das execuções.

---

# Classificação de Saúde Operacional

A solução implementa classificação operacional baseada em thresholds de SLA.

| Status | Regra |
|---|---|
| GREEN | SLA < 60 segundos |
| YELLOW | SLA < 120 segundos |
| RED | SLA >= 120 segundos |

---

# Benefícios Operacionais

A arquitetura de observabilidade fornece os seguintes benefícios:

- visibilidade de SLA em tempo real;
- detecção mais rápida de incidentes;
- rastreamento de confiabilidade operacional;
- visibilidade de performance streaming;
- monitoramento centralizado;
- reporting executivo operacional;
- melhoria de troubleshooting;
- análise histórica operacional;
- observabilidade de workloads streaming.

---

# Decisões de Engenharia

## Por que Databricks SQL Dashboards

Os dashboards Databricks SQL foram escolhidos porque fornecem:

- integração nativa com Delta Lake;
- baixo overhead operacional;
- governança nativa Databricks;
- simplificação de deployment operacional;
- recursos enterprise de visualização.

---

## Por que Logging Centralizado

O logging centralizado permite:

- visibilidade operacional unificada;
- rastreamento histórico de execuções;
- auditoria de SLA;
- lineage operacional;
- análise de confiabilidade.

---

## Por que Tabelas Delta de Monitoramento

As tabelas Delta fornecem:

- confiabilidade ACID;
- rastreamento histórico;
- capacidade de time-travel;
- armazenamento operacional escalável;
- consultas analíticas eficientes.

---

# Evoluções Futuras

Possíveis evoluções futuras incluem:

- integração com alertas em tempo real;
- detecção automatizada de anomalias;
- análise preditiva de degradação de SLA;
- integração com plataformas externas de observabilidade;
- integração Grafana;
- pipelines avançados de telemetria;
- notificações operacionais automatizadas;
- diagnósticos operacionais assistidos por IA.

---

# Conclusão

A arquitetura de observabilidade implementada entrega monitoramento operacional enterprise para pipelines legislativos em streaming.

A solução fornece:

- visibilidade de SLA;
- monitoramento de throughput;
- rastreamento de taxa de erro;
- classificação de saúde operacional;
- observabilidade executiva operacional.

Esta implementação aumenta significativamente a maturidade operacional, confiabilidade e manutenibilidade da plataforma de dados streaming implementada no projeto `camara-data-pipeline`.