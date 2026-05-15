# Streaming SLA Observability

🇺🇸 Technical document — Real-Time Legislative Pipeline Monitoring and Operational Observability

---

# Overview

This document describes the operational observability architecture implemented in the `camara-data-pipeline` project for monitoring streaming legislative pipelines executed on Databricks.

The solution was designed to provide enterprise-grade operational visibility over micro-batch legislative processing pipelines, including:

- end-to-end SLA monitoring;
- throughput monitoring;
- operational reliability tracking;
- execution error-rate monitoring;
- pipeline health classification;
- streaming operational observability;
- executive operational dashboards.

The implementation was developed as part of the optional challenge:

> "SLA dashboard: latência end-to-end, volume e taxa de erro por execução"

from the final Databricks/Tiller Engineering challenge.

---

# Dashboard Overview

## Operational Health Monitor

![Figure 1 — Legislative Pipeline Observability Dashboard](../assets/images/figure_1_legislative_pipeline_observability_dashboard.png)

*Figure 1 — Enterprise operational observability dashboard for streaming legislative pipelines, including SLA monitoring, throughput tracking, error-rate analysis and operational health classification.*

---

## Legislative Volume Monitoring

![Figure 2 — Legislative Volume Monitoring](../assets/images/figure_2_legislative_volume_monitoring.png)

*Figure 2 — Legislative throughput, proposition volume and contextual operational indicators used to support streaming workload observability.*

---

# Objectives

The observability solution was designed with the following goals:

- Monitor end-to-end streaming latency
- Track operational throughput
- Measure execution error rates
- Detect SLA degradation
- Provide operational visibility for streaming pipelines
- Support troubleshooting and incident analysis
- Enable executive operational reporting
- Centralize pipeline execution metrics
- Improve monitoring maturity for streaming workloads

---

# Architectural Context

The monitoring architecture is integrated into the Medallion enterprise data platform implemented in the project.

```text
API
  ↓
Bronze Layer
  ↓
Silver Base Layer
  ↓
Silver Curated Layer
  ↓
Gold Layer
  ↓
Streaming Monitoring & Observability
  ↓
Databricks SQL Dashboard
```

---

# Streaming Monitoring Architecture

The operational observability flow follows the architecture below:

```text
Streaming Micro-Batch Job
        ↓
Pipeline Execution Logging
        ↓
Monitoring Delta Tables
        ↓
Operational Metrics Aggregation
        ↓
Databricks SQL Datasets
        ↓
Operational Dashboard
```

---

# Monitoring Strategy

The solution uses centralized pipeline execution logging through the reusable helper:

```python
log_pipeline_event()
```

Operational events are persisted into Delta monitoring tables for analytical and operational visibility.

Each pipeline execution stores:

- batch identifiers;
- execution timestamps;
- execution duration;
- records read;
- records written;
- discarded records;
- execution level;
- operational status;
- execution metadata.

---

# Monitoring Table

## Table

```text
monitoring.pipeline_log
```

## Main Columns

| Column | Description |
|---|---|
| `batch_id` | Unique pipeline execution identifier |
| `pipeline_name` | Pipeline execution name |
| `layer` | Medallion layer |
| `started_at` | Pipeline start timestamp |
| `finished_at` | Pipeline finish timestamp |
| `duration_seconds` | Total execution duration |
| `records_read` | Total records consumed |
| `records_written` | Total successfully written records |
| `records_discarded` | Total rejected/discarded records |
| `level` | Execution severity level |
| `event_name` | Execution event classification |

---

# Dashboard

## Dashboard Name

```text
Operational Health Monitor
```

The dashboard was designed to provide enterprise operational observability for streaming legislative workloads.

The solution follows observability concepts commonly found in:

- Databricks monitoring platforms
- Grafana operational dashboards
- Azure Monitor
- DataDog
- enterprise streaming observability systems

---

# Dashboard Sections

## SECTION 1 — SLA & Reliability

Executive operational indicators focused on pipeline health and SLA compliance.

### KPIs

- Avg Streaming SLA
- Pipeline Error Rate
- Success Rate
- Critical Alerts

### Charts

- SLA Performance Trend
- Health Status Distribution
- Error Rate Trend
- Throughput Trend

---

## SECTION 2 — Throughput & Volume

Operational visibility over legislative streaming volume.

### KPIs

- Total Propositions
- Active Propositions
- Streaming Throughput

---

## SECTION 3 — Legislative Context

Provides contextual visibility over legislative operational status.

### Charts

- Legislative Status Distribution

---

# KPI Definitions

## Avg Streaming SLA

Measures the average end-to-end latency of streaming micro-batches.

### SQL

```sql
SELECT
    ROUND(AVG(duration_seconds), 2) AS avg_streaming_sla
FROM monitoring.pipeline_log
```

### Operational Meaning

Represents the average processing latency of the streaming pipeline.

---

# Pipeline Error Rate

Measures the percentage of discarded records relative to total processed records.

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

### Operational Meaning

Indicates data quality degradation or operational failures during execution.

---

# Success Rate

Measures the percentage of successful pipeline executions.

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

### Operational Meaning

Represents operational reliability and execution stability.

---

# Health Status Classification

The monitoring solution implements operational health classification based on SLA thresholds.

| Status | Rule |
|---|---|
| GREEN | SLA < 60 seconds |
| YELLOW | SLA < 120 seconds |
| RED | SLA >= 120 seconds |

---

# Operational Benefits

The observability architecture provides the following operational advantages:

- Real-time SLA visibility
- Faster incident detection
- Operational reliability tracking
- Streaming performance visibility
- Centralized monitoring
- Executive operational reporting
- Improved troubleshooting capabilities
- Historical operational analysis
- Streaming workload observability

---

# Engineering Decisions

## Why Databricks SQL Dashboards

Databricks SQL dashboards were selected because they provide:

- native Delta Lake integration;
- low operational overhead;
- native Databricks governance;
- simplified operational deployment;
- enterprise visualization capabilities.

---

## Why Centralized Logging

Centralized logging enables:

- unified operational visibility;
- historical execution tracking;
- SLA auditing;
- operational lineage;
- reliability analysis.

---

## Why Delta Monitoring Tables

Delta Lake monitoring tables provide:

- ACID reliability;
- historical tracking;
- time-travel capabilities;
- scalable operational storage;
- efficient analytical querying.

---

# Future Improvements

Potential future evolutions include:

- real-time alert integrations;
- automated anomaly detection;
- predictive SLA degradation analysis;
- external observability integrations;
- Grafana integration;
- advanced telemetry pipelines;
- automated operational notifications;
- AI-assisted operational diagnostics.

---

# Conclusion

The implemented observability architecture successfully delivers enterprise-grade operational monitoring for streaming legislative pipelines.

The solution provides:

- SLA visibility;
- throughput monitoring;
- error-rate tracking;
- operational health classification;
- executive operational observability.

This implementation significantly increases the operational maturity, reliability and maintainability of the streaming data platform implemented in the `camara-data-pipeline` project.