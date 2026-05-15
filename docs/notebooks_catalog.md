# Notebook Catalog

Technical catalog for the `camara-data-pipeline` repository. This document explains the purpose, location, layer, inputs, outputs and responsibilities of each notebook or reusable module in the project.

The catalog follows the project Medallion architecture: Bronze → Silver Base → Silver Curated → Gold → Analytics, with additional support for CDC/SCD Type 2, streaming micro-batch, Delta Live Tables and Databricks workflow orchestration.

---

## Execution Flow

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

Optional / advanced workloads:
00_setup + 01_bronze + 02_silver + 04_analytics + 05_dlt + 99_jobs
for CDC/SCD2, streaming micro-batch, SLA monitoring and DLT pipelines.
```

---

## Layer Summary

| Layer | Responsibility |
|---|---|
| `00_setup` | Creates schemas, control tables, monitoring structures, streaming objects and CDC/SCD2 support objects. |
| `01_bronze` | Ingests raw API/file data, preserving payloads, metadata, batch lineage and replayability. |
| `02_silver/01_base` | Performs technical parsing, typing, standardization, quality validation and deduplication. |
| `02_silver/02_curated` | Builds reusable business entities with enrichment, fallback rules and curated standardization. |
| `03_gold` | Builds conformed dimensions and fact tables following a star schema model. |
| `04_analytics` | Builds analytical views, marts, validations, SLA monitoring and challenge-oriented data products. |
| `05_dlt` | Defines Delta Live Tables streaming pipeline with declarative expectations. |
| `90_common` | Stores reusable utility modules for API access, pagination, logging, writing and validation helpers. |
| `99_jobs` | Orchestrates layer-level execution in Databricks Workflows. |

---

## Complete Notebook Index

| Path | Layer | Output / Target |
|---|---|---|
| `00_setup/00_create_schemas.py` | Setup | `Not applicable / support object` |
| `00_setup/01_create_control_tables.py` | Setup | `Not applicable / support object` |
| `00_setup/02_create_streaming_objects.py` | Setup | `Not applicable / support object` |
| `00_setup/03_create_cdc_scd2_objects.py` | Setup | `Not applicable / support object` |
| `00_setup/90_admin_test_api_connection.py` | Admin | `Not applicable / support object` |
| `00_setup/91_admin_reset_environment.py` | Admin | `Not applicable / support object` |
| `00_setup/92_admin_check_quality_tables.py` | Admin | `Not applicable / support object` |
| `00_setup/93_admin_export_volume_csv.py` | Admin | `Not applicable / support object` |
| `01_bronze/01_ingest_deputados.py` | Bronze | `bronze.deputados` |
| `01_bronze/02_ingest_deputados_detalhes.py` | Bronze | `bronze.deputados_detalhes` |
| `01_bronze/03_ingest_frentes.py` | Bronze | `bronze.frentes` |
| `01_bronze/04_ingest_eventos.py` | Bronze | `bronze.eventos` |
| `01_bronze/05_ingest_frentes_membros.py` | Bronze | `bronze.frentes_membros` |
| `01_bronze/06_ingest_proposicoes.py` | Bronze | `bronze.proposicoes` |
| `01_bronze/06b_ingest_proposicoes_file.py` | Bronze | `bronze.proposicoes` |
| `01_bronze/07_ingest_despesas.py` | Bronze | `bronze.despesas` |
| `01_bronze/07b_ingest_despesas_file.py` | Bronze | `bronze.despesas` |
| `01_bronze/08_ingest_orgaos.py` | Bronze | `bronze.orgaos` |
| `01_bronze/09_ingest_orgaos_membros.py` | Bronze | `bronze.orgaos_membros` |
| `01_bronze/09b_ingest_orgaos_membros_file.py` | Bronze | `bronze.orgaos_membros` |
| `01_bronze/10_ingest_votacoes.py` | Bronze | `bronze.votacoes` |
| `01_bronze/10b_ingest_votacoes_file.py` | Bronze | `bronze.votacoes` |
| `01_bronze/11_ingest_votacoes_orientacoes.py` | Bronze | `bronze.votacoes_orientacoes` |
| `01_bronze/11b_ingest_votacoes_orientacoes_file.py` | Bronze | `bronze.votacoes_orientacoes` |
| `01_bronze/12_ingest_votacoes_votos.py` | Bronze | `bronze.votacoes_votos` |
| `01_bronze/12b_ingest_votacoes_votos_file.py` | Bronze | `bronze.votacoes_votos` |
| `01_bronze/13_ingest_legislaturas.py` | Bronze | `bronze.legislaturas` |
| `01_bronze/14_ingest_proposicoes_tramitacoes_cdc.py` | Bronze CDC | `bronze_cdc.proposicoes_tramitacoes_raw` |
| `01_bronze/90_validate_bronze.py` | Validation | `Not applicable / support object` |
| `01_bronze/99_ingest_votacoes_microbatch.py` | Bronze Stream | `bronze_stream.votacoes_raw` |
| `02_silver/01_base/01_base_deputados.py` | Silver Base | `silver_base.deputados` |
| `02_silver/01_base/02_base_deputados_detalhes.py` | Silver Base | `silver_base.deputados_detalhes` |
| `02_silver/01_base/03_base_frentes.py` | Silver Base | `silver_base.frentes` |
| `02_silver/01_base/04_base_eventos.py` | Silver Base | `silver_base.eventos` |
| `02_silver/01_base/05_base_frentes_membros.py` | Silver Base | `silver_base.frentes_membros` |
| `02_silver/01_base/06_base_proposicoes.py` | Silver Base | `silver_base.proposicoes` |
| `02_silver/01_base/07_base_despesas.py` | Silver Base | `silver_base.despesas` |
| `02_silver/01_base/08_base_orgaos.py` | Silver Base | `silver_base.orgaos` |
| `02_silver/01_base/09_base_orgaos_membros.py` | Silver Base | `silver_base.orgaos_membros` |
| `02_silver/01_base/10_base_votacoes.py` | Silver Base | `silver_base.votacoes` |
| `02_silver/01_base/11_base_votacoes_orientacoes.py` | Silver Base | `silver_base.votacoes_orientacoes` |
| `02_silver/01_base/12_base_votacoes_votos.py` | Silver Base | `silver_base.votacoes_votos` |
| `02_silver/01_base/13_base_legislaturas.py` | Silver Base | `silver_base.legislaturas` |
| `02_silver/01_base/14_base_fornecedores.py` | Silver Curated | `silver_curated.fornecedores` |
| `02_silver/01_base/15_base_proposicoes_tramitacoes_cdc.py` | Silver Base CDC | `silver_cdc.proposicoes_tramitacoes_base` |
| `02_silver/02_curated/01_curated_deputados.py` | Silver Curated | `silver_curated.deputados` |
| `02_silver/02_curated/03_curated_frentes.py` | Silver Curated | `silver_curated.frentes` |
| `02_silver/02_curated/04_curated_eventos.py` | Silver Curated | `silver_curated.eventos` |
| `02_silver/02_curated/05_curated_frentes_membros.py` | Silver Curated | `silver_curated.frentes_membros` |
| `02_silver/02_curated/06_curated_proposicoes.py` | Silver Curated | `silver_curated.proposicoes` |
| `02_silver/02_curated/07_curated_despesas.py` | Silver Curated | `silver_curated.despesas` |
| `02_silver/02_curated/08_curated_orgaos.py` | Silver Curated | `silver_curated.orgaos` |
| `02_silver/02_curated/09_curated_orgaos_membros.py` | Silver Curated | `silver_curated.orgaos_membros` |
| `02_silver/02_curated/10_curated_votacoes.py` | Silver Curated | `silver_curated.votacoes` |
| `02_silver/02_curated/11_curated_votacoes_orientacoes.py` | Silver Curated | `silver_curated.votacoes_orientacoes` |
| `02_silver/02_curated/12_curated_votacoes_votos.py` | Silver Curated | `silver_curated.votacoes_votos` |
| `02_silver/02_curated/13_curated_legislaturas.py` | Silver Curated | `silver_curated.legislaturas` |
| `02_silver/02_curated/14_curated_fornecedores.py` | Silver Curated | `silver_curated.fornecedores` |
| `02_silver/02_curated/15_curated_proposicoes_tramitacoes_scd.py` | Silver Curated CDC | `silver_cdc.proposicoes_tramitacoes_scd2` |
| `03_gold/00_create_gold_schema.py` | Gold | `gold` |
| `03_gold/01_build_dm_data.py` | Gold | `gold.dm_data` |
| `03_gold/02_build_dm_legislatura.py` | Gold | `gold.dm_legislatura` |
| `03_gold/03_build_dm_partido.py` | Gold | `gold.dm_partido` |
| `03_gold/04_build_dm_deputado.py` | Gold | `gold.dm_deputado` |
| `03_gold/05_build_dm_proposicao.py` | Gold | `gold.dm_proposicao` |
| `03_gold/06_build_dm_orgao.py` | Gold | `gold.dm_orgao` |
| `03_gold/07_build_dm_gabinete.py` | Gold | `gold.dm_gabinete` |
| `03_gold/08_build_dm_fornecedor.py` | Gold | `gold.dm_fornecedor` |
| `03_gold/09_build_dm_evento.py` | Gold | `gold.dm_evento` |
| `03_gold/10_build_dm_frente.py` | Gold | `gold.dm_frente` |
| `03_gold/11_build_dm_uf.py` | Gold | `gold.dm_uf` |
| `03_gold/12_build_dm_tipo_despesa.py` | Gold | `gold.dm_tipo_despesa` |
| `03_gold/13_build_dm_bancada.py` | Gold | `gold.dm_bancada` |
| `03_gold/14_build_dm_responsavel_ceap.py` | Gold | `gold.dm_responsavel_ceap` |
| `03_gold/15_build_ft_despesas_ceap.py` | Gold | `gold.ft_despesas_ceap` |
| `03_gold/16_build_ft_votacoes.py` | Gold | `gold.ft_votacoes` |
| `03_gold/17_build_ft_votos.py` | Gold | `gold.ft_votos` |
| `03_gold/18_build_ft_orientacoes_bancada.py` | Gold | `gold.ft_orientacoes_bancada` |
| `03_gold/19_build_ft_atividade_parlamentar.py` | Gold | `gold.ft_atividade_parlamentar` |
| `03_gold/20_build_ft_presenca_eventos.py` | Gold | `gold.ft_presenca_eventos` |
| `03_gold/21_build_ft_frentes_membros.py` | Gold | `gold.ft_frentes_membros` |
| `04_analytics/01_build_gold_ceap_analytics.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/02_build_gold_frentes_analytics.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/03_build_gold_eventos_analytics.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/04_build_gold_votacoes_analytics.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/05_build_gold_engajamento_analytics.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/06_build_gold_parliamentary_intelligence.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/07_build_gold_sla_votacoes_streaming.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/08_build_gold_proposicoes_cdc_analytics.py` | Gold Analytics | `Not applicable / support object` |
| `04_analytics/90_build_gold_validations.py` | Gold Analytics | `Not applicable / support object` |
| `05_dlt/01_dlt_votacoes_streaming.py` | DLT / Lakeflow | `Not applicable / support object` |
| `90_common/api_client.py` | Core | `Not applicable / support object` |
| `90_common/bronze_writer.py` | Core | `Not applicable / support object` |
| `90_common/cnpj_utils.py` | Common Utilities | `Not applicable / support object` |
| `90_common/config.py` | Core | `Not applicable / support object` |
| `90_common/logger.py` | Core | `Not applicable / support object` |
| `90_common/pagination.py` | Core | `Not applicable / support object` |
| `90_common/table_logger.py` | Core | `Not applicable / support object` |
| `99_jobs/01_run_bronze_pipeline.py` | Orchestration | `bronze layer tables` |
| `99_jobs/02_run_silver_base_pipeline.py` | Orchestration | `silver_base layer tables` |
| `99_jobs/03_run_silver_curated_pipeline.py` | Orchestration | `silver_curated layer tables` |
| `99_jobs/04_run_gold_pipeline.py` | Orchestration | `gold layer tables` |
| `99_jobs/05_run_votacoes_streaming_pipeline.py` | Jobs / Orchestration | `Not applicable / support object` |

---

## `00_setup`

Environment preparation notebooks. These objects must be executed before ingestion, CDC, streaming or analytics workloads.

### `00_create_schemas.py`

**Location:** `00_setup/00_create_schemas.py`

**Layer:** Setup

**Purpose:** Creates the schemas required for the data pipeline.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Create schemas or support objects
* Prepare control and monitoring structures
* Support idempotent environment setup

---

### `01_create_control_tables.py`

**Location:** `00_setup/01_create_control_tables.py`

**Layer:** Setup

**Purpose:** Creates control and monitoring tables used by ingestion control, pipeline logging and operational governance.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Create schemas or support objects
* Prepare control and monitoring structures
* Support idempotent environment setup

---

### `02_create_streaming_objects.py`

**Location:** `00_setup/03_create_streaming_objects.py`

**Layer:** Setup

**Purpose:** Creates schemas, Delta tables and offset structures required by the voting streaming micro-batch workload.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Create schemas or support objects
* Prepare control and monitoring structures
* Support idempotent environment setup

---

### `03_create_cdc_scd2_objects.py`

**Location:** `00_setup/04_create_cdc_scd2_objects.py`

**Layer:** Setup

**Purpose:** Creates control, CDC, SCD Type 2 and analytics objects for proposition processing historization.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Create schemas or support objects
* Prepare control and monitoring structures
* Support idempotent environment setup

---

### `90_admin_test_api_connection.py`

**Location:** `00_setup/90_admin_test_api_connection.py`

**Layer:** Admin

**Purpose:** Validates connectivity with the Câmara dos Deputados Open Data API.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Create schemas or support objects
* Prepare control and monitoring structures
* Support idempotent environment setup

---

### `91_admin_reset_environment.py`

**Location:** `00_setup/91_admin_reset_environment.py`

**Layer:** Admin

**Purpose:** Resets the data pipeline environment by clearing tables and execution state.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Create schemas or support objects
* Prepare control and monitoring structures
* Support idempotent environment setup

---

### `92_admin_check_quality_tables.py`

**Location:** `00_setup/92_admin_check_quality_tables.py`

**Layer:** Admin

**Purpose:** Performs row count and basic quality checks across pipeline tables.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Create schemas or support objects
* Prepare control and monitoring structures
* Support idempotent environment setup

---

### `93_admin_export_volume_csv.py`

**Location:** `00_setup/93_admin_export_volume_csv.py`

**Layer:** Admin

**Purpose:** Exports pipeline volume and operational monitoring datasets to CSV files for external analysis, validation and reporting purposes.

**Input / Source:** `Monitoring and analytical Delta tables`

**Output / Target:** `CSV export files`

**Main responsibilities:**

* Export operational monitoring datasets
* Support external validation and analysis
* Generate CSV outputs for reporting purposes
* Facilitate operational data inspection
* Support volume and throughput analysis
* Enable data extraction for auditing and troubleshooting

---

## `01_bronze`

Raw ingestion notebooks. This layer preserves source data, operational metadata, batch lineage and replay capability.

### `01_ingest_deputados.py`

**Location:** `01_bronze/01_ingest_deputados.py`

**Layer:** Bronze

**Purpose:** Ingests deputies data from the Câmara dos Deputados API (/deputados), retrieving records per legislature defined in LEGISLATURAS_PADRAO.

**Input / Source:** `/deputados`

**Output / Target:** `bronze.deputados`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `02_ingest_deputados_detalhes.py`

**Location:** `01_bronze/02_ingest_deputados_detalhes.py`

**Layer:** Bronze

**Purpose:** Retrieves detailed information for each deputy using the /deputados/{id} endpoint based on previously ingested IDs.

**Input / Source:** `bronze.deputados`

**Output / Target:** `bronze.deputados_detalhes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `03_ingest_frentes.py`

**Location:** `01_bronze/03_ingest_frentes.py`

**Layer:** Bronze

**Purpose:** Ingests parliamentary fronts data from the Câmara dos Deputados API using the /frentes endpoint.

**Input / Source:** `/frentes`

**Output / Target:** `bronze.frentes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `04_ingest_eventos.py`

**Location:** `01_bronze/04_ingest_eventos.py`

**Layer:** Bronze

**Purpose:** Ingests legislative events data from the Câmara dos Deputados API using the /eventos endpoint.

**Input / Source:** `/eventos`

**Output / Target:** `bronze.eventos`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `05_ingest_frentes_membros.py`

**Location:** `01_bronze/05_ingest_frentes_membros.py`

**Layer:** Bronze

**Purpose:** Ingests members of parliamentary fronts from the Câmara dos Deputados API using the /frentes/{id}/membros endpoint.

**Input / Source:** `bronze.frentes`

**Output / Target:** `bronze.frentes_membros`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `06_ingest_proposicoes.py`

**Location:** `01_bronze/06_ingest_proposicoes.py`

**Layer:** Bronze

**Purpose:** Ingests legislative propositions data from the Câmara dos Deputados API using the /proposicoes endpoint.

**Input / Source:** `/proposicoes`

**Output / Target:** `bronze.proposicoes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `06b_ingest_proposicoes_file.py`

**Location:** `01_bronze/06b_ingest_proposicoes_file.py`

**Layer:** Bronze

**Purpose:** Ingests legislative propositions data from CSV files stored in the Unity Catalog volume.

**Input / Source:** `file://proposicoes`

**Output / Target:** `bronze.proposicoes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `07_ingest_despesas.py`

**Location:** `01_bronze/07_ingest_despesas.py`

**Layer:** Bronze

**Purpose:** Ingests parliamentary expense data from the Câmara dos Deputados API using the /deputados/{id}/despesas endpoint.

**Input / Source:** `bronze.deputados_detalhes`

**Output / Target:** `bronze.despesas`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `07b_ingest_despesas_file.py`

**Location:** `01_bronze/07b_ingest_despesas_file.py`

**Layer:** Bronze

**Purpose:** Ingests parliamentary expense data from CSV files stored in the Unity Catalog volume.

**Input / Source:** `file://despesas`

**Output / Target:** `bronze.despesas`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `08_ingest_orgaos.py`

**Location:** `01_bronze/08_ingest_orgaos.py`

**Layer:** Bronze

**Purpose:** Ingests legislative bodies data from the Câmara dos Deputados API using the /orgaos endpoint.

**Input / Source:** `/orgaos`

**Output / Target:** `bronze.orgaos`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `09_ingest_orgaos_membros.py`

**Location:** `01_bronze/09_ingest_orgaos_membros.py`

**Layer:** Bronze

**Purpose:** Ingests members of legislative bodies from the Câmara dos Deputados API using the /orgaos/{id}/membros endpoint.

**Input / Source:** `bronze.orgaos`

**Output / Target:** `bronze.orgaos_membros`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `09b_ingest_orgaos_membros_file.py`

**Location:** `01_bronze/09b_ingest_orgaos_membros_file.py`

**Layer:** Bronze

**Purpose:** Ingests members of legislative bodies from CSV files stored in the Unity Catalog volume.

**Input / Source:** `file://orgaos_membros`

**Output / Target:** `bronze.orgaos_membros`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `10_ingest_votacoes.py`

**Location:** `01_bronze/10_ingest_votacoes.py`

**Layer:** Bronze

**Purpose:** Ingests voting sessions data from the Câmara dos Deputados API using the /votacoes endpoint.

**Input / Source:** `/votacoes`

**Output / Target:** `bronze.votacoes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `10b_ingest_votacoes_file.py`

**Location:** `01_bronze/10b_ingest_votacoes_file.py`

**Layer:** Bronze

**Purpose:** Ingests voting sessions data from CSV files stored in the Unity Catalog volume.

**Input / Source:** `file://votacoes`

**Output / Target:** `bronze.votacoes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `11_ingest_votacoes_orientacoes.py`

**Location:** `01_bronze/11_ingest_votacoes_orientacoes.py`

**Layer:** Bronze

**Purpose:** Ingests voting guidance data from the Câmara dos Deputados API using the /votacoes/{id}/orientacoes endpoint.

**Input / Source:** `bronze.votacoes`

**Output / Target:** `bronze.votacoes_orientacoes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `11b_ingest_votacoes_orientacoes_file.py`

**Location:** `01_bronze/11b_ingest_votacoes_orientacoes_file.py`

**Layer:** Bronze

**Purpose:** Ingests voting guidance data from CSV files stored in the Unity Catalog volume.

**Input / Source:** `file://votacoes_orientacoes`

**Output / Target:** `bronze.votacoes_orientacoes`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `12_ingest_votacoes_votos.py`

**Location:** `01_bronze/12_ingest_votacoes_votos.py`

**Layer:** Bronze

**Purpose:** Ingests individual voting records from the Câmara dos Deputados API using the /votacoes/{id}/votos endpoint.

**Input / Source:** `bronze.votacoes`

**Output / Target:** `bronze.votacoes_votos`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `12b_ingest_votacoes_votos_file.py`

**Location:** `01_bronze/12b_ingest_votacoes_votos_file.py`

**Layer:** Bronze

**Purpose:** Ingests individual voting records from CSV files stored in the Unity Catalog volume.

**Input / Source:** `file://votacoes_votos`

**Output / Target:** `bronze.votacoes_votos`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `13_ingest_legislaturas.py`

**Location:** `01_bronze/13_ingest_legislaturas.py`

**Layer:** Bronze

**Purpose:** Ingests legislature reference data from the Câmara dos Deputados Open Data API.

**Input / Source:** `API Dados Abertos Câmara dos Deputados - /legislaturas`

**Output / Target:** `bronze.legislaturas`

**Main responsibilities:**

* Call the /legislaturas endpoint
* Extract legislature records from the API response
* Persist raw records with Bronze lineage metadata
* Register operational execution metrics

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `14_ingest_proposicoes_tramitacoes_cdc.py`

**Location:** `01_bronze/14_ingest_proposicoes_tramitacoes_cdc.py`

**Layer:** Bronze CDC

**Purpose:** Incremental ingestion of proposicoes tramitacoes for CDC/SCD Type 2 analysis. Consumes /proposicoes/{id}/tramitacoes and stores raw payload events with hash.

**Input / Source:** `silver_base.proposicoes`

**Output / Target:** `bronze_cdc.proposicoes_tramitacoes_raw`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `90_validate_bronze.py`

**Location:** `01_bronze/90_validate_bronze.py`

**Layer:** Validation

**Purpose:** Validates the Bronze layer after ingestion.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

### `99_ingest_votacoes_microbatch.py`

**Location:** `01_bronze/99_ingest_votacoes_microbatch.py`

**Layer:** Bronze Stream

**Purpose:** get_data Incremental micro-batch ingestion for voting events from /votacoes endpoint. Uses offset control by voting ID and persists raw payloads into Bronze Stream.

**Input / Source:** `/votacoes`

**Output / Target:** `bronze_stream.votacoes_raw`

**Main responsibilities:**

* Extract data from API or source files
* Preserve raw payload and ingestion metadata
* Generate batch lineage and record hash
* Persist replayable Bronze Delta table
* Register operational execution logs

**Key engineering notes:**

* Supports replay from raw records
* Keeps batch lineage and hash metadata
* Feeds Silver Base processing

---

## `02_silver/01_base`

Technical standardization notebooks. This layer validates, types, deduplicates and prepares reliable tables by endpoint or technical entity.

### `01_base_deputados.py`

**Location:** `02_silver/01_base/01_base_deputados.py`

**Layer:** Silver Base

**Purpose:** Performs standardization, typing, deduplication and quality validation for deputies data from the Bronze layer.

**Input / Source:** `bronze.deputados`

**Output / Target:** `silver_base.deputados`

**Main responsibilities:**

* Apply schema standardization
* Cast and normalize fields
* Remove invalid records
* Perform technical deduplication
* Add traceability columns
* Persist Silver Base Delta table
* Validate technical email quality

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `02_base_deputados_detalhes.py`

**Location:** `02_silver/01_base/02_base_deputados_detalhes.py`

**Layer:** Silver Base

**Purpose:** Performs parsing, standardization, typing, deduplication and quality validation for deputies detail data from the Bronze layer.

**Input / Source:** `bronze.deputados_detalhes`

**Output / Target:** `silver_base.deputados_detalhes`

**Main responsibilities:**

* Parse raw JSON payload
* Apply schema standardization
* Cast and normalize fields
* Remove invalid records
* Perform technical deduplication
* Preserve lineage and traceability columns
* Persist Silver Base Delta table
* Validate technical CPF quality
* Validate technical email quality
* Validate technical telephone quality
* Validate technical date quality

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `03_base_frentes.py`

**Location:** `02_silver/01_base/03_base_frentes.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates parliamentary front data from the Bronze layer.

**Input / Source:** `bronze.frentes`

**Output / Target:** `silver_base.frentes`

**Main responsibilities:**

* Parse raw JSON payload
* Apply schema standardization
* Cast identifiers
* Preserve legislature relationship
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `04_base_eventos.py`

**Location:** `02_silver/01_base/04_base_eventos.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates legislative events data from the Bronze layer.

**Input / Source:** `bronze.eventos`

**Output / Target:** `silver_base.eventos`

**Main responsibilities:**

* Parse raw JSON payload
* Apply schema standardization
* Cast dates and timestamps
* Preserve event location and related bodies
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table
* Validate technical date quality
* Validate event period consistency

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `05_base_frentes_membros.py`

**Location:** `02_silver/01_base/05_base_frentes_membros.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates parliamentary front membership data from the Bronze layer.

**Input / Source:** `bronze.frentes_membros`

**Output / Target:** `silver_base.frentes_membros`

**Main responsibilities:**

* Parse raw JSON payload
* Apply schema standardization
* Cast identifiers and dates
* Preserve deputy, party and parliamentary front relationships
* Preserve membership role information
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table
* Validate technical email quality
* Validate technical date quality
* Validate membership period consistency

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `06_base_proposicoes.py`

**Location:** `02_silver/01_base/06_base_proposicoes.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates legislative proposition data from the Bronze layer.

**Input / Source:** `bronze.proposicoes`

**Output / Target:** `silver_base.proposicoes`

**Main responsibilities:**

* Parse raw CSV-like payload embedded in JSON structure
* Apply schema standardization
* Cast identifiers, dates and timestamps
* Preserve proposition lifecycle and status relationships
* Preserve legislative organization references
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table
* Validate technical date quality
* Validate proposition lifecycle consistency

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `07_base_despesas.py`

**Location:** `02_silver/01_base/07_base_despesas.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates CEAP expenses data from the Bronze layer.

**Input / Source:** `bronze.despesas`

**Output / Target:** `silver_base.despesas`

**Main responsibilities:**

* Parse raw CSV payload stored as JSON
* Standardize expense fields
* Cast dates and monetary values
* Normalize supplier and CNPJ/CPF fields
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table
* Validate technical CPF/CNPJ quality
* Validate technical date quality

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `08_base_orgaos.py`

**Location:** `02_silver/01_base/08_base_orgaos.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates legislative organization data from the Bronze layer.

**Input / Source:** `bronze.orgaos`

**Output / Target:** `silver_base.orgaos`

**Main responsibilities:**

* Parse raw JSON payload
* Apply schema standardization
* Cast identifiers
* Preserve organization classification fields
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `09_base_orgaos_membros.py`

**Location:** `02_silver/01_base/09_base_orgaos_membros.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates legislative organization membership data from the Bronze layer.

**Input / Source:** `bronze.orgaos_membros`

**Output / Target:** `silver_base.orgaos_membros`

**Main responsibilities:**

* Parse raw CSV-like payload embedded in JSON structure
* Apply schema standardization
* Cast dates
* Preserve organization and deputy relationships
* Preserve role and membership period information
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table
* Validate technical date quality
* Validate membership period consistency

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `10_base_votacoes.py`

**Location:** `02_silver/01_base/10_base_votacoes.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates voting session data from the Bronze layer.

**Input / Source:** `bronze.votacoes`

**Output / Target:** `silver_base.votacoes`

**Main responsibilities:**

* Parse raw JSON payload
* Apply schema standardization
* Cast dates, timestamps and vote counts
* Preserve voting-event and proposition relationships
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table
* Validate technical date quality
* Validate voting period consistency

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `11_base_votacoes_orientacoes.py`

**Location:** `02_silver/01_base/11_base_votacoes_orientacoes.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates voting orientation data from the Bronze layer.

**Input / Source:** `bronze.votacoes_orientacoes`

**Output / Target:** `silver_base.votacoes_orientacoes`

**Main responsibilities:**

* Parse raw CSV-like payload embedded in JSON structure
* Apply schema standardization
* Cast identifiers where applicable
* Preserve voting and political bench relationships
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `12_base_votacoes_votos.py`

**Location:** `02_silver/01_base/12_base_votacoes_votos.py`

**Layer:** Silver Base

**Purpose:** Parses, structures, types, deduplicates and validates parliamentary voting records from the Bronze layer.

**Input / Source:** `bronze.votacoes_votos`

**Output / Target:** `silver_base.votacoes_votos`

**Main responsibilities:**

* Parse raw CSV-like payload embedded in JSON structure
* Apply schema standardization
* Cast identifiers and timestamps
* Preserve deputy and voting relationships
* Preserve party and federation information
* Preserve lineage and traceability columns
* Apply technical deduplication
* Persist Silver Base Delta table

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `13_base_legislaturas.py`

**Location:** `02_silver/01_base/13_base_legislaturas.py`

**Layer:** Silver Base

**Purpose:** Standardizes legislature reference data from Bronze.

**Input / Source:** `bronze.legislaturas`

**Output / Target:** `silver_base.legislaturas`

**Main responsibilities:**

* Read legislature records from Bronze
* Parse and type source fields
* Standardize column names
* Deduplicate by legislature identifier
* Preserve Bronze lineage metadata
* Validate Silver Base consistency
* Persist Silver Base Delta table
* Register operational execution metrics

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `14_base_fornecedores.py`

**Location:** `02_silver/01_base/14_base_fornecedores.py`

**Layer:** Silver Curated

**Purpose:** Builds the curated supplier dataset enriched with public CNPJ validation data.

**Input / Source:** `silver_base.fornecedores`

**Output / Target:** `silver_curated.fornecedores`

**Main responsibilities:**

* Read standardized supplier records from Silver Base
* Prioritize CNPJ suppliers based on CEAP usage
* Validate selected CNPJs using public API enrichment
* Create analytical supplier status and suspicion flags
* Preserve lineage metadata
* Validate curated entity consistency
* Persist Silver Curated Delta table
* Register operational execution metrics

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

### `15_base_proposicoes_tramitacoes_cdc.py`

**Location:** `02_silver/01_base/15_base_proposicoes_tramitacoes_cdc.py`

**Layer:** Silver Base CDC

**Purpose:** Normalizes raw proposicoes tramitacoes CDC payloads from Bronze into a structured Silver Base table, preparing data for SCD Type 2 processing.

**Input / Source:** `bronze_cdc.proposicoes_tramitacoes_raw`

**Output / Target:** `silver_cdc.proposicoes_tramitacoes_base`

**Main responsibilities:**

* Read raw CDC tramitacao records from Bronze CDC
* Parse JSON payload into structured columns
* Preserve CDC hash and lineage metadata
* Validate required business and CDC fields
* Persist rejected records
* Persist Silver CDC Delta table
* Register operational execution metrics

**Key engineering notes:**

* Preserves Bronze lineage
* Uses explicit validations and discarded-record accounting
* Feeds Silver Curated entities

---

## `02_silver/02_curated`

### 02_curated_deputados_detalhes.py

Status: Intentionally not implemented

Architectural Decision:
The deputy detail enrichment process was intentionally consolidated into:

01_curated_deputados.py

This decision avoids redundant curated entities and centralizes all
parliamentary identity, profile, contact, office and enrichment logic
into a single analytics-ready curated dataset.

Rationale:
- Reduce downstream joins
- Avoid duplicated curated entities
- Simplify analytical consumption
- Centralize deputy business attributes
- Improve maintainability and governance

Notes:
The original silver_base.deputados_detalhes dataset remains available
as a normalized source entity within the Silver Base layer.


### `01_curated_deputados.py`

**Location:** `02_silver/02_curated/01_curated_deputados.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates parliamentary deputy data for the Silver Curated layer.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `silver_curated.deputados`

**Main responsibilities:**

* Read and integrate Silver Base deputy datasets
* Consolidate standardized deputy attributes
* Resolve fallback attributes between source datasets
* Preserve deputy, party and legislature relationships
* Create business-friendly descriptive attributes
* Preserve technical validation and quality flags from Silver Base
* Preserve lineage, audit and processing metadata
* Validate curated-level uniqueness and consistency
* Persist a curated Delta table for Gold consumption
* Sources:
* silver_base.deputados
* silver_base.deputados_detalhes

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `03_curated_frentes.py`

**Location:** `02_silver/02_curated/03_curated_frentes.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates parliamentary front data from Silver Base.

**Input / Source:** `silver_base.frentes`

**Output / Target:** `silver_curated.frentes`

**Main responsibilities:**

* Consolidate standardized parliamentary front attributes from Silver Base
* Preserve legislature relationships
* Create analytical thematic classification flags
* Preserve complete lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `04_curated_eventos.py`

**Location:** `02_silver/02_curated/04_curated_eventos.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates legislative event data from Silver Base.

**Input / Source:** `silver_base.eventos`

**Output / Target:** `silver_curated.eventos`

**Main responsibilities:**

* Consolidate standardized event attributes from Silver Base
* Curate event type, situation and location indicators
* Create analytical event flags
* Extract primary organization information from event organization array
* Preserve event temporal attributes and technical validation flags
* Preserve lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `05_curated_frentes_membros.py`

**Location:** `02_silver/02_curated/05_curated_frentes_membros.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates parliamentary front membership data from Silver Base.

**Input / Source:** `silver_base.frentes_membros`

**Output / Target:** `silver_curated.frentes_membros`

**Main responsibilities:**

* Consolidate standardized parliamentary front membership attributes from Silver Base
* Curate membership role and status indicators
* Create analytical membership flags
* Preserve deputy, party, UF, legislature and front relationships
* Preserve membership temporal attributes and technical validation flags
* Preserve complete lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `06_curated_proposicoes.py`

**Location:** `02_silver/02_curated/06_curated_proposicoes.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates legislative proposition data from Silver Base.

**Input / Source:** `silver_base.proposicoes`

**Output / Target:** `silver_curated.proposicoes`

**Main responsibilities:**

* Consolidate standardized proposition attributes from Silver Base
* Curate legislative status and proposition type indicators
* Create analytical proposition flags
* Create proposition lifecycle indicators
* Preserve lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `07_curated_despesas.py`

**Location:** `02_silver/02_curated/07_curated_despesas.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates parliamentary expense data from Silver Base.

**Input / Source:** `silver_base.despesas`

**Output / Target:** `silver_curated.despesas`

**Main responsibilities:**

* Consolidate standardized expense attributes from Silver Base
* Preserve financial values and document references
* Preserve supplier, deputy and legislature relationships
* Preserve technical validation flags from Silver Base
* Create analytical expense flags
* Preserve lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `08_curated_orgaos.py`

**Location:** `02_silver/02_curated/08_curated_orgaos.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates legislative organization data from Silver Base.

**Input / Source:** `silver_base.orgaos`

**Output / Target:** `silver_curated.orgaos`

**Main responsibilities:**

* Consolidate standardized organization attributes from Silver Base
* Curate organization type classification indicators
* Create analytical organization flags
* Preserve organization identifiers and relationships
* Preserve lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `09_curated_orgaos_membros.py`

**Location:** `02_silver/02_curated/09_curated_orgaos_membros.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates legislative organization membership data from Silver Base.

**Input / Source:** `silver_base.orgaos_membros`

**Output / Target:** `silver_curated.orgaos_membros`

**Main responsibilities:**

* Consolidate standardized organization membership attributes from Silver Base
* Curate membership role and status indicators
* Create analytical membership flags
* Preserve deputy, party, UF and organization relationships
* Preserve membership temporal attributes and technical validation flags
* Preserve complete lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `10_curated_votacoes.py`

**Location:** `02_silver/02_curated/10_curated_votacoes.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, standardizes, enriches and validates voting session data from Silver Base.

**Input / Source:** `silver_base.votacoes`

**Output / Target:** `silver_curated.votacoes`

**Main responsibilities:**

* Consolidate standardized voting attributes from Silver Base
* Create analytical voting flags and voting result indicators
* Preserve proposition, event and organization relationships
* Preserve voting result counts
* Preserve complete lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `11_curated_votacoes_orientacoes.py`

**Location:** `02_silver/02_curated/11_curated_votacoes_orientacoes.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates voting orientation data from Silver Base.

**Input / Source:** `silver_base.votacoes_orientacoes`

**Output / Target:** `silver_curated.votacoes_orientacoes`

**Main responsibilities:**

* Consolidate standardized voting orientation attributes from Silver Base
* Normalize voting orientation values into curated analytical categories
* Create analytical orientation flags
* Preserve voting, organization and bench relationships
* Preserve lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `12_curated_votacoes_votos.py`

**Location:** `02_silver/02_curated/12_curated_votacoes_votos.py`

**Layer:** Silver Curated

**Purpose:** Consolidates, enriches and validates deputy voting records from Silver Base.

**Input / Source:** `silver_base.votacoes_votos`

**Output / Target:** `silver_curated.votacoes_votos`

**Main responsibilities:**

* Consolidate standardized deputy voting attributes from Silver Base
* Normalize vote values into curated analytical categories
* Create analytical voting behavior flags
* Preserve deputy, party, UF, legislature and voting relationships
* Preserve lineage and traceability columns
* Validate curated-level uniqueness
* Persist Delta table for Gold consumption

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `13_curated_legislaturas.py`

**Location:** `02_silver/02_curated/13_curated_legislaturas.py`

**Layer:** Silver Curated

**Purpose:** Builds the curated legislature entity for downstream dimensional modeling.

**Input / Source:** `silver_base.legislaturas`

**Output / Target:** `silver_curated.legislaturas`

**Main responsibilities:**

* Read standardized legislature records from Silver Base
* Preserve valid legislature attributes
* Create analytical period attributes
* Preserve lineage metadata
* Validate curated entity consistency
* Persist Silver Curated Delta table
* Register operational execution metrics

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `14_curated_fornecedores.py`

**Location:** `02_silver/02_curated/14_curated_fornecedores.py`

**Layer:** Silver Curated

**Purpose:** Builds the curated supplier dataset enriched with public CNPJ validation data.

**Input / Source:** `silver_base.fornecedores`

**Output / Target:** `silver_curated.fornecedores`

**Main responsibilities:**

* Read standardized supplier records from Silver Base
* Prioritize CNPJ suppliers based on CEAP usage
* Validate selected CNPJs using public API enrichment
* Create analytical supplier status and suspicion flags
* Preserve lineage metadata
* Validate curated entity consistency
* Persist Silver Curated Delta table
* Register operational execution metrics

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

### `15_curated_proposicoes_tramitacoes_scd.py`

**Location:** `02_silver/02_curated/15_curated_proposicoes_tramitacoes_scd.py`

**Layer:** Silver Curated CDC

**Purpose:** Builds the SCD Type 2 historical table for proposicoes tramitacoes.

**Input / Source:** `silver_cdc.proposicoes_tramitacoes_base`

**Output / Target:** `silver_cdc.proposicoes_tramitacoes_scd2`

**Main responsibilities:**

* Read normalized tramitacoes CDC records from Silver CDC Base
* Validate required CDC and temporal attributes
* Preserve historical changes using SCD Type 2
* Close previous active versions when changes are detected
* Insert new current versions
* Persist rejected records
* Register operational execution metrics

**Key engineering notes:**

* Applies business-friendly standardization
* Prepares reusable entities for dimensional modeling
* Feeds Gold dimensions and facts

---

## `03_gold`

Dimensional modeling notebooks. This layer builds conformed dimensions and fact tables with defined analytical grain.

### `00_create_gold_schema.py`

**Location:** `03_gold/00_create_gold_schema.py`

**Layer:** Gold

**Purpose:** Initializes the Gold analytical layer used by the dimensional Star Schema.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `gold`

**Main responsibilities:**

* Create the Gold database if it does not exist
* Establish the analytical layer for dimensional modeling
* Support Star Schema organization for business analytics
* Provide centralized storage for dimensions and fact tables
* Gold Layer Scope:
* Conformed dimensions
* Analytical fact tables
* Business-oriented aggregations
* BI-ready datasets

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `01_build_dm_data.py`

**Location:** `03_gold/01_build_dm_data.py`

**Layer:** Gold

**Purpose:** Builds the conformed date dimension for the Gold Star Schema.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `gold.dm_data`

**Main responsibilities:**

* Generate a complete analytical calendar
* Create a surrogate date key for dimensional joins
* Create date hierarchy attributes for BI consumption
* Support Star Schema modeling in the Gold layer
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `02_build_dm_legislatura.py`

**Location:** `03_gold/02_build_dm_legislatura.py`

**Layer:** Gold

**Purpose:** Builds the conformed legislature dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.legislaturas`

**Output / Target:** `gold.dm_legislatura`

**Main responsibilities:**

* Read curated legislature records
* Extract valid legislature identifiers
* Ensure one record per legislature
* Preserve Gold processing metadata
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `03_build_dm_partido.py`

**Location:** `03_gold/03_build_dm_partido.py`

**Layer:** Gold

**Purpose:** Builds the conformed political party dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.deputados`

**Output / Target:** `gold.dm_partido`

**Main responsibilities:**

* Read curated deputy records
* Extract valid political party attributes
* Ensure one record per political party
* Preserve Gold processing metadata
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `04_build_dm_deputado.py`

**Location:** `03_gold/04_build_dm_deputado.py`

**Layer:** Gold

**Purpose:** Builds the conformed deputy dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.deputados`

**Output / Target:** `gold.dm_deputado`

**Main responsibilities:**

* Read curated deputy records
* Extract analytical deputy attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per deputy
* Preserve lineage and Gold processing metadata
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `05_build_dm_proposicao.py`

**Location:** `03_gold/05_build_dm_proposicao.py`

**Layer:** Gold

**Purpose:** Builds the conformed proposition dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.proposicoes`

**Output / Target:** `gold.dm_proposicao`

**Main responsibilities:**

* Read curated proposition records
* Extract analytical proposition attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per proposition
* Preserve lineage and Gold processing metadata
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `06_build_dm_orgao.py`

**Location:** `03_gold/06_build_dm_orgao.py`

**Layer:** Gold

**Purpose:** Builds the conformed legislative body dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.orgaos`

**Output / Target:** `gold.dm_orgao`

**Main responsibilities:**

* Read curated legislative body records
* Extract analytical organization attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per legislative body
* Preserve lineage and Gold processing metadata
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `07_build_dm_gabinete.py`

**Location:** `03_gold/07_build_dm_gabinete.py`

**Layer:** Gold

**Purpose:** Builds the conformed office dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.deputados`

**Output / Target:** `gold.dm_gabinete`

**Main responsibilities:**

* Read curated deputy records
* Extract cabinet/office attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per deputy office
* Preserve lineage and Gold processing metadata
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `08_build_dm_fornecedor.py`

**Location:** `03_gold/08_build_dm_fornecedor.py`

**Layer:** Gold

**Purpose:** Builds the conformed supplier dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.fornecedores`

**Output / Target:** `gold.dm_fornecedor`

**Main responsibilities:**

* Read curated supplier records
* Extract supplier attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per supplier document
* Preserve CNPJ validation and supplier risk attributes
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `09_build_dm_evento.py`

**Location:** `03_gold/09_build_dm_evento.py`

**Layer:** Gold

**Purpose:** Builds the conformed event dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.eventos`

**Output / Target:** `gold.dm_evento`

**Main responsibilities:**

* Read curated event records
* Extract analytical event attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per event
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `10_build_dm_frente.py`

**Location:** `03_gold/10_build_dm_frente.py`

**Layer:** Gold

**Purpose:** Builds the conformed parliamentary front dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.frentes`

**Output / Target:** `gold.dm_frente`

**Main responsibilities:**

* Read curated parliamentary front records
* Extract analytical front attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per parliamentary front
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `11_build_dm_uf.py`

**Location:** `03_gold/11_build_dm_uf.py`

**Layer:** Gold

**Purpose:** Builds the conformed Brazilian state dimension for the Gold Star Schema.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `gold.dm_uf`

**Main responsibilities:**

* Read curated datasets with UF attributes
* Consolidate unique UF values
* Create a surrogate key for dimensional modeling
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics
* Sources:
* silver_curated.deputados
* silver_curated.despesas
* silver_curated.votacoes_votos
* silver_curated.frentes_membros

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `12_build_dm_tipo_despesa.py`

**Location:** `03_gold/12_build_dm_tipo_despesa.py`

**Layer:** Gold

**Purpose:** Builds the conformed expense type dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.despesas`

**Output / Target:** `gold.dm_tipo_despesa`

**Main responsibilities:**

* Read curated expense records
* Extract CEAP expense type and specification attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per expense type/specification combination
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `13_build_dm_bancada.py`

**Location:** `03_gold/13_build_dm_bancada.py`

**Layer:** Gold

**Purpose:** Builds the conformed bench/party bloc dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.votacoes_orientacoes`

**Output / Target:** `gold.dm_bancada`

**Main responsibilities:**

* Read curated voting orientation records
* Extract bancada attributes
* Create a surrogate key for dimensional modeling
* Ensure one record per bancada
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `14_build_dm_responsavel_ceap.py`

**Location:** `03_gold/14_build_dm_responsavel_ceap.py`

**Layer:** Gold

**Purpose:** Builds the conformed CEAP expense responsible dimension for the Gold Star Schema.

**Input / Source:** `silver_curated.despesas`

**Output / Target:** `gold.dm_responsavel_ceap`

**Main responsibilities:**

* Read curated CEAP expense records
* Extract CEAP responsible attributes
* Classify responsible type
* Create a sequential surrogate key for dimensional modeling
* Ensure one record per CEAP responsible
* Validate dimension consistency
* Persist the Gold Delta dimension table
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `15_build_ft_despesas_ceap.py`

**Location:** `03_gold/15_build_ft_despesas_ceap.py`

**Layer:** Gold

**Purpose:** Builds the CEAP expenses fact table for the Gold Star Schema.

**Input / Source:** `silver_curated.despesas`

**Output / Target:** `gold.ft_despesas_ceap`

**Main responsibilities:**

* Read curated expense records
* Join Gold dimensions
* Create dimensional foreign keys
* Preserve CEAP analytical measures and flags
* Persist a partitioned Gold Delta fact table
* Optimize the Delta table for analytical queries
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `16_build_ft_votacoes.py`

**Location:** `03_gold/16_build_ft_votacoes.py`

**Layer:** Gold

**Purpose:** Builds the voting summary fact table for the Gold Star Schema.

**Input / Source:** `silver_curated.votacoes`

**Output / Target:** `gold.ft_votacoes`

**Main responsibilities:**

* Read Silver Curated and/or Gold dimensional dependencies
* Define analytical grain
* Build conformed dimension or fact table
* Apply dimensional keys and metrics
* Persist optimized Gold Delta object

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `17_build_ft_votos.py`

**Location:** `03_gold/17_build_ft_votos.py`

**Layer:** Gold

**Purpose:** Builds the individual deputy votes fact table for the Gold Star Schema.

**Input / Source:** `silver_curated.votacoes_votos`

**Output / Target:** `gold.ft_votos`

**Main responsibilities:**

* Read Silver Curated and/or Gold dimensional dependencies
* Define analytical grain
* Build conformed dimension or fact table
* Apply dimensional keys and metrics
* Persist optimized Gold Delta object

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `18_build_ft_orientacoes_bancada.py`

**Location:** `03_gold/18_build_ft_orientacoes_bancada.py`

**Layer:** Gold

**Purpose:** Builds the voting orientation fact table for the Gold Star Schema.

**Input / Source:** `silver_curated.votacoes_orientacoes`

**Output / Target:** `gold.ft_orientacoes_bancada`

**Main responsibilities:**

* Read Silver Curated and/or Gold dimensional dependencies
* Define analytical grain
* Build conformed dimension or fact table
* Apply dimensional keys and metrics
* Persist optimized Gold Delta object

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `19_build_ft_atividade_parlamentar.py`

**Location:** `03_gold/19_build_ft_atividade_parlamentar.py`

**Layer:** Gold

**Purpose:** Builds the parliamentary activity fact table for the Gold Star Schema.

**Input / Source:** `silver_curated.deputados; silver_curated.despesas; silver_curated.votacoes_votos; silver_curated.frentes_membros`

**Output / Target:** `gold.ft_atividade_parlamentar`

**Main responsibilities:**

* Read Silver Curated and/or Gold dimensional dependencies
* Define analytical grain
* Build conformed dimension or fact table
* Apply dimensional keys and metrics
* Persist optimized Gold Delta object

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `20_build_ft_presenca_eventos.py`

**Location:** `03_gold/20_build_ft_presenca_eventos.py`

**Layer:** Gold

**Purpose:** Builds the parliamentary event attendance fact table for the Gold Star Schema.

**Input / Source:** `silver_curated.eventos`

**Output / Target:** `gold.ft_presenca_eventos`

**Main responsibilities:**

* Read curated event participation records
* Join conformed Gold dimensions
* Create dimensional foreign keys
* Preserve attendance analytical attributes
* Persist the Gold Delta fact table
* Optimize analytical query performance
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

### `21_build_ft_frentes_membros.py`

**Location:** `03_gold/21_build_ft_frentes_membros.py`

**Layer:** Gold

**Purpose:** Builds the parliamentary front membership fact table for the Gold Star Schema.

**Input / Source:** `silver_curated.frentes_membros; Dimensions:; gold.dm_frente; gold.dm_deputado; gold.dm_partido; gold.dm_uf; gold.dm_legislatura`

**Output / Target:** `gold.ft_frentes_membros`

**Main responsibilities:**

* Read curated parliamentary front membership records
* Join Gold conformed dimensions
* Resolve dimensional surrogate keys
* Preserve front, deputy, party, UF and legislature relationships
* Preserve membership dates, roles, status and analytical flags
* Preserve lineage and audit metadata
* Validate Gold fact consistency
* Persist a partitioned Gold Delta fact table
* Optimize the Delta table for analytical workloads
* Register operational execution metrics

**Key engineering notes:**

* Implements star schema design
* Avoids fact-to-fact relationships
* Provides analytical consumption layer

---

## `04_analytics`

Analytical product notebooks. This layer creates views, marts, indicators, validations, SLA metrics and challenge-oriented outputs.

### `01_build_gold_ceap_analytics.py`

**Location:** `04_analytics/01_build_gold_ceap_analytics.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold analytical views and marts for ceap analytics.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `02_build_gold_frentes_analytics.py`

**Location:** `04_analytics/02_build_gold_frentes_analytics.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold analytical views and marts for frentes analytics.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `03_build_gold_eventos_analytics.py`

**Location:** `04_analytics/03_build_gold_eventos_analytics.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold analytical views and marts for eventos analytics.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `04_build_gold_votacoes_analytics.py`

**Location:** `04_analytics/04_build_gold_votacoes_analytics.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold analytical views and marts for votacoes analytics.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `05_build_gold_engajamento_analytics.py`

**Location:** `04_analytics/05_build_gold_engajamento_analytics.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold analytical views and marts for engajamento analytics.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `06_build_gold_parliamentary_intelligence.py`

**Location:** `04_analytics/06_build_gold_parliamentary_intelligence.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold analytical views and marts for parliamentary intelligence.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `07_build_gold_sla_votacoes_streaming.py`

**Location:** `04_analytics/07_build_gold_sla_votacoes_streaming.py`

**Layer:** Gold Analytics

**Purpose:** Builds SLA and observability analytical objects for the voting streaming workload.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `08_build_gold_proposicoes_cdc_analytics.py`

**Location:** `04_analytics/08_build_gold_proposicoes_cdc_analytics.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold CDC analytical views and alert objects for proposition processing historization.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

### `90_build_gold_validations.py`

**Location:** `04_analytics/90_build_gold_validations.py`

**Layer:** Gold Analytics

**Purpose:** Builds Gold analytical views and marts for validations.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Read Gold facts and dimensions
* Create analytical views or marts
* Compute business indicators
* Support dashboarding and final challenge requirements

**Key engineering notes:**

* Translates challenge requirements into analytical views
* Supports dashboards and technical defense
* Uses Gold objects as trusted inputs

---

## `05_dlt`

Delta Live Tables notebook for declarative streaming quality and alert generation.

### `01_dlt_votacoes_streaming.py`

**Location:** `05_dlt/01_dlt_votacoes_streaming.py`

**Layer:** DLT / Lakeflow

**Purpose:** Declarative pipeline for voting micro-batch data. Transforms Bronze Stream into Silver and Gold streaming tables. Flow: bronze_stream.votacoes_raw -> silver_stream_votacoes_validas -> gold_stream_votacoes_alertas Important: This notebook must NOT be executed manually from a standard Databricks notebook cluster. It must be executed only through a Databricks Lakeflow / Delta Live Tables pipeline, because the dlt module is available only in the DLT runtime context. Execution: Jobs & Pipelines -> dlt_votacoes_streaming -> Run / Start

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Define streaming pipeline tables
* Apply declarative expectations
* Promote records across Bronze, Silver and Gold streaming layers
* Generate alert-ready output

**Key engineering notes:**

* Must be executed in a Databricks Lakeflow / DLT context
* Uses declarative quality expectations
* Builds streaming alert-ready outputs

---

## `90_common`

Reusable modules shared across notebooks to keep implementation consistent and avoid duplicated logic.

### `api_client.py`

**Location:** `90_common/api_client.py`

**Layer:** Core

**Purpose:** Provides reusable functions to interact with the Câmara dos Deputados Open Data API.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Provide reusable support logic
* Reduce duplicated code
* Standardize implementation across notebooks

---

### `bronze_writer.py`

**Location:** `90_common/bronze_writer.py`

**Layer:** Core

**Purpose:** Provides reusable functions to standardize the creation and persistence of Bronze layer DataFrames.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Provide reusable support logic
* Reduce duplicated code
* Standardize implementation across notebooks

---

### `cnpj_utils.py`

**Location:** `90_common/cnpj_utils.py`

**Layer:** Common Utilities

**Purpose:** Utility functions for CPF/CNPJ cleaning, classification and validation.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Provide reusable support logic
* Reduce duplicated code
* Standardize implementation across notebooks

---

### `config.py`

**Location:** `90_common/config.py`

**Layer:** Core

**Purpose:** Defines global configuration parameters used across the data pipeline.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Provide reusable support logic
* Reduce duplicated code
* Standardize implementation across notebooks

---

### `logger.py`

**Location:** `90_common/logger.py`

**Layer:** Core

**Purpose:** Provides standardized logging utilities for the data pipeline.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Provide reusable support logic
* Reduce duplicated code
* Standardize implementation across notebooks

---

### `pagination.py`

**Location:** `90_common/pagination.py`

**Layer:** Core

**Purpose:** Provides reusable functions to handle API pagination.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Provide reusable support logic
* Reduce duplicated code
* Standardize implementation across notebooks

---

### `table_logger.py`

**Location:** `90_common/table_logger.py`

**Layer:** Core

**Purpose:** Provides utilities to log pipeline execution events into Delta tables.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Provide reusable support logic
* Reduce duplicated code
* Standardize implementation across notebooks

---

## `99_jobs`

Databricks workflow orchestration notebooks used to run each layer in a controlled order.

### `01_run_bronze_pipeline.py`

**Location:** `99_jobs/01_run_bronze_pipeline.py`

**Layer:** Orchestration

**Purpose:** Executes the complete Bronze ingestion pipeline.

**Input / Source:** `01_bronze notebooks`

**Output / Target:** `bronze layer tables`

**Main responsibilities:**

* Execute Bronze ingestion notebooks in deterministic order
* Prioritize *_file ingestion versions when available
* Register orchestration execution metrics
* Register notebook-level execution status
* Stop execution on failure to avoid inconsistent downstream refreshes
* Provide operational visibility for Bronze refresh jobs

**Key engineering notes:**

* Separates execution by responsibility
* Supports operational control and dependency management
* Can be scheduled as Databricks Jobs

---

### `02_run_silver_base_pipeline.py`

**Location:** `99_jobs/02_run_silver_base_pipeline.py`

**Layer:** Orchestration

**Purpose:** Executes the complete Silver Base pipeline.

**Input / Source:** `bronze layer tables`

**Output / Target:** `silver_base layer tables`

**Main responsibilities:**

* Execute Silver Base notebooks in deterministic order
* Register orchestration execution metrics
* Register notebook-level execution status
* Stop execution on failure to avoid inconsistent downstream refreshes
* Provide operational visibility for Silver Base refresh jobs

**Key engineering notes:**

* Separates execution by responsibility
* Supports operational control and dependency management
* Can be scheduled as Databricks Jobs

---

### `03_run_silver_curated_pipeline.py`

**Location:** `99_jobs/03_run_silver_curated_pipeline.py`

**Layer:** Orchestration

**Purpose:** Executes the complete Silver Curated pipeline.

**Input / Source:** `silver_base layer tables`

**Output / Target:** `silver_curated layer tables`

**Main responsibilities:**

* Execute Silver Curated notebooks in deterministic order
* Register orchestration execution metrics
* Register notebook-level execution status
* Stop execution on failure to avoid inconsistent downstream refreshes
* Provide operational visibility for Silver Curated refresh jobs

**Key engineering notes:**

* Separates execution by responsibility
* Supports operational control and dependency management
* Can be scheduled as Databricks Jobs

---

### `04_run_gold_pipeline.py`

**Location:** `99_jobs/04_run_gold_pipeline.py`

**Layer:** Orchestration

**Purpose:** Executes the complete Gold dimensional and fact pipeline.

**Input / Source:** `silver_curated layer tables`

**Output / Target:** `gold layer tables`

**Main responsibilities:**

* Execute Gold notebooks in deterministic dependency order
* Create Gold schema before tables
* Build dimensions before facts
* Register orchestration execution metrics
* Register notebook-level execution status
* Stop execution on failure to avoid inconsistent downstream refreshes
* Provide operational visibility for Gold refresh jobs

**Key engineering notes:**

* Separates execution by responsibility
* Supports operational control and dependency management
* Can be scheduled as Databricks Jobs

---

### `05_run_votacoes_streaming_pipeline.py`

**Location:** `99_jobs/05_run_votacoes_streaming_pipeline.py`

**Layer:** Jobs / Orchestration

**Purpose:** Executes the complete real-time voting streaming pipeline.

**Input / Source:** `Not applicable / internal support`

**Output / Target:** `Not applicable / support object`

**Main responsibilities:**

* Execute notebooks in controlled order
* Separate responsibilities by layer
* Support operational orchestration and replay
* Register workflow-level execution

**Key engineering notes:**

* Separates execution by responsibility
* Supports operational control and dependency management
* Can be scheduled as Databricks Jobs

---

## Operational Notes

* The recommended execution order is `00_setup` → `01_bronze` → `02_silver/01_base` → `02_silver/02_curated` → `03_gold` → `04_analytics`.
* Streaming and CDC/SCD2 workloads use additional setup and control objects under `00_setup`, `01_bronze`, `02_silver`, `04_analytics`, `05_dlt` and `99_jobs`.
* Gold objects should be rebuilt from Silver Curated whenever possible, preserving Bronze as the replay foundation rather than as a direct analytical source.
* DLT notebooks must be executed in the Databricks Delta Live Tables / Lakeflow context, not as a regular standalone notebook when pipeline semantics are required.
* The project uses `records_read`, `records_written` and `records_discarded` as the standard execution volume metrics.
