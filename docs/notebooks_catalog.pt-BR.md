# Catálogo de Notebooks

Catálogo técnico do repositório `camara-data-pipeline`. Este documento explica a finalidade, localização, camada, entradas, saídas e responsabilidades de cada notebook ou módulo reutilizável do projeto.

O catálogo segue a arquitetura Medallion do projeto: Bronze → Prata Base → Prata Selecionada → Ouro → Analytics, com suporte adicional para CDC/SCD Tipo 2, micro-lotes de streaming, tabelas Delta Live e orquestração de fluxos de trabalho do Databricks.

---

## Fluxo de Execução

```texto
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

Cargas de trabalho opcionais/avançadas:
00_setup + 01_bronze + 02_silver + 04_analytics + 05_dlt + 99_jobs
para pipelines de CDC/SCD2, micro-lotes de streaming, monitoramento de SLA e DLT.

```

---

## Resumo das Camadas

| Camada | Responsabilidade |
|---|---|
| `00_setup` | Cria schemas, tabelas de controle, estruturas de monitoramento, objetos de streaming e objetos de suporte para CDC/SCD2. |
| `01_bronze` | Realiza a ingestão de dados brutos de APIs/arquivos, preservando payloads, metadados, linhagem de batch e capacidade de replay. |
| `02_silver/01_base` | Executa parsing técnico, tipagem, padronização, validação de qualidade e deduplicação. |
| `02_silver/02_curated` | Constrói entidades de negócio reutilizáveis com enriquecimento, regras de fallback e padronização curada. |
| `03_gold` | Constrói dimensões conformadas e tabelas fato seguindo um modelo Star Schema. |
| `04_analytics` | Constrói views analíticas, marts, validações, monitoramento de SLA e produtos de dados orientados a desafios. |
| `05_dlt` | Define pipeline de streaming com Delta Live Tables utilizando expectativas declarativas. |
| `90_common` | Armazena módulos utilitários reutilizáveis para acesso a APIs, paginação, logging, escrita e helpers de validação. |
| `99_jobs` | Orquestra execuções por camada utilizando Databricks Workflows. |

---

---


## Índice Completo de Notebooks

| Caminho | Camada | Saída / Destino |
|---|---|---|
| `00_setup/00_create_schemas.py` | Setup | `Não aplicável / objeto de suporte` |
| `00_setup/01_create_control_tables.py` | Setup | `Não aplicável / objeto de suporte` |
| `00_setup/02_create_streaming_objects.py` | Setup | `Não aplicável / objeto de suporte` |
| `00_setup/03_create_cdc_scd2_objects.py` | Setup | `Não aplicável / objeto de suporte` |
| `00_setup/90_admin_test_api_connection.py` | Admin | `Não aplicável / objeto de suporte` |
| `00_setup/91_admin_reset_environment.py` | Admin | `Não aplicável / objeto de suporte` |
| `00_setup/92_admin_check_quality_tables.py` | Admin | `Não aplicável / objeto de suporte` |
| `00_setup/93_admin_export_volume_csv.py` | Admin | `Não aplicável / objeto de suporte` |
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
| `01_bronze/90_validate_bronze.py` | Validação | `Não aplicável / objeto de suporte` |
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
| `03_gold/01_build_dm_tempo.py` | Gold | `gold.dm_data` |
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
| `04_analytics/01_build_gold_ceap_analytics.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/02_build_gold_frentes_analytics.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/03_build_gold_eventos_analytics.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/04_build_gold_votacoes_analytics.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/05_build_gold_engajamento_analytics.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/06_build_gold_parliamentary_intelligence.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/07_build_gold_sla_votacoes_streaming.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/08_build_gold_proposicoes_cdc_analytics.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `04_analytics/90_build_gold_validations.py` | Gold Analytics | `Não aplicável / objeto de suporte` |
| `05_dlt/01_dlt_votacoes_streaming.py` | DLT / Lakeflow | `Não aplicável / objeto de suporte` |
| `90_common/api_client.py` | Core | `Não aplicável / objeto de suporte` |
| `90_common/bronze_writer.py` | Core | `Não aplicável / objeto de suporte` |
| `90_common/cnpj_utils.py` | Utilitários Comuns | `Não aplicável / objeto de suporte` |
| `90_common/config.py` | Core | `Não aplicável / objeto de suporte` |
| `90_common/logger.py` | Core | `Não aplicável / objeto de suporte` |
| `90_common/pagination.py` | Core | `Não aplicável / objeto de suporte` |
| `90_common/table_logger.py` | Core | `Não aplicável / objeto de suporte` |
| `99_jobs/01_run_bronze_pipeline.py` | Orquestração | `tabelas da camada bronze` |
| `99_jobs/02_run_silver_base_pipeline.py` | Orquestração | `tabelas da camada silver_base` |
| `99_jobs/03_run_silver_curated_pipeline.py` | Orquestração | `tabelas da camada silver_curated` |
| `99_jobs/04_run_gold_pipeline.py` | Orquestração | `tabelas da camada gold` |
| `99_jobs/05_run_votacoes_streaming_pipeline.py` | Jobs / Orquestração | `Não aplicável / objeto de suporte` |

---

## `00_setup`

Notebooks de preparação do ambiente. Esses objetos devem ser executados antes das cargas de trabalho de ingestão, CDC, streaming ou análise.

### `00_create_schemas.py`

**Local:** `00_setup/00_create_schemas.py`

**Camada:** Setup

**Finalidade:** Cria os esquemas necessários para o pipeline de dados.

**Entrada/Origem:** `Não aplicável/suporte interno`

**Saída/Destino:** `Não aplicável/objeto de suporte`

**Principais responsabilidades:**

* Criar esquemas ou objetos de suporte
* Preparar estruturas de controle e monitoramento
* Dar suporte à configuração de ambiente idempotente

---

### `01_create_control_tables.py`

**Local:** `00_setup/01_create_control_tables.py`

**Camada:** Setup

**Finalidade:** Cria tabelas de controle e monitoramento usadas pelo controle de ingestão, registro de pipelines e governança operacional.

**Entrada/Origem:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Criar esquemas ou objetos de suporte
* Preparar estruturas de controle e monitoramento
* Dar suporte à configuração de ambiente idempotente

---

### `02_create_streaming_objects.py`

**Local:** `00_setup/03_create_streaming_objects.py`

**Camada:** Setup

**Objetivo:** Cria esquemas, tabelas Delta e estruturas de deslocamento necessárias para a carga de trabalho de micro-lotes de streaming de votação.

**Entrada/Fonte:** `Não aplicável/suporte interno`

**Saída/Destino:** `Não aplicável/objeto de suporte`

**Principais responsabilidades:**

* Criar esquemas ou objetos de suporte
* Preparar estruturas de controle e monitoramento
* Dar suporte à configuração de ambiente idempotente

---

### `03_create_cdc_scd2_objects.py`

**Local:** `00_setup/04_create_cdc_scd2_objects.py`

**Camada:** Setup

**Objetivo:** Cria objetos de controle, CDC, SCD Tipo 2 e de análise para a historização do processamento de proposições.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Criar esquemas ou objetos de suporte
* Preparar estruturas de controle e monitoramento
* Dar suporte à configuração de ambiente idempotente

---

### `90_admin_test_api_connection.py`

**Local:** `00_setup/90_admin_test_api_connection.py`

**Camada:** Admin

**Objetivo:** Valida a conectividade com a API de Dados Abertos da Câmara dos Deputados.

**Entrada/Origem:** `Não aplicável/suporte interno`

**Saída/Destino:** `Não aplicável/objeto de suporte`

**Principais responsabilidades:**

* Criar esquemas ou objetos de suporte
* Preparar estruturas de controle e monitoramento
* Suportar a configuração de ambiente idempotente

---

### `91_admin_reset_environment.py`

**Local:** `00_setup/91_admin_reset_environment.py`

**Camada:** Administração

**Finalidade:** Reinicia o ambiente do pipeline de dados, limpando tabelas e o estado de execução.

**Entrada/Origem:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Criar esquemas ou objetos de suporte
* Preparar estruturas de controle e monitoramento
* Dar suporte à configuração de ambiente idempotente

---

### `92_admin_check_quality_tables.py`

**Local:** `00_setup/92_admin_check_quality_tables.py`

**Camada:** Administração

**Objetivo:** Realiza a contagem de linhas e verificações básicas de qualidade em tabelas de pipeline.

**Entrada/Origem:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Criar esquemas ou objetos de suporte
* Preparar estruturas de controle e monitoramento
* Dar suporte à configuração de ambiente idempotente

---

### `93_admin_export_volume_csv.py`

**Localização:** `00_setup/93_admin_export_volume_csv.py`

**Camada:** Admin

**Objetivo:** Exporta datasets de volume do pipeline e monitoramento operacional para arquivos CSV utilizados em análises externas, validações e reporting operacional.

**Entrada / Origem:** `Tabelas Delta analíticas e de monitoramento`

**Saída / Destino:** `Arquivos CSV de exportação`

**Principais responsabilidades:**

* Exportar datasets de monitoramento operacional
* Suportar validações e análises externas
* Gerar arquivos CSV para reporting
* Facilitar inspeção operacional dos dados
* Apoiar análises de volume e throughput
* Permitir extração de dados para auditoria e troubleshooting

---

## `01_bronze`

Cadernos de ingestão crua. Essa camada preserva os dados de origem, os metadados operacionais, a linhagem do lote e a capacidade de reprodução.

### `01_ingest_deputados.py`

**Local:** `01_bronze/01_ingest_deputados.py`

**Camada:** Bronze

**Objetivo:** ingere dados de deputados da API da Câmara dos Deputados (/deputados), recuperando registros por legislatura definida em LEGISLATURAS_PADRAO.

**Entrada/Fonte:** `/deputados`

**Saída/Destino:** `bronze.deputados`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `02_ingest_deputados_detalhes.py`

**Local:** `01_bronze/02_ingest_deputados_detalhes.py`

**Camada:** Bronze

**Objetivo:** Recupera informações detalhadas para cada delegado usando o endpoint /deputados/{id} com base nos IDs previamente ingeridos.

**Entrada/Fonte:** `bronze.deputados`

**Saída/Destino:** `bronze.deputados_detalhes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `03_ingest_frentes.py`

**Local:** `01_bronze/03_ingest_frentes.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de frentes parlamentares da API da Câmara dos Deputados usando o endpoint /frentes.

**Entrada/Fonte:** `/frentes`

**Saída/Destino:** `bronze.frentes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `04_ingest_eventos.py`

**Local:** `01_bronze/04_ingest_eventos.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de eventos legislativos da API da Câmara dos Deputados usando o endpoint /eventos.

**Entrada/Fonte:** `/eventos`

**Saída/Destino:** `bronze.eventos`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `05_ingest_frentes_membros.py`

**Local:** `01_bronze/05_ingest_frentes_membros.py`

**Camada:** Bronze

**Objetivo:** Ingerir membros de frentes parlamentares da API da Câmara dos Deputados usando o endpoint /frentes/{id}/membros.

**Entrada/Fonte:** `bronze.frentes`

**Saída/Destino:** `bronze.frentes_membros`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `06_ingest_proposicoes.py`

**Local:** `01_bronze/06_ingest_proposicoes.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de propostas legislativas da API da Câmara dos Deputados usando o endpoint /proposicoes.

**Entrada/Fonte:** `/proposicoes`

**Saída/Destino:** `bronze.proposicoes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Bronze Delta reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `06b_ingest_proposicoes_file.py`

**Local:** `01_bronze/06b_ingest_proposicoes_file.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de propostas legislativas de arquivos CSV armazenados no volume do Catálogo Unity.

**Entrada/Origem:** `file://proposicoes`

**Saída/Destino:** `bronze.proposicoes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `07_ingest_despesas.py`

**Local:** `01_bronze/07_ingest_despesas.py`

**Camada:** Bronze

**Purpose:** ingere dados de despesas parlamentares da API da Câmara dos Deputados usando o endpoint /deputados/{id}/despesas.

**Entrada/Fonte:** `bronze.deputados_detalhes`

**Saída/Destino:** `bronze.despesas`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `07b_ingest_despesas_file.py`

**Local:** `01_bronze/07b_ingest_despesas_file.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de despesas parlamentares de arquivos CSV armazenados no volume do Catálogo Unity.

**Entrada/Fonte:** `file://despesas`

**Saída/Destino:** `bronze.despesas`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `08_ingest_orgaos.py`

**Local:** `01_bronze/08_ingest_orgaos.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de órgãos legislativos da API da Câmara dos Deputados usando o endpoint /orgaos.

**Entrada/Fonte:** `/orgaos`

**Saída/Destino:** `bronze.orgaos`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `09_ingest_orgaos_membros.py`

**Local:** `01_bronze/09_ingest_orgaos_membros.py`

**Camada:** Bronze

**Objetivo:** Ingerir membros de órgãos legislativos da API da Câmara dos Deputados usando o endpoint /orgaos/{id}/membros.

**Entrada/Fonte:** `bronze.orgaos`

**Saída/Destino:** `bronze.orgaos_membros`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar os logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `09b_ingest_orgaos_membros_file.py`

**Local:** `01_bronze/09b_ingest_orgaos_membros_file.py`

**Camada:** Bronze

**Objetivo:** Ingerir membros de órgãos legislativos a partir de arquivos CSV armazenados no volume do Catálogo Unity.

**Entrada/Fonte:** `file://orgaos_membros`

**Saída/Destino:** `bronze.orgaos_membros`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `10_ingest_votacoes.py`

**Local:** `01_bronze/10_ingest_votacoes.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de sessões de votação da API da Câmara dos Deputados usando o endpoint /votacoes.

**Entrada/Origem:** `/votacoes`

**Saída/Destino:** `bronze.votacoes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `10b_ingest_votacoes_file.py`

**Local:** `01_bronze/10b_ingest_votacoes_file.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de sessões de votação de arquivos CSV armazenados no volume do Catálogo Unity.

**Entrada/Fonte:** `file://votacoes`

**Saída/Destino:** `bronze.votacoes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `11_ingest_votacoes_orientacoes.py`

**Local:** `01_bronze/11_ingest_votacoes_orientacoes.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de orientação eleitoral da API da Câmara dos Deputados usando o endpoint /votacoes/{id}/orientacoes.

**Entrada/Fonte:** `bronze.votacoes`

**Saída/Destino:** `bronze.votacoes_orientacoes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar os dados brutos
Carga útil e metadados de ingestão
* Gera linhagem de lote e hash de registro
* Persiste a tabela Delta Bronze reproduzível
* Registra logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem de lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `11b_ingest_votacoes_orientacoes_file.py`

**Local:** `01_bronze/11b_ingest_votacoes_orientacoes_file.py`

**Camada:** Bronze

**Finalidade:** Ingere dados de orientação de votação de arquivos CSV armazenados no volume do Catálogo Unity.

**Entrada/Fonte:** `file://votacoes_orientacoes`

**Saída/Destino:** `bronze.votacoes_orientacoes`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `12_ingest_votacoes_votos.py`

**Local:** `01_bronze/12_ingest_votacoes_votos.py`

**Camada:** Bronze

**Objetivo:** Ingerir registros de votação individuais da API da Câmara dos Deputados usando o endpoint /votacoes/{id}/votos.

**Entrada/Fonte:** `bronze.votacoes`

**Saída/Destino:** `bronze.votacoes_votos`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Prata

---

### `12b_ingest_votacoes_votos_file.py`

**Local:** `01_bronze/12b_ingest_votacoes_votos_file.py`

**Camada:** Bronze

**Objetivo:** Ingerir registros de votação individuais de arquivos CSV armazenados no volume do Catálogo Unity.

**Entrada/Fonte:** `file://votacoes_votos`

**Saída/Destino:** `bronze.votacoes_votos`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Camada Base Prata

---

### `13_ingest_legislaturas.py`

**Local:** `01_bronze/13_ingest_legislaturas.py`

**Camada:** Bronze

**Objetivo:** Ingerir dados de referência legislativa da API de Dados Abertos da Câmara dos Deputados.

**Entrada/Fonte:** `API Dados Abertos Câmara dos Deputados - /legislaturas`

**Saída/Destino:** `bronze.legislaturas`

**Principais responsabilidades:**

* Chamar o endpoint /legislaturas
* Extrair registros legislativos da resposta da API
* Persistir registros brutos com metadados de linhagem Bronze
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém metadados de linhagem e hash de lotes
* Alimenta o processamento da Base Prata

---

### `14_ingest_proposicoes_tramitacoes_cdc.py`

**Local:** `01_bronze/14_ingest_proposicoes_tramitacoes_cdc.py`

**Camada:** Bronze CDC

**Objetivo:** Ingestão incremental de propostas de tramitação para análise CDC/SCD Tipo 2. Consome /proposicoes/{id}/tramitacoes e armazena eventos de carga útil bruta com hash.

**Entrada/Fonte:** `silver_base.propoficies`

**Saída/Destino:** `bronze_cdc.propoficies_tramitacoes_raw`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Delta Bronze reproduzível
* Registrar os logs de execução operacional

**Principais observações técnicas:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Silver

---

### `90_validate_bronze.py`

**Local:** `01_bronze/90_validate_bronze.py`

**Camada:** Validação

**Objetivo:** Valida a camada Bronze após a ingestão.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar a carga útil bruta e os metadados de ingestão
* Gerar a linhagem do lote e o hash do registro
* Persistir a tabela Bronze Delta reproduzível
* Registrar logs de execução operacional

**Principais observações de engenharia:**

* Suporta reprodução a partir de registros brutos
* Mantém a linhagem do lote e os metadados de hash
* Alimenta o processamento da Base Silver

---

### `99_ingest_votacoes_microbatch.py`

**Local:** `01_bronze/99_ingest_votacoes_microbatch.py`

**Camada:** Bronze Stream

**Objetivo:** get_data Ingestão incremental de micro-lotes para eventos de votação do endpoint /votacoes. Utiliza controle de deslocamento por ID de votação e persiste as cargas úteis brutas no Bronze Stream.

**Entrada/Origem:** `/votacoes`

**Saída/Destino:** `bronze_stream.votacoes_raw`

**Principais responsabilidades:**

* Extrair dados da API ou de arquivos de origem
* Preservar o payload bruto e os metadados de ingestão
* Gerar lotes

## `02_silver/01_base`

Notebooks de padronização técnica. Esta camada valida, tipifica, remove duplicatas e prepara tabelas confiáveis ​​por endpoint ou entidade técnica.

### `01_base_deputados.py`

**Localização:** `02_silver/01_base/01_base_deputados.py`

**Camada:** Base Prata

**Finalidade:** Realiza a padronização, tipagem, remoção de duplicatas e validação de qualidade dos dados de delegados da camada Bronze.

**Entrada/Fonte:** `bronze.deputados`

**Saída/Destino:** `silver_base.deputados`

**Principais responsabilidades:**

* Aplicar padronização de esquema
* Converter e normalizar campos
* Remover registros inválidos
* Realizar deduplicação técnica
* Adicionar colunas de rastreabilidade
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica dos e-mails

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Utiliza validações explícitas e contabilização de registros descartados
* Alimenta entidades Curated da Base Prata

---

### `02_base_deputados_detalhes.py`

**Local:** `02_silver/01_base/02_base_deputados_detalhes.py`

**Camada:** Base Prata

**Objetivo:** Realiza análise sintática, padronização, tipagem, deduplicação e validação de qualidade para dados de detalhes de deputados da camada Bronze.

**Entrada/Origem:** `bronze.deputados_detalhes`

**Saída/Destino:** `silver_base.deputados_detalhes`

**Principais responsabilidades:**

* Analisar o payload JSON bruto
* Aplicar padronização de esquema
* Converter e normalizar campos
* Remover registros inválidos
* Realizar deduplicação técnica
* Preservar colunas de linhagem e rastreabilidade
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica do CPF
* Validar a qualidade técnica do e-mail
* Validar a qualidade técnica do telefone
* Validar a qualidade técnica da data

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e contabilização de registros descartados
* Alimenta entidades curadas da Base Prata

---

### `03_base_frentes.py`

**Local:** `02_silver/01_base/03_base_frentes.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipos, Remove duplicatas e valida dados de eventos legislativos da camada Bronze.

**Entrada/Fonte:** `bronze.frentes`

**Saída/Destino:** `silver_base.frentes`

**Principais responsabilidades:**

* Analisar o payload JSON bruto
* Aplicar padronização de esquema
* Converter identificadores
* Preservar o relacionamento entre legislaturas
* Preservar colunas de linhagem e rastreabilidade
* Aplicar desduplicação técnica
* Persistir a tabela Delta da Base Prata

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e contabilização de registros descartados
* Alimenta entidades curadas da Base Prata

---

### `04_base_eventos.py`

**Local:** `02_silver/01_base/04_base_eventos.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipifica, remove duplicatas e valida dados de eventos legislativos da camada Bronze.

**Entrada/Origem:** `bronze.eventos`

**Saída/Destino:** `silver_base.eventos`

**Principais responsabilidades:**

* Analisar o payload JSON bruto
* Aplicar padronização de esquema
* Converter datas e timestamps
* Preservar a localização do evento e os corpos relacionados
* Preservar as colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica das datas
* Validar a consistência do período do evento

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e contabilização de registros descartados
* Alimenta as entidades Curated da Base Prata

---

### `05_base_frentes_membros.py`

**Local:** `02_silver/01_base/05_base_frentes_membros.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipifica, deduplica e valida a composição da frente parlamentar Dados da camada de bronze.

**Entrada/Origem:** `bronze.frentes_membros`

**Saída/Destino:** `silver_base.frentes_membros`

**Principais responsabilidades:**

* Analisar o payload JSON bruto
* Aplicar padronização de esquema
* Converter identificadores e datas
* Preservar os relacionamentos entre deputados, partidos e frentes parlamentares
* Preservar informações sobre funções de membros
* Preservar colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica dos e-mails
* Validar a qualidade técnica das datas
* Validar a consistência do período de associação

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e contabilização de registros descartados
* Alimenta entidades curadas da Base Prata

---

### `06_base_proposicoes.py`

**Local:** `02_silver/01_base/06_base_proposicoes.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipifica, remove duplicatas e valida dados de propostas legislativas da camada Bronze.

**Entrada/Fonte:** `bronze.proposicoes`

**Saída/Destino:** `silver_base.proposicoes`

**Principais responsabilidades:**

* Analisar dados brutos em formato CSV incorporados em estrutura JSON
* Aplicar padronização de esquema
* Converter identificadores, datas e carimbos de data/hora
* Preservar o ciclo de vida e os relacionamentos de status das propostas
* Preservar referências à organização legislativa
* Preservar colunas de linhagem e rastreabilidade
* Aplicar remoção de duplicatas técnicas
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica dos dados
* Validar a consistência do ciclo de vida das propostas


**Principais observações de engenharia:**

* Preserva a linhagem Bronze
* Utiliza validações explícitas e contabilização de registros descartados
* Alimenta entidades Silver Curated

---

### `07_base_despesas.py`

**Local:** `02_silver/01_base/07_base_despesas.py`

**Camada:** Base Silver

**Finalidade:** Analisa, estrutura, tipifica, remove duplicatas e valida dados de despesas CEAP da camada Bronze.

**Entrada/Origem:** `bronze.despesas`

**Saída/Destino:** `silver_base.despesas`

**Principais responsabilidades:**

* Analisar dados CSV brutos armazenados como JSON
* Padronizar campos de despesas
* Converter datas e valores monetários
* Normalizar campos de fornecedor e CNPJ/CPF
* Preservar colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica do CPF/CNPJ
* Validar a qualidade técnica das datas

**Notas importantes de engenharia:**

* Preserva a linhagem do Bronze
* Utiliza validações explícitas e contabilização de registros descartados
* Alimenta entidades curadas da Base Prata

---

### `08_base_orgaos.py`

**Local:** `02_silver/01_base/08_base_orgaos.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipos, Remove dados duplicados e valida dados da organização legislativa da camada Bronze.

**Entrada/Fonte:** `bronze.orgaos`

**Saída/Destino:** `silver_base.orgaos`

**Principais responsabilidades:**

* Analisar o payload JSON bruto
* Aplicar padronização de esquema
* Converter identificadores
* Preservar os campos de classificação da organização
* Preservar as colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e contabilização de registros descartados
* Alimenta as entidades Curated da Base Prata

---

### `09_base_orgaos_membros.py`

**Local:** `02_silver/01_base/09_base_orgaos_membros.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipifica, deduplica e valida os dados de associação de organizações legislativas da camada Bronze.

**Entrada/Origem:** `bronze.orgaos_membros`

**Saída/Destino:** `silver_base.orgaos_membros`

**Principais responsabilidades:**

* Analisar dados brutos em formato CSV incorporados em uma estrutura JSON
* Aplicar padronização de esquema
* Converter datas
* Preservar relações entre organizações e representantes
* Preservar informações sobre funções e períodos de associação
* Preservar colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica das datas
* Validar a consistência dos períodos de associação

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Utiliza validações explícitas e contabilização de registros descartados
* Alimenta entidades curadas da Base Prata

---

### `10_base_votacoes.py`

**Local:** `02_silver/01_base/10_base_votacoes.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipifica, remove duplicados e valida dados de sessões de votação da camada Bronze.

**Entrada/Origem:** `bronze.votacoes`

**Saída/Destino:** `silver_base.votacoes`

**Principais responsabilidades:**

* Analisar o payload JSON bruto
* Aplicar padronização de esquema
* Converter datas, timestamps e contagens de votos
* Preservar as relações entre eventos de votação e proposições
* Preservar as colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata
* Validar a qualidade técnica das datas
* Validar a consistência do período de votação

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e contabilização de registros descartados
* Alimenta as entidades Curated da Base Prata

---

### `11_base_votacoes_orientacoes.py`

**Local:** `02_silver/01_base/11_base_votacoes_orientacoes.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipifica, remove duplicados e valida dados de orientação de voto da camada Bronze.

**Entrada/Origem:** `bronze.votacoes_orientacoes`

**Saída/Destino:** `silver_base.votacoes_orientacoes`

**Principais responsabilidades:**

* Analisar dados brutos em formato CSV incorporados em uma estrutura JSON
* Aplicar padronização de esquema
* Converter identificadores quando aplicável
* Preservar relações de votação e de bancada política
* Preservar colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Utiliza validações explícitas e contabilização de registros descartados
* Alimenta entidades curadas da Base Prata

---

### `12_base_votacoes_votos.py`

**Local:** `02_silver/01_base/12_base_votacoes_votos.py`

**Camada:** Base Prata

**Objetivo:** Analisa, estrutura, tipifica e deduplica e valida os registros de votação parlamentar da camada Bronze.

**Entrada/Origem:** `bronze.votacoes_votos`

**Saída/Destino:** `silver_base.votacoes_votos`

**Principais responsabilidades:**

* Analisar dados brutos em formato CSV incorporados em uma estrutura JSON
* Aplicar padronização de esquema
* Converter identificadores e carimbos de data/hora
* Preservar relações de deputados e votantes
* Preservar informações de partidos e federações
* Preservar colunas de linhagem e rastreabilidade
* Aplicar deduplicação técnica
* Persistir a tabela Delta da Base Prata

**Notas importantes de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e descaracterização

Contabilidade de registros ded
* Alimenta entidades selecionadas do Silver

---

### `13_base_legislaturas.py`

**Localização:** `02_silver/01_base/13_base_legislaturas.py`

**Camada:** Base Silver

**Finalidade:** Padroniza os dados de referência legislativa do Bronze.

**Entrada/Fonte:** `bronze.legislaturas`

**Saída/Destino:** `silver_base.legislaturas`

**Principais responsabilidades:**

* Ler registros de legislaturas do Bronze
* Analisar e tipar os campos de origem
* Padronizar os nomes das colunas
* Remover duplicatas por identificador de legislatura
* Preservar os metadados de linhagem do Bronze
* Validar a consistência com o Silver Base
* Persistir a tabela Delta do Silver Base
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Preserva a linhagem do Bronze
* Utiliza validações explícitas e contabilização de registros descartados
* Alimenta entidades do Silver Curated

---

### `14_base_fornecedores.py`

**Local:** `02_silver/01_base/14_base_fornecedores.py`

**Camada:** Silver Curated

**Objetivo:** Constrói o conjunto de dados de fornecedores curados, enriquecido com dados públicos de validação do CNPJ.

**Entrada/Fonte:** `silver_base.fornecedores`

**Saída/Destino:** `silver_curated.fornecedores`

**Principais responsabilidades:**

* Ler registros padronizados de fornecedores da Silver Base
* Priorizar fornecedores CNPJ com base no uso do CEAP
* Validar CNPJs selecionados usando enriquecimento de API pública
* Criar indicadores analíticos de status e suspeita de fornecedores
* Preservar metadados de linhagem
* Validar a consistência das entidades curadas
* Persistir a tabela Delta da Silver Curated
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Preserva a linhagem Bronze
* Usa validações explícitas e contabilização de registros descartados
* Alimenta entidades da Silver Curated

---

### `15_base_proposicoes_tramitacoes_cdc.py`

**Local:** `02_silver/01_base/15_base_proposicoes_tramitacoes_cdc.py`

**Camada:** CDC Base Prata

**Objetivo:** Normaliza os dados brutos de encaminhamento de propostas do CDC Bronze em uma tabela estruturada da Base Prata, preparando os dados para o processamento SCD Tipo 2.

**Entrada/Fonte:** `bronze_cdc.proposicoes_tramitacoes_raw`

**Saída/Destino:** `silver_cdc.proposicoes_tramitacoes_base`

**Principais responsabilidades:**

* Ler registros brutos de encaminhamento de propostas do CDC Bronze
* Analisar o payload JSON em colunas estruturadas
* Preservar o hash e os metadados de linhagem do CDC
* Validar os campos de negócios e do CDC obrigatórios
* Persistir registros rejeitados
* Persistir a tabela Delta do CDC Prata
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Preserva a linhagem do Bronze
* Utiliza validações explícitas e contabilização de registros descartados
* Alimenta entidades curadas do Prata

---

## `02_silver/02_curated`

### 02_curated_deputados_detalhes.py

Status: Intencionalmente não implementado

Decisão Arquitetural:
O processo de enriquecimento de detalhes dos deputados foi intencionalmente consolidado em:

01_curated_deputados.py

Essa decisão evita entidades curadas redundantes e centraliza toda a
lógica de identidade parlamentar, perfil, contato, gabinete e enriquecimento
em um único conjunto de dados curado pronto para análise.

Justificativa:
- Reduzir junções subsequentes
- Evitar entidades curadas duplicadas
- Simplificar o consumo analítico
- Centralizar os atributos de negócios dos deputados
- Melhorar a manutenção e a governança

Observações:
O conjunto de dados original silver_base.deputados_detalhes permanece disponível
como uma entidade de origem normalizada na camada Silver Base.

### `01_curated_deputados.py`

**Localização:** `02_silver/02_curated/01_curated_deputados.py`

**Camada:** Silver Curated

**Finalidade:** Consolida, enriquece e valida os dados dos deputados parlamentares para a camada Silver Curated.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `silver_curated.deputados`

**Principais responsabilidades:**

* Ler e integrar conjuntos de dados de deputados do Silver Base
* Consolidar atributos padronizados de deputados
* Resolver atributos alternativos entre os conjuntos de dados de origem
* Preservar as relações entre deputados, partidos e legislaturas
* Criar atributos descritivos adequados para uso comercial
* Preservar a validação técnica e os indicadores de qualidade do Silver Base
* Preservar metadados de linhagem, auditoria e processamento
* Validar a unicidade e a consistência em nível de curadoria
* Persistir uma tabela Delta curada para consumo no Gold
* Fontes:
* silver_base.deputados
* silver_base.deputados_detalhes

**Principais observações de engenharia:**

* Aplica padronização adequada para uso comercial
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `03_curated_frentes.py`

**Localização:** `02_silver/02_curated/03_curated_frentes.py`

**Camada:** Silver Curated

**Finalidade:** Consolida, enriquece e valida dados de frentes parlamentares da Silver Base.

**Entrada/Fonte:** `silver_base.frentes`

**Saída/Destino:** `silver_curated.frentes`

**Principais responsabilidades:**

* Consolidar atributos padronizados de frentes parlamentares da Silver Base
* Preservar relações entre legislaturas
* Criar indicadores de classificação temática analítica
* Preservar colunas completas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo em Gold

**Principais observações técnicas:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos em Gold

---

### `04_curated_eventos.py`

**Local:** `02_silver/02_curated/04_curated_eventos.py`

**Camada:** Silver Curated

**Objetivo:** Consolida, enriquece e valida dados de eventos legislativos da Silver Base.

**Entrada/Fonte:** `silver_base.eventos`

**Saída/Destino:** `silver_curated.eventos`

**Principais responsabilidades:**

* Consolidar atributos de eventos padronizados da Base de Dados Silver
* Selecionar indicadores de tipo, situação e localização de eventos
* Criar indicadores analíticos para eventos
* Extrair informações da organização principal do array de organização de eventos
* Preservar atributos temporais e indicadores de validação técnica de eventos
* Preservar colunas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo no Gold

**Principais observações de engenharia:**

* Aplica padronização amigável ao negócio
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `05_curated_frentes_membros.py`

**Local:** `02_silver/02_curated/05_curated_frentes_membros.py`

**Camada:** Silver Curated

**Objetivo:** Consolida, Enriquece e valida dados de membros de frentes parlamentares da Silver Base.

**Entrada/Fonte:** `silver_base.frentes_membros`

**Saída/Destino:** `silver_curated.frentes_membros`

**Principais responsabilidades:**

* Consolidar atributos padronizados de membros de frentes parlamentares da Silver Base
* Selecionar indicadores de função e status de membros
* Criar indicadores analíticos de membros
* Preservar relações entre deputados, partidos, frentes parlamentares, legislaturas e frentes
* Preservar atributos temporais de membros e indicadores de validação técnica
* Preservar colunas completas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo no Gold

**Principais observações técnicas:**

* Aplica padronização amigável ao negócio
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos no Gold

---
### `06_curated_proposicoes.py`

**Localização:** `02_silver/02_curated/06_curated_proposicoes.py`

**Camada:** Silver Curated

**Finalidade:** Consolida, enriquece e valida dados de propostas legislativas da Silver Base.

**Entrada/Fonte:** `silver_base.proposicoes`

**Saída/Destino:** `silver_curated.proposicoes`

**Principais responsabilidades:**

* Consolidar atributos padronizados de proposições da Silver Base
* Selecionar indicadores de status legislativo e tipo de proposição
* Criar indicadores analíticos de proposição
* Criar indicadores de ciclo de vida da proposição
* Preservar colunas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo em Gold

**Principais observações de engenharia:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos em Gold

---

### `07_curated_despesas.py`

**Local:** `02_silver/02_curated/07_curated_despesas.py`

**Camada:** Silver Curated

**Objetivo:** Consolida, enriquece e valida dados de despesas parlamentares da Silver Base.

**Entrada/Fonte:** `silver_base.despesas`

**Saída/Destino:** `silver_curated.despesas`

**Principais responsabilidades:**

* Consolidar atributos de despesas padronizados da Silver Base
* Preservar valores financeiros e referências de documentos
* Preservar relacionamentos com fornecedores, deputados e legisladores
* Preservar indicadores de validação técnica da Silver Base
* Criar indicadores de despesas analíticas
* Preservar colunas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo no Gold

**Principais observações de engenharia:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `08_curated_orgaos.py`

**Local:** `02_silver/02_curated/08_curated_orgaos.py`

**Camada:** Silver Curated

**Objetivo:** Consolida, enriquece e Valida dados de organização legislativa da Silver Base.

**Entrada/Fonte:** `silver_base.orgaos`

**Saída/Destino:** `silver_curated.orgaos`

**Principais responsabilidades:**

* Consolidar atributos padronizados de organizações da Silver Base
* Curar indicadores de classificação de tipo de organização
* Criar indicadores analíticos de organização
* Preservar identificadores e relacionamentos entre organizações
* Preservar colunas de linhagem e rastreabilidade
* Validar a unicidade em nível curado
* Persistir a tabela Delta para consumo no Gold

**Principais observações técnicas:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `09_curated_orgaos_membros.py`

**Local:** `02_silver/02_curated/09_curated_orgaos_membros.py`

**Camada:** Silver Curated

**Objetivo:** Consolida, enriquece e valida dados de membros de organizações legislativas da Silver Base.

**Entrada/Fonte:** `silver_base.orgaos_membros`

**Saída/Destino:** `silver_curated.orgaos_membros`

**Principais responsabilidades:**

* Consolidar atributos padronizados de associação organizacional da Silver Base
* Selecionar indicadores de função e status de associação
* Criar indicadores analíticos de associação
* Preservar relações entre deputados, partidos, UF e organizações
* Preservar atributos temporais de associação e indicadores de validação técnica
* Preservar colunas completas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo no Gold

**Principais observações de engenharia:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `10_curated_votacoes.py`

**Local:** `02_silver/02_curated/10_curated_votacoes.py`

**Camada:** Silver Selecionado

**Objetivo:** Consolida, padroniza, enriquece e valida os dados das sessões de votação do Silver Base.

**Entrada/Fonte:** `silver_base.votacoes`

**Saída/Destino:** `silver_curated.votacoes`

**Principais responsabilidades:**

* Consolidar atributos de votação padronizados da Base de Dados Silver
* Criar indicadores analíticos de votação e resultados de votação
* Preservar relacionamentos entre proposições, eventos e organizações
* Preservar a contagem de resultados de votação
* Preservar colunas completas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo no Gold

**Principais observações de engenharia:**

* Aplica padronização amigável ao negócio
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `11_curated_votacoes_orientacoes.py`

**Local:** `02_silver/02_curated/11_curated_votacoes_orientacoes.py`

**Camada:** Silver Curated

**Objetivo:** Consolida, Enriquece e valida dados de orientação de voto do Silver Base.

**Entrada/Fonte:** `silver_base.votacoes_orientacoes`

**Saída/Destino:** `silver_curated.votacoes_orientacoes`

**Principais responsabilidades:**

* Consolidar atributos padronizados de orientação de voto do Silver Base
* Normalizar valores de orientação de voto em categorias analíticas selecionadas
* Criar indicadores analíticos de orientação
* Preservar a relação entre voto, organização e bancada

Relações
* Preservar colunas de linhagem e rastreabilidade
* Validar a unicidade em nível curado
* Persistir a tabela Delta para consumo no Gold

**Principais observações de engenharia:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `12_curated_votacoes_votos.py`

**Local:** `02_silver/02_curated/12_curated_votacoes_votos.py`

**Camada:** Silver Curated

**Objetivo:** Consolida, enriquece e valida registros de votação de representantes do Silver Base.

**Entrada/Fonte:** `silver_base.votacoes_votos`

**Saída/Destino:** `silver_curated.votacoes_votos`

**Principais responsabilidades:**

* Consolidar os atributos padronizados de votação dos deputados da Silver Base
* Normalizar os valores dos votos em categorias analíticas selecionadas
* Criar indicadores analíticos de comportamento de votação
* Preservar as relações entre deputado, partido, UF, legislatura e votação
* Preservar as colunas de linhagem e rastreabilidade
* Validar a unicidade em nível de curadoria
* Persistir a tabela Delta para consumo no Gold

**Principais observações de engenharia:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `13_curated_legislaturas.py`

**Local:** `02_silver/02_curated/13_curated_legislaturas.py`

**Camada:** Silver Curated

**Objetivo:** Constrói a entidade legislativa selecionada para modelagem dimensional subsequente.

**Entrada/Fonte:** `silver_base.legislaturas`

**Saída/Destino:** `silver_curated.legislaturas`

**Principais responsabilidades:**

* Ler registros legislativos padronizados do Silver Base
* Preservar atributos legislativos válidos
* Criar atributos de período analítico
* Preservar metadados de linhagem
* Validar a consistência das entidades curadas
* Persistir a tabela Delta do Silver Curated
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Aplica padronização amigável para negócios
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `14_curated_fornecedores.py`

**Local:** `02_silver/02_curated/14_curated_fornecedores.py`

**Camada:** Silver Curated

**Objetivo:** Constrói o conjunto de dados de fornecedores curados, enriquecido com dados públicos de validação do CNPJ.

**Entrada/Fonte:** `silver_base.fornecedores`

**Saída/Destino:** `silver_curated.fornecedores`

**Principais responsabilidades:**

* Ler registros padronizados de fornecedores do Silver Base
* Priorizar fornecedores CNPJ com base no uso do CEAP
* Validar os CNPJs selecionados usando enriquecimento de API pública
* Criar indicadores analíticos de status e suspeita de fornecedores
* Preservar metadados de linhagem
* Validar a consistência das entidades curadas
* Persistir a tabela Delta Curada do Silver
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Aplica padronização amigável ao negócio
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

---

### `15_curated_proposicoes_tramitacoes_scd.py`

**Local:** `02_silver/02_curated/15_curated_proposicoes_tramitacoes_scd.py`

**Camada:** CDC com Curadoria Prateada

**Finalidade:** Constrói a tabela histórica SCD Tipo 2 para proposicoes tramitacoes.

**Entrada/Fonte:** `silver_cdc.proposicoes_tramitacoes_base`

**Saída/Destino:** `silver_cdc.proposicoes_tramitacoes_scd2`

**Principais responsabilidades:**

* Ler registros CDC de tramitações normalizados da Base de Dados CDC do Silver
* Validar os atributos CDC e temporais necessários
* Preservar o histórico de alterações usando SCD Tipo 2
* Fechar as versões ativas anteriores quando alterações forem detectadas
* Inserir novas versões atuais
* Persistir registros rejeitados
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Aplica padronização amigável ao negócio
* Prepara entidades reutilizáveis ​​para modelagem dimensional
* Alimenta dimensões e fatos do Gold

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

### `01_build_dm_tempo.py`

**Location:** `03_gold/01_build_dm_tempo.py`

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
* Garantir um registro por órgão legislativo
* Preservar a linhagem e os metadados de processamento do Gold
* Validar a consistência das dimensões
* Persistir a tabela de dimensões Delta do Gold
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Implementa o design de esquema em estrela
* Evita relacionamentos de fato para fato
* Fornece uma camada de consumo analítico

---

### `07_build_dm_gabinete.py`

**Local:** `03_gold/07_build_dm_gabinete.py`

**Camada:** Gold

**Objetivo:** Constrói a dimensão de escritório conformada para o Esquema Estrela do Gold.

**Entrada/Fonte:** `silver_curated.deputados`

**Saída/Destino:** `gold.dm_gabinete`

**Principais responsabilidades:**

* Ler registros de representantes selecionados
* Extrair atributos de gabinete/escritório
* Criar uma chave substituta para modelagem dimensional
* Garantir um registro por gabinete/escritório
* Preservar a linhagem e os metadados de processamento do Gold
* Validar a consistência da dimensão
* Persistir a tabela de dimensão Delta do Gold
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Implementa o design de esquema em estrela
* Evita relacionamentos de fato para fato
* Fornece uma camada de consumo analítico

---

### `08_build_dm_fornecedor.py`

**Local:** `03_gold/08_build_dm_fornecedor.py`

**Camada:** Gold

**Objetivo:** Constrói a dimensão de fornecedor conformada para o Esquema Estrela do Gold.

**Entrada/Fonte:** `silver_curated.fornecedores`

**Saída/Destino:** `gold.dm_fornecedor`

**Principais responsabilidades:**

* Ler registros de fornecedores selecionados
* Extrair atributos de fornecedores
* Criar uma chave substituta para modelagem dimensional
* Garantir um registro por documento de fornecedor
* Preservar a validação CNPJ e os atributos de risco do fornecedor
* Validar a consistência da dimensão
* Persistir a tabela de dimensão Gold Delta
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Implementa o design de esquema estrela
* Evita relacionamentos de fato para fato
* Fornece uma camada de consumo analítico

---

### `09_build_dm_evento.py`

**Local:** `03_gold/09_build_dm_evento.py`

**Camada:** Gold

**Objetivo:** Constrói a dimensão de evento conformada para o Esquema Estrela Gold.

**Entrada/Fonte:** `silver_curated.eventos`

**Saída/Destino:** `gold.dm_evento`

**Principais responsabilidades:**

* Ler registros de eventos selecionados
* Extrair atributos analíticos de eventos
* Criar uma chave substituta para modelagem dimensional
* Garantir um registro por evento
* Validar a consistência da dimensão
* Persistir a tabela de dimensão Gold Delta
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Implementa o design de esquema estrela
* Evita relacionamentos de fato para fato
* Fornece uma camada de consumo analítico

---

### `10_build_dm_frente.py`

**Local:** `03_gold/10_build_dm_frente.py`

**Camada:** Gold

**Objetivo:** Constrói a dimensão frontal parlamentar conformada para o Esquema Estrela Gold.

**Entrada/Fonte:** `silver_curated.frentes`

**Saída/Destino:** `gold.dm_frente`

**Principais responsabilidades:**

* Ler registros de frentes parlamentares curadas
* Extrair atributos analíticos das frentes
* Criar uma chave substituta para modelagem dimensional
* Garantir um registro por frente parlamentar
* Validar a consistência da dimensão
* Persistir a tabela de dimensão Delta do Gold
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Implementa o design de esquema estrela
* Evita relacionamentos de fato para fato
* Fornece uma camada de consumo analítico

---

### `11_build_dm_uf.py`

**Local:** `03_gold/11_build_dm_uf.py`

**Camada:** Gold

**Objetivo:** Constrói a dimensão do estado brasileiro conformada para o Esquema Estrela Gold.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `gold.dm_uf`

**Principais responsabilidades:**

* Ler conjuntos de dados selecionados com atributos UF
* Consolidar valores UF únicos
* Criar uma chave substituta para modelagem dimensional
* Validar a consistência das dimensões
* Persistir a tabela de dimensões Gold Delta
* Registrar métricas de execução operacional
* Fontes:
* silver_curated.deputados
* silver_curated.despesas
* silver_curated.votacoes_votos
* silver_curated.frentes_membros

**Principais observações de engenharia:**

* Implementa um design de esquema em estrela
* Evita relacionamentos de fato para fato
* Fornece uma camada de consumo analítico

---

### `21_build_ft_frentes_membros.py`

**Local:** `03_gold/21_build_ft_frentes_membros.py`

**Camada:** Gold

**Finalidade:** Constrói a tabela de fatos de membros da frente parlamentar para o Esquema Gold Star.

**Entrada/Fonte:** `silver_curated.frentes_membros; Dimensões:; gold.dm_frente; gold.dm_deputado; gold.dm_partido; gold.dm_uf; `gold.dm_legislatura`

**Saída/Destino:** `gold.ft_frentes_membros`

**Principais responsabilidades:**

* Ler registros de membros da frente parlamentar selecionados
* Unir dimensões conformadas ao Gold
* Resolver chaves substitutas dimensionais
* Preservar relacionamentos entre frente, deputado, partido, UF e legislatura
* Preservar datas de filiação, funções, status e indicadores analíticos
* Preservar metadados de linhagem e auditoria
* Validar a consistência dos fatos do Gold
* Persistir uma tabela de fatos Delta do Gold particionada
* Otimizar a tabela Delta para cargas de trabalho analíticas
* Registrar métricas de execução operacional

**Principais observações de engenharia:**

* Implementa design de esquema em estrela
* Evita relacionamentos de fato para fato
* Fornece camada de consumo analítico

---

## `04_analytics`

Notebooks de produtos analíticos. Esta camada cria visualizações, marts, indicadores, validações, métricas de SLA e saídas orientadas a desafios.

### `01_build_gold_ceap_analytics.py`

**Local:** `04_analytics/01_build_gold_ceap_analytics.py`

**Camada:** Gold Analytics

**Finalidade:** Constrói visualizações e data marts analíticos do Gold para o Ceap Analytics.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte aos requisitos de dashboards e desafios finais

**Notas importantes de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e à defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `02_build_gold_frentes_analytics.py`

**Local:** `04_analytics/02_build_gold_frentes_analytics.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói visualizações analíticas e marts do Gold para o Frentes Analytics.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte aos requisitos de dashboards e do desafio final

**Notas importantes de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e à defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `03_build_gold_eventos_analytics.py`

**Local:** `04_analytics/03_build_gold_eventos_analytics.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói visualizações analíticas e marts do Gold para análise de eventos.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte aos requisitos de dashboards e desafios finais

**Notas importantes de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e à defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `04_build_gold_votacoes_analytics.py`

**Local:** `04_analytics/04_build_gold_votacoes_analytics.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói visualizações analíticas e marts do Gold para a análise de votações.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte aos requisitos de dashboards e desafios finais

**Notas importantes de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e à defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `05_build_gold_engajamento_analytics.py`

**Local:** `04_analytics/05_build_gold_engajamento_analytics.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói visualizações analíticas e marts do Gold para a análise de engajamento.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte aos requisitos de dashboards e desafios finais

**Notas importantes de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `06_build_gold_parliamentary_intelligence.py`

**Local:** `04_analytics/06_build_gold_parliamentary_intelligence.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói visualizações analíticas e marts do Gold para inteligência parlamentar.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões
* Criar visualizações analíticas ou data marts
* Calcular indicadores de negócios
* Suportar dashboards e requisitos do desafio final

**Principais observações de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Suporta dashboards e defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `07_build_gold_sla_votacoes_streaming.py`

**Local:** `04_analytics/07_build_gold_sla_votacoes_streaming.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói objetos analíticos de SLA e observabilidade para a carga de trabalho de streaming de votação.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte aos requisitos de dashboards e desafios finais

**Principais observações de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `08_build_gold_proposicoes_cdc_analytics.py`

**Local:** `04_analytics/08_build_gold_proposicoes_cdc_analytics.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói visualizações analíticas e objetos de alerta do Gold CDC para o processamento histórico de proposições.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte a dashboards e aos requisitos do desafio final

**Notas importantes de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e à defesa técnica
* Usa objetos Gold como entradas confiáveis

---

### `90_build_gold_validations.py`

**Local:** `04_analytics/90_build_gold_validations.py`

**Camada:** Gold Analytics

**Objetivo:** Constrói visualizações analíticas e marts do Gold para validações.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Ler fatos e dimensões do Gold
* Criar visualizações analíticas ou marts
* Calcular indicadores de negócios
* Dar suporte a dashboards e aos requisitos do desafio final

**Principais observações de engenharia:**

* Traduz os requisitos do desafio em visualizações analíticas
* Dá suporte a dashboards e à defesa técnica
* Usa objetos Gold como entradas confiáveis

---

## `05_dlt`

Notebook do Delta Live Tables para geração declarativa de alertas e qualidade de streaming.

### `01_dlt_votacoes_streaming.py`

**Local:** `05_dlt/01_dlt_votacoes_streaming.py`

**Camada:** DLT / Lakeflow

**Objetivo:** Pipeline declarativo para dados de micro-lotes de votação. Transforma o fluxo Bronze em tabelas de streaming Silver e Gold. Fluxo: bronze_stream.votacoes_raw -> silver_stream_votacoes_validas -> gold_stream_votacoes_alertas Importante: Este notebook NÃO deve ser executado manualmente a partir de um cluster de notebooks Databricks padrão. Ele deve ser executado somente por meio de um pipeline Databricks Lakeflow / Delta Live Tables, pois o módulo dlt está disponível apenas no contexto de tempo de execução do DLT. Execução: Jobs & Pipelines -> dlt_votacoes_streaming -> Executar / Iniciar

**Entrada / Origem:** `Não aplicável / suporte interno`

**Saída / Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Definir tabelas de pipeline de streaming
* Aplicar expectativas declarativas
* Promover registros entre as camadas de streaming Bronze, Silver e Gold
* Gerar saídas prontas para alertas

**Observações importantes de engenharia:**

* Deve ser executado em um contexto Databricks Lakeflow / DLT
* Usa expectativas de qualidade declarativas
* Gera saídas de streaming prontas para alertas

---

## `90_common`

Módulos reutilizáveis ​​compartilhados entre notebooks para manter a implementação consistente e evitar lógica duplicada.

### `api_client.py`

**Localização:** `90_common/api_client.py`

**Camada:** Core

**Objetivo:** Fornece funções reutilizáveis ​​para interagir com a API de Dados Abertos da Câmara dos Deputados.

**Entrada/Fonte:** `Não aplicável/suporte interno`

**Saída/Destino:** `Não aplicável/objeto de suporte`

**Principais responsabilidades:**

* Fornecer lógica de suporte reutilizável
* Reduzir código duplicado
* Padronizar a implementação entre notebooks

---

### `bronze_writer.py`

**Localização:** `90_common/bronze_writer.py`

**Camada:** Core

**Objetivo:** Fornece funções reutilizáveis ​​para padronizar a criação e persistência de DataFrames da camada Bronze.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Fornecer lógica de suporte reutilizável
* Reduzir código duplicado
* Padronizar a implementação entre notebooks

---

### `cnpj_utils.py`

**Local:** `90_common/cnpj_utils.py`

**Camada:** Utilitários Comuns

**Finalidade:** Funções utilitárias para limpeza, classificação e validação de CPF/CNPJ.

**Entrada/Fonte:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Fornecer lógica de suporte reutilizável

Reduzir código duplicado
* Padronizar a implementação entre notebooks

---

### `config.py`

**Local:** `90_common/config.py`

**Camada:** Core

**Objetivo:** Define parâmetros de configuração globais usados ​​em todo o pipeline de dados.

**Entrada/Origem:** `Não aplicável/suporte interno`

**Saída/Destino:** `Não aplicável/objeto de suporte`

**Principais responsabilidades:**

* Fornecer lógica de suporte reutilizável
* Reduzir código duplicado
* Padronizar a implementação entre notebooks

---

### `logger.py`

**Local:** `90_common/logger.py`

**Camada:** Core

**Objetivo:** Fornece utilitários de registro padronizados para o pipeline de dados.

**Entrada/Origem:** `Não aplicável / suporte interno`

**Saída/Destino:** `Não aplicável / objeto de suporte`

**Principais responsabilidades:**

* Fornecer lógica de suporte reutilizável
* Reduzir código duplicado
* Padronizar a implementação entre notebooks

---

### `pagination.py`

**Local:** `90_common/pagination.py`

**Camada:** Core

**Objetivo:** Fornece funções reutilizáveis ​​para lidar com a paginação da API.

**Entrada/Origem:** `Não aplicável/suporte interno`

**Saída/Destino:** `Não aplicável/objeto de suporte`

**Principais responsabilidades:**

* Fornecer lógica de suporte reutilizável
* Reduzir código duplicado
* Padronizar a implementação entre os notebooks

---

### `table_logger.py`

**Local:** `90_common/table_logger.py`

**Camada:** Core

**Objetivo:** Fornece utilitários para registrar eventos de execução de pipelines em tabelas Delta.

**Entrada/Origem:** `Não aplicável/suporte interno`

**Saída/Destino:** `Não aplicável/objeto de suporte`

**Principais responsabilidades:**

* Fornecer lógica de suporte reutilizável
* Reduzir código duplicado
* Padronizar a implementação entre os notebooks

---

## `99_jobs`

Notebooks de orquestração de fluxo de trabalho do Databricks usados ​​para executar cada camada em uma ordem controlada.

### `01_run_bronze_pipeline.py`

**Local:** `99_jobs/01_run_bronze_pipeline.py`

**Camada:** Orquestração

**Objetivo:** Executa o pipeline completo de ingestão do Bronze.

**Entrada/Origem:** `notebooks 01_bronze`

**Saída/Destino:** `tabelas da camada Bronze`

**Principais responsabilidades:**

* Executar notebooks de ingestão do Bronze em ordem determinística
* Priorizar versões de ingestão de arquivos *_file quando disponíveis
* Registrar métricas de execução da orquestração
* Registrar o status de execução em nível de notebook
* Interromper a execução em caso de falha para evitar atualizações inconsistentes em instâncias posteriores
* Fornecer visibilidade operacional para tarefas de atualização do Bronze

**Principais observações técnicas:**

* Separa a execução por responsabilidade
* Suporta controle operacional e gerenciamento de dependências
* Pode ser agendado como Tarefas do Databricks

---
