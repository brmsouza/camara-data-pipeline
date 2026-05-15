# Gold Layer — Dimensions and Facts Dictionary

## Overview

This document consolidates the analytical dictionary of the Gold layer of the `camara-data-pipeline` project.

The purpose of this material is to document:

- analytical dimensions;
- fact tables;
- granularity;
- business objectives;
- functional descriptions;
- analytical columns;
- dimensional relationships;
- corporate metrics.

---

# Column Conventions

| Prefix | Meaning |
|---|---|
| `sk_` | Surrogate key |
| `id_` | Business key |
| `dt_` | Date |
| `vl_` | Monetary value |
| `qt_` | Quantity |
| `tx_` | Text |
| `fl_` | Flag |
| `sg_` | Acronym |
| `cd_` | Code |

---

# `00_create_gold_schema`

## Type
Fact

## Notebook
`notebooks/03_gold/00_create_gold_schema.py`

## Description
Initializes the Gold analytical layer used by the dimensional Star Schema.

## Granularity
Analytical granularity defined by the dimensional model.

## Analytical Objectives

- Provide a conformed entity for analytics;
- Standardize corporate analytical consumption;
- Centralize business rules;
- Enable integration between facts and dimensions;
- Serve dashboards and analytical marts.

## Technical Notes

- Persisted in Delta Lake;
- Compatible with Databricks SQL;
- Prepared for scalable analytics;
- Maintains analytical traceability;
- Structure prepared for incremental evolution.

---

# `dm_data`

## Type
Dimension

## Notebook
`notebooks/03_gold/01_build_dm_data.py`

## Description
Builds the conformed date dimension for the Gold Star Schema.

## Granularity
Analytical granularity defined by the dimensional model.

## Analytical Objectives

- Provide a conformed entity for analytics;
- Standardize corporate analytical consumption;
- Centralize business rules;
- Enable integration between facts and dimensions;
- Serve dashboards and analytical marts.

## Columns

| Column | Description |
|---|---|
| `sk_data` | Surrogate key of the date dimension used to relate Gold fact tables. |
| `dt_data` | Reference calendar date of the date dimension. Represents the calendar day used in temporal analysis. |
| `nr_ano` | Calendar year of the reference date. |
| `nr_semestre` | Semester number of the year, enabling first-half and second-half aggregations. |
| `nr_trimestre` | Quarter number of the year, used for quarterly aggregations. |
| `nr_mes` | Month number of the reference date, ranging from 1 to 12. |
| `tx_nome_mes` | Full month name of the reference date. |
| `tx_nome_mes_abrev` | Abbreviated month name of the reference date. |
| `nr_dia_mes` | Day number within the month of the reference date. |
| `nr_dia_ano` | Sequential day number within the year. |
| `nr_semana_ano` | Week number within the year, used for weekly analysis. |
| `nr_dia_semana` | Day of week number of the reference date. |
| `tx_nome_dia_semana` | Full weekday name. |
| `tx_nome_dia_semana_abrev` | Abbreviated weekday name. |
| `fl_fim_semana` | Indicator whether the reference date falls on a weekend (Saturday or Sunday). |
| `fl_dia_util` | Indicator whether the reference date is considered a business day. |
| `dt_inicio_mes` | First day of the month corresponding to the reference date. |
| `dt_fim_mes` | Last day of the month corresponding to the reference date. |
| `dt_inicio_trimestre` | First day of the quarter corresponding to the reference date. |
| `dt_fim_trimestre` | Last day of the quarter corresponding to the reference date. |
| `dt_inicio_ano` | First day of the year corresponding to the reference date. |
| `dt_fim_ano` | Last day of the year corresponding to the reference date. |
| `tx_ano_mes` | Text representation of year and month used for monthly grouping and sorting. |
| `tx_mes_ano` | Friendly textual representation of month and year used in dashboards and reports. |
| `nr_ano_mes` | Numeric representation of year and month used for chronological sorting. |
| `dt_processamento` | Processing timestamp indicating when the date dimension record was generated. |

## Technical Notes

- Persisted in Delta Lake;
- Compatible with Databricks SQL;
- Prepared for scalable analytics;
- Maintains analytical traceability;
- Structure prepared for incremental evolution.

---
# `gold.dm_legislatura`

## Type
Dimension

## Description
Conformed dimension of legislatures of the Chamber of Deputies. Represents the formal parliamentary operating periods defined by the Chamber as the interval between the inauguration of one group of deputies and the eve of the inauguration of the following group. The API provides the identifier, start date, end date and election year of the parliamentarians within the legislature.

## Granularity
One row per legislature.

## Analytical Objectives
- Enable analysis of expenses, votes, legislative propositions, events and parliamentary activity by legislature.
- Standardize the legislative period as a parliamentary time dimension.
- Identify the current legislature.
- Relate parliamentary facts to the corresponding political-electoral cycle.

## Columns

| Column | Description |
|---|---|
| `sk_leg` | Sequential surrogate key of the legislature dimension in the Gold layer, created for internal dimensional relationships in the star schema model. |
| `leg_id_legislatura` | Official legislature identifier according to the Chamber of Deputies Open Data API. Represents a parliamentary working period of the Chamber. |
| `leg_nr_ano_eleicao` | Election year of the federal deputies composing the legislature. In the project, it is derived from the year preceding the beginning of the legislature. |
| `leg_nr_ano_inicio` | Calendar year in which the legislature begins. |
| `leg_nr_ano_fim` | Calendar year in which the legislature ends. |
| `leg_dt_inicio` | Official start date of the legislature, corresponding to the beginning of the parliamentary mandate period of that group of deputies. |
| `leg_dt_fim` | Official end date of the legislature, corresponding to the closing period before the inauguration of the next legislature. |
| `leg_qt_meses_duracao` | Approximate number of months of the legislature duration, calculated between `leg_dt_inicio` and `leg_dt_fim`. |
| `leg_fl_legislatura_atual` | Indicator showing whether the current date falls within the legislature validity period. Value `1` indicates the current legislature; value `0` indicates a historical or future legislature. |
| `leg_tx_descricao` | Standardized legislature description in the format “Legislature {id} ({start_year} - {end_year})”, created to improve readability in reports, dashboards and analytical filters. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. Indicates when the dimension was generated or updated in the pipeline. |
| `gold_id_batch` | Unique execution batch identifier responsible for generating the Gold dimension. Used for traceability, auditing and troubleshooting. |

## Relationships
- Relates to parliamentary facts through `sk_leg`.
- Can be used by CEAP expense facts, votes, parliamentary voting records, event attendance and parliamentary activity.
- The official business key is `leg_id_legislatura`.

## Technical Notes
- Gold source: `silver_curated.legislaturas`.
- Target table: `gold.dm_legislatura`.
- The notebook guarantees a single row per `leg_id_legislatura`.
- Records without `leg_id_legislatura` are discarded before the Gold load.
- The `sk_leg` key is generated using `row_number()` ordered by `leg_id_legislatura`.
- The dimension receives Gold-specific metadata: `gold_ts_processamento` and `gold_id_batch`.

---
---

# `dm_partido`

## Type
Dimension

## Notebook
`notebooks/03_gold/03_build_dm_partido.py`

## Description
Conformed dimension of political parties identified in curated deputy records. The table standardizes the political party acronym used by parliamentarians and serves as an integration entity between dimensions, facts and analytical views within the Gold layer.

## Granularity
One row per political party acronym.

## Analytical Objectives
- Enable parliamentary analysis by political party.
- Integrate expenses, votes, deputies and party indicators.
- Support rankings, dashboards and comparative analyses between political parties.
- Centralize the party dimensional key used in the star schema model.

## Columns

| Column | Description |
|---|---|
| `sk_part` | Sequential surrogate key of the political party dimension in the Gold layer, created for internal relationships with facts and analytical views. |
| `part_sg_partido` | Official acronym of the political party associated with the deputy in Chamber data, such as PT, PL, MDB, PSD, PSOL or UNIÃO. |
| `gold_ts_processamento` | Timestamp indicating when the record was processed and written into the Gold dimension. |
| `gold_id_batch` | Unique execution batch identifier responsible for generating the `dm_partido` dimension. |

## Relationships
- Relates to `gold.dm_deputado` through `part_sg_partido`.
- Can be used by expense facts, votes and parliamentary activity through `sk_part`.
- Serves as the basis for consolidated party analyses, such as `vw_partidos_analitica`.

## Technical Notes
- Source: `silver_curated.deputados`.
- Target table: `gold.dm_partido`.
- The notebook removes records without `part_sg_partido`.
- The dimension maintains a single row per political party acronym.
- The `sk_part` key is generated using `row_number()` ordered by `part_sg_partido`.
- The table is persisted in Delta Lake with schema overwrite enabled.
- After writing, `OPTIMIZE gold.dm_partido` is executed.

---
# `dm_deputado`

## Type
Dimension

## Notebook
`notebooks/03_gold/04_build_dm_deputado.py`

## Description
Conformed dimension of federal deputies of the Chamber of Deputies. Consolidates registration, party, electoral, geographic and parliamentary information of deputies identified in the official Chamber API data and enriched by the Silver Curated layer.

The dimension represents the primary parliamentary entity of the Gold analytical model and is used as the central axis for integrating CEAP expenses, parliamentary votes, event attendance, legislative propositions, parliamentary activity indicators and political analyses.

## Granularity
One row per federal deputy.

## Analytical Objectives
- Enable individual parliamentary analyses.
- Relate deputies to political parties, legislatures and federative units.
- Consolidate parliamentary registration data for corporate analytical consumption.
- Integrate expenses, votes and parliamentary indicators.
- Enable electoral, party and geographic analyses.
- Support transparency and parliamentary intelligence dashboards.

## Columns

| Column | Description |
|---|---|
| `sk_dept` | Sequential surrogate key of the deputy dimension used in the Gold layer star schema model. |
| `dept_id_deputado` | Official unique identifier of the federal deputy in the Chamber of Deputies API. |
| `dept_nm_deputado` | Full civil name of the federal deputy. |
| `dept_nm_parlamentar` | Parliamentary name officially used by the deputy in legislative activities. |
| `dept_sg_partido` | Official acronym of the political party to which the deputy is affiliated. |
| `dept_sg_uf` | Acronym of the federative unit through which the deputy was elected, such as RJ, SP, MG or BA. |
| `dept_nr_legislatura` | Legislature number associated with the deputy’s active parliamentary mandate. |
| `dept_tx_email` | Official institutional email address of the deputy in the Chamber of Deputies. |
| `dept_tx_url_foto` | Official URL of the deputy’s institutional photograph provided by the Chamber. |
| `dept_tx_sexo` | Gender informed in the deputy’s parliamentary registration. |
| `dept_dt_nascimento` | Birth date of the federal deputy. |
| `dept_nr_idade` | Calculated age of the deputy based on the birth date and the current processing date. |
| `dept_nm_municipio_nascimento` | Name of the deputy’s birth municipality. |
| `dept_sg_uf_nascimento` | Acronym of the federative unit where the deputy was born. |
| `dept_fl_mandato_ativo` | Indicator showing whether the deputy has an active parliamentary mandate at the moment of data collection/processing. |
| `dept_dt_inicio_mandato` | Start date of the deputy’s current parliamentary mandate. |
| `dept_dt_fim_mandato` | Expected end date of the deputy’s parliamentary mandate. |
| `dept_nm_gabinete` | Textual identification of the deputy’s parliamentary office within the Chamber of Deputies. |
| `dept_nr_gabinete` | Official number of the parliamentary office occupied by the deputy. |
| `dept_nr_andar_gabinete` | Floor number of the Chamber building where the parliamentary office is located. |
| `dept_tx_telefone_gabinete` | Official telephone number of the deputy’s parliamentary office. |
| `dept_tx_situacao` | Parliamentary status of the deputy according to Chamber records, such as Active, Licensed or Alternate. |
| `dept_tx_condicao_eleitoral` | Electoral condition associated with the deputy’s mandate, such as incumbent or alternate status. |
| `dept_fl_reeleito` | Derived indicator showing whether the deputy participated in previous legislatures identified in the available historical data. |
| `dept_qt_mandatos` | Number of parliamentary mandates identified for the deputy in the analyzed historical data. |
| `part_sk_part` | Surrogate key of the `dm_partido` dimension related to the deputy’s political party. |
| `leg_sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the deputy’s active legislature. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for loading the deputy dimension. |

## Relationships
- Relates to `gold.dm_partido` through `part_sk_part`.
- Relates to `gold.dm_legislatura` through `leg_sk_leg`.
- Used by CEAP expense facts, parliamentary voting facts, events, propositions and analytical indicators.
- Serves as the primary dimension for parliamentary analytical views in the Gold layer.

## Technical Notes
- Main source: `silver_curated.deputados`.
- Enriched with party and legislative information.
- Maintains only one consolidated row per deputy.
- Records without `dept_id_deputado` are discarded before persistence.
- The `sk_dept` key is generated using `row_number()` ordered by `dept_id_deputado`.
- Persisted in Delta Lake within the `gold` schema.
- Compatible with Databricks SQL and scalable analytical workloads.
- Used as the central conformed dimension of the parliamentary star schema.
- The dimension supports future evolution for SCD Type 2 historization of political party affiliation and parliamentary registration changes.

---
# `dm_proposicao`

## Type
Dimension

## Notebook
`notebooks/03_gold/05_build_dm_proposicao.py`

## Description
Conformed dimension of legislative propositions of the Chamber of Deputies. Consolidates official information regarding bills, constitutional amendment proposals, requests, provisional measures and other legislative matters made available through the Chamber Open Data API.

The dimension centralizes legislative metadata used in parliamentary analyses, voting processes, legislative procedures, legislative production and political intelligence. Each record represents a legislative proposition officially identified by the Chamber of Deputies.

## Granularity
One row per legislative proposition.

## Analytical Objectives
- Enable analysis of parliamentary legislative production.
- Relate parliamentary votes and legislative procedures to official propositions.
- Identify legislative matter types of the Chamber of Deputies.
- Support thematic, legislative and political analyses.
- Consolidate reusable legislative metadata within the Gold layer.
- Integrate parliamentary facts with the legislative lifecycle of propositions.

## Columns

| Column | Description |
|---|---|
| `sk_prop` | Sequential surrogate key of the proposition dimension used in the Gold layer star schema model. |
| `prop_id_proposicao` | Official unique identifier of the legislative proposition in the Chamber of Deputies API. |
| `prop_tx_uri` | Official proposition URI in the Chamber Open Data API, used for navigation and integration between legislative endpoints. |
| `prop_sg_tipo` | Acronym of the legislative type of the proposition, such as PL, PEC, MPV, REQ or PDL. |
| `prop_tx_descricao_tipo` | Official textual description of the legislative type of the proposition, such as Bill, Constitutional Amendment Proposal or Provisional Measure. |
| `prop_nr_numero` | Official number of the legislative proposition within its type and presentation year. |
| `prop_nr_ano` | Official year in which the legislative proposition was presented in the Chamber of Deputies. |
| `prop_cd_tipo` | Internal proposition type code used by the Chamber of Deputies for legislative categorization. |
| `prop_tx_ementa` | Official summary text of the legislative proposition, describing its primary objective. |
| `prop_tx_keywords` | Keywords associated with the legislative proposition used for thematic indexing and text search. |
| `prop_ts_apresentacao` | Official date and time when the legislative proposition was presented in the Chamber of Deputies. |
| `prop_tx_identificacao` | Consolidated textual identification of the proposition in the format “TYPE NUMBER/YEAR”, such as “PL 2630/2020”. |
| `prop_nr_ano_apresentacao` | Year extracted from the proposition presentation date to facilitate legislative temporal analyses. |
| `prop_fl_proposicao_recente` | Derived indicator identifying propositions presented during the most recent legislative periods analyzed by the project. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Unique execution batch identifier responsible for generating the proposition dimension. |

## Relationships
- Relates to parliamentary voting facts through `sk_prop`.
- Can be integrated with CDC/SCD Type 2 pipelines for legislative procedures.
- Used in parliamentary legislative production analyses.
- Serves as a central dimension for legislative indicators and parliamentary intelligence.
- Relates to Chamber events, votes and legislative procedures.

## Technical Notes
- Main source: `silver_curated.proposicoes`.
- Target table: `gold.dm_proposicao`.
- Records without `prop_id_proposicao` are discarded before persistence.
- The `sk_prop` key is generated using `row_number()` ordered by `prop_id_proposicao`.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Structure prepared for future integration with CDC/SCD Type 2 parliamentary procedure pipelines.
- The dimension is used by legislative analytical pipelines and Gold parliamentary intelligence views.
- The `prop_tx_identificacao` column is derived to improve analytical readability and dashboard visualization.

---

# `dm_partido`

## Type
Dimension

## Notebook
`notebooks/03_gold/03_build_dm_partido.py`

## Description
Conformed dimension of political parties identified in curated deputy records. The table standardizes the party acronym used by parliamentarians and serves as an integration entity between dimensions, facts and analytical views in the Gold layer.

## Granularity
One row per political party acronym.

## Analytical Objectives
- Enable parliamentary analysis by political party.
- Integrate expenses, votes, deputies and party indicators.
- Support rankings, dashboards and comparative analyses between political parties.
- Centralize the party dimensional key used in the star schema model.

## Columns

| Column | Description |
|---|---|
| `sk_part` | Sequential surrogate key of the political party dimension in the Gold layer, created for internal relationships with facts and analytical views. |
| `part_sg_partido` | Official acronym of the political party associated with the deputy in the Chamber data, such as PT, PL, MDB, PSD, PSOL or UNIÃO. |
| `gold_ts_processamento` | Timestamp indicating when the record was processed and written to the Gold dimension. |
| `gold_id_batch` | Unique execution batch identifier responsible for generating the `dm_partido` dimension. |

## Relationships
- Relates to `gold.dm_deputado` through `part_sg_partido`.
- Can be used by expense facts, votes and parliamentary activity through `sk_part`.
- Serves as the basis for consolidated party analyses, such as `vw_partidos_analitica`.

## Technical Notes
- Source: `silver_curated.deputados`.
- Target table: `gold.dm_partido`.
- The notebook removes records without `part_sg_partido`.
- The dimension maintains a single row per party acronym.
- The `sk_part` key is generated by `row_number()` ordered by `part_sg_partido`.
- The table is persisted in Delta Lake with schema overwrite enabled.
- After writing, `OPTIMIZE gold.dm_partido` is executed.

---

# `dm_deputado`

## Type
Dimension

## Notebook
`notebooks/03_gold/04_build_dm_deputado.py`

## Description
Conformed dimension of federal deputies of the Chamber of Deputies. Consolidates registration, party, electoral, geographic and parliamentary information of deputies identified in the official Chamber API data and enriched by the Silver Curated layer.

The dimension represents the main parliamentary entity of the Gold analytical model, being used as the central axis for integrating CEAP expenses, votes, event attendance, legislative propositions, parliamentary activity indicators and political analyses.

## Granularity
One row per federal deputy.

## Analytical Objectives
- Enable individual parliamentary analyses.
- Relate deputies to political parties, legislatures and federative units.
- Consolidate parliamentary registration data for corporate analytical consumption.
- Integrate expenses, votes and parliamentary indicators.
- Enable electoral, party and geographic analyses.
- Support transparency and parliamentary intelligence dashboards.

## Columns

| Column | Description |
|---|---|
| `sk_dept` | Sequential surrogate key of the deputy dimension used in the Gold layer star schema model. |
| `dept_id_deputado` | Official unique identifier of the federal deputy in the Chamber of Deputies API. |
| `dept_nm_deputado` | Full civil name of the federal deputy. |
| `dept_nm_parlamentar` | Parliamentary name officially used by the deputy in legislative activities. |
| `dept_sg_partido` | Official acronym of the political party to which the deputy is affiliated. |
| `dept_sg_uf` | Acronym of the federative unit by which the deputy was elected, such as RJ, SP, MG or BA. |
| `dept_nr_legislatura` | Legislature number associated with the deputy’s current parliamentary term. |
| `dept_tx_email` | Official institutional email address of the deputy in the Chamber of Deputies. |
| `dept_tx_url_foto` | Official URL of the deputy’s institutional photo provided by the Chamber. |
| `dept_tx_sexo` | Gender informed in the deputy’s parliamentary registration. |
| `dept_dt_nascimento` | Birth date of the federal deputy. |
| `dept_nr_idade` | Calculated age of the deputy based on the birth date and current processing date. |
| `dept_nm_municipio_nascimento` | Name of the deputy’s birth municipality. |
| `dept_sg_uf_nascimento` | Acronym of the federative unit where the deputy was born. |
| `dept_fl_mandato_ativo` | Indicator showing whether the deputy has an active parliamentary mandate at the moment of data collection/processing. |
| `dept_dt_inicio_mandato` | Start date of the deputy’s current parliamentary term. |
| `dept_dt_fim_mandato` | Expected end date of the deputy’s parliamentary term. |
| `dept_nm_gabinete` | Textual identification of the deputy’s parliamentary office within the Chamber of Deputies. |
| `dept_nr_gabinete` | Official number of the parliamentary office occupied by the deputy. |
| `dept_nr_andar_gabinete` | Floor number of the Chamber building where the parliamentary office is located. |
| `dept_tx_telefone_gabinete` | Official telephone number of the deputy’s parliamentary office. |
| `dept_tx_situacao` | Parliamentary status of the deputy according to Chamber records, such as Active, Licensed or Alternate. |
| `dept_tx_condicao_eleitoral` | Electoral condition associated with the deputy’s mandate, such as incumbent or alternate. |
| `dept_fl_reeleito` | Derived indicator showing whether the deputy participated in previous legislatures identified in the available history. |
| `dept_qt_mandatos` | Number of parliamentary terms identified for the deputy in the analyzed historical data. |
| `part_sk_part` | Surrogate key of the `dm_partido` dimension related to the deputy’s political party. |
| `leg_sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the deputy’s current legislature. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for loading the deputy dimension. |

## Relationships
- Relates to `gold.dm_partido` through `part_sk_part`.
- Relates to `gold.dm_legislatura` through `leg_sk_leg`.
- Used by CEAP expense facts, parliamentary votes, events, propositions and analytical indicators.
- Serves as the main dimension for parliamentary analytical views in the Gold layer.

## Technical Notes
- Main source: `silver_curated.deputados`.
- Enriched with party and legislative information.
- Maintains only one consolidated row per deputy.
- Records without `dept_id_deputado` are discarded before persistence.
- The `sk_dept` key is generated by `row_number()` ordered by `dept_id_deputado`.
- Persisted in Delta Lake within the `gold` schema.
- Compatible with Databricks SQL and scalable analytical workloads.
- Used as the central conformed dimension of the parliamentary star schema.
- The dimension supports future evolution for SCD Type 2 historization of party affiliation and parliamentary registration changes.

---
# `dm_gabinete`

## Type
Dimension

## Notebook
`notebooks/03_gold/07_build_dm_gabinete.py`

## Description
Conformed dimension of parliamentary offices assigned to federal deputies of the Brazilian Chamber of Deputies. Consolidates structural, physical and contact information related to offices linked to active parliamentarians.

This dimension enables organizational and administrative analyses related to office occupancy, parliamentary location, institutional distribution and official communication channels of federal deputies.

## Grain
One row per parliamentary office assigned to a federal deputy.

## Analytical Objectives
- Enable administrative analysis of parliamentary offices.
- Relate deputies to their physical office structures within the Chamber of Deputies.
- Identify office locations by building, room and floor.
- Support institutional and operational analyses of parliamentary structures.
- Provide official parliamentary contact information.
- Validate telephone number and institutional e-mail data quality.

## Columns

| Column | Description |
|---|---|
| `sk_gab` | Sequential surrogate key of the parliamentary office dimension used in the Gold layer star schema model. |
| `dept_id_deputado` | Official identifier of the federal deputy responsible for the parliamentary office. |
| `gab_tx_nome` | Official name of the deputy's parliamentary office. |
| `gab_tx_predio` | Building identification within the Chamber of Deputies where the parliamentary office is located. |
| `gab_tx_sala` | Room number or room identification assigned to the parliamentary office. |
| `gab_tx_andar` | Floor of the Chamber building where the parliamentary office is located. |
| `gab_tx_telefone` | Official telephone number of the deputy's parliamentary office. |
| `gab_fl_telefone_valido` | Derived indicator that identifies whether the office telephone number follows a valid format according to pipeline validation rules. |
| `gab_tx_email` | Official institutional e-mail address of the parliamentary office. |
| `gab_fl_email_valido` | Derived indicator that identifies whether the office e-mail address follows a valid format according to validation rules implemented in the project. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the parliamentary office dimension. |

## Relationships
- Related to `gold.dm_deputado` through `dept_id_deputado`.
- Can be used in administrative and institutional parliamentary analyses.
- Supports cross-analysis with internal geographic data of the Chamber of Deputies.
- Serves as a supporting dimension for analyses of parliamentary physical infrastructure.

## Technical Notes
- Main source: `silver_curated.deputados`.
- Target table: `gold.dm_gabinete`.
- The dimension maintains one consolidated row per identified parliamentary office.
- Records without `dept_id_deputado` are discarded before persistence.
- The `sk_gab` key is generated using `row_number()` ordered by `dept_id_deputado`.
- Persisted in Delta Lake with Databricks SQL support.
- The columns `gab_fl_telefone_valido` and `gab_fl_email_valido` are derived from technical validation rules implemented in the Silver/Gold layers.
- Structure prepared for future historical tracking of parliamentary office changes.
---

# `dm_fornecedor`

## Type
Dimension

## Notebook
`notebooks/03_gold/08_build_dm_fornecedor.py`

## Description
Conformed dimension of suppliers associated with parliamentary expenses from the Parliamentary Activity Quota (CEAP). Consolidates individuals and legal entities that issued fiscal documents used in the expense reporting process of federal deputies from the Brazilian Chamber of Deputies.

This dimension enables the identification of recurring suppliers, analysis of parliamentary spending concentration, monitoring of payments executed with CEAP resources, and supports transparency, auditing and parliamentary financial intelligence analyses.

## Grain
One row per supplier identified by CPF or CNPJ.

## Analytical Objectives
- Enable financial analysis of parliamentary suppliers.
- Identify expense concentration by supplier.
- Support auditing and transparency of CEAP expenditures.
- Consolidate suppliers used by federal deputies.
- Enable geographic and economic analyses of parliamentary expenses.
- Detect recurrence patterns and parliamentary financial relationships.

## Columns

| Column | Description |
|---|---|
| `sk_forn` | Sequential surrogate key of the supplier dimension used in the Gold layer star schema model. |
| `forn_nr_cnpj_cpf` | CPF or CNPJ number of the supplier associated with the fiscal document of the parliamentary expense. |
| `forn_tx_nome_fornecedor` | Name of the supplier, company or service provider informed in the fiscal document related to the CEAP expense. |
| `forn_tx_tipo_pessoa` | Supplier classification as Individual or Legal Entity derived from the structure of the informed CPF/CNPJ document. |
| `forn_fl_cnpj_valido` | Derived indicator that identifies whether the supplier CNPJ follows a valid format according to validation rules implemented in the pipeline. |
| `forn_fl_cpf_valido` | Derived indicator that identifies whether the supplier CPF follows a valid format according to validation rules implemented in the pipeline. |
| `forn_fl_documento_valido` | Consolidated indicator that identifies whether the supplier CPF or CNPJ passed the technical validation rules implemented in the project. |
| `forn_tx_raiz_cnpj` | Root CNPJ of the supplier used for enterprise grouping of branches and consolidated economic group analysis. |
| `forn_nr_quantidade_despesas` | Total number of parliamentary expenses associated with the supplier identified in the processed historical dataset. |
| `forn_vl_total_recebido` | Total amount received by the supplier considering all parliamentary expenses processed in the project. |
| `forn_dt_primeira_despesa` | Date of the first parliamentary expense identified for the supplier in the available historical dataset. |
| `forn_dt_ultima_despesa` | Date of the most recent parliamentary expense identified for the supplier in the available historical dataset. |
| `forn_fl_fornecedor_recorrente` | Derived indicator that identifies suppliers with significant recurrence in parliamentary expense usage. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the expense record associated with the supplier. |
| `bronze_id_origem` | Original technical identifier of the expense record used during Bronze ingestion. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the record associated with the supplier. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the supplier dimension. |

## Relationships
- Related to parliamentary CEAP expense fact tables.
- Used by parliamentary financial analyses and public spending audits.
- Can be related to deputies, political parties and legislatures through parliamentary expenses.
- Serves as a central dimension for financial concentration and parliamentary transparency analyses.

## Technical Notes
- Main source: `silver_curated.despesas`.
- Target table: `gold.dm_fornecedor`.
- The dimension consolidates distinct suppliers by CPF/CNPJ.
- Records without valid fiscal identification may be classified as inconsistent according to pipeline validation rules.
- The `sk_forn` key is generated using `row_number()` ordered by `forn_nr_cnpj_cpf`.
- Persisted in Delta Lake with Databricks SQL support.
- Aggregated expense metrics are derived during Gold layer processing.
- Structure prepared for antifraud analyses, financial concentration detection and parliamentary auditing.
- Maintains Bronze traceability columns for complete financial data lineage.

---

# `dm_evento`

## Type
Dimension

## Notebook
`notebooks/03_gold/09_build_dm_evento.py`

## Description
Conformed dimension of parliamentary events from the Brazilian Chamber of Deputies. Consolidates official information related to sessions, public hearings, meetings, seminars, general committees and other legislative events registered in the Chamber Open Data API.

This dimension enables contextualization of parliamentary activities performed over time, relating events to legislative bodies, propositions, voting sessions and parliamentary participation.

## Grain
One row per parliamentary event from the Brazilian Chamber of Deputies.

## Analytical Objectives
- Enable temporal analysis of parliamentary activities.
- Relate legislative events to legislative bodies and propositions.
- Support monitoring of parliamentary sessions and meetings.
- Identify volume and frequency of legislative activities.
- Provide a temporal dimension for parliamentary events.
- Integrate participation and legislative productivity analyses.

## Columns

| Column | Description |
|---|---|
| `sk_evt` | Sequential surrogate key of the event dimension used in the Gold layer star schema model. |
| `evt_id_evento` | Official unique identifier of the parliamentary event in the Chamber of Deputies API. |
| `evt_tx_uri` | Official URI of the parliamentary event in the Chamber Open Data API. |
| `evt_nr_ano_referencia` | Reference year of the parliamentary event derived from the event execution date. |
| `evt_ts_inicio` | Official start timestamp of the parliamentary event registered by the Chamber of Deputies. |
| `evt_ts_fim` | Official end timestamp of the parliamentary event registered by the Chamber of Deputies. |
| `evt_dt_inicio` | Start date of the parliamentary event derived from the official event opening timestamp. |
| `evt_dt_fim` | End date of the parliamentary event derived from the official event closing timestamp. |
| `evt_qt_duracao_minutos` | Total event duration in minutes calculated between the start and end timestamps. |
| `evt_fl_evento_encerrado` | Derived indicator that identifies whether the parliamentary event has an official closing timestamp registered. |
| `evt_fl_evento_mesmo_dia` | Derived indicator that identifies whether the parliamentary event started and ended on the same calendar day. |
| `evt_tx_periodo_dia` | Derived classification of the time period in which the event started, such as morning, afternoon or evening. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the parliamentary event record. |
| `bronze_id_origem` | Original technical identifier of the event in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original event load in the Bronze layer. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the parliamentary event record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the event dimension. |

## Relationships
- Related to legislative bodies responsible for parliamentary events.
- Can be integrated with parliamentary attendance and event participation fact tables.
- Used in legislative productivity and parliamentary activity frequency analyses.
- Related to plenary sessions, committee meetings and public hearings.
- Can be integrated with legislative voting sessions and propositions discussed during events.

## Technical Notes
- Main source: `silver_curated.eventos`.
- Target table: `gold.dm_evento`.
- Maintains one consolidated row per `evt_id_evento`.
- Records without an official event identifier are discarded before persistence.
- The `sk_evt` key is generated using `row_number()` ordered by `evt_id_evento`.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Derived duration and time-period columns are calculated during Gold layer processing.
- Maintains complete Bronze ingestion traceability for auditing and lineage purposes.
- Structure prepared for future integration with streaming pipelines and real-time monitoring of parliamentary events.

# `dm_frente`

## Type
Dimension

## Notebook
`notebooks/03_gold/10_build_dm_frente.py`

## Description
Conformed dimension of Parliamentary Fronts from the Brazilian Chamber of Deputies. Consolidates institutional and thematic information related to parliamentary fronts officially registered in the Chamber Open Data API.

Parliamentary fronts represent cross-party associations of deputies organized around interests, economic sectors, social agendas or specific legislative themes. This dimension enables political, thematic and parliamentary articulation analyses among deputies, political parties and interest groups.

## Grain
One row per Parliamentary Front.

## Analytical Objectives
- Enable thematic analysis of parliamentary activity.
- Relate deputies to cross-party political groups.
- Support analyses of political alignment and legislative interests.
- Identify thematic concentration of parliamentary fronts.
- Enable political segmentation by area of activity.
- Integrate legislative, partisan and institutional analyses.

## Columns

| Column | Description |
|---|---|
| `sk_frente` | Sequential surrogate key of the parliamentary front dimension used in the Gold layer star schema model. |
| `frente_id_frente` | Official unique identifier of the Parliamentary Front in the Chamber of Deputies API. |
| `frente_tx_uri` | Official URI of the Parliamentary Front in the Chamber Open Data API. |
| `frente_tx_titulo` | Official full name of the Parliamentary Front registered by the Chamber of Deputies. |
| `leg_id_legislatura` | Identifier of the legislature associated with the operational period of the Parliamentary Front. |
| `frente_fl_tema_saude` | Derived indicator that identifies parliamentary fronts related to public health, medicine, hospitals, SUS or healthcare policies. |
| `frente_fl_tema_educacao` | Derived indicator that identifies parliamentary fronts related to education, teaching, universities or educational policies. |
| `frente_fl_tema_seguranca` | Derived indicator that identifies parliamentary fronts related to public security, police, penal system or social defense. |
| `frente_fl_tema_agro` | Derived indicator that identifies parliamentary fronts related to agribusiness, agriculture, livestock or rural production. |
| `frente_fl_tema_mulher` | Derived indicator that identifies parliamentary fronts related to women’s rights, gender equality or women’s protection. |
| `frente_fl_tema_meio_ambiente` | Derived indicator that identifies parliamentary fronts related to environment, sustainability, climate change or environmental preservation. |
| `frente_tx_categoria_tematica` | Consolidated thematic classification of the Parliamentary Front derived from textual analysis of the front title. |
| `frente_qt_temas_identificados` | Number of thematic categories automatically identified for the Parliamentary Front during analytical processing. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the Parliamentary Front record. |
| `bronze_id_origem` | Original technical identifier of the record in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the Parliamentary Front. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original Parliamentary Front record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the Parliamentary Front dimension. |

## Relationships
- Related to deputies who are members of Parliamentary Fronts.
- Can be integrated with political parties and legislatures.
- Used in political alignment and thematic parliamentary articulation analyses.
- Serves as a thematic dimension for political and legislative dashboards.
- Related to parliamentary front composition fact tables and parliamentary analytical indicators.

## Technical Notes
- Main source: `silver_curated.frentes`.
- Target table: `gold.dm_frente`.
- Maintains one consolidated row per `frente_id_frente`.
- Records without an official identifier are discarded before persistence.
- The `sk_frente` key is generated using `row_number()` ordered by `frente_id_frente`.
- Persisted in Delta Lake with Databricks SQL compatibility.
- The thematic indicators (`frente_fl_tema_*`) are derived through heuristic rules applied to the Parliamentary Front title.
- The consolidated thematic classification is generated during Gold layer processing to support aggregated political analyses.
- Maintains complete Bronze layer traceability for auditing and lineage purposes.
- Structure prepared for future parliamentary network and cross-party political relationship analyses.

---

# `dm_uf`

## Type
Dimension

## Notebook
`notebooks/03_gold/11_build_dm_uf.py`

## Description
Conformed dimension of Brazilian Federative Units (UFs) used in parliamentary data from the Brazilian Chamber of Deputies. Consolidates Brazilian states associated with the electoral representation of federal deputies, geographic origin of parliamentarians, suppliers and regional distribution of legislative activity.

This dimension standardizes the state-level geographic reference used in political, electoral, financial and parliamentary analyses in the Gold layer.

## Grain
One row per Brazilian Federative Unit (UF).

## Analytical Objectives
- Enable parliamentary analyses by Brazilian state.
- Relate deputies to their electoral Federative Units.
- Support regional analyses of parliamentary expenses.
- Consolidate the state-level geographic dimension for the star schema model.
- Enable regional political and legislative segmentation.
- Support geographic dashboards and analytical maps.

## Columns

| Column | Description |
|---|---|
| `sk_uf` | Sequential surrogate key of the Federative Unit dimension used in the Gold layer star schema model. |
| `uf_sg_uf` | Official abbreviation of the Brazilian Federative Unit, such as RJ, SP, MG, BA or DF. |
| `uf_tx_nome` | Official full name of the Brazilian Federative Unit corresponding to the UF abbreviation. |
| `uf_tx_regiao` | Official geographic region of Brazil to which the Federative Unit belongs, such as North, Northeast, Central-West, Southeast or South. |
| `uf_fl_capital_federal` | Derived indicator that identifies whether the UF corresponds to the Federal District. |
| `uf_nr_quantidade_deputados` | Number of federal deputies associated with the Federative Unit identified in the current project processing. |
| `uf_fl_regiao_norte` | Derived indicator that identifies whether the UF belongs to the North Region of Brazil. |
| `uf_fl_regiao_nordeste` | Derived indicator that identifies whether the UF belongs to the Northeast Region of Brazil. |
| `uf_fl_regiao_centro_oeste` | Derived indicator that identifies whether the UF belongs to the Central-West Region of Brazil. |
| `uf_fl_regiao_sudeste` | Derived indicator that identifies whether the UF belongs to the Southeast Region of Brazil. |
| `uf_fl_regiao_sul` | Derived indicator that identifies whether the UF belongs to the South Region of Brazil. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the Federative Unit dimension. |

## Relationships
- Related to `gold.dm_deputado` through the deputy's electoral UF.
- Can be integrated with suppliers and parliamentary expenses by geographic location.
- Used in regional legislative and electoral analyses.
- Serves as a geographic dimension for dashboards and analytical maps.
- Enables cross-analysis between parliamentary activity and regional distribution.

## Technical Notes
- Main source derived from `silver_curated.deputados`.
- Target table: `gold.dm_uf`.
- Maintains one consolidated row per UF abbreviation.
- The `sk_uf` key is generated using `row_number()` ordered by `uf_sg_uf`.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Full UF names and regions are derived through internal project mapping.
- Regional indicators are calculated during Gold layer processing.
- Structure prepared for geospatial analyses and regional parliamentary dashboards.
---

# `dm_tipo_despesa`

## Type
Dimension

## Notebook
`notebooks/03_gold/12_build_dm_tipo_despesa.py`

## Description
Conformed dimension of parliamentary expense types from the Parliamentary Activity Quota (CEAP). Consolidates the official classifications of subquotas and expense specifications used by the Brazilian Chamber of Deputies to categorize expenses incurred by parliamentarians.

This dimension standardizes financial categories used in parliamentary public spending analyses, enabling segmentation by expense nature, contracted service type and official administrative classification defined by the Chamber of Deputies.

## Grain
One row per combination of parliamentary subquota and expense specification.

## Analytical Objectives
- Enable analytical segmentation of parliamentary expenses.
- Standardize financial categories used in CEAP.
- Support auditing and transparency of public spending.
- Consolidate official expense classifications from the Chamber of Deputies.
- Enable financial analyses by parliamentary expense nature.
- Facilitate dashboards for expense composition and distribution.

## Columns

| Column | Description |
|---|---|
| `sk_tipo_desp` | Sequential surrogate key of the expense type dimension used in the Gold layer star schema model. |
| `desp_cd_subcota` | Official parliamentary subquota code used by the Chamber of Deputies to classify the primary CEAP expense type. |
| `desp_tx_tipo_despesa` | Official description of the parliamentary expense type associated with the subquota, such as airline tickets, fuel, parliamentary communication, lodging or vehicle rental. |
| `desp_cd_especificacao_subcota` | Complementary subquota specification code used for additional detailing of the parliamentary expense type. |
| `desp_tx_especificacao` | Detailed textual description of the parliamentary expense specification associated with the CEAP subquota. |
| `desp_tx_categoria_macro` | Analytical financial category derived by the project for corporate grouping of parliamentary expenses, such as transportation, food, communication, office or consulting. |
| `desp_fl_despesa_transporte` | Derived indicator that identifies expenses related to parliamentary transportation, including airline tickets, fuel and mobility. |
| `desp_fl_despesa_hospedagem` | Derived indicator that identifies parliamentary expenses related to lodging and accommodation. |
| `desp_fl_despesa_divulgacao` | Derived indicator that identifies expenses related to parliamentary activity promotion and institutional communication. |
| `desp_fl_despesa_combustivel` | Derived indicator that identifies parliamentary fuel and vehicle refueling expenses. |
| `desp_fl_despesa_consultoria` | Derived indicator that identifies expenses related to consulting, technical advisory or specialized services. |
| `desp_fl_despesa_escritorio` | Derived indicator that identifies expenses related to administrative maintenance and parliamentary office operations. |
| `desp_fl_possui_especificacao` | Derived indicator that identifies whether the expense has an additional specification registered beyond the primary subquota. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the parliamentary expense record. |
| `bronze_id_origem` | Original technical identifier of the expense record in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the record associated with the expense type. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original parliamentary expense record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the expense type dimension. |

## Relationships
- Related to parliamentary CEAP expense fact tables.
- Can be integrated with deputies, political parties and suppliers through parliamentary expenses.
- Used in parliamentary financial analyses and public transparency dashboards.
- Serves as a categorical dimension for grouping and consolidating parliamentary expenses.
- Enables comparative analyses across categories of parliamentary public expenses.

## Technical Notes
- Main source: `silver_curated.despesas`.
- Target table: `gold.dm_tipo_despesa`.
- Maintains one consolidated row per combination of subquota and specification.
- The `sk_tipo_desp` key is generated using `row_number()` ordered by `desp_cd_subcota` and `desp_cd_especificacao_subcota`.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Macro categories and thematic indicators are derived during Gold layer processing.
- Maintains complete Bronze layer traceability for financial auditing and data lineage.
- Structure prepared for future evolution with more detailed parliamentary financial taxonomies.

---
# `dm_bancada`

## Type
Dimension

## Notebook
`notebooks/03_gold/13_build_dm_bancada.py`

## Description
Conformed dimension of parliamentary caucuses and party blocs from the Brazilian Chamber of Deputies. Consolidates political groupings used in voting sessions, party orientations and parliamentary articulations registered in Chamber legislative data.

This dimension standardizes party caucuses, parliamentary blocs and political groupings identified in voting and parliamentary orientation records, enabling analyses of political alignment, party loyalty and collective legislative behavior.

## Grain
One row per parliamentary caucus or party bloc identified in legislative data.

## Analytical Objectives
- Enable political analysis by parliamentary caucus.
- Consolidate party groupings used in voting sessions.
- Support analyses of party loyalty and political alignment.
- Identify parliamentary blocs and legislative coalitions.
- Standardize political nomenclatures used in legislative records.
- Integrate analyses of collective parliamentary behavior.

## Columns

| Column | Description |
|---|---|
| `sk_banc` | Sequential surrogate key of the caucus dimension used in the Gold layer star schema model. |
| `banc_tx_bancada_curada` | Standardized name of the parliamentary caucus or political bloc used by the project for analytical consolidation of legislative orientations. |
| `banc_tx_tipo_bancada` | Classification of the parliamentary caucus, such as political party, parliamentary bloc, government leadership, opposition or independents. |
| `banc_tx_uri` | URI or reference identifier associated with the parliamentary caucus when available in the processed legislative data. |
| `banc_fl_bancada_valida` | Derived indicator that identifies whether the parliamentary caucus has consistent and valid identification according to standardization rules implemented in the project. |
| `banc_fl_bloco_parlamentar` | Derived indicator that identifies whether the record represents a parliamentary bloc composed of multiple political parties. |
| `banc_fl_partido_politico` | Derived indicator that identifies whether the caucus directly corresponds to an individual political party. |
| `banc_fl_governo` | Derived indicator that identifies caucuses associated with the governing base in the analyzed parliamentary orientations. |
| `banc_fl_oposicao` | Derived indicator that identifies caucuses classified as parliamentary opposition during the analyzed voting sessions. |
| `banc_qt_partidos_bloco` | Number of political parties identified in the composition of the parliamentary bloc when applicable. |
| `banc_tx_composicao_bloco` | Consolidated textual description of the party composition of the parliamentary bloc identified in legislative data. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the record related to the parliamentary caucus. |
| `bronze_id_origem` | Original technical identifier of the legislative record used during Bronze ingestion. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the record associated with the parliamentary caucus. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original record related to the parliamentary caucus. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the parliamentary caucus dimension. |

## Relationships
- Related to parliamentary voting fact tables and party orientation records.
- Used in analyses of party loyalty and political alignment.
- Can be integrated with deputies, political parties and legislative propositions.
- Serves as a political dimension for legislative dashboards and parliamentary analyses.
- Enables studies of political coalitions and collective legislative behavior.

## Technical Notes
- Main source derived from `silver_curated.votacoes_orientacoes`.
- Target table: `gold.dm_bancada`.
- Maintains one consolidated row per standardized parliamentary caucus or bloc.
- The `sk_banc` key is generated using `row_number()` ordered by `banc_tx_bancada_curada`.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Political classifications and indicators are derived from curation rules implemented in the Silver/Gold layers.
- The composition of parliamentary blocs may be derived through textual parsing of legislative orientations.
- Maintains complete Bronze layer traceability for auditing and lineage purposes.
- Structure prepared for future analyses of political networks and parliamentary coalition dynamics.
---

# `dm_responsavel_ceap`

## Type
Dimension

## Notebook
`notebooks/03_gold/14_build_dm_responsavel_ceap.py`

## Description
Conformed dimension of entities responsible for Parliamentary Activity Quota (CEAP) expenses. Consolidates parliamentarians, leadership structures and institutional entities associated with the accountability process of parliamentary expenses registered by the Brazilian Chamber of Deputies.

This dimension was created to standardize financial responsible entities identified in CEAP records, enabling differentiation between expenses directly linked to federal deputies, party leaderships and other parliamentary structures involved in the execution of public expenditures.

## Grain
One row per responsible entity identified in parliamentary CEAP expenses.

## Analytical Objectives
- Enable financial analysis by responsible entity for parliamentary expenses.
- Differentiate individual deputy expenses from party leadership expenses.
- Consolidate entities responsible for CEAP financial execution.
- Support auditing and transparency of parliamentary expenditures.
- Facilitate expense analyses by parliamentary structure.
- Standardize financial responsible entities used in Chamber accountability records.

## Columns

| Column | Description |
|---|---|
| `sk_resp_ceap` | Sequential surrogate key of the CEAP responsible entity dimension used in the Gold layer star schema model. |
| `dept_tx_nome_parlamentar` | Parliamentary name of the entity responsible for the CEAP expense as registered in official Chamber of Deputies data. |
| `resp_tx_tipo_responsavel` | Analytical classification of the entity responsible for the parliamentary expense, such as DEPUTY, LEADERSHIP or NOT_IDENTIFIED. |
| `resp_tx_nome_responsavel_curado` | Standardized name of the responsible entity used for analytical consolidation and elimination of textual inconsistencies. |
| `resp_fl_lideranca` | Derived indicator that identifies expenses associated with party leaderships or parliamentary leadership structures. |
| `resp_fl_deputado` | Derived indicator that identifies expenses directly associated with individual federal deputies. |
| `resp_fl_responsavel_identificado` | Derived indicator that identifies whether the responsible entity was correctly identified and classified during processing. |
| `resp_qt_despesas` | Total number of parliamentary expenses associated with the identified responsible entity. |
| `resp_vl_total_despesas` | Total amount of parliamentary expenses linked to the CEAP responsible entity. |
| `resp_dt_primeira_despesa` | Date of the first parliamentary expense identified for the responsible entity in the processed historical dataset. |
| `resp_dt_ultima_despesa` | Date of the most recent parliamentary expense identified for the responsible entity in the available historical dataset. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the CEAP expense record associated with the financial responsible entity. |
| `bronze_id_origem` | Original technical identifier of the expense record used during Bronze ingestion. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the parliamentary expense associated with the responsible entity. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original parliamentary expense record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the CEAP responsible entity dimension. |

## Relationships
- Related to parliamentary CEAP expense fact tables.
- Can be integrated with the deputy dimension through parliamentary name or resolved identifier.
- Used in parliamentary financial analyses and public transparency dashboards.
- Enables segmentation between individual expenses and party leadership expenses.
- Serves as an organizational dimension for analyses of parliamentary financial responsibility.

## Technical Notes
- Main source: `silver_curated.despesas`.
- Target table: `gold.dm_responsavel_ceap`.
- Maintains one consolidated row per responsible entity identified in parliamentary expenses.
- The `sk_resp_ceap` key is generated using `row_number()` ordered by `dept_tx_nome_parlamentar`.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Responsible entity classification is derived from heuristic rules applied to the parliamentary name.
- Records containing terms related to “LEADERSHIP” are classified as parliamentary leadership structures.
- Maintains complete Bronze layer traceability for financial auditing and lineage purposes.
- Structure prepared for future integration with parliamentary financial governance models and political structure analyses.

---

# `ft_despesas_ceap`

## Type
Fact

## Notebook
`notebooks/03_gold/15_build_ft_despesas_ceap.py`

## Description
Fact table of parliamentary expenses from the Parliamentary Activity Quota (CEAP). Consolidates expenses incurred by federal deputies of the Brazilian Chamber of Deputies based on fiscal documents submitted for reimbursement or payment through CEAP.

The table records parliamentary financial operations related to airline tickets, fuel, lodging, parliamentary communication, consulting services, vehicle rentals, food and other expenses authorized by the Chamber of Deputies.

Represents the primary financial fact table of the parliamentary star schema model in the Gold layer.

## Grain
One row per parliamentary CEAP expense fiscal document.

## Analytical Objectives
- Enable detailed analysis of parliamentary expenditures.
- Consolidate public expenses by deputy, political party and legislature.
- Support financial auditing and parliamentary transparency.
- Identify CEAP spending patterns.
- Enable temporal, geographic and financial analyses of public expenditures.
- Integrate suppliers, expense types and financial responsible entities.
- Support anomaly detection and parliamentary spending concentration analyses.

## Columns

| Column | Description |
|---|---|
| `sk_despesa_ceap` | Sequential surrogate key of the CEAP expense fact table used in the Gold layer star schema model. |
| `resp.sk_resp_ceap` | Surrogate key of the `dm_responsavel_ceap` dimension associated with the financial responsible entity for the parliamentary expense. |
| `dept.sk_dept` | Surrogate key of the `dm_deputado` dimension associated with the federal deputy responsible for the expense. |
| `part.sk_part` | Surrogate key of the `dm_partido` dimension associated with the political party of the parliamentarian at the time of the expense. |
| `leg.sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the legislative period of the parliamentary expense. |
| `forn.sk_forn` | Surrogate key of the `dm_fornecedor` dimension associated with the supplier of the expense fiscal document. |
| `tipo.sk_desp_tipo` | Surrogate key of the `dm_tipo_despesa` dimension associated with the financial classification of the CEAP expense. |
| `uf.sk_uf` | Surrogate key of the `dm_uf` dimension associated with the federative unit of the parliamentarian or expense. |
| `desp.desp_dt_emissao` | Issuance date of the fiscal document associated with the parliamentary expense. |
| `desp_nr_ano` | Reference year of the parliamentary expense derived from the fiscal document issuance date. |
| `desp_nr_mes` | Reference month of the parliamentary expense derived from the fiscal document issuance date. |
| `desp_vl_documento` | Original gross amount of the fiscal document submitted by the parliamentarian in the CEAP accountability process. |
| `desp_vl_liquido` | Net amount effectively considered for reimbursement or payment of the parliamentary expense. |
| `desp_vl_glosa` | Amount rejected or disallowed by the Chamber of Deputies during the parliamentary expense accountability analysis. |
| `desp_vl_restituicao` | Amount returned or reimbursed related to the parliamentary expense when applicable. |
| `desp_tx_numero_documento` | Identification number of the fiscal document used in parliamentary accountability. |
| `desp_tx_tipo_documento` | Type of fiscal document submitted, such as invoice, receipt, airline ticket or billing statement. |
| `desp_tx_url_documento` | Official URL of the digitized fiscal document made publicly available by the Chamber of Deputies for transparency purposes. |
| `desp_fl_documento_digital` | Derived indicator that identifies whether the expense has a digital document available for public consultation. |
| `desp_fl_glosada` | Derived indicator that identifies parliamentary expenses with amounts disallowed by the Chamber of Deputies. |
| `desp_fl_restituida` | Derived indicator that identifies parliamentary expenses with registered financial reimbursement or restitution. |
| `desp_fl_despesa_alta` | Derived indicator used to identify expenses considered above the statistical threshold defined by the project. |
| `desp_tx_observacao` | Complementary text or observation associated with the parliamentary expense record when available in the source system. |
| `desp_qt_documentos_mesmo_fornecedor` | Number of fiscal documents issued by the same supplier identified within the analyzed period. |
| `desp_vl_media_fornecedor` | Average value of expenses incurred with the same supplier calculated during analytical processing. |
| `desp_fl_fornecedor_recorrente` | Derived indicator that identifies recurring suppliers in parliamentary expenses associated with the deputy or political party. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the parliamentary expense record. |
| `bronze_id_origem` | Original technical identifier of the parliamentary expense in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the CEAP expense. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original financial record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the CEAP expense fact table. |

## Relationships
- Related to `dm_deputado`, `dm_partido`, `dm_legislatura`, `dm_fornecedor`, `dm_tipo_despesa`, `dm_responsavel_ceap` and `dm_uf`.
- Serves as the primary parliamentary financial fact table in the star schema model.
- Used in transparency dashboards, auditing and parliamentary financial intelligence analyses.
- Can be integrated with analytical models of parliamentary behavior and financial efficiency.
- Supports temporal, partisan, regional and thematic analyses of public expenditures.

## Technical Notes
- Main source: `silver_curated.despesas`.
- Target table: `gold.ft_despesas_ceap`.
- Maintains granularity at the individual fiscal document level.
- Invalid records or records rejected by quality rules are segregated during pipeline processing.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Derived metrics and financial indicators are calculated during Gold layer processing.
- Maintains complete Bronze layer traceability for financial auditing and data lineage.
- Structure prepared for antifraud analyses, anomaly detection and public transparency monitoring.
- Compatible with analytical streaming pipelines and incremental monitoring of parliamentary expenses.

---
# `ft_votacoes`

## Type
Fact

## Notebook
`notebooks/03_gold/16_build_ft_votacoes.py`

## Description
Consolidated fact table of parliamentary voting sessions held in the Brazilian Chamber of Deputies. Stores aggregated information related to legislative voting sessions involving legislative propositions, including voting results, number of favorable and opposing votes, responsible legislative body, political orientation and legislative deliberation context.

This table represents the primary analytical fact table for parliamentary deliberations in the Gold layer, enabling analyses of legislative behavior, political alignment, parliamentary productivity and proposition approval dynamics in the Brazilian Chamber of Deputies.

## Grain
One row per parliamentary voting session held in the Brazilian Chamber of Deputies.

## Analytical Objectives
- Enable consolidated analysis of legislative voting sessions.
- Relate voting sessions to legislative propositions, legislative bodies and legislatures.
- Support analyses of legislative approval and rejection.
- Identify political behavior and parliamentary alignment.
- Enable temporal and institutional analyses of legislative deliberations.
- Serve as the foundation for party loyalty and parliamentary intelligence indicators.
- Integrate analyses of legislative productivity and political governance.

## Columns

| Column | Description |
|---|---|
| `sk_votacao` | Sequential surrogate key of the voting fact table used in the Gold layer star schema model. |
| `sk_prop` | Surrogate key of the `dm_proposicao` dimension associated with the voted legislative proposition. |
| `sk_org` | Surrogate key of the `dm_orgao` dimension associated with the legislative body responsible for the voting session. |
| `sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the legislative period of the parliamentary voting session. |
| `vot_id_votacao` | Official unique identifier of the parliamentary voting session in the Chamber of Deputies API. |
| `vot.vot_dt_votacao` | Official date of the parliamentary voting session registered by the Chamber of Deputies. |
| `vot_ts_votacao` | Complete timestamp of the parliamentary voting session used for detailed temporal analyses. |
| `vot_nr_ano` | Year of the parliamentary voting session derived from the official legislative deliberation date. |
| `vot_nr_mes` | Month of the parliamentary voting session derived from the official legislative deliberation date. |
| `vot_tx_descricao` | Short textual description of the parliamentary voting session registered by the Chamber of Deputies. |
| `vot_tx_resultado` | Official result of the parliamentary voting session, such as Approved, Rejected or Impaired. |
| `vot_fl_aprovada` | Derived indicator that identifies whether the voting session resulted in approval of the legislative matter. |
| `vot_fl_rejeitada` | Derived indicator that identifies whether the voting session resulted in rejection of the legislative matter. |
| `vot_qt_votos_sim` | Total number of favorable (“Yes”) votes registered in the parliamentary voting session. |
| `vot_qt_votos_nao` | Total number of opposing (“No”) votes registered in the parliamentary voting session. |
| `vot_qt_abstencoes` | Total number of abstentions registered in the parliamentary voting session. |
| `vot_qt_obstrucoes` | Total number of votes classified as parliamentary obstruction. |
| `vot_qt_presentes` | Total number of parliamentarians present during the legislative voting session. |
| `vot_qt_ausentes` | Total number of parliamentarians absent during the legislative voting session. |
| `vot_qt_total_votos` | Total number of votes counted in the parliamentary voting session. |
| `vot_pc_aprovacao` | Percentage of favorable votes relative to the total number of valid votes in the parliamentary voting session. |
| `vot_pc_rejeicao` | Percentage of opposing votes relative to the total number of valid votes in the parliamentary voting session. |
| `vot_fl_votacao_unanime` | Derived indicator that identifies parliamentary voting sessions without divergence between favorable and opposing votes. |
| `vot_fl_votacao_apertada` | Derived indicator that identifies voting sessions decided by a narrow margin according to analytical criteria defined by the project. |
| `vot_tx_tipo_votacao` | Classification of the parliamentary voting session, such as symbolic, nominal or secret, when available in the source system. |
| `vot_tx_orientacao_governo` | Official government political orientation registered for the parliamentary voting session when available. |
| `vot_tx_orientacao_oposicao` | Official opposition political orientation registered for the parliamentary voting session when available. |
| `vot_fl_quorum_alto` | Derived indicator that identifies voting sessions with high parliamentary participation according to analytical criteria defined by the project. |
| `vot_tx_resumo_resultado` | Consolidated analytical text summarizing the result and quantitative context of the parliamentary voting session. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the parliamentary voting session record. |
| `bronze_id_origem` | Original technical identifier of the parliamentary voting session in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the parliamentary voting session. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original parliamentary voting session record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the parliamentary voting fact table. |

## Relationships
- Related to `dm_proposicao`, `dm_orgao` and `dm_legislatura`.
- Can be integrated with the individual parliamentary votes fact table.
- Used in analyses of political behavior and party alignment.
- Serves as the foundation for party loyalty and governability indicators.
- Integrated into legislative dashboards and parliamentary intelligence analyses.

## Technical Notes
- Main source: `silver_curated.votacoes`.
- Target table: `gold.ft_votacoes`.
- Maintains granularity at the individual parliamentary voting session level.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Derived indicators related to approval, unanimity and voting margin are calculated during Gold layer processing.
- Maintains complete Bronze layer traceability for legislative auditing and lineage purposes.
- Structure prepared for integration with real-time parliamentary voting streaming pipelines.
- Compatible with incremental analyses and continuous monitoring of legislative activity.

---

# `ft_votos`

## Type
Fact

## Notebook
`notebooks/03_gold/17_build_ft_votos.py`

## Description
Fact table of individual votes cast by federal deputies in parliamentary voting sessions of the Brazilian Chamber of Deputies. Consolidates the nominal positioning of each parliamentarian in legislative deliberations, enabling detailed analysis of political behavior, party alignment, parliamentary loyalty and legislative dynamics.

Each record represents the individual vote of a deputy in a specific voting session, including the official vote registered by the Chamber, political party, legislature, federative unit and temporal context of the parliamentary deliberation.

This table represents the primary analytical fact table of individual parliamentary behavior in the Gold layer.

## Grain
One row per individual deputy vote in a parliamentary voting session.

## Analytical Objectives
- Enable individual analysis of parliamentary behavior.
- Identify political alignment and party loyalty.
- Support analyses of governability and opposition dynamics.
- Relate deputies to legislative voting sessions.
- Measure parliamentary participation in nominal voting sessions.
- Detect party divergences and voting patterns.
- Integrate political, legislative and partisan analyses.

## Columns

| Column | Description |
|---|---|
| `sk_voto` | Sequential surrogate key of the parliamentary votes fact table used in the Gold layer star schema model. |
| `dept.sk_dept` | Surrogate key of the `dm_deputado` dimension associated with the federal deputy who cast the parliamentary vote. |
| `part.sk_part` | Surrogate key of the `dm_partido` dimension associated with the deputy's political party at the time of the voting session. |
| `leg.sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the legislative period of the parliamentary voting session. |
| `uf.sk_uf` | Surrogate key of the `dm_uf` dimension associated with the federative unit of the federal deputy. |
| `sk_votacao` | Surrogate key of the `ft_votacoes` fact table associated with the corresponding parliamentary voting session. |
| `voto_id_votacao` | Official identifier of the parliamentary voting session associated with the deputy's individual vote. |
| `voto.vot_ts_voto` | Official timestamp of the individual parliamentary vote cast by the deputy. |
| `voto_dt_votacao` | Date of the parliamentary voting session derived from the official individual vote timestamp. |
| `voto_tx_tipo_voto` | Official vote position registered by the deputy in the parliamentary voting session, such as Yes, No, Abstention, Obstruction or Article 17. |
| `voto_fl_voto_sim` | Derived indicator that identifies votes favorable to the legislative proposition. |
| `voto_fl_voto_nao` | Derived indicator that identifies votes opposing the legislative proposition. |
| `voto_fl_abstencao` | Derived indicator that identifies votes classified as parliamentary abstention. |
| `voto_fl_obstrucao` | Derived indicator that identifies votes classified as parliamentary obstruction. |
| `voto_fl_ausencia` | Derived indicator that identifies absence of the parliamentarian during the legislative voting session. |
| `voto_fl_acompanhou_partido` | Derived indicator that identifies whether the deputy voted according to the official orientation of the political party or parliamentary caucus. |
| `voto_fl_divergiu_partido` | Derived indicator that identifies whether the deputy diverged from the official orientation of the political party or parliamentary caucus. |
| `voto_fl_acompanhou_governo` | Derived indicator that identifies whether the parliamentary vote followed the official government orientation registered for the voting session. |
| `voto_fl_acompanhou_oposicao` | Derived indicator that identifies whether the parliamentary vote followed the official opposition orientation registered for the legislative voting session. |
| `voto_tx_orientacao_partido` | Official orientation of the political party caucus associated with the individual parliamentary vote. |
| `voto_tx_orientacao_governo` | Official orientation of the federal government registered for the parliamentary voting session. |
| `voto_tx_orientacao_oposicao` | Official orientation of the parliamentary opposition registered for the legislative voting session. |
| `voto_fl_voto_valido` | Derived indicator that identifies whether the vote was considered valid for statistical and legislative analyses. |
| `voto_nr_ano` | Year of the parliamentary voting session derived from the official vote date. |
| `voto_nr_mes` | Month of the parliamentary voting session derived from the official vote date. |
| `voto_tx_resumo_politico` | Consolidated analytical text summarizing the political positioning of the parliamentarian in the analyzed voting session. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the individual parliamentary vote record. |
| `bronze_id_origem` | Original technical identifier of the parliamentary vote in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the parliamentary vote. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original parliamentary vote record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the parliamentary votes fact table. |

## Relationships
- Related to `dm_deputado`, `dm_partido`, `dm_legislatura`, `dm_uf` and `ft_votacoes`.
- Used in analyses of party loyalty and political alignment.
- Serves as the foundation for governability and parliamentary behavior indicators.
- Integrated into legislative dashboards and political intelligence analyses.
- Enables both individual and collective parliamentary voting analyses.

## Technical Notes
- Main source: `silver_curated.votacoes_votos`.
- Target table: `gold.ft_votos`.
- Maintains granularity at the individual parliamentary vote level.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Party and government alignment indicators are derived during Gold layer processing.
- Maintains complete Bronze layer traceability for legislative auditing and lineage purposes.
- Structure prepared for advanced analyses of political behavior, parliamentary networks and legislative intelligence.
- Compatible with incremental pipelines and real-time parliamentary voting streaming.

---
# `ft_orientacoes_bancada`

## Type
Fact

## Notebook
`notebooks/03_gold/18_build_ft_orientacoes_bancada.py`

## Description
Fact table of voting orientations issued by parliamentary caucuses, political parties and legislative blocs during voting sessions in the Brazilian Chamber of Deputies. Consolidates the official positioning of party leaderships and parliamentary caucuses regarding legislative propositions submitted to parliamentary deliberation.

This table enables analyses of collective political behavior, party alignment, legislative strategies and institutional positioning of political parties, government and opposition during parliamentary voting sessions.

Each record represents the official orientation of a parliamentary caucus in a specific voting session.

## Grain
One row per caucus orientation in a parliamentary voting session.

## Analytical Objectives
- Enable analysis of political positioning of parliamentary caucuses.
- Relate political parties and blocs to legislative voting sessions.
- Support analyses of political alignment and party loyalty.
- Identify official orientations issued by government, opposition and political parties.
- Consolidate collective political behavior in parliamentary deliberations.
- Integrate analyses of legislative coalitions and governability.
- Serve as the foundation for comparative analysis between party orientation and individual parliamentary voting behavior.

## Columns

| Column | Description |
|---|---|
| `sk_orientacao` | Sequential surrogate key of the parliamentary orientations fact table used in the Gold layer star schema model. |
| `banc.sk_banc` | Surrogate key of the `dm_bancada` dimension associated with the parliamentary caucus responsible for the voting orientation. |
| `org.sk_org` | Surrogate key of the `dm_orgao` dimension associated with the legislative body responsible for the parliamentary voting session. |
| `ori.vot_id_votacao` | Official identifier of the parliamentary voting session associated with the caucus orientation. |
| `ori.vot_tx_uri` | Official URI of the parliamentary voting session in the Chamber of Deputies Open Data API. |
| `ori.org_sg_orgao` | Official abbreviation of the legislative body where the parliamentary voting session occurred, such as PLEN or CCJC. |
| `ori.banc_tx_bancada_curada` | Standardized name of the parliamentary caucus or political bloc used in the analytical consolidation of legislative orientations. |
| `ori.vot_tx_orientacao` | Original official orientation registered by the parliamentary caucus for the voting session, such as Yes, No, Released, Obstruction or Abstention. |
| `ori.vot_tx_orientacao_curada` | Parliamentary orientation standardized by the project for analytical consolidation and elimination of textual inconsistencies. |
| `ori.vot_tx_descricao_resultado` | Consolidated textual description of the political orientation issued by the parliamentary caucus. |
| `ori.vot_fl_orientacao_sim` | Derived indicator that identifies orientations favorable to approval of the legislative proposition. |
| `ori.vot_fl_orientacao_nao` | Derived indicator that identifies orientations opposing approval of the legislative proposition. |
| `ori.vot_fl_orientacao_liberado` | Derived indicator that identifies orientations in which the caucus released its parliamentarians to vote freely. |
| `ori.vot_fl_orientacao_obstrucao` | Derived indicator that identifies parliamentary obstruction orientations. |
| `ori.vot_fl_orientacao_abstencao` | Derived indicator that identifies parliamentary abstention orientations. |
| `ori.vot_fl_orientacao_governo` | Derived indicator that identifies orientations issued by the governing leadership during the parliamentary voting session. |
| `ori.vot_fl_orientacao_oposicao` | Derived indicator that identifies orientations issued by the parliamentary opposition during the legislative voting session. |
| `ori.vot_fl_orientacao_coesa` | Derived indicator used in analyses of party cohesion and collective political alignment. |
| `ori.vot_tx_dedup_key` | Technical deduplication key used by the pipeline to guarantee uniqueness of the processed parliamentary orientation. |
| `ori.bronze_nr_ano_referencia` | Reference year of the original ingestion of the record in the Bronze layer. |
| `ori.bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `ori.bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `ori.bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the parliamentary orientation record. |
| `ori.bronze_id_origem` | Original technical identifier of the parliamentary orientation in the Bronze ingestion source. |
| `ori.bronze_tx_source_file` | Source file name used during ingestion of the parliamentary orientation when applicable to the processing pipeline. |
| `ori.bronze_id_batch` | Ingestion batch identifier responsible for the original load of the parliamentary orientation. |
| `ori.bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original parliamentary orientation record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the caucus orientations fact table. |

## Relationships
- Related to `dm_bancada` and `dm_orgao`.
- Can be integrated with `ft_votacoes` through the parliamentary voting session identifier.
- Used in analyses of party loyalty and political alignment.
- Serves as the foundation for comparison between party orientation and individual parliamentary voting behavior.
- Integrated into legislative dashboards and parliamentary governability analyses.
- Enables analyses of political coalitions and collective behavior of political parties and parliamentary blocs.

## Technical Notes
- Main source: `silver_curated.votacoes_orientacoes`.
- Target table: `gold.ft_orientacoes_bancada`.
- Maintains granularity at the caucus orientation level for each parliamentary voting session.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Orientations are standardized during Gold layer processing to guarantee analytical consistency.
- The column `ori.vot_tx_dedup_key` is used for duplicate control and record integrity validation.
- Maintains complete Bronze layer traceability for legislative auditing and lineage purposes.
- Compatible with incremental pipelines, legislative CDC and parliamentary voting streaming.
- Structure prepared for advanced analyses of party cohesion, governability and parliamentary political dynamics.

---

# `ft_atividade_parlamentar`

## Type
Fact

## Notebook
`notebooks/03_gold/19_build_ft_atividade_parlamentar.py`

## Description
Consolidated fact table of parliamentary activity of federal deputies from the Brazilian Chamber of Deputies. Aggregates quantitative and financial indicators related to parliamentary activity, including CEAP expenses, legislative participation, political behavior and derived analytical metrics used by the project.

This table was designed to serve as a consolidated analytical layer of parliamentary performance, enabling integrated analysis of the political, financial and legislative activity of federal deputies.

Each record represents an analytical summary of the parliamentary activity of a deputy within a specific legislature.

## Grain
One row per federal deputy within a legislature.

## Analytical Objectives
- Consolidate analytical indicators of parliamentary activity.
- Integrate financial, legislative and political metrics.
- Enable parliamentary activity ranking.
- Support executive dashboards and political intelligence.
- Identify legislative productivity patterns.
- Measure CEAP utilization intensity.
- Integrate parliamentary behavior, expenses and voting activity.

## Columns

| Column | Description |
|---|---|
| `sk_atividade_parlamentar` | Sequential surrogate key of the parliamentary activity fact table used in the Gold layer star schema model. |
| `dept.sk_dept` | Surrogate key of the `dm_deputado` dimension associated with the analyzed federal deputy. |
| `part.sk_part` | Surrogate key of the `dm_partido` dimension associated with the deputy's political party during the analyzed period. |
| `leg.sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the analyzed legislative period. |
| `uf.sk_uf` | Surrogate key of the `dm_uf` dimension associated with the federal deputy's federative unit. |
| `dept_src.dept_id_deputado` | Official identifier of the federal deputy used as the business key during parliamentary activity analytical consolidation. |
| `dept_src.part_sg_partido` | Official abbreviation of the political party associated with the deputy during parliamentary activity processing. |
| `dept_src.uf_sg_uf` | Abbreviation of the federative unit representing the deputy's electoral constituency. |
| `dept_src.leg_id_legislatura` | Official identifier of the legislature associated with the consolidated parliamentary activity period. |
| `qt_despesas` | Total number of CEAP parliamentary expenses registered for the deputy during the analyzed period. |
| `vl_total_despesas` | Total financial amount of CEAP parliamentary expenses incurred by the deputy during the analyzed period. |
| `vl_total_liquido` | Consolidated net amount of parliamentary expenses effectively considered after disallowances and financial adjustments. |
| `vl_total_glosado` | Total amount disallowed by the Chamber of Deputies in the deputy's parliamentary expenses. |
| `qt_fornecedores_distintos` | Number of distinct suppliers used by the deputy in registered parliamentary expenses. |
| `qt_tipos_despesa` | Number of distinct parliamentary expense categories used by the deputy. |
| `qt_votacoes_participadas` | Total number of parliamentary voting sessions in which the deputy participated. |
| `qt_votos_sim` | Total number of favorable votes cast by the deputy in the analyzed parliamentary voting sessions. |
| `qt_votos_nao` | Total number of opposing votes cast by the deputy in the analyzed parliamentary voting sessions. |
| `qt_abstencoes` | Total number of abstentions registered by the deputy in the analyzed parliamentary voting sessions. |
| `qt_obstrucoes` | Total number of votes classified as parliamentary obstruction cast by the deputy. |
| `pc_presenca_votacoes` | Percentage of deputy attendance in analyzed parliamentary voting sessions relative to the total number of eligible voting sessions. |
| `pc_alinhamento_partido` | Percentage of alignment between the deputy and the official orientation of the political party or parliamentary caucus. |
| `pc_alinhamento_governo` | Percentage of alignment between the deputy and official government orientations in the analyzed voting sessions. |
| `pc_divergencia_partido` | Percentage of voting sessions in which the deputy diverged from the official orientation of the political party. |
| `vl_media_despesa` | Average value of parliamentary expenses incurred by the deputy during the analyzed period. |
| `vl_maior_despesa` | Highest individual parliamentary expense amount registered for the deputy during the analyzed period. |
| `qt_eventos_participados` | Number of parliamentary events associated with the deputy's legislative activity when available in analytical processing. |
| `qt_proposicoes_relacionadas` | Number of legislative propositions related to the deputy's parliamentary activity during the analyzed period. |
| `fl_deputado_alta_atividade` | Derived indicator that identifies deputies with high parliamentary activity intensity according to analytical criteria defined by the project. |
| `fl_deputado_alto_custo` | Derived indicator that identifies deputies with high financial volume of parliamentary expenses. |
| `fl_deputado_baixa_presenca` | Derived indicator that identifies deputies with low participation in parliamentary voting sessions. |
| `tx_classificacao_atividade` | Consolidated analytical classification of the deputy's parliamentary activity derived from productivity, attendance and legislative behavior metrics. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the base data used in parliamentary activity consolidation. |
| `bronze_dt_ingestao` | Technical ingestion date of the records used in the parliamentary activity composition process. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the base data used to generate the analytical fact table. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the parliamentary activity fact table. |

## Relationships
- Related to `dm_deputado`, `dm_partido`, `dm_legislatura` and `dm_uf`.
- Consolidates information derived from `ft_despesas_ceap`, `ft_votos` and `ft_votacoes`.
- Serves as the foundation for executive parliamentary dashboards.
- Used in productivity, transparency and political behavior analyses.
- Can feed analytical ranking and parliamentary scoring models.

## Technical Notes
- Main source derived from multiple Gold fact tables and dimensions.
- Target table: `gold.ft_atividade_parlamentar`.
- Maintains consolidated granularity per deputy and legislature.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Analytical metrics are calculated during Gold layer processing.
- Derived indicators use business rules defined in the parliamentary intelligence project.
- Maintains partial Bronze layer traceability for auditing and analytical lineage purposes.
- Structure prepared for incremental evolution with new parliamentary indicators.
- Compatible with analytical dashboards, parliamentary scorecards and political intelligence models.

---

# `ft_presenca_eventos`

## Type
Fact

## Notebook
`notebooks/03_gold/20_build_ft_presenca_eventos.py`

## Description
Fact table of parliamentary attendance in legislative events of the Brazilian Chamber of Deputies. Consolidates the participation of federal deputies in plenary sessions, public hearings, committee meetings, seminars and other parliamentary events registered in the Chamber Open Data API.

This table enables measurement of parliamentary engagement, institutional attendance, participation in legislative bodies and in-person parliamentary behavior during official activities of the Chamber of Deputies.

Each record represents the attendance of a deputy in a specific parliamentary event.

## Grain
One row per deputy participating in a parliamentary event.

## Analytical Objectives
- Enable analysis of parliamentary attendance in legislative events.
- Measure institutional participation of federal deputies.
- Relate parliamentarians to legislative bodies and activities.
- Support parliamentary engagement and productivity indicators.
- Identify participation patterns in committees and plenary sessions.
- Integrate temporal and institutional analyses of parliamentary attendance.
- Serve as the foundation for legislative attendance indicators.

## Columns

| Column | Description |
|---|---|
| `sk_presenca_evento` | Sequential surrogate key of the event attendance fact table used in the Gold layer star schema model. |
| `dm_evt.sk_evt` | Surrogate key of the `dm_evento` dimension associated with the parliamentary event attended by the deputy. |
| `org.sk_org` | Surrogate key of the `dm_orgao` dimension associated with the legislative body responsible for the parliamentary event. |
| `dept.sk_dept` | Surrogate key of the `dm_deputado` dimension associated with the parliamentarian participating in the legislative event. |
| `part.sk_part` | Surrogate key of the `dm_partido` dimension associated with the political party of the participating deputy. |
| `leg.sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the legislative period of the parliamentary event. |
| `uf.sk_uf` | Surrogate key of the `dm_uf` dimension associated with the federative unit of the participating deputy. |
| `evt.evt_dt_inicio` | Official start date of the parliamentary event used as the temporal reference for legislative participation. |
| `evt_ts_inicio` | Official start timestamp of the parliamentary event. |
| `evt_ts_fim` | Official end timestamp of the parliamentary event. |
| `evt_id_evento` | Official unique identifier of the parliamentary event in the Chamber of Deputies API. |
| `evt_tx_tipo_evento` | Classification of the parliamentary event type, such as plenary session, public hearing, committee meeting or legislative seminar. |
| `evt_tx_nome_evento` | Name or summarized description of the parliamentary event registered by the Chamber of Deputies. |
| `evt_qt_duracao_minutos` | Total duration of the parliamentary event in minutes calculated between the start and end timestamps. |
| `pres_fl_presente` | Derived indicator that identifies whether the parliamentarian effectively participated in the registered legislative event. |
| `pres_fl_ausente` | Derived indicator that identifies absence of the parliamentarian in the analyzed legislative event. |
| `pres_fl_evento_plenario` | Derived indicator that identifies parliamentary events held in plenary sessions of the Chamber of Deputies. |
| `pres_fl_evento_comissao` | Derived indicator that identifies parliamentary events held in legislative committees. |
| `pres_fl_evento_longa_duracao` | Derived indicator that identifies parliamentary events with duration above the analytical threshold defined by the project. |
| `pres_nr_ano` | Year of the parliamentary event derived from the official event date. |
| `pres_nr_mes` | Month of the parliamentary event derived from the official event date. |
| `pres_tx_periodo_dia` | Classification of the time period during which the parliamentary event occurred, such as morning, afternoon or evening. |
| `pres_qt_eventos_dia` | Number of parliamentary events in which the deputy participated during the same legislative day. |
| `pres_tx_resumo_participacao` | Consolidated analytical text summarizing parliamentary participation in the legislative event. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer used for operational traceability. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the parliamentary attendance record. |
| `bronze_id_origem` | Original technical identifier of the attendance record in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load of the parliamentary attendance record. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and auditing of the original parliamentary attendance record. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the parliamentary event attendance fact table. |

## Relationships
- Related to `dm_evento`, `dm_orgao`, `dm_deputado`, `dm_partido`, `dm_legislatura` and `dm_uf`.
- Used in analyses of parliamentary attendance and productivity.
- Can be integrated with voting and parliamentary activity fact tables.
- Serves as the foundation for parliamentary institutional attendance indicators.
- Integrated into legislative dashboards and parliamentary participation monitoring.

## Technical Notes
- Main source: `silver_curated.eventos` and derived parliamentary relationship datasets.
- Target table: `gold.ft_presenca_eventos`.
- Maintains granularity at the participating parliamentarian per legislative event level.
- Persisted in Delta Lake with Databricks SQL compatibility.
- Derived attendance indicators and event classifications are calculated during Gold layer processing.
- Maintains complete Bronze layer traceability for institutional auditing and lineage purposes.
- Structure prepared for future integration with real-time parliamentary event streaming pipelines.
- Compatible with temporal analyses, legislative monitoring and institutional parliamentary intelligence.

---

# `ft_frentes_membros`

## Type
Fact

## Notebook
`notebooks/03_gold/21_build_ft_frentes_membros.py`

## Description
Fact table of Parliamentary Front composition in the Brazilian Chamber of Deputies. Represents the relationship between federal deputies and the parliamentary fronts in which they participate, preserving political party, federative unit, legislature, parliamentary role within the front and participation indicators.

This table supports the Parliamentary Fronts Atlas, enabling analysis of political composition, party diversity, regional distribution, internal leadership, simultaneous participation of deputies in multiple fronts and evolution of parliamentary fronts across legislatures.

## Grain
One row per deputy membership relationship within a Parliamentary Front.

## Analytical Objectives
- Analyze Parliamentary Front composition by deputy.
- Identify political parties and federative units represented in each front.
- Measure parliamentary participation in thematic fronts.
- Identify coordinators, presidents, vice-presidents and members.
- Support analysis of cross-party political articulation.
- Enable cross-analysis between parliamentary front themes and deputy political profiles.

## Columns

| Column | Description |
|---|---|
| `sk_frente` | Surrogate key of the `dm_frente` dimension associated with the Parliamentary Front. |
| `sk_dept` | Surrogate key of the `dm_deputado` dimension associated with the deputy who is a member of the front. |
| `sk_part` | Surrogate key of the `dm_partido` dimension associated with the political party of the deputy member. |
| `sk_uf` | Surrogate key of the `dm_uf` dimension associated with the federative unit of the deputy member. |
| `sk_leg` | Surrogate key of the `dm_legislatura` dimension associated with the legislature of the Parliamentary Front. |
| `frente_id_frente` | Official identifier of the Parliamentary Front in the Chamber of Deputies API. |
| `dept_id_deputado` | Official identifier of the federal deputy who is a member of the Parliamentary Front. |
| `part_sg_partido` | Official abbreviation of the political party of the deputy member of the front. |
| `uf_sg_uf` | Abbreviation of the federative unit representing the electoral constituency of the deputy member. |
| `leg_id_legislatura` | Identifier of the legislature associated with the relationship between the deputy and the Parliamentary Front. |
| `memb_tx_dedup_key` | Technical deduplication key for the relationship between deputy, Parliamentary Front and legislature. |
| `frente_tx_uri` | Official URI of the Parliamentary Front in the Chamber Open Data API. |
| `frente_tx_titulo` | Official full name of the Parliamentary Front. |
| `frente_fl_tema_saude` | Indicator identifying whether the front is related to healthcare topics. |
| `frente_fl_tema_educacao` | Indicator identifying whether the front is related to education topics. |
| `frente_fl_tema_seguranca` | Indicator identifying whether the front is related to public security topics. |
| `frente_fl_tema_agro` | Indicator identifying whether the front is related to agribusiness, agriculture or livestock topics. |
| `frente_fl_tema_mulher` | Indicator identifying whether the front is related to women's rights or gender equality topics. |
| `frente_fl_tema_meio_ambiente` | Indicator identifying whether the front is related to environmental and sustainability topics. |
| `dept_tx_uri` | Official URI of the federal deputy in the Chamber Open Data API. |
| `dept_tx_nome_parlamentar` | Official parliamentary name used by the deputy member of the front. |
| `dept_tx_email` | Institutional e-mail address of the deputy member. |
| `dept_fl_email_valido` | Indicator identifying whether the deputy's institutional e-mail address follows a valid format according to pipeline rules. |
| `dept_tx_url_foto` | Official URL of the deputy's institutional photograph. |
| `memb_cd_titulo` | Code of the title or role exercised by the deputy within the Parliamentary Front. |
| `memb_tx_titulo` | Description of the deputy's title or role within the Parliamentary Front. |
| `memb_tx_status` | Status of the deputy's relationship with the Parliamentary Front. |
| `memb_fl_ativo` | Indicator identifying whether the deputy's relationship with the Parliamentary Front is active. |
| `memb_fl_coordenador` | Indicator identifying whether the deputy exercises a coordinator role within the Parliamentary Front. |
| `memb_fl_presidente` | Indicator identifying whether the deputy exercises a president role within the Parliamentary Front. |
| `memb_fl_vice` | Indicator identifying whether the deputy exercises a vice-president or vice-coordinator role within the Parliamentary Front. |
| `memb_fl_membro` | Indicator identifying whether the deputy participates as a regular member of the Parliamentary Front. |
| `qt_membro_frente` | Unit metric with value `1`, used for counting member relationships within parliamentary fronts. |
| `qt_membro_ativo` | Unit metric with value `1` when the deputy's relationship with the front is active. |
| `qt_coordenador` | Unit metric with value `1` when the deputy is a coordinator of the Parliamentary Front. |
| `qt_presidente` | Unit metric with value `1` when the deputy is president of the Parliamentary Front. |
| `bronze_ts_ingestao` | Original technical ingestion timestamp of the record in the Bronze layer. |
| `bronze_dt_ingestao` | Technical ingestion date of the record in the Bronze layer. |
| `bronze_tx_endpoint` | Chamber of Deputies API endpoint used to retrieve the original record. |
| `bronze_id_origem` | Technical identifier of the record in the Bronze ingestion source. |
| `bronze_id_batch` | Ingestion batch identifier responsible for the original load. |
| `bronze_tx_record_hash` | Technical hash used for change tracking, deduplication and traceability. |
| `silver_base_ts_processamento` | Processing timestamp of the record in the Silver Base layer. |
| `silver_curated_ts_processamento` | Processing timestamp of the record in the Silver Curated layer. |
| `gold_ts_processamento` | Processing timestamp of the record in the Gold layer. |
| `gold_id_batch` | Execution batch identifier responsible for generating the Gold fact table. |

## Relationships
- Related to `gold.dm_frente` through `sk_frente`.
- Related to `gold.dm_deputado` through `sk_dept`.
- Related to `gold.dm_partido` through `sk_part`.
- Related to `gold.dm_uf` through `sk_uf`.
- Related to `gold.dm_legislatura` through `sk_leg`.

## Technical Notes
- Main source: `silver_curated.frentes_membros`.
- Target table: `gold.ft_frentes_membros`.
- The table is partitioned by `leg_id_legislatura`.
- The pipeline filters only legislatures defined in `LEGISLATURAS_PADRAO`.
- The notebook validates `sk_frente` and `sk_leg` as mandatory fields.
- Records without `sk_dept` generate quality alerts but do not interrupt the load process.
- Uniqueness is validated through `memb_tx_dedup_key`.
- The table is persisted in Delta Lake and optimized using `OPTIMIZE`.

---

# Relationship Between Facts and Dimensions

The Gold layer fact tables use conformed dimensions to guarantee:

- analytical consistency;
- enterprise reusability;
- dimensional integrity;
- metric standardization;
- analytical performance.

---

# Analytical Capabilities

The Gold layer supports:

- parliamentary analysis;
- political party analysis;
- financial analysis;
- voting analysis;
- legislative behavior analysis;
- transparency indicators;
- parliamentary efficiency;
- intelligence analytics;
- executive dashboards.

---

# Final Considerations

This document represents the technical and functional catalog of the project's Gold layer.

It should continuously evolve as new dimensions, fact tables and analytical marts are added to the pipeline.

---