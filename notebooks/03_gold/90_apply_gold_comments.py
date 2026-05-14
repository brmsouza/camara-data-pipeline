# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Metadata Governance
# MAGIC
# MAGIC **Notebook:** 90_apply_gold_comments
# MAGIC
# MAGIC Applies table and column comments to Gold layer objects in Databricks.
# MAGIC
# MAGIC Source of truth: `docs/gold_layer_enterprise_data_dictionary.md`

# COMMAND ----------

def sql_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")

def run_comment(sql_statement: str) -> None:
    try:
        spark.sql(sql_statement)
        print("OK")
    except Exception as exc:
        print("WARN: metadata statement failed")
        print(sql_statement)
        print(f"Reason: {exc}")
        print("-" * 120)

# COMMAND ----------


metadata_comments = [{'table': 'gold.dm_bancada',
  'comment': 'Conformed dimension of parliamentary caucuses and party blocs from the Brazilian Chamber of Deputies. '
             'Consolidates political groupings used in voting sessions, party orientations and parliamentary '
             'articulations registered in Chamber legislative data. This dimension standardizes party caucuses, '
             'parliamentary blocs and political groupings identified in voting and parliamentary orientation records, '
             'enabling analyses of political alignment, party loyalty and collective legislative behavior.',
  'columns': {'sk_banc': 'Sequential surrogate key of the caucus dimension used in the Gold layer star schema model.',
              'banc_tx_bancada_curada': 'Standardized name of the parliamentary caucus or political bloc used by the '
                                        'project for analytical consolidation of legislative orientations.',
              'banc_tx_tipo_bancada': 'Classification of the parliamentary caucus, such as political party, '
                                      'parliamentary bloc, government leadership, opposition or independents.',
              'banc_tx_uri': 'URI or reference identifier associated with the parliamentary caucus when available in '
                             'the processed legislative data.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the parliamentary caucus '
                               'dimension.'}},
 {'table': 'gold.dm_data',
  'comment': 'Conformed calendar date dimension used by the Gold layer to support temporal analysis, chronological '
             'filtering and date-based relationships across analytical facts.',
  'columns': {'sk_data': 'Surrogate key of the date dimension used to relate Gold fact tables to calendar dates.',
              'data_dt_data': 'Reference calendar date of the date dimension.',
              'data_nr_ano': 'Calendar year of the reference date.',
              'data_nr_mes': 'Month number of the reference date, ranging from 1 to 12.',
              'data_nr_dia': 'Day number within the month of the reference date.',
              'data_nr_semana_ano': 'Week number within the year used for weekly analysis.',
              'data_nr_trimestre': 'Quarter number of the year used for quarterly aggregations.',
              'data_nr_semestre': 'Semester number of the year used for half-year aggregations.',
              'data_tx_ano_mes': 'Text representation of year and month used for monthly grouping.',
              'data_nr_ano_mes': 'Numeric representation of year and month used for chronological sorting.',
              'data_tx_nome_dia_semana': 'Full weekday name of the reference date.',
              'data_tx_nome_mes': 'Full month name of the reference date.',
              'data_nr_dia_semana': 'Day of week number of the reference date.',
              'data_fl_fim_semana': 'Indicator identifying whether the reference date falls on a weekend.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the Gold object.'}},
 {'table': 'gold.dm_deputado',
  'comment': 'Conformed dimension of federal deputies of the Chamber of Deputies. Consolidates registration, party, '
             'electoral, geographic and parliamentary information of deputies identified in the official Chamber API '
             'data and enriched by the Silver Curated layer. The dimension represents the main parliamentary entity of '
             'the Gold analytical model, being used as the central axis for integrating CEAP expenses, votes, event '
             'attendance, legislative propositions, parliamentary activity indicators and political analyses.',
  'columns': {'sk_dept': 'Sequential surrogate key of the deputy dimension used in the Gold layer star schema model.',
              'id_deputado': 'Official or business identifier associated with id deputado.',
              'dept_tx_nome_parlamentar': 'Official parliamentary name used by the deputy in legislative activities.',
              'dept_tx_nome_civil': 'Full civil name of the federal deputy.',
              'dept_tx_nome_eleitoral': 'Electoral name associated with the federal deputy when available.',
              'uf_sg_uf': 'Acronym of the Brazilian Federative Unit associated with the parliamentary record.',
              'part_sg_partido': 'Official acronym of the political party associated with the parliamentary record.',
              'leg_id_legislatura': 'Official or business identifier associated with leg id legislatura.',
              'dept_tx_sexo': 'Gender informed in the deputy’s parliamentary registration.',
              'dept_dt_nascimento': 'Birth date of the federal deputy.',
              'dept_qt_idade': 'Calculated age of the federal deputy based on birth date and processing date.',
              'dept_tx_escolaridade': 'Calculated age of the federal deputy based on birth date and processing date.',
              'dept_tx_situacao_mandato': 'Parliamentary mandate status informed in Chamber data.',
              'dept_tx_condicao_eleitoral': 'Electoral condition associated with the deputy’s mandate, such as '
                                            'incumbent or alternate.',
              'dept_tx_status_mandato_curado': 'Standardized mandate status created by the project for analytical '
                                               'consolidation.',
              'dept_tx_email': 'Official institutional email address of the deputy in the Chamber of Deputies.',
              'dept_fl_email_valido': 'Deputy attribute related to dept fl email valido.',
              'dept_tx_url_foto': 'Official URL of the deputy’s institutional photo provided by the Chamber.',
              'dept_tx_url_referencia': 'Official reference URL of the deputy in the Chamber Open Data API.',
              'gab_tx_nome': 'Parliamentary office attribute related to gab tx nome.',
              'gab_tx_predio': 'Building identification where the parliamentary office is located.',
              'gab_tx_sala': 'Room number or room identification assigned to the parliamentary office.',
              'gab_tx_andar': 'Floor of the Chamber building where the parliamentary office is located.',
              'gab_tx_telefone': 'Official telephone number of the parliamentary office.',
              'gab_fl_telefone_valido': 'Indicator identifying whether the office telephone follows the validation '
                                        'rules implemented in the pipeline.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the record.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for loading the deputy dimension.'}},
 {'table': 'gold.dm_evento',
  'comment': 'Conformed dimension of parliamentary events from the Brazilian Chamber of Deputies. Consolidates '
             'official information related to sessions, public hearings, meetings, seminars, general committees and '
             'other legislative events registered in the Chamber Open Data API. This dimension enables '
             'contextualization of parliamentary activities performed over time, relating events to legislative '
             'bodies, propositions, voting sessions and parliamentary participation.',
  'columns': {'sk_evt': 'Sequential surrogate key of the event dimension used in the Gold layer star schema model.',
              'evt_id_evento': 'Official unique identifier of the parliamentary event in the Chamber of Deputies API.',
              'evt_tx_uri': 'Official URI of the parliamentary event in the Chamber Open Data API.',
              'evt_nr_ano_referencia': 'Reference year of the parliamentary event derived from the event execution '
                                       'date.',
              'evt_ts_inicio': 'Official start timestamp of the parliamentary event registered by the Chamber of '
                               'Deputies.',
              'evt_ts_fim': 'Official end timestamp of the parliamentary event registered by the Chamber of Deputies.',
              'evt_dt_inicio': 'Start date of the parliamentary event derived from the official event opening '
                               'timestamp.',
              'evt_dt_fim': 'End date of the parliamentary event derived from the official event closing timestamp.',
              'sk_data_inicio': 'Surrogate key of the data inicio entity used for dimensional relationships in the '
                                'Gold layer.',
              'sk_data_fim': 'Surrogate key of the data fim entity used for dimensional relationships in the Gold '
                             'layer.',
              'evt_nr_ano_inicio': 'Year extracted from the event start date.',
              'evt_nr_mes_inicio': 'Month extracted from the event start date.',
              'evt_fl_inicio_valido': 'Indicator showing whether the event start timestamp is valid.',
              'evt_fl_fim_valido': 'Indicator showing whether the event end timestamp is valid.',
              'evt_fl_periodo_valido': 'Indicator showing whether the event period is valid considering start and end '
                                       'timestamps.',
              'evt_tx_descricao': 'Official description or title of the parliamentary event.',
              'evt_tx_tipo': 'Original event type informed by the Chamber of Deputies API.',
              'evt_tx_situacao': 'Original event status informed by the Chamber of Deputies API.',
              'evt_tx_tipo_curado': 'Standardized event type created by the project for analytical consolidation.',
              'evt_tx_situacao_curada': 'Standardized event status created by the project for analytical '
                                        'consolidation.',
              'evt_fl_sessao': 'Indicator identifying whether the event is a parliamentary session.',
              'evt_fl_audiencia_publica': 'Indicator identifying whether the event is a public hearing.',
              'evt_fl_reuniao': 'Indicator identifying whether the event is a meeting.',
              'evt_fl_encerrado': 'Indicator identifying whether the event was closed or concluded.',
              'evt_fl_cancelado': 'Indicator identifying whether the event was canceled.',
              'evt_fl_possui_registro': 'Indicator identifying whether the event has an official record or '
                                        'registration URL.',
              'evt_tx_local_interno': 'Internal location of the parliamentary event within the Chamber of Deputies.',
              'evt_tx_predio': 'Building where the parliamentary event took place.',
              'evt_tx_sala': 'Room where the parliamentary event took place.',
              'evt_tx_andar': 'Floor where the parliamentary event took place.',
              'evt_tx_local_externo': 'External location of the event when it occurred outside Chamber facilities.',
              'evt_tx_tipo_local': 'Classification of the event location as internal, external or not informed.',
              'evt_tx_url_registro': 'Official URL of the event record when available.',
              'evt_qt_orgaos': 'Number of legislative bodies associated with the parliamentary event.',
              'org_id_orgao_principal': 'Official or business identifier associated with org id orgao principal.',
              'org_sg_orgao_principal': 'Attribute of the main legislative body associated with the record: org sg '
                                        'orgao principal.',
              'org_tx_nome_principal': 'Attribute of the main legislative body associated with the record: org tx nome '
                                       'principal.',
              'org_tx_tipo_principal': 'Attribute of the main legislative body associated with the record: org tx tipo '
                                       'principal.',
              'org_tx_siglas_relacionadas': 'Legislative body attribute related to org tx siglas relacionadas.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the parliamentary event record.',
              'bronze_id_origem': 'Original technical identifier of the event in the Bronze ingestion source.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original event load in the Bronze '
                                 'layer.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'parliamentary event record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the event dimension.'}},
 {'table': 'gold.dm_fornecedor',
  'comment': 'Conformed dimension of suppliers associated with parliamentary expenses from the Parliamentary Activity '
             'Quota (CEAP). Consolidates individuals and legal entities that issued fiscal documents used in the '
             'expense reporting process of federal deputies from the Brazilian Chamber of Deputies. This dimension '
             'enables the identification of recurring suppliers, analysis of parliamentary spending concentration, '
             'monitoring of payments executed with CEAP resources, and supports transparency, auditing and '
             'parliamentary financial intelligence analyses.',
  'columns': {'sk_forn': 'Sequential surrogate key of the supplier dimension used in the Gold layer star schema model.',
              'forn_nr_cnpj_cpf': 'CPF or CNPJ number of the supplier associated with the fiscal document of the '
                                  'parliamentary expense.',
              'forn_tx_nome': 'Supplier attribute related to forn tx nome.',
              'forn_tx_tipo_documento': 'Supplier attribute related to forn tx tipo documento.',
              'forn_fl_documento_valido': 'Consolidated indicator that identifies whether the supplier CPF or CNPJ '
                                          'passed the technical validation rules implemented in the project.',
              'forn_fl_documento_repetido': 'Supplier attribute related to forn fl documento repetido.',
              'forn_tx_status_consulta_cnpj': 'Supplier attribute related to forn tx status consulta cnpj.',
              'forn_cd_http_status_cnpj': 'Supplier attribute related to forn cd http status cnpj.',
              'forn_tx_erro_consulta_cnpj': 'Supplier attribute related to forn tx erro consulta cnpj.',
              'forn_fl_cnpj_encontrado': 'Supplier attribute related to forn fl cnpj encontrado.',
              'forn_fl_cnpj_ativo': 'Supplier attribute related to forn fl cnpj ativo.',
              'forn_fl_cnpj_suspeito': 'Supplier attribute related to forn fl cnpj suspeito.',
              'forn_tx_motivo_cnpj_suspeito': 'Supplier attribute related to forn tx motivo cnpj suspeito.',
              'forn_tx_razao_social_receita': 'Supplier attribute related to forn tx razao social receita.',
              'forn_tx_nome_fantasia_receita': 'Supplier attribute related to forn tx nome fantasia receita.',
              'forn_tx_situacao_cadastral': 'Supplier attribute related to forn tx situacao cadastral.',
              'forn_tx_cnae_principal': 'Supplier attribute related to forn tx cnae principal.',
              'forn_sg_uf_receita': 'Supplier attribute related to forn sg uf receita.',
              'forn_tx_municipio_receita': 'Supplier attribute related to forn tx municipio receita.',
              'forn_tx_porte_empresa': 'Supplier attribute related to forn tx porte empresa.',
              'forn_vl_capital_social': 'Supplier attribute related to forn vl capital social.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the expense record associated '
                                    'with the supplier.',
              'bronze_id_origem': 'Original technical identifier of the expense record used during Bronze ingestion.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the record '
                                 'associated with the supplier.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original record.',
              'silver_base_ts_processamento': 'Processing timestamp of the record in the Silver Base layer.',
              'silver_base_id_batch': 'Execution batch identifier responsible for generating the Silver Base record.',
              'silver_curated_ts_processamento': 'Processing timestamp of the record in the Silver Curated layer.',
              'silver_curated_id_batch': 'Execution batch identifier responsible for generating the Silver Curated '
                                         'record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the supplier dimension.'}},
 {'table': 'gold.dm_frente',
  'comment': 'Conformed dimension of Parliamentary Fronts from the Brazilian Chamber of Deputies. Consolidates '
             'institutional and thematic information related to parliamentary fronts officially registered in the '
             'Chamber Open Data API. Parliamentary fronts represent cross-party associations of deputies organized '
             'around interests, economic sectors, social agendas or specific legislative themes. This dimension '
             'enables political, thematic and parliamentary articulation analyses among deputies, political parties '
             'and interest groups.',
  'columns': {'sk_frente': 'Sequential surrogate key of the parliamentary front dimension used in the Gold layer star '
                           'schema model.',
              'frente_id_frente': 'Official unique identifier of the Parliamentary Front in the Chamber of Deputies '
                                  'API.',
              'frente_tx_uri': 'Official URI of the Parliamentary Front in the Chamber Open Data API.',
              'frente_tx_titulo': 'Official full name of the Parliamentary Front registered by the Chamber of '
                                  'Deputies.',
              'leg_id_legislatura': 'Identifier of the legislature associated with the operational period of the '
                                    'Parliamentary Front.',
              'frente_fl_tema_saude': 'Derived indicator that identifies parliamentary fronts related to public '
                                      'health, medicine, hospitals, SUS or healthcare policies.',
              'frente_fl_tema_educacao': 'Derived indicator that identifies parliamentary fronts related to education, '
                                         'teaching, universities or educational policies.',
              'frente_fl_tema_seguranca': 'Derived indicator that identifies parliamentary fronts related to public '
                                          'security, police, penal system or social defense.',
              'frente_fl_tema_agro': 'Derived indicator that identifies parliamentary fronts related to agribusiness, '
                                     'agriculture, livestock or rural production.',
              'frente_fl_tema_mulher': 'Derived indicator that identifies parliamentary fronts related to women’s '
                                       'rights, gender equality or women’s protection.',
              'frente_fl_tema_meio_ambiente': 'Derived indicator that identifies parliamentary fronts related to '
                                              'environment, sustainability, climate change or environmental '
                                              'preservation.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the Parliamentary Front record.',
              'bronze_id_origem': 'Original technical identifier of the record in the Bronze ingestion source.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the Parliamentary '
                                 'Front.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original Parliamentary Front record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the Parliamentary Front '
                               'dimension.'}},
 {'table': 'gold.dm_gabinete',
  'comment': 'Conformed dimension of parliamentary offices assigned to federal deputies of the Brazilian Chamber of '
             'Deputies. Consolidates structural, physical and contact information related to offices linked to active '
             'parliamentarians. This dimension enables organizational and administrative analyses related to office '
             'occupancy, parliamentary location, institutional distribution and official communication channels of '
             'federal deputies.',
  'columns': {'sk_gab': 'Sequential surrogate key of the parliamentary office dimension used in the Gold layer star '
                        'schema model.',
              'dept_id_deputado': 'Official identifier of the federal deputy responsible for the parliamentary office.',
              'gab_tx_nome': "Official name of the deputy's parliamentary office.",
              'gab_tx_predio': 'Building identification within the Chamber of Deputies where the parliamentary office '
                               'is located.',
              'gab_tx_sala': 'Room number or room identification assigned to the parliamentary office.',
              'gab_tx_andar': 'Floor of the Chamber building where the parliamentary office is located.',
              'gab_tx_telefone': "Official telephone number of the deputy's parliamentary office.",
              'gab_fl_telefone_valido': 'Derived indicator that identifies whether the office telephone number follows '
                                        'a valid format according to pipeline validation rules.',
              'gab_tx_email': 'Official institutional e-mail address of the parliamentary office.',
              'gab_fl_email_valido': 'Derived indicator that identifies whether the office e-mail address follows a '
                                     'valid format according to validation rules implemented in the project.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the record.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the parliamentary office '
                               'dimension.'}},
 {'table': 'gold.dm_legislatura',
  'comment': 'Conformed dimension of legislatures of the Chamber of Deputies. Represents the formal parliamentary '
             'operating periods defined by the Chamber as the interval between the inauguration of one group of '
             'deputies and the eve of the inauguration of the following group. The API provides the identifier, start '
             'date, end date and election year of the parliamentarians within the legislature.',
  'columns': {'sk_leg': 'Sequential surrogate key of the legislature dimension in the Gold layer, created for internal '
                        'dimensional relationships in the star schema model.',
              'leg_id_legislatura': 'Official legislature identifier according to the Chamber of Deputies Open Data '
                                    'API. Represents a parliamentary working period of the Chamber.',
              'leg_nr_ano_eleicao': 'Election year of the federal deputies composing the legislature. In the project, '
                                    'it is derived from the year preceding the beginning of the legislature.',
              'leg_nr_ano_inicio': 'Calendar year in which the legislature begins.',
              'leg_nr_ano_fim': 'Calendar year in which the legislature ends.',
              'leg_dt_inicio': 'Official start date of the legislature, corresponding to the beginning of the '
                               'parliamentary mandate period of that group of deputies.',
              'leg_dt_fim': 'Official end date of the legislature, corresponding to the closing period before the '
                            'inauguration of the next legislature.',
              'leg_qt_meses_duracao': 'Approximate number of months of the legislature duration, calculated between '
                                      '`leg_dt_inicio` and `leg_dt_fim`.',
              'leg_fl_legislatura_atual': 'Indicator showing whether the current date falls within the legislature '
                                          'validity period. Value `1` indicates the current legislature; value `0` '
                                          'indicates a historical or future legislature.',
              'leg_tx_descricao': 'Standardized legislature description in the format “Legislature {id} ({start_year} '
                                  '- {end_year})”, created to improve readability in reports, dashboards and '
                                  'analytical filters.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer. Indicates when the '
                                       'dimension was generated or updated in the pipeline.',
              'gold_id_batch': 'Unique execution batch identifier responsible for generating the Gold dimension. Used '
                               'for traceability, auditing and troubleshooting.'}},
 {'table': 'gold.dm_orgao',
  'comment': 'Conformed dimension of legislative bodies from the Brazilian Chamber of Deputies, including committees, '
             'plenary structures and institutional bodies used in events and voting sessions.',
  'columns': {'sk_org': 'Surrogate key of the org entity used for dimensional relationships in the Gold layer.',
              'org_id_orgao': 'Official or business identifier associated with org id orgao.',
              'org_tx_uri': 'Legislative body attribute related to org tx uri.',
              'org_sg_orgao': 'Legislative body attribute related to org sg orgao.',
              'org_tx_nome': 'Legislative body attribute related to org tx nome.',
              'org_tx_apelido': 'Legislative body attribute related to org tx apelido.',
              'org_tx_nome_publicacao': 'Legislative body attribute related to org tx nome publicacao.',
              'org_tx_nome_resumido': 'Legislative body attribute related to org tx nome resumido.',
              'org_cd_tipo_orgao': 'Legislative body attribute related to org cd tipo orgao.',
              'org_tx_tipo_orgao': 'Legislative body attribute related to org tx tipo orgao.',
              'org_tx_tipo_curado': 'Legislative body attribute related to org tx tipo curado.',
              'org_fl_plenario': 'Legislative body attribute related to org fl plenario.',
              'org_fl_comissao': 'Legislative body attribute related to org fl comissao.',
              'org_fl_mesa': 'Legislative body attribute related to org fl mesa.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the source record.',
              'bronze_id_origem': 'Original technical identifier of the record in the Bronze ingestion source.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the record.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the Gold object.'}},
 {'table': 'gold.dm_partido',
  'comment': 'Conformed dimension of political parties identified in curated deputy records. The table standardizes '
             'the party acronym used by parliamentarians and serves as an integration entity between dimensions, facts '
             'and analytical views in the Gold layer.',
  'columns': {'sk_part': 'Sequential surrogate key of the political party dimension in the Gold layer, created for '
                         'internal relationships with facts and analytical views.',
              'part_sg_partido': 'Official acronym of the political party associated with the deputy in the Chamber '
                                 'data, such as PT, PL, MDB, PSD, PSOL or UNIÃO.',
              'gold_ts_processamento': 'Timestamp indicating when the record was processed and written to the Gold '
                                       'dimension.',
              'gold_id_batch': 'Unique execution batch identifier responsible for generating the `dm_partido` '
                               'dimension.'}},
 {'table': 'gold.dm_proposicao',
  'comment': 'Conformed dimension of legislative propositions of the Chamber of Deputies. Consolidates official '
             'information regarding bills, constitutional amendment proposals, requests, provisional measures and '
             'other legislative matters made available through the Chamber Open Data API. The dimension centralizes '
             'legislative metadata used in parliamentary analyses, voting processes, legislative procedures, '
             'legislative production and political intelligence. Each record represents a legislative proposition '
             'officially identified by the Chamber of Deputies.',
  'columns': {'sk_prop': 'Sequential surrogate key of the proposition dimension used in the Gold layer star schema '
                         'model.',
              'prop_id_proposicao': 'Official unique identifier of the legislative proposition in the Chamber of '
                                    'Deputies API.',
              'prop_tx_uri': 'Official proposition URI in the Chamber Open Data API, used for navigation and '
                             'integration between legislative endpoints.',
              'prop_sg_tipo': 'Acronym of the legislative type of the proposition, such as PL, PEC, MPV, REQ or PDL.',
              'prop_tx_descricao_tipo': 'Official textual description of the legislative type of the proposition, such '
                                        'as Bill, Constitutional Amendment Proposal or Provisional Measure.',
              'prop_nr_numero': 'Official number of the legislative proposition within its type and presentation year.',
              'prop_nr_ano': 'Official year in which the legislative proposition was presented in the Chamber of '
                             'Deputies.',
              'prop_cd_tipo': 'Internal proposition type code used by the Chamber of Deputies for legislative '
                              'categorization.',
              'prop_tx_ementa': 'Official summary text of the legislative proposition, describing its primary '
                                'objective.',
              'prop_tx_keywords': 'Keywords associated with the legislative proposition used for thematic indexing and '
                                  'text search.',
              'prop_ts_apresentacao': 'Official date and time when the legislative proposition was presented in the '
                                      'Chamber of Deputies.',
              'sk_data_apresentacao': 'Surrogate key of the data apresentacao entity used for dimensional '
                                      'relationships in the Gold layer.',
              'prop_fl_data_apresentacao_valida': 'Legislative proposition attribute related to prop fl data '
                                                  'apresentacao valida.',
              'prop_nr_ano_apresentacao': 'Year extracted from the proposition presentation date to facilitate '
                                          'legislative temporal analyses.',
              'prop_nr_mes_apresentacao': 'Legislative proposition attribute related to prop nr mes apresentacao.',
              'prop_tx_url_inteiro_teor': 'Legislative proposition attribute related to prop tx url inteiro teor.',
              'prop_tx_urn_final': 'Legislative proposition attribute related to prop tx urn final.',
              'prop_ts_status_data_hora': 'Legislative proposition attribute related to prop ts status data hora.',
              'sk_data_status': 'Surrogate key of the data status entity used for dimensional relationships in the '
                                'Gold layer.',
              'prop_sg_status_orgao': 'Legislative proposition attribute related to prop sg status orgao.',
              'prop_tx_status_regime': 'Legislative proposition attribute related to prop tx status regime.',
              'prop_tx_status_descricao_tramitacao': 'Legislative proposition attribute related to prop tx status '
                                                     'descricao tramitacao.',
              'prop_tx_status_descricao_situacao': 'Legislative proposition attribute related to prop tx status '
                                                   'descricao situacao.',
              'prop_tx_status_apreciacao': 'Legislative proposition attribute related to prop tx status apreciacao.',
              'prop_tx_status_curado': 'Legislative proposition attribute related to prop tx status curado.',
              'prop_fl_tramitando': 'Legislative proposition attribute related to prop fl tramitando.',
              'prop_fl_aprovada': 'Legislative proposition attribute related to prop fl aprovada.',
              'prop_fl_rejeitada': 'Legislative proposition attribute related to prop fl rejeitada.',
              'prop_tx_tipo_curado': 'Legislative proposition attribute related to prop tx tipo curado.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the source record.',
              'bronze_id_origem': 'Original technical identifier of the record in the Bronze ingestion source.',
              'bronze_tx_source_file': 'Source file name used during ingestion when the record originated from '
                                       'file-based processing.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the record.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Unique execution batch identifier responsible for generating the proposition '
                               'dimension.'}},
 {'table': 'gold.dm_responsavel_ceap',
  'comment': 'Conformed dimension of entities responsible for Parliamentary Activity Quota (CEAP) expenses. '
             'Consolidates parliamentarians, leadership structures and institutional entities associated with the '
             'accountability process of parliamentary expenses registered by the Brazilian Chamber of Deputies. This '
             'dimension was created to standardize financial responsible entities identified in CEAP records, enabling '
             'differentiation between expenses directly linked to federal deputies, party leaderships and other '
             'parliamentary structures involved in the execution of public expenditures.',
  'columns': {'sk_resp_ceap': 'Sequential surrogate key of the CEAP responsible entity dimension used in the Gold '
                              'layer star schema model.',
              'resp_tx_tipo_responsavel': 'Analytical classification of the entity responsible for the parliamentary '
                                          'expense, such as DEPUTY, LEADERSHIP or NOT_IDENTIFIED.',
              'resp_tx_nome_responsavel': 'CEAP responsible entity attribute related to resp tx nome responsavel.',
              'id_deputado': 'Official or business identifier associated with id deputado.',
              'id_cadastro_ceap': 'Official or business identifier associated with id cadastro ceap.',
              'id_deputado_ceap': 'Official or business identifier associated with id deputado ceap.',
              'resp_nr_cpf': 'CEAP responsible entity attribute related to resp nr cpf.',
              'part_sg_partido': 'Official acronym of the political party associated with the parliamentary record.',
              'uf_sg_uf': 'Acronym of the Brazilian Federative Unit associated with the parliamentary record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the CEAP responsible entity '
                               'dimension.'}},
 {'table': 'gold.dm_tipo_despesa',
  'comment': 'Conformed dimension of parliamentary expense types from the Parliamentary Activity Quota (CEAP). '
             'Consolidates the official classifications of subquotas and expense specifications used by the Brazilian '
             'Chamber of Deputies to categorize expenses incurred by parliamentarians. This dimension standardizes '
             'financial categories used in parliamentary public spending analyses, enabling segmentation by expense '
             'nature, contracted service type and official administrative classification defined by the Chamber of '
             'Deputies.',
  'columns': {'sk_desp_tipo': 'Surrogate key of the desp tipo entity used for dimensional relationships in the Gold '
                              'layer.',
              'desp_cd_subcota': 'Official parliamentary subquota code used by the Chamber of Deputies to classify the '
                                 'primary CEAP expense type.',
              'desp_tx_tipo_despesa': 'Official description of the parliamentary expense type associated with the '
                                      'subquota, such as airline tickets, fuel, parliamentary communication, lodging '
                                      'or vehicle rental.',
              'desp_cd_especificacao_subcota': 'Complementary subquota specification code used for additional '
                                               'detailing of the parliamentary expense type.',
              'desp_tx_especificacao': 'Detailed textual description of the parliamentary expense specification '
                                       'associated with the CEAP subquota.',
              'desp_tx_segmento_despesa': 'Parliamentary expense attribute related to desp tx segmento despesa.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the expense type dimension.'}},
 {'table': 'gold.dm_uf',
  'comment': 'Conformed dimension of Brazilian Federative Units (UFs) used in parliamentary data from the Brazilian '
             'Chamber of Deputies. Consolidates Brazilian states associated with the electoral representation of '
             'federal deputies, geographic origin of parliamentarians, suppliers and regional distribution of '
             'legislative activity. This dimension standardizes the state-level geographic reference used in '
             'political, electoral, financial and parliamentary analyses in the Gold layer.',
  'columns': {'sk_uf': 'Sequential surrogate key of the Federative Unit dimension used in the Gold layer star schema '
                       'model.',
              'uf_sg_uf': 'Official abbreviation of the Brazilian Federative Unit, such as RJ, SP, MG, BA or DF.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the Federative Unit dimension.'}},
 {'table': 'gold.ft_atividade_parlamentar',
  'comment': 'Consolidated fact table of parliamentary activity of federal deputies from the Brazilian Chamber of '
             'Deputies. Aggregates quantitative and financial indicators related to parliamentary activity, including '
             'CEAP expenses, legislative participation, political behavior and derived analytical metrics used by the '
             'project. This table was designed to serve as a consolidated analytical layer of parliamentary '
             'performance, enabling integrated analysis of the political, financial and legislative activity of '
             'federal deputies. Each record represents an analytical summary of the parliamentary activity of a deputy '
             'within a specific legislature.',
  'columns': {'sk_dept': 'Surrogate key of the `dm_deputado` dimension associated with the analyzed federal deputy.',
              'sk_part': "Surrogate key of the `dm_partido` dimension associated with the deputy's political party "
                         'during the analyzed period.',
              'sk_leg': 'Surrogate key of the `dm_legislatura` dimension associated with the analyzed legislative '
                        'period.',
              'sk_uf': "Surrogate key of the `dm_uf` dimension associated with the federal deputy's federative unit.",
              'dept_id_deputado': 'Official identifier of the federal deputy used as the business key during '
                                  'parliamentary activity analytical consolidation.',
              'part_sg_partido': 'Official abbreviation of the political party associated with the deputy during '
                                 'parliamentary activity processing.',
              'uf_sg_uf': "Abbreviation of the federative unit representing the deputy's electoral constituency.",
              'leg_id_legislatura': 'Official identifier of the legislature associated with the consolidated '
                                    'parliamentary activity period.',
              'qt_despesas': 'Total number of CEAP parliamentary expenses registered for the deputy during the '
                             'analyzed period.',
              'vl_total_documento': 'Monetary value metric for vl total documento.',
              'vl_total_glosa': 'Monetary value metric for vl total glosa.',
              'vl_total_liquido': 'Consolidated net amount of parliamentary expenses effectively considered after '
                                  'disallowances and financial adjustments.',
              'vl_total_restituicao': 'Monetary value metric for vl total restituicao.',
              'fl_possui_glosa': 'Indicator flag for fl possui glosa.',
              'fl_possui_restituicao': 'Indicator flag for fl possui restituicao.',
              'qt_votos': 'Quantity metric for qt votos.',
              'qt_votos_sim': 'Total number of favorable votes cast by the deputy in the analyzed parliamentary voting '
                              'sessions.',
              'qt_votos_nao': 'Total number of opposing votes cast by the deputy in the analyzed parliamentary voting '
                              'sessions.',
              'qt_votos_abstencao': 'Quantity metric for qt votos abstencao.',
              'qt_votos_obstrucao': 'Quantity metric for qt votos obstrucao.',
              'qt_frentes': 'Quantity metric for qt frentes.',
              'fl_coordenador_frente': 'Indicator flag for fl coordenador frente.',
              'fl_presidente_frente': 'Indicator flag for fl presidente frente.',
              'fl_vice_frente': 'Indicator flag for fl vice frente.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the parliamentary activity fact '
                               'table.'}},
 {'table': 'gold.ft_despesas_ceap',
  'comment': 'Fact table of parliamentary expenses from the Parliamentary Activity Quota (CEAP). Consolidates expenses '
             'incurred by federal deputies of the Brazilian Chamber of Deputies based on fiscal documents submitted '
             'for reimbursement or payment through CEAP. The table records parliamentary financial operations related '
             'to airline tickets, fuel, lodging, parliamentary communication, consulting services, vehicle rentals, '
             'food and other expenses authorized by the Chamber of Deputies. Represents the primary financial fact '
             'table of the parliamentary star schema model in the Gold layer.',
  'columns': {'sk_resp_ceap': 'Surrogate key of the `dm_responsavel_ceap` dimension associated with the financial '
                              'responsible entity for the parliamentary expense.',
              'sk_dept': 'Surrogate key of the `dm_deputado` dimension associated with the federal deputy responsible '
                         'for the expense.',
              'sk_part': 'Surrogate key of the `dm_partido` dimension associated with the political party of the '
                         'parliamentarian at the time of the expense.',
              'sk_leg': 'Surrogate key of the `dm_legislatura` dimension associated with the legislative period of the '
                        'parliamentary expense.',
              'sk_forn': 'Surrogate key of the `dm_fornecedor` dimension associated with the supplier of the expense '
                         'fiscal document.',
              'sk_desp_tipo': 'Surrogate key of the `dm_tipo_despesa` dimension associated with the financial '
                              'classification of the CEAP expense.',
              'sk_uf': 'Surrogate key of the `dm_uf` dimension associated with the federative unit of the '
                       'parliamentarian or expense.',
              'sk_data_emissao': 'Surrogate key of the data emissao entity used for dimensional relationships in the '
                                 'Gold layer.',
              'desp_id_documento': 'Official or business identifier associated with desp id documento.',
              'desp_nr_documento': 'Parliamentary expense attribute related to desp nr documento.',
              'desp_cd_tipo_documento': 'Parliamentary expense attribute related to desp cd tipo documento.',
              'desp_tx_url_documento': 'Official URL of the digitized fiscal document made publicly available by the '
                                       'Chamber of Deputies for transparency purposes.',
              'desp_dt_emissao': 'Issuance date of the fiscal document associated with the parliamentary expense.',
              'desp_nr_ano': 'Reference year of the parliamentary expense derived from the fiscal document issuance '
                             'date.',
              'desp_nr_mes': 'Reference month of the parliamentary expense derived from the fiscal document issuance '
                             'date.',
              'desp_nr_parcela': 'Parliamentary expense attribute related to desp nr parcela.',
              'desp_vl_documento': 'Original gross amount of the fiscal document submitted by the parliamentarian in '
                                   'the CEAP accountability process.',
              'desp_vl_glosa': 'Amount rejected or disallowed by the Chamber of Deputies during the parliamentary '
                               'expense accountability analysis.',
              'desp_vl_liquido': 'Net amount effectively considered for reimbursement or payment of the parliamentary '
                                 'expense.',
              'desp_vl_restituicao': 'Amount returned or reimbursed related to the parliamentary expense when '
                                     'applicable.',
              'desp_fl_possui_documento_url': 'Parliamentary expense attribute related to desp fl possui documento '
                                              'url.',
              'desp_fl_possui_glosa': 'Parliamentary expense attribute related to desp fl possui glosa.',
              'desp_fl_possui_restituicao': 'Parliamentary expense attribute related to desp fl possui restituicao.',
              'desp_fl_valor_negativo': 'Parliamentary expense attribute related to desp fl valor negativo.',
              'desp_tx_passageiro': 'Parliamentary expense attribute related to desp tx passageiro.',
              'desp_tx_trecho': 'Parliamentary expense attribute related to desp tx trecho.',
              'desp_nr_lote': 'Parliamentary expense attribute related to desp nr lote.',
              'desp_nr_ressarcimento': 'Parliamentary expense attribute related to desp nr ressarcimento.',
              'desp_dt_pagamento_restituicao': 'Parliamentary expense attribute related to desp dt pagamento '
                                               'restituicao.',
              'desp_tx_dedup_key': 'Parliamentary expense attribute related to desp tx dedup key.',
              'bronze_id_origem': 'Original technical identifier of the parliamentary expense in the Bronze ingestion '
                                  'source.',
              'bronze_tx_source_file': 'Source file name used during ingestion when the record originated from '
                                       'file-based processing.',
              'bronze_nr_ano_referencia': 'Reference year associated with the original Bronze ingestion or source '
                                          'file.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the CEAP expense.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original financial record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the CEAP expense fact table.'}},
 {'table': 'gold.ft_frentes_membros',
  'comment': 'Fact table of Parliamentary Front composition in the Brazilian Chamber of Deputies. Represents the '
             'relationship between federal deputies and the parliamentary fronts in which they participate, preserving '
             'political party, federative unit, legislature, parliamentary role within the front and participation '
             'indicators. This table supports the Parliamentary Fronts Atlas, enabling analysis of political '
             'composition, party diversity, regional distribution, internal leadership, simultaneous participation of '
             'deputies in multiple fronts and evolution of parliamentary fronts across legislatures.',
  'columns': {'sk_frente': 'Surrogate key of the `dm_frente` dimension associated with the Parliamentary Front.',
              'sk_dept': 'Surrogate key of the `dm_deputado` dimension associated with the deputy who is a member of '
                         'the front.',
              'sk_part': 'Surrogate key of the `dm_partido` dimension associated with the political party of the '
                         'deputy member.',
              'sk_uf': 'Surrogate key of the `dm_uf` dimension associated with the federative unit of the deputy '
                       'member.',
              'sk_leg': 'Surrogate key of the `dm_legislatura` dimension associated with the legislature of the '
                        'Parliamentary Front.',
              'frente_id_frente': 'Official identifier of the Parliamentary Front in the Chamber of Deputies API.',
              'dept_id_deputado': 'Official identifier of the federal deputy who is a member of the Parliamentary '
                                  'Front.',
              'part_sg_partido': 'Official abbreviation of the political party of the deputy member of the front.',
              'uf_sg_uf': 'Abbreviation of the federative unit representing the electoral constituency of the deputy '
                          'member.',
              'leg_id_legislatura': 'Identifier of the legislature associated with the relationship between the deputy '
                                    'and the Parliamentary Front.',
              'memb_tx_dedup_key': 'Technical deduplication key for the relationship between deputy, Parliamentary '
                                   'Front and legislature.',
              'frente_tx_uri': 'Official URI of the Parliamentary Front in the Chamber Open Data API.',
              'frente_tx_titulo': 'Official full name of the Parliamentary Front.',
              'frente_fl_tema_saude': 'Indicator identifying whether the front is related to healthcare topics.',
              'frente_fl_tema_educacao': 'Indicator identifying whether the front is related to education topics.',
              'frente_fl_tema_seguranca': 'Indicator identifying whether the front is related to public security '
                                          'topics.',
              'frente_fl_tema_agro': 'Indicator identifying whether the front is related to agribusiness, agriculture '
                                     'or livestock topics.',
              'frente_fl_tema_mulher': "Indicator identifying whether the front is related to women's rights or gender "
                                       'equality topics.',
              'frente_fl_tema_meio_ambiente': 'Indicator identifying whether the front is related to environmental and '
                                              'sustainability topics.',
              'dept_tx_uri': 'Official URI of the federal deputy in the Chamber Open Data API.',
              'dept_tx_nome_parlamentar': 'Official parliamentary name used by the deputy member of the front.',
              'dept_tx_email': 'Institutional e-mail address of the deputy member.',
              'dept_fl_email_valido': "Indicator identifying whether the deputy's institutional e-mail address follows "
                                      'a valid format according to pipeline rules.',
              'dept_tx_url_foto': "Official URL of the deputy's institutional photograph.",
              'memb_cd_titulo': 'Code of the title or role exercised by the deputy within the Parliamentary Front.',
              'memb_tx_titulo': "Description of the deputy's title or role within the Parliamentary Front.",
              'memb_tx_status': "Status of the deputy's relationship with the Parliamentary Front.",
              'memb_fl_ativo': "Indicator identifying whether the deputy's relationship with the Parliamentary Front "
                               'is active.',
              'memb_fl_coordenador': 'Indicator identifying whether the deputy exercises a coordinator role within the '
                                     'Parliamentary Front.',
              'memb_fl_presidente': 'Indicator identifying whether the deputy exercises a president role within the '
                                    'Parliamentary Front.',
              'memb_fl_vice': 'Indicator identifying whether the deputy exercises a vice-president or vice-coordinator '
                              'role within the Parliamentary Front.',
              'memb_fl_membro': 'Indicator identifying whether the deputy participates as a regular member of the '
                                'Parliamentary Front.',
              'qt_membro_frente': 'Unit metric with value `1`, used for counting member relationships within '
                                  'parliamentary fronts.',
              'qt_membro_ativo': "Unit metric with value `1` when the deputy's relationship with the front is active.",
              'qt_coordenador': 'Unit metric with value `1` when the deputy is a coordinator of the Parliamentary '
                                'Front.',
              'qt_presidente': 'Unit metric with value `1` when the deputy is president of the Parliamentary Front.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the original record.',
              'bronze_id_origem': 'Technical identifier of the record in the Bronze ingestion source.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and traceability.',
              'silver_base_ts_processamento': 'Processing timestamp of the record in the Silver Base layer.',
              'silver_curated_ts_processamento': 'Processing timestamp of the record in the Silver Curated layer.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the Gold fact table.'}},
 {'table': 'gold.ft_orientacoes_bancada',
  'comment': 'Fact table of voting orientations issued by parliamentary caucuses, political parties and legislative '
             'blocs during voting sessions in the Brazilian Chamber of Deputies. Consolidates the official positioning '
             'of party leaderships and parliamentary caucuses regarding legislative propositions submitted to '
             'parliamentary deliberation. This table enables analyses of collective political behavior, party '
             'alignment, legislative strategies and institutional positioning of political parties, government and '
             'opposition during parliamentary voting sessions. Each record represents the official orientation of a '
             'parliamentary caucus in a specific voting session.',
  'columns': {'sk_banc': 'Surrogate key of the `dm_bancada` dimension associated with the parliamentary caucus '
                         'responsible for the voting orientation.',
              'sk_org': 'Surrogate key of the `dm_orgao` dimension associated with the legislative body responsible '
                        'for the parliamentary voting session.',
              'vot_id_votacao': 'Official identifier of the parliamentary voting session associated with the caucus '
                                'orientation.',
              'vot_tx_uri': 'Official URI of the parliamentary voting session in the Chamber of Deputies Open Data '
                            'API.',
              'org_sg_orgao': 'Official abbreviation of the legislative body where the parliamentary voting session '
                              'occurred, such as PLEN or CCJC.',
              'banc_tx_bancada_curada': 'Standardized name of the parliamentary caucus or political bloc used in the '
                                        'analytical consolidation of legislative orientations.',
              'vot_tx_orientacao': 'Original official orientation registered by the parliamentary caucus for the '
                                   'voting session, such as Yes, No, Released, Obstruction or Abstention.',
              'vot_tx_orientacao_curada': 'Parliamentary orientation standardized by the project for analytical '
                                          'consolidation and elimination of textual inconsistencies.',
              'vot_tx_descricao_resultado': 'Consolidated textual description of the political orientation issued by '
                                            'the parliamentary caucus.',
              'vot_fl_orientacao_sim': 'Derived indicator that identifies orientations favorable to approval of the '
                                       'legislative proposition.',
              'vot_fl_orientacao_nao': 'Derived indicator that identifies orientations opposing approval of the '
                                       'legislative proposition.',
              'vot_fl_orientacao_liberado': 'Derived indicator that identifies orientations in which the caucus '
                                            'released its parliamentarians to vote freely.',
              'vot_fl_orientacao_obstrucao': 'Derived indicator that identifies parliamentary obstruction '
                                             'orientations.',
              'vot_fl_orientacao_abstencao': 'Derived indicator that identifies parliamentary abstention orientations.',
              'vot_tx_dedup_key': 'Technical deduplication key used by the pipeline to guarantee uniqueness of the '
                                  'processed parliamentary orientation.',
              'bronze_nr_ano_referencia': 'Reference year of the original ingestion of the record in the Bronze layer.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the parliamentary orientation '
                                    'record.',
              'bronze_id_origem': 'Original technical identifier of the parliamentary orientation in the Bronze '
                                  'ingestion source.',
              'bronze_tx_source_file': 'Source file name used during ingestion of the parliamentary orientation when '
                                       'applicable to the processing pipeline.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the parliamentary '
                                 'orientation.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original parliamentary orientation record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the caucus orientations fact '
                               'table.'}},
 {'table': 'gold.ft_presenca_eventos',
  'comment': 'Fact table of parliamentary attendance in legislative events of the Brazilian Chamber of Deputies. '
             'Consolidates the participation of federal deputies in plenary sessions, public hearings, committee '
             'meetings, seminars and other parliamentary events registered in the Chamber Open Data API. This table '
             'enables measurement of parliamentary engagement, institutional attendance, participation in legislative '
             'bodies and in-person parliamentary behavior during official activities of the Chamber of Deputies. Each '
             'record represents the attendance of a deputy in a specific parliamentary event.',
  'columns': {'sk_evt': 'Surrogate key of the `dm_evento` dimension associated with the parliamentary event attended '
                        'by the deputy.',
              'sk_org': 'Surrogate key of the `dm_orgao` dimension associated with the legislative body responsible '
                        'for the parliamentary event.',
              'sk_data_inicio': 'Surrogate key of the data inicio entity used for dimensional relationships in the '
                                'Gold layer.',
              'sk_data_fim': 'Surrogate key of the data fim entity used for dimensional relationships in the Gold '
                             'layer.',
              'evt_id_evento': 'Official unique identifier of the parliamentary event in the Chamber of Deputies API.',
              'org_id_orgao_principal': 'Official or business identifier associated with org id orgao principal.',
              'evt_tx_uri': 'Official URI of the parliamentary event in the Chamber Open Data API.',
              'evt_ts_inicio': 'Official start timestamp of the parliamentary event.',
              'evt_ts_fim': 'Official end timestamp of the parliamentary event.',
              'evt_dt_inicio': 'Official start date of the parliamentary event used as the temporal reference for '
                               'legislative participation.',
              'evt_dt_fim': 'End date of the parliamentary event.',
              'evt_nr_ano_referencia': 'Reference year of the parliamentary event.',
              'evt_nr_ano_inicio': 'Year extracted from the event start date.',
              'evt_nr_mes_inicio': 'Month extracted from the event start date.',
              'evt_tx_descricao': 'Official description or title of the parliamentary event.',
              'evt_tx_tipo': 'Original event type informed by the Chamber of Deputies API.',
              'evt_tx_situacao': 'Original event status informed by the Chamber of Deputies API.',
              'evt_tx_tipo_curado': 'Standardized event type created by the project for analytical consolidation.',
              'evt_tx_situacao_curada': 'Standardized event status created by the project for analytical '
                                        'consolidation.',
              'evt_fl_inicio_valido': 'Indicator showing whether the event start timestamp is valid.',
              'evt_fl_fim_valido': 'Indicator showing whether the event end timestamp is valid.',
              'evt_fl_periodo_valido': 'Indicator showing whether the event period is valid considering start and end '
                                       'timestamps.',
              'evt_fl_sessao': 'Indicator identifying whether the event is a parliamentary session.',
              'evt_fl_audiencia_publica': 'Indicator identifying whether the event is a public hearing.',
              'evt_fl_reuniao': 'Indicator identifying whether the event is a meeting.',
              'evt_fl_encerrado': 'Indicator identifying whether the event was closed or concluded.',
              'evt_fl_cancelado': 'Indicator identifying whether the event was canceled.',
              'evt_fl_possui_registro': 'Indicator identifying whether the event has an official record or '
                                        'registration URL.',
              'evt_tx_url_registro': 'Official URL of the event record when available.',
              'evt_tx_local_interno': 'Internal location of the parliamentary event within the Chamber of Deputies.',
              'evt_tx_predio': 'Building where the parliamentary event took place.',
              'evt_tx_sala': 'Room where the parliamentary event took place.',
              'evt_tx_andar': 'Floor where the parliamentary event took place.',
              'evt_tx_local_externo': 'External location of the event when it occurred outside Chamber facilities.',
              'evt_tx_tipo_local': 'Classification of the event location as internal, external or not informed.',
              'evt_qt_orgaos': 'Number of legislative bodies associated with the parliamentary event.',
              'org_sg_orgao_principal': 'Attribute of the main legislative body associated with the record: org sg '
                                        'orgao principal.',
              'org_tx_nome_principal': 'Attribute of the main legislative body associated with the record: org tx nome '
                                       'principal.',
              'org_tx_tipo_principal': 'Attribute of the main legislative body associated with the record: org tx tipo '
                                       'principal.',
              'org_tx_siglas_relacionadas': 'Legislative body attribute related to org tx siglas relacionadas.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the parliamentary attendance '
                                    'record.',
              'bronze_id_origem': 'Original technical identifier of the attendance record in the Bronze ingestion '
                                  'source.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the parliamentary '
                                 'attendance record.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original parliamentary attendance record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the parliamentary event '
                               'attendance fact table.'}},
 {'table': 'gold.ft_votacoes',
  'comment': 'Consolidated fact table of parliamentary voting sessions held in the Brazilian Chamber of Deputies. '
             'Stores aggregated information related to legislative voting sessions involving legislative propositions, '
             'including voting results, number of favorable and opposing votes, responsible legislative body, '
             'political orientation and legislative deliberation context. This table represents the primary analytical '
             'fact table for parliamentary deliberations in the Gold layer, enabling analyses of legislative behavior, '
             'political alignment, parliamentary productivity and proposition approval dynamics in the Brazilian '
             'Chamber of Deputies.',
  'columns': {'sk_data_votacao': 'Surrogate key of the data votacao entity used for dimensional relationships in the '
                                 'Gold layer.',
              'sk_evt': 'Surrogate key of the evt entity used for dimensional relationships in the Gold layer.',
              'sk_org': 'Surrogate key of the `dm_orgao` dimension associated with the legislative body responsible '
                        'for the voting session.',
              'sk_prop': 'Surrogate key of the `dm_proposicao` dimension associated with the voted legislative '
                         'proposition.',
              'vot_id_votacao': 'Official unique identifier of the parliamentary voting session in the Chamber of '
                                'Deputies API.',
              'vot_tx_uri': 'Voting attribute related to vot tx uri.',
              'vot_dt_votacao': 'Official date of the parliamentary voting session registered by the Chamber of '
                                'Deputies.',
              'vot_ts_registro': 'Voting attribute related to vot ts registro.',
              'vot_nr_ano_referencia': 'Voting attribute related to vot nr ano referencia.',
              'vot_tx_descricao': 'Short textual description of the parliamentary voting session registered by the '
                                  'Chamber of Deputies.',
              'vot_tx_status_aprovacao': 'Voting attribute related to vot tx status aprovacao.',
              'vot_tx_resultado_curado': 'Voting attribute related to vot tx resultado curado.',
              'vot_fl_data_valida': 'Indicator related to vot fl data valida.',
              'vot_fl_timestamp_registro_valido': 'Indicator related to vot fl timestamp registro valido.',
              'vot_fl_periodo_valido': 'Indicator related to vot fl periodo valido.',
              'vot_fl_aprovada': 'Derived indicator that identifies whether the voting session resulted in approval of '
                                 'the legislative matter.',
              'vot_fl_rejeitada': 'Derived indicator that identifies whether the voting session resulted in rejection '
                                  'of the legislative matter.',
              'vot_qt_sim': 'Quantity metric related to vot qt sim.',
              'vot_qt_nao': 'Quantity metric related to vot qt nao.',
              'vot_qt_outros': 'Quantity metric related to vot qt outros.',
              'vot_qt_total': 'Quantity metric related to vot qt total.',
              'evt_id_evento': 'Official or business identifier associated with evt id evento.',
              'org_id_orgao': 'Official or business identifier associated with org id orgao.',
              'prop_id_proposicao': 'Official or business identifier associated with prop id proposicao.',
              'vot_fl_possui_proposicao': 'Indicator related to vot fl possui proposicao.',
              'vot_fl_possui_evento': 'Indicator related to vot fl possui evento.',
              'vot_fl_possui_orgao': 'Indicator related to vot fl possui orgao.',
              'vot_fl_possui_votos_contabilizados': 'Indicator related to vot fl possui votos contabilizados.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the parliamentary voting '
                                    'session record.',
              'bronze_id_origem': 'Original technical identifier of the parliamentary voting session in the Bronze '
                                  'ingestion source.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the parliamentary '
                                 'voting session.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original parliamentary voting session record.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the parliamentary voting fact '
                               'table.'}},
 {'table': 'gold.ft_votos',
  'comment': 'Fact table of individual votes cast by federal deputies in parliamentary voting sessions of the '
             'Brazilian Chamber of Deputies. Consolidates the nominal positioning of each parliamentarian in '
             'legislative deliberations, enabling detailed analysis of political behavior, party alignment, '
             'parliamentary loyalty and legislative dynamics. Each record represents the individual vote of a deputy '
             'in a specific voting session, including the official vote registered by the Chamber, political party, '
             'legislature, federative unit and temporal context of the parliamentary deliberation. This table '
             'represents the primary analytical fact table of individual parliamentary behavior in the Gold layer.',
  'columns': {'sk_dept': 'Surrogate key of the `dm_deputado` dimension associated with the federal deputy who cast the '
                         'parliamentary vote.',
              'sk_part': "Surrogate key of the `dm_partido` dimension associated with the deputy's political party at "
                         'the time of the voting session.',
              'sk_leg': 'Surrogate key of the `dm_legislatura` dimension associated with the legislative period of the '
                        'parliamentary voting session.',
              'sk_uf': 'Surrogate key of the `dm_uf` dimension associated with the federative unit of the federal '
                       'deputy.',
              'sk_data_voto': 'Surrogate key of the data voto entity used for dimensional relationships in the Gold '
                              'layer.',
              'vot_id_votacao': 'Official or business identifier associated with vot id votacao.',
              'vot_tx_uri': 'Voting attribute related to vot tx uri.',
              'vot_ts_voto': 'Official timestamp of the individual parliamentary vote cast by the deputy.',
              'dept_id_deputado': 'Official or business identifier associated with dept id deputado.',
              'part_sg_partido': 'Official acronym of the political party associated with the parliamentary record.',
              'uf_sg_uf': 'Acronym of the Brazilian Federative Unit associated with the parliamentary record.',
              'leg_id_legislatura': 'Official or business identifier associated with leg id legislatura.',
              'vot_tx_voto': 'Voting attribute related to vot tx voto.',
              'vot_tx_voto_curado': 'Voting attribute related to vot tx voto curado.',
              'vot_fl_sim': 'Indicator related to vot fl sim.',
              'vot_fl_nao': 'Indicator related to vot fl nao.',
              'vot_fl_abstencao': 'Indicator related to vot fl abstencao.',
              'vot_fl_obstrucao': 'Indicator related to vot fl obstrucao.',
              'vot_tx_dedup_key': 'Voting attribute related to vot tx dedup key.',
              'bronze_nr_ano_referencia': 'Reference year associated with the original Bronze ingestion or source '
                                          'file.',
              'bronze_ts_ingestao': 'Original technical ingestion timestamp of the record in the Bronze layer.',
              'bronze_dt_ingestao': 'Technical ingestion date of the record in the Bronze layer used for operational '
                                    'traceability.',
              'bronze_tx_endpoint': 'Chamber of Deputies API endpoint used to retrieve the individual parliamentary '
                                    'vote record.',
              'bronze_id_origem': 'Original technical identifier of the parliamentary vote in the Bronze ingestion '
                                  'source.',
              'bronze_id_batch': 'Ingestion batch identifier responsible for the original load of the parliamentary '
                                 'vote.',
              'bronze_tx_record_hash': 'Technical hash used for change tracking, deduplication and auditing of the '
                                       'original parliamentary vote record.',
              'gold_ts_processamento': 'Processing timestamp of the record in the Gold layer.',
              'gold_id_batch': 'Execution batch identifier responsible for generating the parliamentary votes fact '
                               'table.'}}]

# COMMAND ----------

for item in metadata_comments:
    table_name = item["table"]
    table_comment = item.get("comment")

    if table_comment:
        run_comment(f"COMMENT ON TABLE {table_name} IS '{sql_literal(table_comment)}'")

    for column_name, column_comment in item.get("columns", {}).items():
        run_comment(f"ALTER TABLE {table_name} ALTER COLUMN {column_name} COMMENT '{sql_literal(column_comment)}'")
