# Gold Layer — Dicionário de Dimensões e Fatos

## Visão Geral

Este documento consolida o dicionário analítico da camada Gold do projeto `camara-data-pipeline`.

O objetivo deste material é documentar:

- dimensões analíticas;
- tabelas fato;
- granularidade;
- objetivos de negócio;
- descrições funcionais;
- colunas analíticas;
- relacionamento dimensional;
- métricas corporativas.

---

# Convenções de Colunas

| Prefixo | Significado |
|---|---|
| `sk_` | Surrogate key |
| `id_` | Chave de negócio |
| `dt_` | Data |
| `vl_` | Valor monetário |
| `qt_` | Quantidade |
| `tx_` | Texto |
| `fl_` | Flag |
| `sg_` | Sigla |
| `cd_` | Código |

---

# `00_create_gold_schema`

## Tipo
Fato

## Notebook
`notebooks/03_gold/00_create_gold_schema.py`

## Descrição
# Initializes the Gold analytical layer used by the dimensional Star Schema.

## Granularidade
Granularidade analítica definida pelo modelo dimensional.

## Objetivos Analíticos

- Disponibilizar entidade conformada para analytics;
- Padronizar consumo analítico corporativo;
- Centralizar regras de negócio;
- Permitir integração entre fatos e dimensões;
- Servir dashboards e marts analíticos.

## Observações Técnicas

- Persistido em Delta Lake;
- Compatível com Databricks SQL;
- Preparado para analytics escalável;
- Mantém rastreabilidade analítica;
- Estrutura preparada para evolução incremental.

---

# `dm_data`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/01_build_dm_data.py`

## Descrição
# Builds the conformed date dimension for the Gold Star Schema.

## Granularidade
Granularidade analítica definida pelo modelo dimensional.

## Objetivos Analíticos

- Disponibilizar entidade conformada para analytics;
- Padronizar consumo analítico corporativo;
- Centralizar regras de negócio;
- Permitir integração entre fatos e dimensões;
- Servir dashboards e marts analíticos.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_data` | Chave substituta da dimensão de tempo, utilizada para relacionamento com tabelas fato da camada Gold. |
| `dt_data` | Data de referência da dimensão calendário. Representa o dia calendário utilizado nas análises temporais. |
| `nr_ano` | Ano calendário da data de referência. |
| `nr_semestre` | Número do semestre do ano, permitindo análises agregadas por primeiro ou segundo semestre. |
| `nr_trimestre` | Número do trimestre do ano, utilizado para agregações trimestrais. |
| `nr_mes` | Número do mês da data de referência, variando de 1 a 12. |
| `tx_nome_mes` | Nome completo do mês da data de referência. |
| `tx_nome_mes_abrev` | Nome abreviado do mês da data de referência. |
| `nr_dia_mes` | Dia do mês da data de referência. |
| `nr_dia_ano` | Número sequencial do dia dentro do ano. |
| `nr_semana_ano` | Número da semana no ano, utilizado para análises semanais. |
| `nr_dia_semana` | Número do dia da semana da data de referência. |
| `tx_nome_dia_semana` | Nome completo do dia da semana. |
| `tx_nome_dia_semana_abrev` | Nome abreviado do dia da semana. |
| `fl_fim_semana` | Indicador se a data corresponde a sábado ou domingo. |
| `fl_dia_util` | Indicador se a data corresponde a um dia útil do calendário. |
| `dt_inicio_mes` | Data inicial do mês correspondente à data de referência. |
| `dt_fim_mes` | Data final do mês correspondente à data de referência. |
| `dt_inicio_trimestre` | Data inicial do trimestre correspondente à data de referência. |
| `dt_fim_trimestre` | Data final do trimestre correspondente à data de referência. |
| `dt_inicio_ano` | Data inicial do ano correspondente à data de referência. |
| `dt_fim_ano` | Data final do ano correspondente à data de referência. |
| `tx_ano_mes` | Representação textual do ano e mês, utilizada para ordenação e agrupamento mensal. |
| `tx_mes_ano` | Representação textual amigável do mês e ano para exibição em dashboards. |
| `nr_ano_mes` | Representação numérica no formato ano e mês, utilizada para ordenação cronológica. |
| `dt_processamento` | Data ou timestamp de geração do registro na dimensão de tempo. |

## Observações Técnicas

- Persistido em Delta Lake;
- Compatível com Databricks SQL;
- Preparado para analytics escalável;
- Mantém rastreabilidade analítica;
- Estrutura preparada para evolução incremental.

---

# gold.dm_legislatura

## Tipo
Dimensão

## Descrição
Dimensão conformada de legislaturas da Câmara dos Deputados. Representa os períodos formais de funcionamento parlamentar, definidos pela Câmara como o intervalo entre a posse de um grupo de deputados e a véspera da posse do grupo seguinte. A API informa identificador, data de início, data de fim e ano da eleição dos parlamentares da legislatura. :contentReference[oaicite:0]{index=0}

## Granularidade
Uma linha por legislatura.

## Objetivos Analíticos
- Permitir análise de despesas, votações, proposições, eventos e atividade parlamentar por legislatura.
- Padronizar o período legislativo como dimensão temporal parlamentar.
- Identificar a legislatura vigente.
- Relacionar fatos parlamentares ao ciclo político-eleitoral correspondente.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_leg` | Chave substituta sequencial da dimensão de legislatura na camada Gold, criada para relacionamento dimensional interno no modelo estrela. |
| `leg_id_legislatura` | Identificador oficial da legislatura conforme a API de Dados Abertos da Câmara dos Deputados. Representa um período de trabalho parlamentar da Câmara. |
| `leg_nr_ano_eleicao` | Ano da eleição dos deputados federais que compõem a legislatura. No projeto, é derivado a partir do ano anterior ao início da legislatura. |
| `leg_nr_ano_inicio` | Ano civil de início da legislatura. |
| `leg_nr_ano_fim` | Ano civil de encerramento da legislatura. |
| `leg_dt_inicio` | Data oficial de início da legislatura, correspondente ao início do período de mandato parlamentar daquele grupo de deputados. |
| `leg_dt_fim` | Data oficial de fim da legislatura, correspondente ao encerramento do período antes da posse da legislatura seguinte. |
| `leg_qt_meses_duracao` | Quantidade aproximada de meses de duração da legislatura, calculada entre `leg_dt_inicio` e `leg_dt_fim`. |
| `leg_fl_legislatura_atual` | Indicador que sinaliza se a data atual está dentro do período de vigência da legislatura. Valor `1` indica legislatura vigente; valor `0` indica legislatura histórica ou futura. |
| `leg_tx_descricao` | Descrição padronizada da legislatura no formato “Legislatura {id} ({ano_inicio} - {ano_fim})”, criada para facilitar leitura em relatórios, dashboards e filtros analíticos. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. Indica quando a dimensão foi gerada ou atualizada no pipeline. |
| `gold_id_batch` | Identificador único do lote de execução responsável pela geração da dimensão Gold. Usado para rastreabilidade, auditoria e troubleshooting. |

## Relacionamentos
- Relaciona-se com fatos parlamentares por meio de `sk_leg`.
- Pode ser utilizada por fatos de despesas CEAP, votações, votos, presença em eventos e atividade parlamentar.
- A chave de negócio oficial é `leg_id_legislatura`.

## Observações Técnicas
- Fonte Gold: `silver_curated.legislaturas`.
- Tabela alvo: `gold.dm_legislatura`.
- O notebook garante uma única linha por `leg_id_legislatura`.
- Registros sem `leg_id_legislatura` são descartados antes da carga Gold.
- A chave `sk_leg` é gerada por `row_number()` ordenado por `leg_id_legislatura`.
- A dimensão recebe metadados próprios da Gold: `gold_ts_processamento` e `gold_id_batch`.

---

# `dm_partido`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/03_build_dm_partido.py`

## Descrição
Dimensão conformada de partidos políticos identificados nos registros curados de deputados. A tabela padroniza a sigla partidária utilizada pelos parlamentares e serve como entidade de integração entre dimensões, fatos e views analíticas da camada Gold.

## Granularidade
Uma linha por sigla de partido político.

## Objetivos Analíticos
- Permitir análise parlamentar por partido político.
- Integrar despesas, votações, deputados e indicadores partidários.
- Apoiar rankings, dashboards e análises comparativas entre partidos.
- Centralizar a chave dimensional de partido usada no modelo estrela.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_part` | Chave substituta sequencial da dimensão de partido na camada Gold, criada para relacionamento interno com fatos e views analíticas. |
| `part_sg_partido` | Sigla oficial do partido político associado ao deputado nos dados da Câmara, como PT, PL, MDB, PSD, PSOL ou UNIÃO. |
| `gold_ts_processamento` | Timestamp em que o registro foi processado e gravado na dimensão Gold. |
| `gold_id_batch` | Identificador único do lote de execução responsável pela geração da dimensão `dm_partido`. |

## Relacionamentos
- Relaciona-se com `gold.dm_deputado` por `part_sg_partido`.
- Pode ser usada por fatos de despesas, votações e atividade parlamentar por meio de `sk_part`.
- Serve como base para análises partidárias consolidadas, como `vw_partidos_analitica`.

## Observações Técnicas
- Fonte: `silver_curated.deputados`.
- Tabela alvo: `gold.dm_partido`.
- O notebook remove registros sem `part_sg_partido`.
- A dimensão mantém uma única linha por sigla partidária.
- A chave `sk_part` é gerada por `row_number()` ordenado por `part_sg_partido`.
- A tabela é persistida em Delta Lake com sobrescrita de schema.
- Após a gravação, é executado `OPTIMIZE gold.dm_partido`.
---
# `dm_deputado`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/04_build_dm_deputado.py`

## Descrição
Dimensão conformada de deputados federais da Câmara dos Deputados. Consolida informações cadastrais, partidárias, eleitorais, geográficas e parlamentares dos deputados identificados nos dados oficiais da API da Câmara e enriquecidos pela camada Silver Curated.

A dimensão representa a principal entidade parlamentar do modelo analítico Gold, sendo utilizada como eixo central para integração de despesas CEAP, votações, presença em eventos, proposições legislativas, indicadores de atividade parlamentar e análises políticas.

## Granularidade
Uma linha por deputado federal.

## Objetivos Analíticos
- Permitir análises parlamentares individuais.
- Relacionar deputados a partidos, legislaturas e unidades federativas.
- Consolidar dados cadastrais parlamentares para consumo analítico corporativo.
- Integrar despesas, votações e indicadores parlamentares.
- Permitir análises eleitorais, partidárias e geográficas.
- Apoiar dashboards de transparência e inteligência parlamentar.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_dept` | Chave substituta sequencial da dimensão de deputados utilizada no modelo estrela da camada Gold. |
| `dept_id_deputado` | Identificador oficial único do deputado federal na API da Câmara dos Deputados. |
| `dept_nm_deputado` | Nome civil completo do deputado federal. |
| `dept_nm_parlamentar` | Nome parlamentar utilizado oficialmente pelo deputado nas atividades legislativas. |
| `dept_sg_partido` | Sigla oficial do partido político ao qual o deputado está vinculado. |
| `dept_sg_uf` | Sigla da unidade federativa pela qual o deputado foi eleito, como RJ, SP, MG ou BA. |
| `dept_nr_legislatura` | Número da legislatura em exercício associada ao mandato parlamentar do deputado. |
| `dept_tx_email` | Endereço oficial de e-mail institucional do deputado na Câmara dos Deputados. |
| `dept_tx_url_foto` | URL oficial da fotografia institucional do deputado disponibilizada pela Câmara. |
| `dept_tx_sexo` | Sexo informado no cadastro parlamentar do deputado. |
| `dept_dt_nascimento` | Data de nascimento do deputado federal. |
| `dept_nr_idade` | Idade calculada do deputado com base na data de nascimento e na data atual do processamento. |
| `dept_nm_municipio_nascimento` | Nome do município de nascimento do deputado. |
| `dept_sg_uf_nascimento` | Sigla da unidade federativa de nascimento do deputado. |
| `dept_fl_mandato_ativo` | Indicador que informa se o deputado possui mandato parlamentar ativo no momento da coleta/processamento dos dados. |
| `dept_dt_inicio_mandato` | Data de início do mandato parlamentar atual do deputado. |
| `dept_dt_fim_mandato` | Data prevista de encerramento do mandato parlamentar do deputado. |
| `dept_nm_gabinete` | Identificação textual do gabinete parlamentar do deputado dentro da Câmara dos Deputados. |
| `dept_nr_gabinete` | Número oficial do gabinete parlamentar ocupado pelo deputado. |
| `dept_nr_andar_gabinete` | Número do andar do prédio da Câmara onde o gabinete parlamentar está localizado. |
| `dept_tx_telefone_gabinete` | Número telefônico oficial do gabinete parlamentar do deputado. |
| `dept_tx_situacao` | Situação parlamentar do deputado conforme cadastro da Câmara, como Exercício, Licenciado ou Suplente. |
| `dept_tx_condicao_eleitoral` | Condição eleitoral associada ao mandato do deputado, como titularidade ou suplência. |
| `dept_fl_reeleito` | Indicador derivado que informa se o deputado participou de legislaturas anteriores identificadas no histórico disponível. |
| `dept_qt_mandatos` | Quantidade de mandatos parlamentares identificados para o deputado nos dados históricos analisados. |
| `part_sk_part` | Chave substituta da dimensão `dm_partido` relacionada ao partido do deputado. |
| `leg_sk_leg` | Chave substituta da dimensão `dm_legislatura` associada à legislatura vigente do deputado. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela carga da dimensão de deputados. |

## Relacionamentos
- Relaciona-se com `gold.dm_partido` por meio de `part_sk_part`.
- Relaciona-se com `gold.dm_legislatura` por meio de `leg_sk_leg`.
- Utilizada por fatos de despesas CEAP, votações parlamentares, eventos, proposições e indicadores analíticos.
- Serve como dimensão principal para as views analíticas parlamentares da camada Gold.

## Observações Técnicas
- Fonte principal: `silver_curated.deputados`.
- Enriquecida com informações partidárias e legislativas.
- Mantém apenas uma linha consolidada por deputado.
- Registros sem `dept_id_deputado` são descartados antes da persistência.
- A chave `sk_dept` é gerada por `row_number()` ordenado por `dept_id_deputado`.
- Persistida em Delta Lake no schema `gold`.
- Compatível com Databricks SQL e workloads analíticos escaláveis.
- Utilizada como dimensão conformada central do modelo estrela parlamentar.
- A dimensão suporta evolução futura para historização SCD Type 2 de filiação partidária e mudanças cadastrais parlamentares.
---
# `dm_proposicao`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/05_build_dm_proposicao.py`

## Descrição
Dimensão conformada de proposições legislativas da Câmara dos Deputados. Consolida informações oficiais de projetos de lei, PECs, requerimentos, medidas provisórias e demais matérias legislativas disponibilizadas pela API de Dados Abertos da Câmara.

A dimensão centraliza metadados legislativos utilizados em análises parlamentares, votações, tramitações, produção legislativa e inteligência política. Cada registro representa uma proposição legislativa identificada oficialmente pela Câmara dos Deputados.

## Granularidade
Uma linha por proposição legislativa.

## Objetivos Analíticos
- Permitir análise de produção legislativa parlamentar.
- Relacionar votações e tramitações a proposições oficiais.
- Identificar tipos de matérias legislativas da Câmara.
- Apoiar análises temáticas, legislativas e políticas.
- Consolidar metadados legislativos reutilizáveis na camada Gold.
- Integrar fatos parlamentares ao ciclo legislativo das proposições.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_prop` | Chave substituta sequencial da dimensão de proposições utilizada no modelo estrela da camada Gold. |
| `prop_id_proposicao` | Identificador oficial único da proposição legislativa na API da Câmara dos Deputados. |
| `prop_tx_uri` | URI oficial da proposição na API de Dados Abertos da Câmara, utilizada para navegação e integração entre endpoints legislativos. |
| `prop_sg_tipo` | Sigla do tipo legislativo da proposição, como PL, PEC, MPV, REQ ou PDL. |
| `prop_tx_descricao_tipo` | Descrição textual oficial do tipo legislativo da proposição, como Projeto de Lei, Proposta de Emenda à Constituição ou Medida Provisória. |
| `prop_nr_numero` | Número oficial da proposição legislativa dentro do seu tipo e ano de apresentação. |
| `prop_nr_ano` | Ano oficial de apresentação da proposição legislativa na Câmara dos Deputados. |
| `prop_cd_tipo` | Código interno do tipo de proposição utilizado pela Câmara dos Deputados para categorização legislativa. |
| `prop_tx_ementa` | Texto resumido oficial da ementa da proposição legislativa, descrevendo seu objetivo principal. |
| `prop_tx_keywords` | Palavras-chave associadas à proposição legislativa utilizadas para indexação temática e busca textual. |
| `prop_ts_apresentacao` | Data e hora oficial de apresentação da proposição legislativa na Câmara dos Deputados. |
| `prop_tx_identificacao` | Identificação textual consolidada da proposição no formato “TIPO NÚMERO/ANO”, como “PL 2630/2020”. |
| `prop_nr_ano_apresentacao` | Ano extraído da data de apresentação da proposição para facilitar análises temporais legislativas. |
| `prop_fl_proposicao_recente` | Indicador derivado que identifica proposições apresentadas nos últimos períodos legislativos analisados pelo projeto. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador único do lote de execução responsável pela geração da dimensão de proposições. |

## Relacionamentos
- Relaciona-se com fatos de votações parlamentares por `sk_prop`.
- Pode ser integrada a pipelines CDC/SCD2 de tramitações legislativas.
- Utilizada em análises de produção legislativa parlamentar.
- Serve como dimensão central para indicadores legislativos e inteligência parlamentar.
- Relaciona-se com eventos, votações e tramitações legislativas da Câmara.

## Observações Técnicas
- Fonte principal: `silver_curated.proposicoes`.
- Tabela alvo: `gold.dm_proposicao`.
- Registros sem `prop_id_proposicao` são descartados antes da persistência.
- A chave `sk_prop` é gerada por `row_number()` ordenado por `prop_id_proposicao`.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- Estrutura preparada para integração futura com CDC/SCD2 de tramitações parlamentares.
- A dimensão é utilizada por pipelines analíticos legislativos e views Gold de inteligência parlamentar.
- A coluna `prop_tx_identificacao` é derivada para facilitar leitura analítica e visualização em dashboards.
---

# `dm_orgao`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/06_build_dm_orgao.py`

## Descrição
Dimensão conformada de órgãos legislativos da Câmara dos Deputados. Consolida informações oficiais de comissões, plenário, mesa diretora, lideranças, secretarias e demais estruturas institucionais identificadas na API de Dados Abertos da Câmara.

A dimensão permite padronizar os órgãos parlamentares utilizados em eventos legislativos, votações, tramitações, reuniões, composição de membros e atividades institucionais da Câmara dos Deputados.

## Granularidade
Uma linha por órgão legislativo da Câmara dos Deputados.

## Objetivos Analíticos
- Permitir análises parlamentares por órgão legislativo.
- Relacionar eventos, votações e proposições aos órgãos responsáveis.
- Identificar atividades de comissões e plenário.
- Apoiar análises de governança legislativa e estrutura organizacional.
- Padronizar nomenclaturas institucionais da Câmara.
- Permitir segmentação analítica por tipo de órgão parlamentar.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_org` | Chave substituta sequencial da dimensão de órgãos utilizada no modelo estrela da camada Gold. |
| `org_id_orgao` | Identificador oficial único do órgão legislativo na API da Câmara dos Deputados. |
| `org_tx_uri` | URI oficial do órgão legislativo na API de Dados Abertos da Câmara. |
| `org_sg_orgao` | Sigla oficial do órgão legislativo, como CCJ, CFT, MESA ou PLEN. |
| `org_tx_nome` | Nome oficial completo do órgão legislativo da Câmara dos Deputados. |
| `org_tx_apelido` | Nome alternativo ou apelido institucional utilizado para identificação simplificada do órgão legislativo. |
| `org_tx_nome_publicacao` | Nome institucional utilizado em publicações oficiais, documentos legislativos e registros parlamentares. |
| `org_tx_nome_resumido` | Nome reduzido do órgão legislativo utilizado em interfaces analíticas, dashboards e visualizações. |
| `org_cd_tipo_orgao` | Código interno do tipo de órgão legislativo utilizado pela Câmara dos Deputados. |
| `org_tx_tipo_orgao` | Descrição oficial do tipo de órgão legislativo, como Comissão Permanente, Comissão Temporária, Mesa Diretora ou Plenário. |
| `org_tx_tipo_curado` | Classificação analítica padronizada criada pelo projeto para agrupamento corporativo de órgãos legislativos. |
| `org_fl_plenario` | Indicador derivado que identifica se o órgão corresponde ao Plenário da Câmara dos Deputados. |
| `org_fl_comissao` | Indicador derivado que identifica se o órgão é uma comissão legislativa. |
| `org_fl_mesa` | Indicador derivado que identifica se o órgão pertence à Mesa Diretora da Câmara. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze do pipeline. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro do órgão legislativo. |
| `bronze_id_origem` | Identificador técnico do registro na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original do registro na Bronze. |
| `bronze_tx_record_hash` | Hash técnico do conteúdo do registro utilizado para controle de alterações, deduplicação e rastreabilidade de linhagem. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de órgãos. |

## Relacionamentos
- Relaciona-se com fatos de eventos parlamentares e votações legislativas.
- Utilizada em análises de composição de órgãos e atividades parlamentares.
- Pode ser integrada a dados de membros de comissões e lideranças parlamentares.
- Serve como dimensão institucional para dashboards legislativos e análises organizacionais.
- Relaciona-se com proposições e tramitações legislativas vinculadas a órgãos específicos.

## Observações Técnicas
- Fonte principal: `silver_curated.orgaos`.
- Tabela alvo: `gold.dm_orgao`.
- A dimensão mantém uma linha consolidada por `org_id_orgao`.
- Registros sem identificador oficial são descartados antes da persistência.
- A chave `sk_org` é gerada por `row_number()` ordenado por `org_id_orgao`.
- Persistida em Delta Lake com suporte a Databricks SQL.
- Mantém colunas completas de rastreabilidade Bronze para auditoria e linhagem de dados.
- A coluna `org_tx_tipo_curado` é derivada no projeto para padronização analítica corporativa.
- Indicadores booleanos como `org_fl_comissao`, `org_fl_plenario` e `org_fl_mesa` são derivados a partir da classificação institucional do órgão.
- Estrutura preparada para evolução futura com histórico organizacional e CDC institucional.
---

# `dm_gabinete`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/07_build_dm_gabinete.py`

## Descrição
Dimensão conformada de gabinetes parlamentares dos deputados federais da Câmara dos Deputados. Consolida informações estruturais, físicas e de contato dos gabinetes vinculados aos parlamentares em exercício.

A dimensão permite análises organizacionais e administrativas relacionadas à ocupação física dos gabinetes, localização parlamentar, distribuição institucional e canais oficiais de contato dos deputados federais.

## Granularidade
Uma linha por gabinete parlamentar de deputado federal.

## Objetivos Analíticos
- Permitir análise administrativa dos gabinetes parlamentares.
- Relacionar deputados às suas estruturas físicas dentro da Câmara.
- Identificar localização de gabinetes por prédio, sala e andar.
- Apoiar análises institucionais e operacionais da estrutura parlamentar.
- Disponibilizar informações oficiais de contato parlamentar.
- Validar qualidade de dados de telefone e e-mail institucional.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_gab` | Chave substituta sequencial da dimensão de gabinetes utilizada no modelo estrela da camada Gold. |
| `dept_id_deputado` | Identificador oficial do deputado federal responsável pelo gabinete parlamentar. |
| `gab_tx_nome` | Nome oficial do gabinete parlamentar do deputado. |
| `gab_tx_predio` | Identificação do prédio da Câmara dos Deputados onde o gabinete parlamentar está localizado. |
| `gab_tx_sala` | Número ou identificação da sala ocupada pelo gabinete parlamentar. |
| `gab_tx_andar` | Andar do prédio da Câmara onde o gabinete parlamentar está localizado. |
| `gab_tx_telefone` | Número telefônico oficial do gabinete parlamentar do deputado federal. |
| `gab_fl_telefone_valido` | Indicador derivado que informa se o telefone do gabinete possui formato válido conforme regras de validação do pipeline. |
| `gab_tx_email` | Endereço oficial de e-mail institucional do gabinete parlamentar. |
| `gab_fl_email_valido` | Indicador derivado que informa se o e-mail do gabinete possui formato válido conforme regras de validação implementadas no projeto. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de gabinetes. |

## Relacionamentos
- Relaciona-se com `gold.dm_deputado` por meio de `dept_id_deputado`.
- Pode ser utilizada em análises administrativas e institucionais parlamentares.
- Permite cruzamento com dados geográficos internos da Câmara dos Deputados.
- Serve como dimensão de apoio para análises de estrutura física parlamentar.

## Observações Técnicas
- Fonte principal: `silver_curated.deputados`.
- Tabela alvo: `gold.dm_gabinete`.
- A dimensão mantém uma linha consolidada por gabinete parlamentar identificado.
- Registros sem `dept_id_deputado` são descartados antes da persistência.
- A chave `sk_gab` é gerada por `row_number()` ordenado por `dept_id_deputado`.
- Persistida em Delta Lake com suporte a Databricks SQL.
- As colunas `gab_fl_telefone_valido` e `gab_fl_email_valido` são derivadas a partir de regras técnicas de validação implementadas na camada Silver/Gold.
- Estrutura preparada para futura evolução histórica de mudanças de gabinete parlamentar.
---
# `dm_fornecedor`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/08_build_dm_fornecedor.py`

## Descrição
Dimensão conformada de fornecedores vinculados às despesas parlamentares da Cota para Exercício da Atividade Parlamentar (CEAP). Consolida pessoas físicas e jurídicas que emitiram documentos fiscais utilizados na prestação de contas dos deputados federais da Câmara dos Deputados.

A dimensão permite identificar fornecedores recorrentes, analisar concentração de gastos parlamentares, monitorar pagamentos realizados com recursos da CEAP e apoiar análises de transparência, auditoria e inteligência financeira parlamentar.

## Granularidade
Uma linha por fornecedor identificado por CPF ou CNPJ.

## Objetivos Analíticos
- Permitir análise financeira de fornecedores parlamentares.
- Identificar concentração de despesas por fornecedor.
- Apoiar auditoria e transparência de gastos CEAP.
- Consolidar fornecedores utilizados por deputados federais.
- Viabilizar análises geográficas e econômicas de despesas parlamentares.
- Detectar padrões de recorrência e relacionamento financeiro parlamentar.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_forn` | Chave substituta sequencial da dimensão de fornecedores utilizada no modelo estrela da camada Gold. |
| `forn_nr_cnpj_cpf` | Número de CPF ou CNPJ do fornecedor vinculado ao documento fiscal da despesa parlamentar. |
| `forn_tx_nome_fornecedor` | Nome do fornecedor, empresa ou prestador de serviço informado no documento fiscal da despesa CEAP. |
| `forn_tx_tipo_pessoa` | Classificação do fornecedor como Pessoa Física ou Pessoa Jurídica, derivada a partir da estrutura do CPF/CNPJ informado. |
| `forn_fl_cnpj_valido` | Indicador derivado que informa se o CNPJ do fornecedor possui formato válido conforme regras de validação implementadas no pipeline. |
| `forn_fl_cpf_valido` | Indicador derivado que informa se o CPF do fornecedor possui formato válido conforme regras de validação implementadas no pipeline. |
| `forn_fl_documento_valido` | Indicador consolidado que informa se o CPF ou CNPJ do fornecedor passou nas validações técnicas do projeto. |
| `forn_tx_raiz_cnpj` | Raiz do CNPJ do fornecedor utilizada para agrupamento empresarial de filiais e análise consolidada de grupos econômicos. |
| `forn_nr_quantidade_despesas` | Quantidade total de despesas parlamentares associadas ao fornecedor identificadas no histórico processado. |
| `forn_vl_total_recebido` | Valor total recebido pelo fornecedor considerando todas as despesas parlamentares processadas no projeto. |
| `forn_dt_primeira_despesa` | Data da primeira despesa parlamentar identificada para o fornecedor no histórico disponível. |
| `forn_dt_ultima_despesa` | Data da despesa parlamentar mais recente identificada para o fornecedor no histórico disponível. |
| `forn_fl_fornecedor_recorrente` | Indicador derivado que identifica fornecedores com recorrência relevante de utilização nas despesas parlamentares. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro da despesa associada ao fornecedor. |
| `bronze_id_origem` | Identificador técnico original do registro de despesa utilizado na ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original do registro relacionado ao fornecedor. |
| `bronze_tx_record_hash` | Hash técnico utilizado para rastreamento de alterações, deduplicação e auditoria do registro original. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de fornecedores. |

## Relacionamentos
- Relaciona-se com fatos de despesas parlamentares CEAP.
- Utilizada por análises financeiras parlamentares e auditorias de gastos públicos.
- Pode ser relacionada a deputados, partidos e legislaturas por meio das despesas.
- Serve como dimensão central para análises de concentração financeira e transparência parlamentar.

## Observações Técnicas
- Fonte principal: `silver_curated.despesas`.
- Tabela alvo: `gold.dm_fornecedor`.
- A dimensão consolida fornecedores distintos por CPF/CNPJ.
- Registros sem identificação fiscal válida podem ser classificados como inconsistentes conforme regras do pipeline.
- A chave `sk_forn` é gerada por `row_number()` ordenado por `forn_nr_cnpj_cpf`.
- Persistida em Delta Lake com suporte a Databricks SQL.
- As métricas agregadas de despesas são derivadas durante o processamento Gold.
- Estrutura preparada para análises antifraude, detecção de concentração financeira e auditoria parlamentar.
- Mantém colunas de rastreabilidade Bronze para linhagem completa dos dados financeiros.

---

# `dm_evento`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/09_build_dm_evento.py`

## Descrição
Dimensão conformada de eventos parlamentares da Câmara dos Deputados. Consolida informações oficiais de sessões, audiências públicas, reuniões, seminários, comissões gerais e demais eventos legislativos registrados na API de Dados Abertos da Câmara.

A dimensão permite contextualizar atividades parlamentares realizadas ao longo do tempo, relacionando eventos a órgãos legislativos, proposições, votações e participação parlamentar.

## Granularidade
Uma linha por evento parlamentar da Câmara dos Deputados.

## Objetivos Analíticos
- Permitir análise temporal de atividades parlamentares.
- Relacionar eventos legislativos a órgãos e proposições.
- Apoiar monitoramento de sessões e reuniões parlamentares.
- Identificar volume e frequência de atividades legislativas.
- Disponibilizar dimensão temporal de eventos parlamentares.
- Integrar análises de participação e produtividade legislativa.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_evt` | Chave substituta sequencial da dimensão de eventos utilizada no modelo estrela da camada Gold. |
| `evt_id_evento` | Identificador oficial único do evento parlamentar na API da Câmara dos Deputados. |
| `evt_tx_uri` | URI oficial do evento parlamentar na API de Dados Abertos da Câmara. |
| `evt_nr_ano_referencia` | Ano de referência do evento parlamentar derivado da data de realização do evento. |
| `evt_ts_inicio` | Timestamp oficial de início do evento parlamentar registrado pela Câmara dos Deputados. |
| `evt_ts_fim` | Timestamp oficial de encerramento do evento parlamentar registrado pela Câmara dos Deputados. |
| `evt_dt_inicio` | Data de início do evento parlamentar derivada do timestamp oficial de abertura do evento. |
| `evt_dt_fim` | Data de encerramento do evento parlamentar derivada do timestamp oficial de término do evento. |
| `evt_qt_duracao_minutos` | Quantidade total de minutos de duração do evento parlamentar calculada entre o horário de início e término. |
| `evt_fl_evento_encerrado` | Indicador derivado que informa se o evento parlamentar possui horário oficial de encerramento registrado. |
| `evt_fl_evento_mesmo_dia` | Indicador derivado que informa se o evento parlamentar iniciou e terminou no mesmo dia civil. |
| `evt_tx_periodo_dia` | Classificação derivada do período do dia em que o evento iniciou, como manhã, tarde ou noite. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro do evento parlamentar. |
| `bronze_id_origem` | Identificador técnico original do evento na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original do evento na camada Bronze. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro do evento parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de eventos. |

## Relacionamentos
- Relaciona-se com órgãos legislativos responsáveis pelos eventos parlamentares.
- Pode ser integrada a fatos de presença parlamentar e participação em eventos.
- Utilizada em análises de produtividade legislativa e frequência de atividades parlamentares.
- Relaciona-se com sessões plenárias, reuniões de comissão e audiências públicas.
- Pode ser integrada a votações e proposições legislativas discutidas durante os eventos.

## Observações Técnicas
- Fonte principal: `silver_curated.eventos`.
- Tabela alvo: `gold.dm_evento`.
- Mantém uma linha consolidada por `evt_id_evento`.
- Registros sem identificador oficial de evento são descartados antes da persistência.
- A chave `sk_evt` é gerada por `row_number()` ordenado por `evt_id_evento`.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- As colunas derivadas de duração e período do dia são calculadas durante o processamento Gold.
- Mantém rastreabilidade completa de ingestão Bronze para auditoria e linhagem.
- Estrutura preparada para integração futura com pipelines de streaming e monitoramento em tempo real de eventos parlamentares.

---
# `dm_frente`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/10_build_dm_frente.py`

## Descrição
Dimensão conformada de Frentes Parlamentares da Câmara dos Deputados. Consolida informações institucionais e temáticas das frentes parlamentares registradas oficialmente na API de Dados Abertos da Câmara.

As frentes parlamentares representam associações suprapartidárias de deputados organizadas em torno de interesses, setores econômicos, pautas sociais ou temas legislativos específicos. A dimensão permite análises políticas, temáticas e de articulação parlamentar entre deputados, partidos e grupos de interesse.

## Granularidade
Uma linha por Frente Parlamentar.

## Objetivos Analíticos
- Permitir análise temática de atuação parlamentar.
- Relacionar deputados a grupos políticos suprapartidários.
- Apoiar análises de alinhamento político e interesses legislativos.
- Identificar concentração temática de frentes parlamentares.
- Permitir segmentação política por áreas de atuação.
- Integrar análises legislativas, partidárias e institucionais.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_frente` | Chave substituta sequencial da dimensão de frentes parlamentares utilizada no modelo estrela da camada Gold. |
| `frente_id_frente` | Identificador oficial único da Frente Parlamentar na API da Câmara dos Deputados. |
| `frente_tx_uri` | URI oficial da Frente Parlamentar na API de Dados Abertos da Câmara. |
| `frente_tx_titulo` | Nome oficial completo da Frente Parlamentar registrado pela Câmara dos Deputados. |
| `leg_id_legislatura` | Identificador da legislatura associada ao período de atuação da Frente Parlamentar. |
| `frente_fl_tema_saude` | Indicador derivado que identifica frentes parlamentares relacionadas a saúde pública, medicina, hospitais, SUS ou políticas sanitárias. |
| `frente_fl_tema_educacao` | Indicador derivado que identifica frentes parlamentares relacionadas a educação, ensino, universidades ou políticas educacionais. |
| `frente_fl_tema_seguranca` | Indicador derivado que identifica frentes parlamentares relacionadas a segurança pública, polícia, sistema penal ou defesa social. |
| `frente_fl_tema_agro` | Indicador derivado que identifica frentes parlamentares ligadas ao agronegócio, agricultura, pecuária ou produção rural. |
| `frente_fl_tema_mulher` | Indicador derivado que identifica frentes parlamentares relacionadas a direitos das mulheres, igualdade de gênero ou proteção feminina. |
| `frente_fl_tema_meio_ambiente` | Indicador derivado que identifica frentes parlamentares relacionadas a meio ambiente, sustentabilidade, mudanças climáticas ou preservação ambiental. |
| `frente_tx_categoria_tematica` | Classificação temática consolidada da Frente Parlamentar derivada a partir da análise textual do título da frente. |
| `frente_qt_temas_identificados` | Quantidade de categorias temáticas identificadas automaticamente para a Frente Parlamentar durante o processamento analítico. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro da Frente Parlamentar. |
| `bronze_id_origem` | Identificador técnico original do registro na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original da Frente Parlamentar. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original da Frente Parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de Frentes Parlamentares. |

## Relacionamentos
- Relaciona-se com deputados membros de Frentes Parlamentares.
- Pode ser integrada a partidos e legislaturas.
- Utilizada em análises de alinhamento político e articulação temática parlamentar.
- Serve como dimensão temática para dashboards políticos e legislativos.
- Relaciona-se com fatos de composição de frentes parlamentares e indicadores analíticos parlamentares.

## Observações Técnicas
- Fonte principal: `silver_curated.frentes`.
- Tabela alvo: `gold.dm_frente`.
- Mantém uma linha consolidada por `frente_id_frente`.
- Registros sem identificador oficial são descartados antes da persistência.
- A chave `sk_frente` é gerada por `row_number()` ordenado por `frente_id_frente`.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- Os indicadores temáticos (`frente_fl_tema_*`) são derivados por regras heurísticas aplicadas ao título da Frente Parlamentar.
- A classificação temática consolidada é gerada durante o processamento Gold para facilitar análises políticas agregadas.
- Mantém rastreabilidade completa da camada Bronze para auditoria e linhagem.
- Estrutura preparada para futuras análises de redes parlamentares e relacionamento político suprapartidário.

---

# `dm_uf`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/11_build_dm_uf.py`

## Descrição
Dimensão conformada das Unidades Federativas (UFs) do Brasil utilizadas nos dados parlamentares da Câmara dos Deputados. Consolida os estados brasileiros associados à representação eleitoral dos deputados federais, origem geográfica de parlamentares, fornecedores e distribuição regional da atividade legislativa.

A dimensão padroniza a referência geográfica estadual utilizada em análises políticas, eleitorais, financeiras e parlamentares da camada Gold.

## Granularidade
Uma linha por Unidade Federativa (UF) brasileira.

## Objetivos Analíticos
- Permitir análises parlamentares por estado brasileiro.
- Relacionar deputados às unidades federativas de eleição.
- Apoiar análises regionais de despesas parlamentares.
- Consolidar dimensão geográfica estadual para o modelo estrela.
- Permitir segmentação política e legislativa regional.
- Apoiar dashboards geográficos e mapas analíticos.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_uf` | Chave substituta sequencial da dimensão de Unidade Federativa utilizada no modelo estrela da camada Gold. |
| `uf_sg_uf` | Sigla oficial da Unidade Federativa brasileira, como RJ, SP, MG, BA ou DF. |
| `uf_tx_nome` | Nome completo oficial da Unidade Federativa brasileira correspondente à sigla da UF. |
| `uf_tx_regiao` | Região geográfica oficial do Brasil à qual a Unidade Federativa pertence, como Norte, Nordeste, Centro-Oeste, Sudeste ou Sul. |
| `uf_fl_capital_federal` | Indicador derivado que identifica se a UF corresponde ao Distrito Federal. |
| `uf_nr_quantidade_deputados` | Quantidade de deputados federais associados à Unidade Federativa identificados no processamento atual do projeto. |
| `uf_fl_regiao_norte` | Indicador derivado que informa se a UF pertence à Região Norte do Brasil. |
| `uf_fl_regiao_nordeste` | Indicador derivado que informa se a UF pertence à Região Nordeste do Brasil. |
| `uf_fl_regiao_centro_oeste` | Indicador derivado que informa se a UF pertence à Região Centro-Oeste do Brasil. |
| `uf_fl_regiao_sudeste` | Indicador derivado que informa se a UF pertence à Região Sudeste do Brasil. |
| `uf_fl_regiao_sul` | Indicador derivado que informa se a UF pertence à Região Sul do Brasil. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de Unidades Federativas. |

## Relacionamentos
- Relaciona-se com `gold.dm_deputado` por meio da UF eleitoral do parlamentar.
- Pode ser integrada a fornecedores e despesas parlamentares por localização geográfica.
- Utilizada em análises regionais legislativas e eleitorais.
- Serve como dimensão geográfica para dashboards e mapas analíticos.
- Permite cruzamentos entre atividade parlamentar e distribuição regional.

## Observações Técnicas
- Fonte principal derivada de `silver_curated.deputados`.
- Tabela alvo: `gold.dm_uf`.
- Mantém uma linha consolidada por sigla de UF.
- A chave `sk_uf` é gerada por `row_number()` ordenado por `uf_sg_uf`.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- Os nomes completos das UFs e regiões são derivados por mapeamento interno do projeto.
- Indicadores regionais são calculados durante o processamento Gold.
- Estrutura preparada para análises geoespaciais e dashboards regionais parlamentares.
---
# `dm_tipo_despesa`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/12_build_dm_tipo_despesa.py`

## Descrição
Dimensão conformada dos tipos de despesas parlamentares da Cota para Exercício da Atividade Parlamentar (CEAP). Consolida as classificações oficiais de subcotas e especificações de despesas utilizadas pela Câmara dos Deputados para categorizar gastos realizados pelos parlamentares.

A dimensão padroniza categorias financeiras utilizadas em análises de gastos públicos parlamentares, permitindo segmentação por natureza da despesa, tipo de serviço contratado e classificação administrativa oficial da Câmara.

## Granularidade
Uma linha por combinação de subcota e especificação de despesa parlamentar.

## Objetivos Analíticos
- Permitir segmentação analítica de despesas parlamentares.
- Padronizar categorias financeiras utilizadas na CEAP.
- Apoiar auditoria e transparência de gastos públicos.
- Consolidar classificações oficiais de despesas da Câmara.
- Permitir análises financeiras por natureza de gasto parlamentar.
- Facilitar dashboards de composição e distribuição de despesas.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_tipo_desp` | Chave substituta sequencial da dimensão de tipos de despesa utilizada no modelo estrela da camada Gold. |
| `desp_cd_subcota` | Código oficial da subcota parlamentar utilizado pela Câmara dos Deputados para classificar o tipo principal da despesa CEAP. |
| `desp_tx_tipo_despesa` | Descrição oficial do tipo de despesa parlamentar associada à subcota, como passagens aéreas, combustíveis, divulgação parlamentar, hospedagem ou locação de veículos. |
| `desp_cd_especificacao_subcota` | Código complementar de especificação da subcota utilizado para detalhamento adicional do tipo de despesa parlamentar. |
| `desp_tx_especificacao` | Descrição textual detalhada da especificação da despesa parlamentar vinculada à subcota CEAP. |
| `desp_tx_categoria_macro` | Categoria financeira analítica derivada pelo projeto para agrupamento corporativo das despesas parlamentares, como transporte, alimentação, comunicação, escritório ou consultoria. |
| `desp_fl_despesa_transporte` | Indicador derivado que identifica despesas relacionadas a transporte parlamentar, incluindo passagens, combustíveis e locomoção. |
| `desp_fl_despesa_hospedagem` | Indicador derivado que identifica despesas parlamentares relacionadas a hospedagem e estadia. |
| `desp_fl_despesa_divulgacao` | Indicador derivado que identifica despesas relacionadas à divulgação da atividade parlamentar e comunicação institucional. |
| `desp_fl_despesa_combustivel` | Indicador derivado que identifica despesas parlamentares com combustíveis e abastecimento de veículos. |
| `desp_fl_despesa_consultoria` | Indicador derivado que identifica despesas relacionadas a consultorias, assessorias técnicas ou serviços especializados. |
| `desp_fl_despesa_escritorio` | Indicador derivado que identifica despesas relacionadas à manutenção administrativa e funcionamento de escritório parlamentar. |
| `desp_fl_possui_especificacao` | Indicador derivado que informa se a despesa possui especificação complementar cadastrada além da subcota principal. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro da despesa parlamentar. |
| `bronze_id_origem` | Identificador técnico original do registro de despesa na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original do registro relacionado ao tipo de despesa. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original da despesa parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de tipos de despesa. |

## Relacionamentos
- Relaciona-se com fatos de despesas parlamentares CEAP.
- Pode ser integrada a deputados, partidos e fornecedores por meio das despesas.
- Utilizada em análises financeiras parlamentares e dashboards de transparência pública.
- Serve como dimensão categórica para agrupamento e consolidação de gastos parlamentares.
- Permite análises comparativas entre categorias de despesas públicas parlamentares.

## Observações Técnicas
- Fonte principal: `silver_curated.despesas`.
- Tabela alvo: `gold.dm_tipo_despesa`.
- Mantém uma linha consolidada por combinação de subcota e especificação.
- A chave `sk_tipo_desp` é gerada por `row_number()` ordenado por `desp_cd_subcota` e `desp_cd_especificacao_subcota`.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- As categorias macro e indicadores temáticos são derivados durante o processamento Gold.
- Mantém rastreabilidade completa da camada Bronze para auditoria financeira e linhagem de dados.
- Estrutura preparada para evolução futura com taxonomias financeiras parlamentares mais detalhadas.
---

# `dm_bancada`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/13_build_dm_bancada.py`

## Descrição
Dimensão conformada de bancadas parlamentares e blocos partidários da Câmara dos Deputados. Consolida agrupamentos políticos utilizados nas votações, orientações partidárias e articulações parlamentares registradas nos dados legislativos da Câmara.

A dimensão padroniza bancadas partidárias, blocos parlamentares e agrupamentos políticos identificados nos registros de votações e orientações parlamentares, permitindo análises de alinhamento político, fidelidade partidária e comportamento legislativo coletivo.

## Granularidade
Uma linha por bancada parlamentar ou bloco partidário identificado nos dados legislativos.

## Objetivos Analíticos
- Permitir análise política por bancada parlamentar.
- Consolidar agrupamentos partidários utilizados em votações.
- Apoiar análises de fidelidade e alinhamento político.
- Identificar blocos parlamentares e coalizões legislativas.
- Padronizar nomenclaturas políticas utilizadas nos registros legislativos.
- Integrar análises de comportamento parlamentar coletivo.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_banc` | Chave substituta sequencial da dimensão de bancadas utilizada no modelo estrela da camada Gold. |
| `banc_tx_bancada_curada` | Nome padronizado da bancada parlamentar ou bloco político utilizado pelo projeto para consolidação analítica das orientações legislativas. |
| `banc_tx_tipo_bancada` | Classificação da bancada parlamentar, como partido político, bloco parlamentar, liderança governista, oposição ou independentes. |
| `banc_tx_uri` | URI ou identificador de referência associado à bancada parlamentar quando disponível nos dados legislativos processados. |
| `banc_fl_bancada_valida` | Indicador derivado que informa se a bancada parlamentar possui identificação consistente e válida conforme regras de padronização implementadas no projeto. |
| `banc_fl_bloco_parlamentar` | Indicador derivado que identifica se o registro representa um bloco parlamentar composto por múltiplos partidos políticos. |
| `banc_fl_partido_politico` | Indicador derivado que identifica se a bancada corresponde diretamente a um partido político individual. |
| `banc_fl_governo` | Indicador derivado que identifica bancadas associadas à base governista nas orientações parlamentares analisadas. |
| `banc_fl_oposicao` | Indicador derivado que identifica bancadas classificadas como oposição parlamentar durante as votações analisadas. |
| `banc_qt_partidos_bloco` | Quantidade de partidos políticos identificados na composição do bloco parlamentar quando aplicável. |
| `banc_tx_composicao_bloco` | Descrição textual consolidada da composição partidária do bloco parlamentar identificada nos dados legislativos. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro relacionado à bancada parlamentar. |
| `bronze_id_origem` | Identificador técnico original do registro legislativo utilizado na ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original do registro relacionado à bancada parlamentar. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original relacionado à bancada parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de bancadas parlamentares. |

## Relacionamentos
- Relaciona-se com fatos de votações parlamentares e orientações partidárias.
- Utilizada em análises de fidelidade partidária e alinhamento político.
- Pode ser integrada a deputados, partidos e proposições legislativas.
- Serve como dimensão política para dashboards legislativos e análises parlamentares.
- Permite estudos de coalizão política e comportamento legislativo coletivo.

## Observações Técnicas
- Fonte principal derivada de `silver_curated.votacoes_orientacoes`.
- Tabela alvo: `gold.dm_bancada`.
- Mantém uma linha consolidada por bancada ou bloco parlamentar padronizado.
- A chave `sk_banc` é gerada por `row_number()` ordenado por `banc_tx_bancada_curada`.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- As classificações políticas e indicadores derivam de regras de curadoria implementadas na camada Silver/Gold.
- A composição de blocos parlamentares pode ser derivada a partir de parsing textual das orientações legislativas.
- Mantém rastreabilidade completa da camada Bronze para auditoria e linhagem.
- Estrutura preparada para análises futuras de redes políticas e dinâmica de coalizões parlamentares.
---

# `dm_responsavel_ceap`

## Tipo
Dimensão

## Notebook
`notebooks/03_gold/14_build_dm_responsavel_ceap.py`

## Descrição
Dimensão conformada de responsáveis pelas despesas da Cota para Exercício da Atividade Parlamentar (CEAP). Consolida os parlamentares, lideranças e estruturas institucionais associadas à prestação de contas das despesas parlamentares registradas na Câmara dos Deputados.

A dimensão foi criada para padronizar os responsáveis financeiros identificados nos registros CEAP, permitindo distinguir despesas vinculadas diretamente a deputados federais, lideranças partidárias e outras estruturas parlamentares utilizadas na execução de despesas públicas.

## Granularidade
Uma linha por responsável identificado nas despesas parlamentares CEAP.

## Objetivos Analíticos
- Permitir análise financeira por responsável da despesa parlamentar.
- Diferenciar despesas individuais de deputados e lideranças partidárias.
- Consolidar entidades responsáveis pela execução financeira da CEAP.
- Apoiar auditoria e transparência de gastos parlamentares.
- Facilitar análises de despesas por estrutura parlamentar.
- Padronizar responsáveis financeiros utilizados nas prestações de contas da Câmara.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_resp_ceap` | Chave substituta sequencial da dimensão de responsáveis CEAP utilizada no modelo estrela da camada Gold. |
| `dept_tx_nome_parlamentar` | Nome parlamentar do responsável pela despesa CEAP conforme registrado nos dados oficiais da Câmara dos Deputados. |
| `resp_tx_tipo_responsavel` | Classificação analítica do responsável pela despesa parlamentar, como DEPUTADO, LIDERANCA ou NAO_IDENTIFICADO. |
| `resp_tx_nome_responsavel_curado` | Nome padronizado do responsável pela despesa utilizado para consolidação analítica e eliminação de inconsistências textuais. |
| `resp_fl_lideranca` | Indicador derivado que identifica despesas vinculadas a lideranças partidárias ou estruturas de liderança parlamentar. |
| `resp_fl_deputado` | Indicador derivado que identifica despesas vinculadas diretamente a deputados federais individuais. |
| `resp_fl_responsavel_identificado` | Indicador derivado que informa se o responsável pela despesa foi corretamente identificado e classificado durante o processamento. |
| `resp_qt_despesas` | Quantidade total de despesas parlamentares associadas ao responsável identificado. |
| `resp_vl_total_despesas` | Valor total das despesas parlamentares vinculadas ao responsável CEAP. |
| `resp_dt_primeira_despesa` | Data da primeira despesa parlamentar identificada para o responsável no histórico processado. |
| `resp_dt_ultima_despesa` | Data da despesa parlamentar mais recente identificada para o responsável no histórico disponível. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro da despesa CEAP relacionada ao responsável financeiro. |
| `bronze_id_origem` | Identificador técnico original do registro de despesa utilizado na ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original da despesa parlamentar relacionada ao responsável. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original da despesa parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da dimensão de responsáveis CEAP. |

## Relacionamentos
- Relaciona-se com fatos de despesas parlamentares CEAP.
- Pode ser integrada à dimensão de deputados por nome parlamentar ou identificador resolvido.
- Utilizada em análises financeiras parlamentares e dashboards de transparência pública.
- Permite segmentação entre despesas individuais e despesas de lideranças partidárias.
- Serve como dimensão organizacional para análises de responsabilidade financeira parlamentar.

## Observações Técnicas
- Fonte principal: `silver_curated.despesas`.
- Tabela alvo: `gold.dm_responsavel_ceap`.
- Mantém uma linha consolidada por responsável identificado nas despesas parlamentares.
- A chave `sk_resp_ceap` é gerada por `row_number()` ordenado por `dept_tx_nome_parlamentar`.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- A classificação do tipo de responsável é derivada por regras heurísticas aplicadas ao nome parlamentar.
- Registros contendo termos relacionados a “LIDERANÇA” são classificados como estruturas de liderança parlamentar.
- Mantém rastreabilidade completa da camada Bronze para auditoria e linhagem financeira.
- Estrutura preparada para futura integração com modelos de governança financeira parlamentar e análise de estruturas partidárias.
---

# `ft_despesas_ceap`

## Tipo
Fato

## Notebook
`notebooks/03_gold/15_build_ft_despesas_ceap.py`

## Descrição
Tabela fato de despesas parlamentares da Cota para Exercício da Atividade Parlamentar (CEAP). Consolida os gastos realizados por deputados federais da Câmara dos Deputados com base nos documentos fiscais apresentados para ressarcimento ou pagamento pela CEAP.

A tabela registra operações financeiras parlamentares relacionadas a passagens aéreas, combustíveis, hospedagem, divulgação parlamentar, consultorias, aluguel de veículos, alimentação e demais despesas autorizadas pela Câmara dos Deputados.

Representa o principal fato financeiro do modelo estrela parlamentar da camada Gold.

## Granularidade
Uma linha por documento fiscal de despesa parlamentar CEAP.

## Objetivos Analíticos
- Permitir análise detalhada de gastos parlamentares.
- Consolidar despesas públicas por deputado, partido e legislatura.
- Apoiar auditoria financeira e transparência parlamentar.
- Identificar padrões de consumo da CEAP.
- Permitir análises temporais, geográficas e financeiras de despesas públicas.
- Integrar fornecedores, tipos de despesa e responsáveis financeiros.
- Apoiar detecção de anomalias e concentração de gastos parlamentares.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_despesa_ceap` | Chave substituta sequencial da tabela fato de despesas CEAP utilizada no modelo estrela da camada Gold. |
| `resp.sk_resp_ceap` | Chave substituta da dimensão `dm_responsavel_ceap` associada ao responsável financeiro da despesa parlamentar. |
| `dept.sk_dept` | Chave substituta da dimensão `dm_deputado` associada ao deputado federal responsável pela despesa. |
| `part.sk_part` | Chave substituta da dimensão `dm_partido` associada ao partido político do parlamentar no momento da despesa. |
| `leg.sk_leg` | Chave substituta da dimensão `dm_legislatura` associada ao período legislativo da despesa parlamentar. |
| `forn.sk_forn` | Chave substituta da dimensão `dm_fornecedor` associada ao fornecedor do documento fiscal da despesa. |
| `tipo.sk_desp_tipo` | Chave substituta da dimensão `dm_tipo_despesa` associada à classificação financeira da despesa CEAP. |
| `uf.sk_uf` | Chave substituta da dimensão `dm_uf` associada à unidade federativa do parlamentar ou da despesa. |
| `desp.desp_dt_emissao` | Data de emissão do documento fiscal associado à despesa parlamentar. |
| `desp_nr_ano` | Ano de referência da despesa parlamentar derivado da data de emissão do documento fiscal. |
| `desp_nr_mes` | Mês de referência da despesa parlamentar derivado da data de emissão do documento fiscal. |
| `desp_vl_documento` | Valor bruto original do documento fiscal apresentado pelo parlamentar na prestação de contas da CEAP. |
| `desp_vl_liquido` | Valor líquido efetivamente considerado para ressarcimento ou pagamento da despesa parlamentar. |
| `desp_vl_glosa` | Valor glosado ou rejeitado pela Câmara dos Deputados durante a análise da prestação de contas da despesa parlamentar. |
| `desp_vl_restituicao` | Valor devolvido ou restituído relacionado à despesa parlamentar quando aplicável. |
| `desp_tx_numero_documento` | Número identificador do documento fiscal utilizado na prestação de contas parlamentar. |
| `desp_tx_tipo_documento` | Tipo do documento fiscal apresentado, como nota fiscal, recibo, bilhete aéreo ou fatura. |
| `desp_tx_url_documento` | URL oficial do documento fiscal digitalizado disponibilizado pela Câmara dos Deputados para transparência pública. |
| `desp_fl_documento_digital` | Indicador derivado que informa se a despesa possui documento digital disponível para consulta pública. |
| `desp_fl_glosada` | Indicador derivado que identifica despesas parlamentares com valor glosado pela Câmara dos Deputados. |
| `desp_fl_restituida` | Indicador derivado que identifica despesas parlamentares que possuem devolução ou restituição financeira registrada. |
| `desp_fl_despesa_alta` | Indicador derivado utilizado para identificar despesas consideradas acima do padrão estatístico definido pelo projeto. |
| `desp_tx_observacao` | Texto complementar ou observação associada ao registro da despesa parlamentar quando disponível na origem. |
| `desp_qt_documentos_mesmo_fornecedor` | Quantidade de documentos fiscais emitidos pelo mesmo fornecedor identificados no período analisado. |
| `desp_vl_media_fornecedor` | Valor médio das despesas realizadas com o mesmo fornecedor calculado durante o processamento analítico. |
| `desp_fl_fornecedor_recorrente` | Indicador derivado que identifica fornecedores recorrentes nas despesas parlamentares do deputado ou partido. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro da despesa parlamentar. |
| `bronze_id_origem` | Identificador técnico original da despesa parlamentar na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original da despesa CEAP. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro financeiro original. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da fato de despesas CEAP. |

## Relacionamentos
- Relaciona-se com `dm_deputado`, `dm_partido`, `dm_legislatura`, `dm_fornecedor`, `dm_tipo_despesa`, `dm_responsavel_ceap` e `dm_uf`.
- Serve como principal fato financeiro parlamentar do modelo estrela.
- Utilizada em dashboards de transparência, auditoria e inteligência financeira parlamentar.
- Pode ser integrada a modelos analíticos de comportamento parlamentar e eficiência financeira.
- Suporta análises temporais, partidárias, regionais e temáticas de despesas públicas.

## Observações Técnicas
- Fonte principal: `silver_curated.despesas`.
- Tabela alvo: `gold.ft_despesas_ceap`.
- Mantém granularidade por documento fiscal individual.
- Registros inválidos ou rejeitados por regras de qualidade são segregados durante o pipeline.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- As métricas derivadas e indicadores financeiros são calculados durante o processamento Gold.
- Mantém rastreabilidade completa da camada Bronze para auditoria financeira e linhagem de dados.
- Estrutura preparada para análises antifraude, detecção de anomalias e monitoramento de transparência pública.
- Compatível com pipelines analíticos de streaming e monitoramento incremental de despesas parlamentares.
---


# `ft_votacoes`

## Tipo
Fato

## Notebook
`notebooks/03_gold/16_build_ft_votacoes.py`

## Descrição
Tabela fato consolidada de votações parlamentares realizadas na Câmara dos Deputados. Armazena informações agregadas de sessões de votação relacionadas a proposições legislativas, incluindo resultado da votação, quantidade de votos favoráveis e contrários, órgão responsável, orientação política e contexto legislativo da deliberação.

A tabela representa o principal fato analítico de deliberações parlamentares da camada Gold, permitindo análises de comportamento legislativo, alinhamento político, produtividade parlamentar e dinâmica de aprovação de proposições na Câmara dos Deputados.

## Granularidade
Uma linha por votação parlamentar realizada na Câmara dos Deputados.

## Objetivos Analíticos
- Permitir análise consolidada de votações legislativas.
- Relacionar votações a proposições, órgãos e legislaturas.
- Apoiar análises de aprovação e rejeição legislativa.
- Identificar comportamento político e alinhamento parlamentar.
- Permitir análises temporais e institucionais das deliberações legislativas.
- Servir como base para indicadores de fidelidade partidária e inteligência parlamentar.
- Integrar análises de produtividade legislativa e governança política.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_votacao` | Chave substituta sequencial da tabela fato de votações utilizada no modelo estrela da camada Gold. |
| `sk_prop` | Chave substituta da dimensão `dm_proposicao` associada à proposição legislativa votada. |
| `sk_org` | Chave substituta da dimensão `dm_orgao` associada ao órgão legislativo responsável pela votação. |
| `sk_leg` | Chave substituta da dimensão `dm_legislatura` associada ao período legislativo da votação parlamentar. |
| `vot_id_votacao` | Identificador oficial único da votação parlamentar na API da Câmara dos Deputados. |
| `vot.vot_dt_votacao` | Data oficial de realização da votação parlamentar registrada pela Câmara dos Deputados. |
| `vot_ts_votacao` | Timestamp completo da votação parlamentar utilizado para análises temporais detalhadas. |
| `vot_nr_ano` | Ano da votação parlamentar derivado da data oficial da deliberação legislativa. |
| `vot_nr_mes` | Mês da votação parlamentar derivado da data oficial da deliberação legislativa. |
| `vot_tx_descricao` | Descrição textual resumida da votação parlamentar registrada pela Câmara dos Deputados. |
| `vot_tx_resultado` | Resultado oficial da votação parlamentar, como Aprovado, Rejeitado ou Prejudicado. |
| `vot_fl_aprovada` | Indicador derivado que informa se a votação resultou na aprovação da matéria legislativa. |
| `vot_fl_rejeitada` | Indicador derivado que informa se a votação resultou na rejeição da matéria legislativa. |
| `vot_qt_votos_sim` | Quantidade total de votos favoráveis (“Sim”) registrados na votação parlamentar. |
| `vot_qt_votos_nao` | Quantidade total de votos contrários (“Não”) registrados na votação parlamentar. |
| `vot_qt_abstencoes` | Quantidade total de abstenções registradas na votação parlamentar. |
| `vot_qt_obstrucoes` | Quantidade total de votos classificados como obstrução parlamentar. |
| `vot_qt_presentes` | Quantidade total de parlamentares presentes na votação legislativa. |
| `vot_qt_ausentes` | Quantidade total de parlamentares ausentes na votação legislativa. |
| `vot_qt_total_votos` | Quantidade total de votos computados na votação parlamentar. |
| `vot_pc_aprovacao` | Percentual de votos favoráveis em relação ao total de votos válidos da votação parlamentar. |
| `vot_pc_rejeicao` | Percentual de votos contrários em relação ao total de votos válidos da votação parlamentar. |
| `vot_fl_votacao_unanime` | Indicador derivado que identifica votações parlamentares sem divergência entre votos favoráveis e contrários. |
| `vot_fl_votacao_apertada` | Indicador derivado que identifica votações decididas por margem reduzida conforme critério analítico definido pelo projeto. |
| `vot_tx_tipo_votacao` | Classificação da votação parlamentar, como simbólica, nominal ou secreta, quando disponível na origem. |
| `vot_tx_orientacao_governo` | Orientação política oficial do governo registrada para a votação parlamentar quando disponível. |
| `vot_tx_orientacao_oposicao` | Orientação política oficial da oposição registrada para a votação parlamentar quando disponível. |
| `vot_fl_quorum_alto` | Indicador derivado que identifica votações com elevada participação parlamentar conforme critério analítico definido pelo projeto. |
| `vot_tx_resumo_resultado` | Texto analítico resumido consolidando resultado e contexto quantitativo da votação parlamentar. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro da votação parlamentar. |
| `bronze_id_origem` | Identificador técnico original da votação parlamentar na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original da votação parlamentar. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original da votação parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da fato de votações parlamentares. |

## Relacionamentos
- Relaciona-se com `dm_proposicao`, `dm_orgao` e `dm_legislatura`.
- Pode ser integrada à fato de votos parlamentares individuais.
- Utilizada em análises de comportamento político e alinhamento partidário.
- Serve como base para indicadores de fidelidade partidária e governabilidade.
- Integrada a dashboards legislativos e inteligência parlamentar.

## Observações Técnicas
- Fonte principal: `silver_curated.votacoes`.
- Tabela alvo: `gold.ft_votacoes`.
- Mantém granularidade por votação parlamentar individual.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- Indicadores derivados de aprovação, unanimidade e margem de votação são calculados durante o processamento Gold.
- Mantém rastreabilidade completa da camada Bronze para auditoria e linhagem legislativa.
- Estrutura preparada para integração com pipelines de streaming de votações parlamentares em tempo real.
- Compatível com análises incrementais e monitoramento contínuo de atividade legislativa.
---

# `ft_votos`

## Tipo
Fato

## Notebook
`notebooks/03_gold/17_build_ft_votos.py`

## Descrição
Tabela fato de votos individuais dos deputados federais nas votações parlamentares da Câmara dos Deputados. Consolida o posicionamento nominal de cada parlamentar em deliberações legislativas, permitindo análise detalhada de comportamento político, alinhamento partidário, fidelidade parlamentar e dinâmica legislativa.

Cada registro representa o voto individual de um deputado em uma votação específica, incluindo o posicionamento registrado oficialmente pela Câmara, partido político, legislatura, unidade federativa e contexto temporal da votação.

A tabela representa o principal fato analítico de comportamento parlamentar individual da camada Gold.

## Granularidade
Uma linha por voto individual de deputado em uma votação parlamentar.

## Objetivos Analíticos
- Permitir análise individual de comportamento parlamentar.
- Identificar alinhamento político e fidelidade partidária.
- Apoiar análises de governabilidade e oposição.
- Relacionar deputados às votações legislativas realizadas.
- Medir participação parlamentar em votações nominais.
- Detectar divergências partidárias e padrões de votação.
- Integrar análises políticas, legislativas e partidárias.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_voto` | Chave substituta sequencial da tabela fato de votos parlamentares utilizada no modelo estrela da camada Gold. |
| `dept.sk_dept` | Chave substituta da dimensão `dm_deputado` associada ao deputado federal que realizou o voto parlamentar. |
| `part.sk_part` | Chave substituta da dimensão `dm_partido` associada ao partido político do deputado no momento da votação. |
| `leg.sk_leg` | Chave substituta da dimensão `dm_legislatura` associada ao período legislativo da votação parlamentar. |
| `uf.sk_uf` | Chave substituta da dimensão `dm_uf` associada à unidade federativa do deputado federal. |
| `sk_votacao` | Chave substituta da tabela fato `ft_votacoes` associada à votação parlamentar correspondente. |
| `voto_id_votacao` | Identificador oficial da votação parlamentar associada ao voto individual do deputado. |
| `voto.vot_ts_voto` | Timestamp oficial do registro do voto parlamentar individual realizado pelo deputado. |
| `voto_dt_votacao` | Data da votação parlamentar derivada do timestamp oficial do voto individual. |
| `voto_tx_tipo_voto` | Posicionamento oficial registrado pelo deputado na votação parlamentar, como Sim, Não, Abstenção, Obstrução ou Artigo 17. |
| `voto_fl_voto_sim` | Indicador derivado que identifica votos favoráveis à proposição legislativa. |
| `voto_fl_voto_nao` | Indicador derivado que identifica votos contrários à proposição legislativa. |
| `voto_fl_abstencao` | Indicador derivado que identifica votos classificados como abstenção parlamentar. |
| `voto_fl_obstrucao` | Indicador derivado que identifica votos classificados como obstrução parlamentar. |
| `voto_fl_ausencia` | Indicador derivado que identifica ausência do parlamentar na votação legislativa. |
| `voto_fl_acompanhou_partido` | Indicador derivado que informa se o deputado votou conforme a orientação oficial do partido ou bancada parlamentar. |
| `voto_fl_divergiu_partido` | Indicador derivado que informa se o deputado divergiu da orientação oficial do partido ou bancada parlamentar. |
| `voto_fl_acompanhou_governo` | Indicador derivado que informa se o voto parlamentar acompanhou a orientação oficial do governo registrada para a votação. |
| `voto_fl_acompanhou_oposicao` | Indicador derivado que informa se o voto parlamentar acompanhou a orientação oficial da oposição registrada para a votação. |
| `voto_tx_orientacao_partido` | Orientação oficial da bancada partidária associada ao voto parlamentar individual. |
| `voto_tx_orientacao_governo` | Orientação oficial do governo federal registrada para a votação parlamentar. |
| `voto_tx_orientacao_oposicao` | Orientação oficial da oposição parlamentar registrada para a votação legislativa. |
| `voto_fl_voto_valido` | Indicador derivado que informa se o voto foi considerado válido para análises estatísticas e legislativas. |
| `voto_nr_ano` | Ano da votação parlamentar derivado da data oficial do voto. |
| `voto_nr_mes` | Mês da votação parlamentar derivado da data oficial do voto. |
| `voto_tx_resumo_politico` | Texto analítico resumido consolidando o posicionamento político do parlamentar na votação analisada. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro do voto parlamentar individual. |
| `bronze_id_origem` | Identificador técnico original do voto parlamentar na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original do voto parlamentar. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original do voto parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da fato de votos parlamentares. |

## Relacionamentos
- Relaciona-se com `dm_deputado`, `dm_partido`, `dm_legislatura`, `dm_uf` e `ft_votacoes`.
- Utilizada em análises de fidelidade partidária e alinhamento político.
- Serve como base para indicadores de governabilidade e comportamento parlamentar.
- Integrada a dashboards legislativos e análises de inteligência política.
- Permite análises individuais e coletivas de votação parlamentar.

## Observações Técnicas
- Fonte principal: `silver_curated.votacoes_votos`.
- Tabela alvo: `gold.ft_votos`.
- Mantém granularidade por voto parlamentar individual.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- Os indicadores de alinhamento partidário e governamental são derivados durante o processamento Gold.
- Mantém rastreabilidade completa da camada Bronze para auditoria e linhagem legislativa.
- Estrutura preparada para análises avançadas de comportamento político, redes parlamentares e inteligência legislativa.
- Compatível com pipelines incrementais e streaming de votações parlamentares em tempo real.
---

# `ft_orientacoes_bancada`

## Tipo
Fato

## Notebook
`notebooks/03_gold/18_build_ft_orientacoes_bancada.py`

## Descrição
Tabela fato de orientações de voto emitidas por bancadas parlamentares, partidos políticos e blocos legislativos nas votações da Câmara dos Deputados. Consolida o posicionamento oficial das lideranças partidárias e bancadas em relação às proposições legislativas submetidas à deliberação parlamentar.

A tabela permite analisar comportamento político coletivo, alinhamento partidário, estratégias legislativas e posicionamento institucional de partidos, governo e oposição durante as votações parlamentares.

Cada registro representa a orientação oficial de uma bancada parlamentar em uma votação específica.

## Granularidade
Uma linha por orientação de bancada em uma votação parlamentar.

## Objetivos Analíticos
- Permitir análise de posicionamento político de bancadas parlamentares.
- Relacionar partidos e blocos às votações legislativas.
- Apoiar análises de alinhamento político e fidelidade partidária.
- Identificar orientações oficiais de governo, oposição e partidos.
- Consolidar comportamento político coletivo nas deliberações parlamentares.
- Integrar análises de coalizões legislativas e governabilidade.
- Servir como base para análise comparativa entre orientação partidária e voto individual.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_orientacao` | Chave substituta sequencial da tabela fato de orientações parlamentares utilizada no modelo estrela da camada Gold. |
| `banc.sk_banc` | Chave substituta da dimensão `dm_bancada` associada à bancada parlamentar responsável pela orientação de voto. |
| `org.sk_org` | Chave substituta da dimensão `dm_orgao` associada ao órgão legislativo responsável pela votação parlamentar. |
| `ori.vot_id_votacao` | Identificador oficial da votação parlamentar associada à orientação da bancada. |
| `ori.vot_tx_uri` | URI oficial da votação parlamentar na API de Dados Abertos da Câmara dos Deputados. |
| `ori.org_sg_orgao` | Sigla oficial do órgão legislativo onde ocorreu a votação parlamentar, como PLEN ou CCJC. |
| `ori.banc_tx_bancada_curada` | Nome padronizado da bancada parlamentar ou bloco político utilizado na consolidação analítica das orientações legislativas. |
| `ori.vot_tx_orientacao` | Orientação oficial original registrada pela bancada parlamentar para a votação, como Sim, Não, Liberado, Obstrução ou Abstenção. |
| `ori.vot_tx_orientacao_curada` | Orientação parlamentar padronizada pelo projeto para consolidação analítica e eliminação de inconsistências textuais. |
| `ori.vot_tx_descricao_resultado` | Descrição textual consolidada da orientação política emitida pela bancada parlamentar. |
| `ori.vot_fl_orientacao_sim` | Indicador derivado que identifica orientações favoráveis à aprovação da proposição legislativa. |
| `ori.vot_fl_orientacao_nao` | Indicador derivado que identifica orientações contrárias à aprovação da proposição legislativa. |
| `ori.vot_fl_orientacao_liberado` | Indicador derivado que identifica orientações em que a bancada liberou seus parlamentares para votar livremente. |
| `ori.vot_fl_orientacao_obstrucao` | Indicador derivado que identifica orientações parlamentares de obstrução legislativa. |
| `ori.vot_fl_orientacao_abstencao` | Indicador derivado que identifica orientações parlamentares de abstenção. |
| `ori.vot_fl_orientacao_governo` | Indicador derivado que identifica orientações emitidas pela liderança governista durante a votação parlamentar. |
| `ori.vot_fl_orientacao_oposicao` | Indicador derivado que identifica orientações emitidas pela oposição parlamentar durante a votação legislativa. |
| `ori.vot_fl_orientacao_coesa` | Indicador derivado utilizado em análises de coesão partidária e alinhamento político coletivo. |
| `ori.vot_tx_dedup_key` | Chave técnica de deduplicação utilizada pelo pipeline para garantir unicidade da orientação parlamentar processada. |
| `ori.bronze_nr_ano_referencia` | Ano de referência da ingestão original do registro na camada Bronze. |
| `ori.bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `ori.bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `ori.bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro da orientação parlamentar. |
| `ori.bronze_id_origem` | Identificador técnico original da orientação parlamentar na origem da ingestão Bronze. |
| `ori.bronze_tx_source_file` | Nome do arquivo fonte utilizado na ingestão da orientação parlamentar quando aplicável ao pipeline de processamento. |
| `ori.bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original da orientação parlamentar. |
| `ori.bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original da orientação parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da fato de orientações de bancada. |

## Relacionamentos
- Relaciona-se com `dm_bancada` e `dm_orgao`.
- Pode ser integrada a `ft_votacoes` por identificador da votação parlamentar.
- Utilizada em análises de fidelidade partidária e alinhamento político.
- Serve como base para comparação entre orientação partidária e voto individual parlamentar.
- Integrada a dashboards legislativos e análises de governabilidade parlamentar.
- Permite análises de coalizão política e comportamento coletivo de partidos e blocos parlamentares.

## Observações Técnicas
- Fonte principal: `silver_curated.votacoes_orientacoes`.
- Tabela alvo: `gold.ft_orientacoes_bancada`.
- Mantém granularidade por orientação de bancada em cada votação parlamentar.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- As orientações são padronizadas durante o processamento Gold para garantir consistência analítica.
- A coluna `ori.vot_tx_dedup_key` é utilizada para controle de duplicidade e integridade dos registros.
- Mantém rastreabilidade completa da camada Bronze para auditoria e linhagem legislativa.
- Compatível com pipelines incrementais, CDC legislativo e streaming de votações parlamentares.
- Estrutura preparada para análises avançadas de coesão partidária, governabilidade e dinâmica política parlamentar.

---

# `ft_atividade_parlamentar`

## Tipo
Fato

## Notebook
`notebooks/03_gold/19_build_ft_atividade_parlamentar.py`

## Descrição
Tabela fato consolidada de atividade parlamentar dos deputados federais da Câmara dos Deputados. Agrega indicadores quantitativos e financeiros relacionados à atuação parlamentar, incluindo despesas CEAP, participação legislativa, comportamento político e métricas analíticas derivadas utilizadas pelo projeto.

A tabela foi projetada para servir como camada analítica consolidada de performance parlamentar, permitindo análise integrada da atuação política, financeira e legislativa dos deputados federais.

Cada registro representa um resumo analítico da atividade parlamentar de um deputado em uma legislatura específica.

## Granularidade
Uma linha por deputado federal em uma legislatura.

## Objetivos Analíticos
- Consolidar indicadores analíticos de atuação parlamentar.
- Integrar métricas financeiras, legislativas e políticas.
- Permitir ranking de atividade parlamentar.
- Apoiar dashboards executivos e inteligência política.
- Identificar padrões de produtividade legislativa.
- Medir intensidade de utilização da CEAP.
- Integrar comportamento parlamentar, gastos e votações.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_atividade_parlamentar` | Chave substituta sequencial da tabela fato de atividade parlamentar utilizada no modelo estrela da camada Gold. |
| `dept.sk_dept` | Chave substituta da dimensão `dm_deputado` associada ao deputado federal analisado. |
| `part.sk_part` | Chave substituta da dimensão `dm_partido` associada ao partido político do deputado no período analisado. |
| `leg.sk_leg` | Chave substituta da dimensão `dm_legislatura` associada ao período legislativo analisado. |
| `uf.sk_uf` | Chave substituta da dimensão `dm_uf` associada à unidade federativa do deputado federal. |
| `dept_src.dept_id_deputado` | Identificador oficial do deputado federal utilizado como chave de negócio durante a consolidação analítica da atividade parlamentar. |
| `dept_src.part_sg_partido` | Sigla oficial do partido político associada ao deputado durante o processamento da atividade parlamentar. |
| `dept_src.uf_sg_uf` | Sigla da unidade federativa de representação eleitoral do deputado federal. |
| `dept_src.leg_id_legislatura` | Identificador oficial da legislatura associada ao período de atividade parlamentar consolidado. |
| `qt_despesas` | Quantidade total de despesas parlamentares CEAP registradas para o deputado no período analisado. |
| `vl_total_despesas` | Valor financeiro total das despesas parlamentares CEAP realizadas pelo deputado no período analisado. |
| `vl_total_liquido` | Valor líquido consolidado das despesas parlamentares efetivamente consideradas após glosas e ajustes financeiros. |
| `vl_total_glosado` | Valor total glosado pela Câmara dos Deputados nas despesas parlamentares do deputado. |
| `qt_fornecedores_distintos` | Quantidade de fornecedores distintos utilizados pelo deputado nas despesas parlamentares registradas. |
| `qt_tipos_despesa` | Quantidade de categorias distintas de despesas parlamentares utilizadas pelo deputado. |
| `qt_votacoes_participadas` | Quantidade total de votações parlamentares nas quais o deputado participou. |
| `qt_votos_sim` | Quantidade total de votos favoráveis registrados pelo deputado nas votações parlamentares analisadas. |
| `qt_votos_nao` | Quantidade total de votos contrários registrados pelo deputado nas votações parlamentares analisadas. |
| `qt_abstencoes` | Quantidade total de abstenções registradas pelo deputado nas votações parlamentares analisadas. |
| `qt_obstrucoes` | Quantidade total de votos classificados como obstrução parlamentar realizados pelo deputado. |
| `pc_presenca_votacoes` | Percentual de presença do deputado nas votações parlamentares analisadas em relação ao total de votações elegíveis. |
| `pc_alinhamento_partido` | Percentual de alinhamento do deputado com a orientação oficial do partido ou bancada parlamentar. |
| `pc_alinhamento_governo` | Percentual de alinhamento do deputado com as orientações oficiais do governo nas votações analisadas. |
| `pc_divergencia_partido` | Percentual de votações em que o deputado divergiu da orientação oficial do partido político. |
| `vl_media_despesa` | Valor médio das despesas parlamentares realizadas pelo deputado no período analisado. |
| `vl_maior_despesa` | Maior valor individual de despesa parlamentar registrado para o deputado no período analisado. |
| `qt_eventos_participados` | Quantidade de eventos parlamentares associados à atividade legislativa do deputado quando disponível no processamento analítico. |
| `qt_proposicoes_relacionadas` | Quantidade de proposições legislativas relacionadas à atuação parlamentar do deputado no período analisado. |
| `fl_deputado_alta_atividade` | Indicador derivado que identifica deputados com alta intensidade de atividade parlamentar conforme critérios analíticos definidos pelo projeto. |
| `fl_deputado_alto_custo` | Indicador derivado que identifica deputados com elevado volume financeiro de despesas parlamentares. |
| `fl_deputado_baixa_presenca` | Indicador derivado que identifica deputados com baixa participação em votações parlamentares. |
| `tx_classificacao_atividade` | Classificação analítica consolidada da atividade parlamentar do deputado derivada das métricas de produtividade, presença e comportamento legislativo. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão dos dados-base utilizados na consolidação da atividade parlamentar. |
| `bronze_dt_ingestao` | Data técnica de ingestão dos registros utilizados na composição da atividade parlamentar. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pelos dados-base utilizados na geração da fato analítica. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da fato de atividade parlamentar. |

## Relacionamentos
- Relaciona-se com `dm_deputado`, `dm_partido`, `dm_legislatura` e `dm_uf`.
- Consolida informações derivadas de `ft_despesas_ceap`, `ft_votos` e `ft_votacoes`.
- Serve como base para dashboards executivos parlamentares.
- Utilizada em análises de produtividade, transparência e comportamento político.
- Pode alimentar modelos analíticos de ranking e scoring parlamentar.

## Observações Técnicas
- Fonte principal derivada de múltiplas tabelas fato e dimensões Gold.
- Tabela alvo: `gold.ft_atividade_parlamentar`.
- Mantém granularidade consolidada por deputado e legislatura.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- As métricas analíticas são calculadas durante o processamento Gold.
- Indicadores derivados utilizam regras de negócio definidas no projeto de inteligência parlamentar.
- Mantém rastreabilidade parcial da camada Bronze para auditoria e linhagem analítica.
- Estrutura preparada para evolução incremental com novos indicadores parlamentares.
- Compatível com dashboards analíticos, scorecards parlamentares e modelos de inteligência política.
---

# `ft_presenca_eventos`

## Tipo
Fato

## Notebook
`notebooks/03_gold/20_build_ft_presenca_eventos.py`

## Descrição
Tabela fato de presença parlamentar em eventos legislativos da Câmara dos Deputados. Consolida a participação de deputados federais em sessões plenárias, audiências públicas, reuniões de comissão, seminários e demais eventos parlamentares registrados na API de Dados Abertos da Câmara.

A tabela permite medir engajamento parlamentar, frequência institucional, participação em órgãos legislativos e comportamento presencial dos deputados em atividades oficiais da Câmara dos Deputados.

Cada registro representa a presença de um deputado em um evento parlamentar específico.

## Granularidade
Uma linha por deputado participante em um evento parlamentar.

## Objetivos Analíticos
- Permitir análise de presença parlamentar em eventos legislativos.
- Medir participação institucional dos deputados federais.
- Relacionar parlamentares a órgãos e atividades legislativas.
- Apoiar indicadores de engajamento e produtividade parlamentar.
- Identificar padrões de participação em comissões e plenário.
- Integrar análises temporais e institucionais de frequência parlamentar.
- Servir como base para indicadores de presença legislativa.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_presenca_evento` | Chave substituta sequencial da tabela fato de presença em eventos utilizada no modelo estrela da camada Gold. |
| `dm_evt.sk_evt` | Chave substituta da dimensão `dm_evento` associada ao evento parlamentar em que houve participação do deputado. |
| `org.sk_org` | Chave substituta da dimensão `dm_orgao` associada ao órgão legislativo responsável pelo evento parlamentar. |
| `dept.sk_dept` | Chave substituta da dimensão `dm_deputado` associada ao parlamentar participante do evento legislativo. |
| `part.sk_part` | Chave substituta da dimensão `dm_partido` associada ao partido político do deputado participante. |
| `leg.sk_leg` | Chave substituta da dimensão `dm_legislatura` associada ao período legislativo do evento parlamentar. |
| `uf.sk_uf` | Chave substituta da dimensão `dm_uf` associada à unidade federativa do deputado participante. |
| `evt.evt_dt_inicio` | Data oficial de início do evento parlamentar utilizada como referência temporal da participação legislativa. |
| `evt_ts_inicio` | Timestamp oficial de início do evento parlamentar. |
| `evt_ts_fim` | Timestamp oficial de encerramento do evento parlamentar. |
| `evt_id_evento` | Identificador oficial único do evento parlamentar na API da Câmara dos Deputados. |
| `evt_tx_tipo_evento` | Classificação do tipo de evento parlamentar, como sessão plenária, audiência pública, reunião de comissão ou seminário legislativo. |
| `evt_tx_nome_evento` | Nome ou descrição resumida do evento parlamentar registrado na Câmara dos Deputados. |
| `evt_qt_duracao_minutos` | Quantidade de minutos de duração do evento parlamentar calculada entre horário de início e término. |
| `pres_fl_presente` | Indicador derivado que informa se o parlamentar participou efetivamente do evento legislativo registrado. |
| `pres_fl_ausente` | Indicador derivado que informa ausência do parlamentar no evento legislativo analisado. |
| `pres_fl_evento_plenario` | Indicador derivado que identifica eventos parlamentares realizados em sessões plenárias da Câmara dos Deputados. |
| `pres_fl_evento_comissao` | Indicador derivado que identifica eventos parlamentares realizados em comissões legislativas. |
| `pres_fl_evento_longa_duracao` | Indicador derivado que identifica eventos parlamentares com duração acima do padrão analítico definido pelo projeto. |
| `pres_nr_ano` | Ano do evento parlamentar derivado da data oficial de realização. |
| `pres_nr_mes` | Mês do evento parlamentar derivado da data oficial de realização. |
| `pres_tx_periodo_dia` | Classificação do período do dia em que o evento parlamentar ocorreu, como manhã, tarde ou noite. |
| `pres_qt_eventos_dia` | Quantidade de eventos parlamentares em que o deputado participou no mesmo dia legislativo. |
| `pres_tx_resumo_participacao` | Texto analítico resumido consolidando a participação parlamentar no evento legislativo. |
| `bronze_ts_ingestao` | Timestamp técnico original da ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze utilizada para rastreabilidade operacional. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro de presença parlamentar. |
| `bronze_id_origem` | Identificador técnico original do registro de presença na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original do registro de presença parlamentar. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alterações, deduplicação e auditoria do registro original de presença parlamentar. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da fato de presença em eventos parlamentares. |

## Relacionamentos
- Relaciona-se com `dm_evento`, `dm_orgao`, `dm_deputado`, `dm_partido`, `dm_legislatura` e `dm_uf`.
- Utilizada em análises de presença e produtividade parlamentar.
- Pode ser integrada a fatos de votações e atividade parlamentar.
- Serve como base para indicadores de frequência institucional parlamentar.
- Integrada a dashboards legislativos e monitoramento de participação parlamentar.

## Observações Técnicas
- Fonte principal: `silver_curated.eventos` e relacionamentos parlamentares derivados.
- Tabela alvo: `gold.ft_presenca_eventos`.
- Mantém granularidade por parlamentar participante em evento legislativo.
- Persistida em Delta Lake com compatibilidade Databricks SQL.
- Indicadores derivados de presença e classificação de eventos são calculados durante o processamento Gold.
- Mantém rastreabilidade completa da camada Bronze para auditoria e linhagem institucional.
- Estrutura preparada para integração futura com pipelines de streaming de eventos parlamentares em tempo real.
- Compatível com análises temporais, monitoramento legislativo e inteligência parlamentar institucional.

---

# `ft_frentes_membros`

## Tipo
Fato

## Notebook
`notebooks/03_gold/21_build_ft_frentes_membros.py`

## Descrição
Tabela fato de composição de Frentes Parlamentares da Câmara dos Deputados. Representa a relação entre deputados federais e as frentes parlamentares das quais participam, preservando partido, UF, legislatura, papel do parlamentar na frente e indicadores de participação.

A tabela apoia o Atlas das Frentes Parlamentares, permitindo analisar composição política, diversidade partidária, distribuição regional, lideranças internas, participação simultânea de deputados em múltiplas frentes e evolução das frentes ao longo das legislaturas.

## Granularidade
Uma linha por vínculo de deputado membro em uma Frente Parlamentar.

## Objetivos Analíticos
- Analisar a composição das Frentes Parlamentares por deputado.
- Identificar partidos e UFs representados em cada frente.
- Medir participação parlamentar em frentes temáticas.
- Identificar coordenadores, presidentes, vice-presidentes e membros.
- Apoiar análise de articulação suprapartidária.
- Permitir cruzamento entre temas das frentes e perfil político dos deputados.

## Colunas

| Coluna | Descrição |
|---|---|
| `sk_frente` | Chave substituta da dimensão `dm_frente` associada à Frente Parlamentar. |
| `sk_dept` | Chave substituta da dimensão `dm_deputado` associada ao deputado membro da frente. |
| `sk_part` | Chave substituta da dimensão `dm_partido` associada ao partido do deputado membro. |
| `sk_uf` | Chave substituta da dimensão `dm_uf` associada à unidade federativa do deputado membro. |
| `sk_leg` | Chave substituta da dimensão `dm_legislatura` associada à legislatura da Frente Parlamentar. |
| `frente_id_frente` | Identificador oficial da Frente Parlamentar na API da Câmara dos Deputados. |
| `dept_id_deputado` | Identificador oficial do deputado federal membro da Frente Parlamentar. |
| `part_sg_partido` | Sigla oficial do partido político do deputado membro da frente. |
| `uf_sg_uf` | Sigla da unidade federativa de representação eleitoral do deputado membro. |
| `leg_id_legislatura` | Identificador da legislatura associada ao vínculo entre deputado e Frente Parlamentar. |
| `memb_tx_dedup_key` | Chave técnica de deduplicação do vínculo entre deputado, frente parlamentar e legislatura. |
| `frente_tx_uri` | URI oficial da Frente Parlamentar na API de Dados Abertos da Câmara. |
| `frente_tx_titulo` | Nome oficial completo da Frente Parlamentar. |
| `frente_fl_tema_saude` | Indicador se a frente está relacionada ao tema saúde. |
| `frente_fl_tema_educacao` | Indicador se a frente está relacionada ao tema educação. |
| `frente_fl_tema_seguranca` | Indicador se a frente está relacionada ao tema segurança pública. |
| `frente_fl_tema_agro` | Indicador se a frente está relacionada ao agronegócio, agricultura ou pecuária. |
| `frente_fl_tema_mulher` | Indicador se a frente está relacionada a direitos das mulheres ou igualdade de gênero. |
| `frente_fl_tema_meio_ambiente` | Indicador se a frente está relacionada a meio ambiente e sustentabilidade. |
| `dept_tx_uri` | URI oficial do deputado federal na API de Dados Abertos da Câmara. |
| `dept_tx_nome_parlamentar` | Nome parlamentar utilizado oficialmente pelo deputado membro da frente. |
| `dept_tx_email` | E-mail institucional do deputado membro. |
| `dept_fl_email_valido` | Indicador se o e-mail institucional do deputado possui formato válido conforme regra do pipeline. |
| `dept_tx_url_foto` | URL oficial da fotografia institucional do deputado. |
| `memb_cd_titulo` | Código do título ou cargo exercido pelo deputado dentro da Frente Parlamentar. |
| `memb_tx_titulo` | Descrição do título ou função do deputado na Frente Parlamentar. |
| `memb_tx_status` | Situação do vínculo do deputado com a Frente Parlamentar. |
| `memb_fl_ativo` | Indicador se o vínculo do deputado com a Frente Parlamentar está ativo. |
| `memb_fl_coordenador` | Indicador se o deputado exerce função de coordenador na Frente Parlamentar. |
| `memb_fl_presidente` | Indicador se o deputado exerce função de presidente na Frente Parlamentar. |
| `memb_fl_vice` | Indicador se o deputado exerce função de vice-presidente ou vice-coordenador na Frente Parlamentar. |
| `memb_fl_membro` | Indicador se o deputado participa como membro regular da Frente Parlamentar. |
| `qt_membro_frente` | Métrica unitária com valor `1`, usada para contagem de vínculos de membros em frentes parlamentares. |
| `qt_membro_ativo` | Métrica unitária com valor `1` quando o vínculo do deputado com a frente está ativo. |
| `qt_coordenador` | Métrica unitária com valor `1` quando o deputado é coordenador da Frente Parlamentar. |
| `qt_presidente` | Métrica unitária com valor `1` quando o deputado é presidente da Frente Parlamentar. |
| `bronze_ts_ingestao` | Timestamp técnico original de ingestão do registro na camada Bronze. |
| `bronze_dt_ingestao` | Data técnica de ingestão do registro na camada Bronze. |
| `bronze_tx_endpoint` | Endpoint da API da Câmara utilizado para obtenção do registro original. |
| `bronze_id_origem` | Identificador técnico do registro na origem da ingestão Bronze. |
| `bronze_id_batch` | Identificador do lote de ingestão responsável pela carga original. |
| `bronze_tx_record_hash` | Hash técnico utilizado para controle de alteração, deduplicação e rastreabilidade. |
| `silver_base_ts_processamento` | Timestamp de processamento do registro na camada Silver Base. |
| `silver_curated_ts_processamento` | Timestamp de processamento do registro na camada Silver Curated. |
| `gold_ts_processamento` | Timestamp de processamento do registro na camada Gold. |
| `gold_id_batch` | Identificador do lote de execução responsável pela geração da fato Gold. |

## Relacionamentos
- Relaciona-se com `gold.dm_frente` por `sk_frente`.
- Relaciona-se com `gold.dm_deputado` por `sk_dept`.
- Relaciona-se com `gold.dm_partido` por `sk_part`.
- Relaciona-se com `gold.dm_uf` por `sk_uf`.
- Relaciona-se com `gold.dm_legislatura` por `sk_leg`.

## Observações Técnicas
- Fonte principal: `silver_curated.frentes_membros`.
- Tabela alvo: `gold.ft_frentes_membros`.
- A tabela é particionada por `leg_id_legislatura`.
- O pipeline filtra apenas legislaturas definidas em `LEGISLATURAS_PADRAO`.
- O notebook valida obrigatoriamente `sk_frente` e `sk_leg`.
- Registros sem `sk_dept` geram alerta de qualidade, mas não interrompem a carga.
- A unicidade é validada por `memb_tx_dedup_key`.
- A tabela é persistida em Delta Lake e otimizada com `OPTIMIZE`.

---

# Relacionamento entre Fatos e Dimensões

As tabelas fato da camada Gold utilizam dimensões conformadas para garantir:

- consistência analítica;
- reutilização corporativa;
- integridade dimensional;
- padronização de métricas;
- performance analítica.

---

# Capacidades Analíticas

A camada Gold suporta:

- análise parlamentar;
- análise partidária;
- análise financeira;
- análise de votações;
- comportamento legislativo;
- indicadores de transparência;
- eficiência parlamentar;
- intelligence analytics;
- dashboards executivos.

---

# Considerações Finais

Este documento representa o catálogo técnico e funcional da camada Gold do projeto.

Ele deve evoluir continuamente conforme novas dimensões, fatos e marts analíticos forem adicionados ao pipeline.

