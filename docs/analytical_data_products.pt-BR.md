# Produtos Analíticos de Dados

## Visão Geral

Este documento descreve os exports analíticos em CSV gerados a partir da camada
Gold analítica e dos datasets de Parliamentary Intelligence.

Os produtos analíticos exportados estão organizados por domínio analítico e
armazenados dentro da estrutura `data/parliamentary_intelligence/`.

Esses datasets fornecem evidências analíticas reproduzíveis para:
- parliamentary intelligence
- análises CEAP
- inteligência de votações
- análises de alinhamento político
- análises de engajamento parlamentar
- análises históricas CDC/SCD2
- observabilidade streaming

---

# Analytics CEAP

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `gold_vw_despesas_ceap_analitica.csv` | `gold.vw_despesas_ceap_analitica` | Dataset analítico consolidado de despesas CEAP com indicadores parlamentares. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_despesas_deputado_segmento.csv` | `gold.vw_despesas_deputado_segmento` | Segmentação de despesas CEAP por deputado e categoria de despesa. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_gastos_segmentados.csv` | `gold.vw_gastos_segmentados` | Análise consolidada segmentada de despesas parlamentares. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_anomalias_ceap_zscore.csv` | `gold.vw_anomalias_ceap_zscore` | Detecção estatística de anomalias utilizando Z-score sobre despesas CEAP. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_ranking_despesas_deputado_mensal.csv` | `gold.vw_ranking_despesas_deputado_mensal` | Ranking mensal de despesas parlamentares por deputado. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_top_10_gastos_partido_mensal.csv` | `gold.vw_top_10_gastos_partido_mensal` | Análise dos 10 maiores gastos mensais por partido político. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_perfil_gasto_partido.csv` | `gold.vw_perfil_gasto_partido` | Análise de perfil de gastos por partido político. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_partidos_despesas_segmento.csv` | `gold.vw_partidos_despesas_segmento` | Análise segmentada de despesas CEAP por partido político. | `data/parliamentary_intelligence/ceap/` |

---

# Analytics de Frentes Parlamentares

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `gold_vw_frentes_membros_analitica.csv` | `gold.vw_frentes_membros_analitica` | Dataset analítico de participação em frentes parlamentares. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_frentes_diversidade_partidaria.csv` | `gold.vw_frentes_diversidade_partidaria` | Análise de diversidade partidária nas frentes parlamentares. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_deputados_mais_frentes.csv` | `gold.vw_deputados_mais_frentes` | Deputados participantes do maior número de frentes parlamentares. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_sobreposicao_frentes.csv` | `gold.vw_sobreposicao_frentes` | Análise de sobreposição e relacionamento entre frentes parlamentares. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_evolucao_frentes_legislatura.csv` | `gold.vw_evolucao_frentes_legislatura` | Evolução histórica de frentes parlamentares por legislatura. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_especializacao_tematica.csv` | `gold.vw_especializacao_tematica` | Análise de especialização temática das frentes parlamentares. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_alinhamento_frente_vs_partido.csv` | `gold.vw_alinhamento_frente_vs_partido` | Comparação de alinhamento político entre frentes e partidos. | `data/parliamentary_intelligence/frentes/` |

---

# Analytics de Eventos Legislativos

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `gold_vw_eventos_analitica.csv` | `gold.vw_eventos_analitica` | Dataset analítico consolidado de eventos legislativos. | `data/parliamentary_intelligence/eventos/` |
| `gold_vw_eventos_futuros.csv` | `gold.vw_eventos_futuros` | Análise de agenda e eventos legislativos futuros. | `data/parliamentary_intelligence/eventos/` |
| `gold_vw_densidade_eventos_semanal.csv` | `gold.vw_densidade_eventos_semanal` | Análise de densidade semanal de eventos legislativos. | `data/parliamentary_intelligence/eventos/` |
| `gold_vw_semanas_sem_atividade.csv` | `gold.vw_semanas_sem_atividade` | Identificação de semanas sem atividade parlamentar. | `data/parliamentary_intelligence/eventos/` |

---

# Analytics de Inteligência de Votações

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `gold_vw_votacoes_analitica.csv` | `gold.vw_votacoes_analitica` | Dataset analítico consolidado de sessões de votação. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_votos_deputados_analitica.csv` | `gold.vw_votos_deputados_analitica` | Dataset analítico de comportamento de votação por deputado. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_orientacoes_bancada_analitica.csv` | `gold.vw_orientacoes_bancada_analitica` | Análise de orientações de bancada em sessões de votação. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_fidelidade_partidaria.csv` | `gold.vw_fidelidade_partidaria` | Análise de fidelidade partidária e alinhamento de votos. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_partidos_fidelidade_votacao.csv` | `gold.vw_partidos_fidelidade_votacao` | Métricas de fidelidade de votação por partido político. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_partidos_votos_distribuicao.csv` | `gold.vw_partidos_votos_distribuicao` | Análise de distribuição de votos por partido político. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_ausencias_votacoes_criticas.csv` | `gold.vw_ausencias_votacoes_criticas` | Análise de ausências parlamentares em votações críticas. | `data/parliamentary_intelligence/votacoes/` |

---

# Analytics de Engajamento Parlamentar

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `gold_vw_atividade_parlamentar_analitica.csv` | `gold.vw_atividade_parlamentar_analitica` | Dataset analítico consolidado de atividade parlamentar. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_indice_eficiencia_parlamentar.csv` | `gold.vw_indice_eficiencia_parlamentar` | Indicadores analíticos de eficiência parlamentar. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_indice_transparencia.csv` | `gold.vw_indice_transparencia` | Indicadores analíticos de transparência parlamentar. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_ranking_ausencias_criticas.csv` | `gold.vw_ranking_ausencias_criticas` | Ranking de ausências parlamentares em votações críticas. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_score_engajamento_parlamentar.csv` | `gold.vw_score_engajamento_parlamentar` | Dataset de pontuação de engajamento parlamentar. | `data/parliamentary_intelligence/engajamento/` |

---

# Inteligência Partidária

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `gold_vw_partidos_analitica.csv` | `gold.vw_partidos_analitica` | Dataset analítico consolidado de partidos políticos. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_partidos_perfil.csv` | `gold.vw_partidos_perfil` | Análise de perfil e posicionamento de partidos políticos. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_dashboard_partidos.csv` | `gold.vw_dashboard_partidos` | Dataset executivo analítico para dashboard de partidos políticos. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_partidos_especializacao_tematica.csv` | `gold.vw_partidos_especializacao_tematica` | Análise de especialização temática por partido político. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_analise_ineficiencia_parlamentar.csv` | `gold.vw_analise_ineficiencia_parlamentar` | Indicadores analíticos de ineficiência parlamentar. | `data/parliamentary_intelligence/partidos/` |

---

# Analytics Históricos CDC / SCD2

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `gold_cdc_vw_proposicoes_tramitacao_historica.csv` | `gold_cdc.vw_proposicoes_tramitacao_historica` | Dataset analítico histórico de tramitações utilizando SCD Type 2. | `data/parliamentary_intelligence/cdc/` |
| `gold_cdc_vw_tempo_tramitacao_proposicoes.csv` | `gold_cdc.vw_tempo_tramitacao_proposicoes` | Análise de duração e ciclo de vida de tramitações legislativas. | `data/parliamentary_intelligence/cdc/` |
| `gold_cdc_vw_alertas_tramitacao_proposicoes.csv` | `gold_cdc.vw_alertas_tramitacao_proposicoes` | Alertas analíticos para anomalias e atrasos em tramitações legislativas. | `data/parliamentary_intelligence/cdc/` |

---

# Observabilidade Streaming

| Export CSV | View Origem | Descrição | Localização |
|---|---|---|---|
| `monitoring_vw_sla_votacoes_streaming.csv` | `monitoring.vw_sla_votacoes_streaming` | Dataset de monitoramento de SLA e observabilidade para pipelines streaming de votações. | `data/parliamentary_intelligence/streaming/` |

---

# Estratégia para Grandes Datasets

Alguns datasets analíticos excedem os limites de tamanho suportados pelo GitHub para versionamento eficiente.

Os seguintes arquivos completos foram mantidos fora do repositório GitHub devido ao volume de dados:

- `vw_votacoes_analitica_full.csv`
- `vw_votos_analitica_full.csv`

Nesses cenários:
- amostras representativas em CSV são versionadas no repositório
- os exports completos permanecem armazenados em Volumes do Databricks Unity Catalog
- arquivos README documentam a localização dos datasets completos

Essa estratégia preserva:
- reprodutibilidade
- governança
- evidência de entrega
- performance do repositório
- boas práticas enterprise para armazenamento analítico