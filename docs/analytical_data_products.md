# Analytical Data Products

## Overview

This document describes the analytical CSV exports generated from the Gold
analytical layer and Parliamentary Intelligence datasets.

The exported analytical products are organized by analytical domain and stored
inside the `data/parliamentary_intelligence/` structure.

These datasets provide reproducible analytical evidence for:
- parliamentary intelligence
- CEAP analytics
- voting intelligence
- political alignment analysis
- engagement analytics
- CDC/SCD2 historical analysis
- streaming observability

---

# CEAP Analytics

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `gold_vw_despesas_ceap_analitica.csv` | `gold.vw_despesas_ceap_analitica` | Consolidated CEAP analytical dataset with parliamentary expense indicators. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_despesas_deputado_segmento.csv` | `gold.vw_despesas_deputado_segmento` | CEAP expense segmentation by deputy and expense category. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_gastos_segmentados.csv` | `gold.vw_gastos_segmentados` | Consolidated segmented parliamentary expense analysis. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_anomalias_ceap_zscore.csv` | `gold.vw_anomalias_ceap_zscore` | Statistical anomaly detection using Z-score over CEAP expenses. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_ranking_despesas_deputado_mensal.csv` | `gold.vw_ranking_despesas_deputado_mensal` | Monthly parliamentary expense ranking by deputy. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_top_10_gastos_partido_mensal.csv` | `gold.vw_top_10_gastos_partido_mensal` | Top 10 monthly expense analysis by political party. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_perfil_gasto_partido.csv` | `gold.vw_perfil_gasto_partido` | Political party expense profile analysis. | `data/parliamentary_intelligence/ceap/` |
| `gold_vw_partidos_despesas_segmento.csv` | `gold.vw_partidos_despesas_segmento` | CEAP segmentation analysis by political party. | `data/parliamentary_intelligence/ceap/` |

---

# Parliamentary Fronts Analytics

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `gold_vw_frentes_membros_analitica.csv` | `gold.vw_frentes_membros_analitica` | Parliamentary front membership analytical dataset. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_frentes_diversidade_partidaria.csv` | `gold.vw_frentes_diversidade_partidaria` | Political party diversity analysis across parliamentary fronts. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_deputados_mais_frentes.csv` | `gold.vw_deputados_mais_frentes` | Deputies participating in the highest number of parliamentary fronts. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_sobreposicao_frentes.csv` | `gold.vw_sobreposicao_frentes` | Front overlap and coalition relationship analysis. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_evolucao_frentes_legislatura.csv` | `gold.vw_evolucao_frentes_legislatura` | Historical evolution of parliamentary fronts by legislature. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_especializacao_tematica.csv` | `gold.vw_especializacao_tematica` | Thematic specialization analysis of parliamentary fronts. | `data/parliamentary_intelligence/frentes/` |
| `gold_vw_alinhamento_frente_vs_partido.csv` | `gold.vw_alinhamento_frente_vs_partido` | Political alignment comparison between fronts and parties. | `data/parliamentary_intelligence/frentes/` |

---

# Legislative Events Analytics

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `gold_vw_eventos_analitica.csv` | `gold.vw_eventos_analitica` | Consolidated legislative events analytical dataset. | `data/parliamentary_intelligence/eventos/` |
| `gold_vw_eventos_futuros.csv` | `gold.vw_eventos_futuros` | Upcoming legislative events and agenda analysis. | `data/parliamentary_intelligence/eventos/` |
| `gold_vw_densidade_eventos_semanal.csv` | `gold.vw_densidade_eventos_semanal` | Weekly legislative event density analysis. | `data/parliamentary_intelligence/eventos/` |
| `gold_vw_semanas_sem_atividade.csv` | `gold.vw_semanas_sem_atividade` | Identification of weeks without parliamentary activity. | `data/parliamentary_intelligence/eventos/` |

---

# Voting Intelligence Analytics

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `gold_vw_votacoes_analitica.csv` | `gold.vw_votacoes_analitica` | Consolidated voting session analytical dataset. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_votos_deputados_analitica.csv` | `gold.vw_votos_deputados_analitica` | Deputy-level voting behavior analytical dataset. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_orientacoes_bancada_analitica.csv` | `gold.vw_orientacoes_bancada_analitica` | Political bench orientation analysis for voting sessions. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_fidelidade_partidaria.csv` | `gold.vw_fidelidade_partidaria` | Political party loyalty and voting alignment analysis. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_partidos_fidelidade_votacao.csv` | `gold.vw_partidos_fidelidade_votacao` | Voting fidelity metrics by political party. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_partidos_votos_distribuicao.csv` | `gold.vw_partidos_votos_distribuicao` | Distribution analysis of votes by political party. | `data/parliamentary_intelligence/votacoes/` |
| `gold_vw_ausencias_votacoes_criticas.csv` | `gold.vw_ausencias_votacoes_criticas` | Analysis of parliamentary absences in critical voting sessions. | `data/parliamentary_intelligence/votacoes/` |

---

# Parliamentary Engagement Analytics

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `gold_vw_atividade_parlamentar_analitica.csv` | `gold.vw_atividade_parlamentar_analitica` | Consolidated parliamentary activity analytical dataset. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_indice_eficiencia_parlamentar.csv` | `gold.vw_indice_eficiencia_parlamentar` | Parliamentary efficiency analytical indicators. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_indice_transparencia.csv` | `gold.vw_indice_transparencia` | Parliamentary transparency analytical indicators. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_ranking_ausencias_criticas.csv` | `gold.vw_ranking_ausencias_criticas` | Ranking of parliamentary absences in critical voting sessions. | `data/parliamentary_intelligence/engajamento/` |
| `gold_vw_score_engajamento_parlamentar.csv` | `gold.vw_score_engajamento_parlamentar` | Parliamentary engagement scoring dataset. | `data/parliamentary_intelligence/engajamento/` |

---

# Political Party Intelligence

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `gold_vw_partidos_analitica.csv` | `gold.vw_partidos_analitica` | Consolidated political party analytical dataset. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_partidos_perfil.csv` | `gold.vw_partidos_perfil` | Political party profile and positioning analysis. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_dashboard_partidos.csv` | `gold.vw_dashboard_partidos` | Executive analytical dashboard dataset for political parties. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_partidos_especializacao_tematica.csv` | `gold.vw_partidos_especializacao_tematica` | Thematic specialization analysis by political party. | `data/parliamentary_intelligence/partidos/` |
| `gold_vw_analise_ineficiencia_parlamentar.csv` | `gold.vw_analise_ineficiencia_parlamentar` | Parliamentary inefficiency analytical indicators. | `data/parliamentary_intelligence/partidos/` |

---

# CDC / SCD2 Historical Analytics

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `gold_cdc_vw_proposicoes_tramitacao_historica.csv` | `gold_cdc.vw_proposicoes_tramitacao_historica` | Historical proposition movement analytical dataset using SCD Type 2. | `data/parliamentary_intelligence/cdc/` |
| `gold_cdc_vw_tempo_tramitacao_proposicoes.csv` | `gold_cdc.vw_tempo_tramitacao_proposicoes` | Proposition movement duration and lifecycle analysis. | `data/parliamentary_intelligence/cdc/` |
| `gold_cdc_vw_alertas_tramitacao_proposicoes.csv` | `gold_cdc.vw_alertas_tramitacao_proposicoes` | Analytical alerts for proposition movement anomalies and delays. | `data/parliamentary_intelligence/cdc/` |

---

# Streaming Observability

| CSV Export | Source View | Description | Location |
|---|---|---|---|
| `monitoring_vw_sla_votacoes_streaming.csv` | `monitoring.vw_sla_votacoes_streaming` | Streaming SLA monitoring and observability dataset for voting pipelines. | `data/parliamentary_intelligence/streaming/` |

---

# Large Dataset Strategy

Some analytical datasets exceed GitHub's file size limits.

For these scenarios:
- representative CSV samples are versioned in the repository
- full exports remain stored in Databricks Unity Catalog Volumes
- README files document the location of complete datasets

This strategy preserves:
- reproducibility
- governance
- delivery evidence
- repository performance
- enterprise analytical storage practices