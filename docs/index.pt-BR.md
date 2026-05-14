# Documentação do Projeto

🇺🇸 Índice central de documentação do projeto `camara-data-pipeline`.

---

# Visão Geral

Este diretório contém a documentação técnica e arquitetural do projeto `camara-data-pipeline`.

A estratégia documental segue práticas enterprise de documentação para Engenharia de Dados com separação entre:

* visão executiva do projeto;
* arquitetura operacional;
* governança;
* estratégia de replay;
* arquitetura analítica;
* catálogo de notebooks;
* decisões técnicas;
* procedimentos operacionais.

---

# Estrutura da Documentação

| Documento | Descrição |
|---|---|
| `README.pt-BR.md` | Visão executiva em português |
| `streaming_architecture.pt-BR.md` | Arquitetura de streaming, CDC, DLT e SLA |
| `governance_and_lineage.pt-BR.md` | Governança, lineage e observabilidade |
| `replay_strategy.pt-BR.md` | Estratégia de replay, recuperação e reprocessamento |
| `parliamentary_intelligence.pt-BR.md` | Camada analítica e inteligência parlamentar |
| `notebooks_catalog.pt-BR.md` | Catálogo de notebooks e responsabilidades |
| `architecture_decisions.pt-BR.md` | Decisões arquiteturais e modelagem |
| `challenge_matrix.pt-BR.md` | Matriz de aderência ao desafio |
| `runbook.pt-BR.md` | Procedimentos operacionais e incidentes |

---

# Principais Tópicos Arquiteturais

## Arquitetura Lakehouse

O projeto implementa uma arquitetura Medallion Lakehouse utilizando:

* Bronze;
* Silver Base;
* Silver Curated;
* Gold;
* Analytics.

---

## Streaming e CDC

A plataforma também implementa capacidades avançadas de processamento incluindo:

* ingestão streaming micro-batch;
* Delta Live Tables (DLT);
* CDC / SCD Type 2;
* monitoramento SLA;
* orquestração de workflows;
* estratégias de replay e recuperação.

---

## Governança e Replay

A estratégia de governança inclui:

* preservação de lineage;
* rastreamento de batches;
* replayabilidade;
* processamento determinístico;
* observabilidade operacional;
* logging estruturado.

---

## Inteligência Parlamentar

A camada analítica inclui:

* indicadores de transparência;
* analytics de eficiência parlamentar;
* analytics de votação;
* analytics de despesas CEAP;
* detecção de anomalias;
* score de engajamento;
* inteligência partidária.

---

# Estrutura do Repositório

```text
camara-data-pipeline/
│
├── README.pt-BR.md
│
├── docs/
│   ├── index.pt-BR.md
│   ├── streaming_architecture.pt-BR.md
│   ├── governance_and_lineage.pt-BR.md
│   ├── replay_strategy.pt-BR.md
│   ├── parliamentary_intelligence.pt-BR.md
│   ├── notebooks_catalog.pt-BR.md
│   ├── architecture_decisions.pt-BR.md
│   ├── challenge_matrix.pt-BR.md
│   └── runbook.pt-BR.md
│
├── assets/
│   └── images/
│
└── notebooks/
```

---

# Princípios da Documentação

A estratégia documental prioriza:

* clareza técnica;
* reprodutibilidade;
* transparência operacional;
* explicabilidade arquitetural;
* visibilidade de governança;
* rastreabilidade de replay;
* consistência analítica.

---

# Público-Alvo

A documentação foi projetada para:

* Engenheiros de Dados;
* Analytics Engineers;
* recrutadores técnicos;
* revisores de arquitetura;
* avaliação de portfólio;
* discussões técnicas em entrevistas;
* estudo educacional.

---

# Conclusão

A arquitetura documental implementada no `camara-data-pipeline` foi desenhada para separar apresentação executiva do projeto da documentação técnica aprofundada, seguindo padrões modernos enterprise para Engenharia de Dados e plataformas Lakehouse.