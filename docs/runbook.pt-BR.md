# Runbook

🇺🇸 Documento operacional — Procedimentos de Resposta a Incidentes, Recuperação e Reprocessamento

---

# Visão Geral

Este runbook documenta os procedimentos operacionais do projeto `camara-data-pipeline`.

Ele fornece orientações para investigação de falhas, recuperação de pipelines, replay de dados, validação de lineage e restauração da consistência analítica entre as camadas Lakehouse.

---

# Escopo

Este runbook cobre:

* falhas de ingestão de APIs;
* mudanças de schema;
* registros duplicados;
* volumes inesperados;
* registros rejeitados;
* problemas de lineage;
* inconsistências na camada Gold;
* falhas analíticas;
* problemas de offset streaming;
* falhas Delta Live Tables;
* degradação de SLA.

---

# Camadas Operacionais

```text
Bronze
    │
    ▼
Silver Base
    │
    ▼
Silver Curated
    │
    ▼
Gold
    │
    ▼
Analytics
```

---

# Procedimento Geral de Incidentes

```text
Detectar problema
    │
    ▼
Identificar pipeline afetado
    │
    ▼
Verificar logs de monitoramento
    │
    ▼
Validar tabelas de origem e destino
    │
    ▼
Aplicar ação de recuperação
    │
    ▼
Reprocessar camada afetada
    │
    ▼
Validar saída
```

---

# Principal Tabela de Monitoramento

```text
monitoring.pipeline_log
```

## Principais campos

| Campo | Objetivo |
|---|---|
| batch_id | Rastreabilidade da execução |
| pipeline_name | Identificação do pipeline |
| layer | Camada de processamento |
| event_name | Evento da execução |
| records_read | Volume de entrada |
| records_written | Volume persistido |
| records_discarded | Volume rejeitado/descartado |
| started_at | Timestamp de início |
| finished_at | Timestamp de fim |
| status | Resultado da execução |
| error_message | Detalhes da falha |

---

# Incidentes Comuns

## Falha de API

### Sintomas

* timeout;
* resposta vazia;
* erro HTTP;
* payload inesperado da API;
* ingestão incompleta.

### Ação

1. Verificar disponibilidade da API.
2. Revisar `monitoring.pipeline_log`.
3. Validar endpoint afetado.
4. Reexecutar notebook de ingestão.
5. Reconstruir camadas downstream se necessário.

### Recuperação recomendada

```text
Executar replay a partir da Bronze caso os registros brutos existam.
Reexecutar ingestão Bronze caso os registros não tenham sido persistidos.
```

---

## Mudança de Schema

### Sintomas

* colunas ausentes;
* novos campos na resposta da API;
* erros de parsing;
* falha de validação Silver.

### Ação

1. Comparar payload Bronze atual com payload anterior.
2. Ajustar lógica de parsing Silver Base.
3. Validar premissas de schema.
4. Reprocessar Silver Base.
5. Reprocessar Silver Curated, Gold e Analytics se necessário.

---

## Registros Duplicados

### Sintomas

* chaves de negócio duplicadas;
* métricas infladas;
* aumento inesperado de contagem;
* linhas fato duplicadas.

### Ação

1. Identificar chave duplicada.
2. Validar lógica de hash.
3. Revisar janela de deduplicação.
4. Reprocessar camada Silver/Gold afetada.
5. Validar contagens finais.

---

## Volume Inesperado

### Sintomas

* `records_read` muito acima ou abaixo do esperado;
* `records_written = 0`;
* excesso de `records_discarded`;
* tabela downstream vazia.

### Ação

1. Revisar parâmetros de origem.
2. Validar paginação da API.
3. Verificar filtros de ano/data/ID.
4. Revisar registros descartados.
5. Reprocessar camada afetada.

---

## Registros Rejeitados Acima do Esperado

### Sintomas

* alto volume de `records_discarded`;
* erros de validação;
* IDs inválidos;
* datas inválidas;
* campos obrigatórios nulos.

### Ação

1. Verificar tabela de rejeitados quando disponível.
2. Revisar motivo da rejeição.
3. Validar qualidade do payload de origem.
4. Ajustar lógica de validação apenas se regra de negócio mudou.
5. Reprocessar camada afetada.

---

# Recuperação por Camada

## Recuperação Bronze

A Bronze deve preservar ingestão bruta e capacidade de replay.

### Ações de recuperação

* reexecutar notebook de ingestão;
* validar endpoint da API;
* validar batch_id;
* validar hash do registro;
* validar arquivo de origem em ingestões baseadas em arquivo.

---

## Recuperação Silver Base

Silver Base pode ser reconstruída a partir da Bronze.

### Ações de recuperação

* corrigir parsing ou lógica de validação;
* reexecutar notebook Silver Base;
* validar records_read, records_written e records_discarded;
* validar registros rejeitados.

---

## Recuperação Silver Curated

Silver Curated pode ser reconstruída a partir da Silver Base.

### Ações de recuperação

* validar regras de fallback de negócio;
* validar lógica de padronização;
* reexecutar notebook curated;
* validar consistência final da entidade.

---

## Recuperação Gold

A Gold deve ser reconstruída a partir da Silver Curated.

### Ações de recuperação

* validar chaves dimensionais;
* validar granularidade da fato;
* validar mapeamento de surrogate keys;
* reconstruir dimensão ou fato afetada;
* reexecutar analytics se necessário.

---

## Recuperação Analytics

Analytics devem ser reconstruídos a partir dos objetos Gold.

### Ações de recuperação

* validar dependências Gold;
* validar granularidade analítica;
* reexecutar notebook analítico;
* comparar KPIs com execução anterior.

---

# Incidentes Streaming

## Inconsistência de Offset

### Sintomas

* registros streaming duplicados;
* sessões de votação ausentes;
* offset abaixo ou acima do esperado.

### Ação

1. Validar `control.votacoes_stream_offset`.
2. Verificar último ID/timestamp processado.
3. Resetar offset se necessário.
4. Executar replay do micro-batch.
5. Validar `bronze_stream.votacoes_raw`.

---

## Falha DLT

### Sintomas

* pipeline DLT falhou;
* expectations descartando registros em excesso;
* tabelas streaming Silver/Gold não atualizadas.

### Ação

1. Validar tabela streaming Bronze.
2. Revisar expectations DLT.
3. Verificar padrões de registros inválidos.
4. Reiniciar pipeline DLT.
5. Validar tabela Gold de alertas.

---

## Atraso de SLA

### Sintomas

* alta latência;
* micro-batch atrasado;
* workflow incompleto.

### Ação

1. Validar `monitoring.vw_sla_votacoes_streaming`.
2. Verificar duração da execução do workflow.
3. Validar disponibilidade da API.
4. Executar replay do intervalo atrasado se necessário.

---

# Incidentes CDC / SCD Type 2

## Versão Histórica Ausente

### Sintomas

* linha SCD2 ausente;
* `is_current` incorreto;
* `valid_from` / `valid_to` incorretos.

### Ação

1. Validar ingestão CDC de origem.
2. Validar comparação de hash do payload.
3. Reprocessar camada base CDC.
4. Reconstruir tabela SCD2.
5. Validar versão atual.

---

# Checklist de Validação

Antes de encerrar um incidente, validar:

* records_read;
* records_written;
* records_discarded;
* contagem da tabela alvo;
* chaves nulas;
* chaves duplicadas;
* campos de lineage;
* fatos e dimensões Gold;
* saídas analíticas;
* logs de monitoramento.

---

# Documentos Relacionados

| Documento | Objetivo |
|---|---|
| `streaming_architecture.md` | Arquitetura Streaming e DLT |
| `governance_and_lineage.md` | Governança e lineage |
| `replay_strategy.md` | Estratégia de replay e recuperação |
| `notebooks_catalog.md` | Responsabilidades dos notebooks |

---

# Conclusão

Este runbook fornece orientações operacionais de recuperação para o projeto `camara-data-pipeline`.

O projeto foi desenhado para suportar reprocessamento confiável, rastreabilidade, replay e recuperação controlada de incidentes dentro da arquitetura Lakehouse.