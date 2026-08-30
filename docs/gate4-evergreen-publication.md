# Gate 4 — build evergreen, lineage e publicação atômica

Status: **em implementação; publicação evergreen ainda fechada por blockers operacionais explícitos**.

Este Gate sucede os contratos metodológicos já fechados nos Gates 1–3. Ele não altera universo regulatório, PLA/CMR, ILT, Conduta, assessment, leaderboards ou a decisão de manter o ranking geral bloqueado.

## 1. Problema operacional

Os workflows v2 nasceram como instrumentos de investigação e validação. Parte deles restaura o último artifact bem-sucedido da branch; outra parte exige artifact do mesmo head. Como os workflows de PR são disparados em paralelo, um downstream pode iniciar antes do upstream correspondente existir.

Consequências observadas:

- falhas por corrida mesmo quando o código é válido;
- necessidade de reruns manuais em ordem topológica;
- possibilidade de combinar artifacts de gerações diferentes quando se usa `latest successful`;
- ausência de uma identidade transversal da geração publicada;
- ausência de um pacote público único cuja completude possa ser verificada antes da troca em produção.

O Gate 4 trata esses problemas como **problemas de build e publicação**, não como motivo para recalibrar a metodologia.

## 2. Invariantes do Gate 4

```text
uma publicação = uma geração
uma geração = um build_id
um build_id = um source_head_sha + uma execução/attempt identificável
```

É proibido no caminho evergreen:

```text
restaurar "latest successful" de outra geração
misturar heads em um pacote
publicar JSON parcialmente atualizado
usar cache antigo sem declarar stale
chamar fonte indisponível de fresh
inferir completude pelo simples fato de arquivos existirem no servidor
```

## 3. Build context

`api/v2/generation.py` define o contexto transversal:

```text
build_id
source_head_sha
generated_at
workflow_run_id
workflow_run_attempt
```

O `build_id` é propagado à lineage das fontes e ao manifest de distribuição. Um source record cujo `build_id` não coincide com a geração atual é rejeitado.

## 4. Estados de fonte

A política inicial é explícita e fail-closed:

```text
fetch atual bem-sucedido + snapshot materializado → fresh
fetch atual falhou + snapshot materializado         → stale
fetch atual falhou + nenhum snapshot                → unavailable
fetch declarado como bem-sucedido sem arquivo      → erro
```

`fresh` e `stale` exigem SHA-256 do conteúdo utilizado. `unavailable` não pode carregar hash como se houvesse conteúdo usado.

O estado `stale` não significa automaticamente que o dado é aceitável para qualquer finalidade. Limites de idade e criticidade devem ser definidos por fonte na etapa de integração. A regra deste contrato é anterior: **fallback nunca pode ser invisível**.

## 5. DAG de uma única geração

`api/v2/gate4_pipeline.py` materializa o encadeamento que antes estava implícito nos workflows:

```text
fontes
→ eligibility / evidência financeira / lifecycle
→ liquidez / operação / Conduta
→ closures Financeiro e Conduta
→ cross-pillar
→ contrato semântico
→ assessment
→ ranking preflight
→ leaderboards
→ busca/perfis
→ distribution_manifest.json
```

O DAG é testado para:

- dependências inexistentes;
- ciclos;
- dois stages reivindicando o mesmo output;
- ordem Financeiro;
- ordem Conduta;
- ancestralidade completa do pacote público.

O caminho evergreen não deve usar `gh run list` para procurar outputs intermediários. Os builders executam no mesmo workspace e consomem arquivos produzidos anteriormente na própria geração.

## 6. Blockers atuais

A publicação permanece propositalmente fechada enquanto estes stages ainda não satisfazem o contrato evergreen:

```text
source_snapshot
eligibility
financial_evidence
consumer_conduct
ranking_preflight
lifecycle
```

Razões:

### `source_snapshot`

A lineage e os estados já estão modelados, mas ainda falta integrar todas as aquisições oficiais a um snapshot transversal materializado antes das derivações.

### `eligibility`, `financial_evidence` e `lifecycle`

Hoje ainda combinam, em graus diferentes, aquisição de fonte e derivação. Precisam consumir o snapshot da geração em vez de abrir conexões oficiais independentes durante o próprio builder.

### `consumer_conduct`

Ainda combina atualização Consumer.gov, resolução Receita e derivação de Conduta. Também precisa de política explícita para indisponibilidade das fontes oficiais.

### `ranking_preflight`

Existe dívida operacional de nome do Stage 2: o builder canônico gera `cross_pillar_architecture_experiment.json`, enquanto o caminho legado de Ranking materializa um alias `cross_pillar_architecture_stage_2.json`. O build único não deve depender desse renomeio entre workflows.

Enquanto qualquer blocker permanecer, o contrato retorna:

```text
publication_ready = false
```

## 7. Pacote público único

O pacote final deve conter, no mínimo:

```text
search_index.json
profile_manifest.json
insurer_explorer.json
explore_index.json
profiles/*.json
leaderboards/*.json
collections/*.json
distribution_manifest.json
```

`distribution_manifest.json` registra:

- build context;
- lineage e estado das fontes;
- todos os JSONs da distribuição;
- tamanho de cada arquivo;
- SHA-256 de cada arquivo;
- SHA-256 determinístico do pacote lógico;
- política de publicação atômica e rollback.

O manifest não inclui seu próprio hash na lista, evitando recursão.

## 8. Publicação no HostGator

Arquitetura-alvo:

```text
GitHub Actions
→ build completo em uma geração
→ validação de contracts + hashes
→ artifact único
→ HostGator baixa nova versão para diretório temporário
→ verifica distribution_manifest.json
→ troca atomicamente a versão ativa
→ conserva a versão anterior para rollback
```

O diretório público previsto continua:

```text
/ranking-seguradoras/data/v2/public/
```

O servidor não recalcula metodologia e não decide se o pacote está completo por contagem informal de arquivos.

## 9. Migração dos workflows existentes

Os workflows de investigação atuais permanecem durante a transição para preservar diagnóstico e não interromper a branch. Eles **não são o desenho final de publicação**.

A migração deve ocorrer em ordem:

1. fechar source snapshot + lineage;
2. separar aquisição de derivação nos stages mistos;
3. eliminar aliases operacionais e `latest successful` do caminho de distribuição;
4. executar o DAG inteiro em um único workspace;
5. produzir e validar o artifact único;
6. testar instalação atômica/rollback no lado HostGator;
7. somente então habilitar `schedule` semanal.

## 10. Workflow de contrato

`V2 Gate 4 Evergreen Contract` valida:

- Ruff dos módulos Gate 4;
- testes de geração/lineage;
- testes de `fresh/stale/unavailable`;
- DAG e blockers;
- materialização de `gate4_pipeline_contract.json`;
- fail-closed de publicação enquanto a migração não termina.

Esse workflow não busca artifacts de outras execuções.

## 11. Critério de fechamento do Gate 4

O Gate 4 só pode ser declarado fechado quando:

```text
publication_blockers = []
publication_ready = true
```

E uma execução completa demonstrar simultaneamente:

- uma única `source_head_sha`;
- um único `build_id`;
- source lineage completa;
- ausência de `latest successful` no caminho evergreen;
- DAG completo em ordem topológica;
- todos os contratos dos Gates 1–3 preservados;
- `ranking_eligible = 0` salvo decisão metodológica futura independente;
- pacote público completo e hash-verificado;
- instalação atômica testada com rollback;
- execução manual reproduzível;
- somente após isso, execução semanal habilitada.

Até esse fechamento, PR #1 permanece Draft e `main`/produção permanecem intocados.
