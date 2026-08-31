# Gate 4 — build evergreen, lineage e publicação atômica

Status: **em implementação; `source_snapshot` fechado como evergreen e publicação ainda bloqueada por `conduct_source_snapshot`**.

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
→ lifecycle
→ eligibility
→ evidência financeira / liquidez / operação / Conduta
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

### Lifecycle, Eligibility e Financial Evidence separados da aquisição

Os modos legados continuam disponíveis para os workflows de investigação atuais. O Gate 4 usa somente inputs materializados da mesma geração:

```text
source_snapshot
→ classification_inventory.json
→ receita_lifecycle_records.json
→ BaseCompleta.zip
→ Lifecycle
→ Eligibility
→ Financial Evidence
```

`Lifecycle` não abre SUSEP ou Receita no modo Gate 4. Ele recebe Classification e Receita lifecycle já materializados e usa o `BaseCompleta.zip` local apenas para grupos econômicos.

`Eligibility` recebe diretamente o Lifecycle materializado, sem reconstruir Classification/Lifecycle.

`Financial Evidence` recebe diretamente Eligibility e o `BaseCompleta.zip` materializados; não refaz Lifecycle/Classification nem chama fontes regulatórias.

Nenhuma regra de classificação, elegibilidade ou evidência financeira mudou com essa separação.

### Relationships: derivação automática versus fato verificado

O contrato de relationships separa duas responsabilidades:

```text
fontes estruturadas oficiais
→ derivação automática de identidade, lifecycle e grupo econômico

marca / risk carrier / sucessão / transferência de carteira
→ somente fato source-backed materializado no registry verificado
```

Nomes semelhantes e pertencimento ao mesmo grupo jamais autorizam inferência de incorporação, sucessão ou transferência de reclamações. O próximo endurecimento evergreen é um watchdog determinístico que descobre e sinaliza automaticamente casos novos ou drift dos registries, mas não altera `verified_relationships.json`, `conduct_subject_relationships.json` ou `sandbox_brand_relationships.json` e não transforma candidato em fato.

### Ranking Stage 2 sem alias operacional

O Ranking Preflight passou a consumir diretamente o artifact canônico:

```text
cross_pillar_architecture_experiment.json
```

O alias legado `cross_pillar_architecture_stage_2.json` deixou de ser necessário no caminho Gate 4. O ranking continua metodologicamente bloqueado (`ranking_eligible = 0`); o que foi removido foi apenas o blocker operacional de nome de arquivo.

## 6. Blocker atual

A publicação permanece propositalmente fechada enquanto este stage ainda não satisfaz o contrato evergreen:

```text
conduct_source_snapshot
```

### `source_snapshot` — fechado em 31/08/2026

A aquisição regulatória/financeira foi centralizada em snapshot de uma geração, com lineage e estados verificáveis para os materiais necessários, incluindo:

```text
BaseCompleta.zip
LISTAEMPRESAS.csv
SUSEP licensed entities
SUSEP special regimes
SUSEP Sandbox
Classification snapshot
Receita lifecycle snapshot
source_lineage.json
```

O cache Receita lifecycle possui contrato explícito `v2-receita-lifecycle-1`. Um cache legado sem versão só pode ser promovido quando o período, o universo regulatório e todas as validações estruturais atuais continuam compatíveis; a promoção preserva `fetched_at`. Versões desconhecidas não são reutilizadas silenciosamente, e um cache incompatível não impede nova tentativa de aquisição oficial.

A prova de integração que autorizou o fechamento foi:

```text
workflow: V2 Gate 4 Source Snapshot Integration
evento: push
branch: refactor/v2-data-foundation
head_sha: f4c2cbbeff80a292a6e5295aefe438a664af66a0
run_id: 33358623692
conclusion: success
```

A execução validou, no mesmo run, restauração do cache da branch, construção do source snapshot, lineage e outputs materializados, salvamento do cache validado e artifact de auditoria. Por isso `source_snapshot.evergreen_ready = true` no DAG.

### `conduct_source_snapshot` — blocker remanescente

A aquisição Consumer.gov e a identidade Receita/Consumer.gov já foram separadas da derivação posterior de Conduta no DAG. O blocker remanescente é provar o mesmo nível de contrato evergreen para esse snapshot misto de fontes: aquisição/cache, chave explícita de validade, lineage e materialização reproduzível em uma única geração.

A chave mínima para o snapshot de identidade Receita/Consumer.gov inclui:

```text
Receita reference_period
target_universe_hash
unresolved_provider/query_hash
schema_version
```

`consumer_conduct` já é stage de derivação e não é blocker operacional. Enquanto `conduct_source_snapshot` permanecer aberto, o contrato retorna:

```text
publication_ready = false
publication_blockers = ["conduct_source_snapshot"]
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

## 8. Produção atual e deploy v2

A infraestrutura atualmente observada no HostGator pertence ao **widget v1 em `main`** e não é o contrato de deploy da v2.

O cron legado executa `/home1/sanid210/scripts/update-widget.sh`, que:

```text
REF=main
→ baixa api/v1/insurers.json
→ baixa widget-ui/dist/assets/widget.css
→ baixa widget-ui/dist/assets/widget.js
→ faz backup desses três arquivos
→ substitui esses três destinos
```

Esse mecanismo deve permanecer intocado durante o desenvolvimento do Gate 4 para não afetar a produção atual.

A v2 terá rotina própria. Não se assume que cron, diretórios de backup ou arquivos `widget.js/widget.css` atuais serão reaproveitados. O desenho do novo cron/deploy só será fechado depois que o artifact v2 único e o `distribution_manifest.json` estiverem estáveis.

Arquitetura-alvo da v2:

```text
GitHub Actions
→ build completo em uma geração
→ validação de contracts + hashes
→ artifact v2 único
→ rotina v2 de instalação no HostGator
→ staging em diretório temporário/versionado
→ verificação de distribution_manifest.json
→ troca atômica da geração ativa
→ conservação da geração anterior para rollback
```

A distribuição pública observada hoje em `/ranking-seguradoras/data/v2/public/` é tratada como instalação de desenvolvimento existente, não como prova de que o mecanismo v1 de atualização seja adequado para a v2.

## 9. Migração dos workflows existentes

Os workflows de investigação atuais permanecem durante a transição para preservar diagnóstico e não interromper a branch. Eles **não são o desenho final de publicação**.

A migração ocorre nesta ordem:

1. `source_snapshot` + lineage — **concluído**;
2. fechar `conduct_source_snapshot` e executar a integração real da Conduta;
3. manter o watchdog de relationships no caminho evergreen para descoberta/sinalização automática de casos novos;
4. eliminar `latest successful` do caminho de distribuição;
5. executar o DAG inteiro em um único workspace;
6. produzir e validar o artifact único;
7. desenhar e testar a rotina v2 de instalação atômica/rollback no HostGator;
8. somente então criar/habilitar o cron v2 apropriado.

O cron v1 existente não deve ser alterado como parte desses passos até o cutover deliberado.

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
- rotina v2 de instalação atômica testada com rollback;
- execução manual reproduzível;
- somente após isso, cron v2 habilitado.

Até esse fechamento, PR #1 permanece Draft e `main`/produção permanecem intocados.
