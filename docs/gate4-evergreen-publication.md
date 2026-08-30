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

### Eligibility já separado da aquisição

O modo legado de `build_eligibility_inventory.py` continua disponível para os workflows atuais. O Gate 4, porém, usa:

```text
build_lifecycle_relationship_inventory
→ entity_lifecycle_relationship_inventory.json
→ build_eligibility_inventory --lifecycle-input <arquivo>
```

Assim, Eligibility deixa de reconstruir Classification/Lifecycle e de repetir chamadas de fonte. A regra de elegibilidade não mudou; apenas a proveniência do input passa a ser explícita e reutilizável dentro da mesma geração.

### Ranking Stage 2 sem alias operacional

O Ranking Preflight passou a consumir diretamente o artifact canônico:

```text
cross_pillar_architecture_experiment.json
```

O alias legado `cross_pillar_architecture_stage_2.json` deixou de ser necessário no caminho Gate 4. O ranking continua metodologicamente bloqueado (`ranking_eligible = 0`); o que foi removido foi apenas o blocker operacional de nome de arquivo.

## 6. Blockers atuais

A publicação permanece propositalmente fechada enquanto estes stages ainda não satisfazem o contrato evergreen:

```text
source_snapshot
financial_evidence
consumer_conduct
lifecycle
```

Razões:

### `source_snapshot`

A lineage e os estados já estão modelados, mas ainda falta integrar todas as aquisições oficiais a um snapshot transversal materializado antes das derivações.

### `financial_evidence` e `lifecycle`

Hoje ainda combinam, em graus diferentes, aquisição de fonte e derivação. Precisam consumir o snapshot da geração em vez de abrir conexões oficiais independentes durante o próprio builder.

### `consumer_conduct`

Ainda combina atualização Consumer.gov, resolução Receita e derivação de Conduta. Também precisa de política explícita para indisponibilidade das fontes oficiais.

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

A migração deve ocorrer em ordem:

1. fechar source snapshot + lineage;
2. separar aquisição de derivação nos stages mistos;
3. eliminar aliases operacionais e `latest successful` do caminho de distribuição;
4. executar o DAG inteiro em um único workspace;
5. produzir e validar o artifact único;
6. desenhar e testar a rotina v2 de instalação atômica/rollback no HostGator;
7. somente então criar/habilitar o cron v2 apropriado.

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
