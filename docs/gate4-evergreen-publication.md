# Gate 4 — build evergreen, lineage e publicação atômica

Status: **fechado para o contrato de build/publicação da v2; cutover de produção permanece fora deste Gate**.

O Gate 4 resolve a fragilidade operacional dos workflows experimentais sem alterar universo regulatório, metodologia financeira, Conduta, assessment, leaderboards ou a decisão de manter o ranking geral bloqueado.

## 1. Contrato final

```text
uma publicação = uma geração
uma geração = um build_id
um build_id = um source_head_sha + run/attempt identificável
```

No caminho evergreen é proibido:

```text
restaurar latest successful de outra geração
misturar heads em um pacote
publicar JSON parcialmente atualizado
usar cache antigo sem declarar stale
chamar fonte indisponível de fresh
inferir completude apenas pela existência de arquivos
```

O contrato final materializado em `api/v2/gate4_pipeline.py` exige:

```text
publication_ready = true
publication_blockers = []
single_generation_workspace_required = true
cross_run_latest_successful_restore_forbidden = true
```

## 2. Build context e lineage

`api/v2/generation.py` define o contexto transversal:

```text
build_id
source_head_sha
generated_at
workflow_run_id
workflow_run_attempt
```

A lineage de fontes registra, por material utilizado:

```text
source_id
state = fresh | stale | unavailable
sha256 quando há conteúdo materializado
build_id
proveniência e timestamps aplicáveis
```

Um source record de outra geração é rejeitado pelo caminho canônico.

## 3. DAG canônico de uma geração

A ordem topológica validada na prova final contém 25 stages:

```text
source_snapshot
→ lifecycle
→ eligibility
→ financial_evidence
→ liquidity
→ operating
→ conduct_source_snapshot
→ financial_closure
→ relationship_watchdog
→ sandbox_brand_conduct
→ consumer_conduct
→ conduct_coverage
→ conduct_calibration
→ conduct_credibility
→ conduct_portfolio
→ conduct_closure
→ cross_stage1
→ cross_coverage
→ cross_stage2
→ semantic_contract
→ assessment_eligibility
→ ranking_preflight
→ leaderboards
→ public_profiles
→ distribution_manifest
```

Os builders do caminho canônico consomem outputs do mesmo workspace. A recomposição por `gh run list` permanece apenas em workflows diagnósticos legados/manualizados e não participa da publicação.

## 4. Fontes e watchdog

A geração canônica centraliza os snapshots regulatório/financeiro e de Conduta, com cache explícito, lineage e hash dos materiais utilizados.

O `relationship_watchdog` é obrigatório antes da derivação de Conduta. Ele separa observações verificadas de candidatos que exigem evidência adicional e mantém:

```text
assertion_effect = none
score_effect = none
complaint_transfer_effect = none
automatic_registry_mutation = forbidden
```

Drift em relação afirmada como verificada é fail-closed. Na prova final, `blocking_registry_drift_count = 0`.

## 5. Pacote público único

A distribuição canônica contém, no mínimo:

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

`distribution_manifest.json` registra build context, source lineage, lista de arquivos, tamanho, SHA-256 individual e SHA-256 determinístico do pacote lógico.

O manifest não inclui seu próprio hash na lista, evitando recursão.

## 6. Prova canônica de fechamento

A prova integrada que fecha o Gate 4 foi executada em 01/09/2026:

```text
workflow: V2 Gate 4 Full Generation Proof
run_id: 33471617517
run_attempt: 1
head_sha: 4f0b8805c999d40fa820c5a030b49d1b687c1db9
build_id: v2-gate4-full-33471617517-a1
conclusion: success
stage_count: 25
publication_ready: true
publication_blockers: []
regulatory_source_count: 6
conduct_source_count: 2
watchdog_blocking_drift: 0
ranking_eligible: 0
financial_publication_audit_status: financial_publication_chain_verified
financial_reference_period: 202606
package_sha256: c5a68480556a325a041553d56a1a040e1ab205d646243aa98c7b16cdea2f2aad
```

Artifact:

```text
name: v2-gate4-full-generation-33471617517-a1
artifact_id: 9787285465
artifact_digest: sha256:a8419e5285386b2ab26fd33bc52cbe71c5152507e4e9286dbfb8b9f9656c7ad1
```

A mesma execução passou, em sequência:

- preflight de código e contratos;
- aquisição/materialização dos snapshots;
- DAG completo em um único workspace;
- validação de `build_id`, `source_head_sha`, estados e hashes das fontes;
- auditoria financeira ponta a ponta;
- preservação das proibições de scoring/ranking;
- `ranking_eligible = 0` e gate metodológico de ranking fechado;
- verificação do pacote público;
- instalação em diretório versionado de teste;
- troca de geração;
- rollback para a geração anterior;
- upload do artifact final.

## 7. Consolidação dos workflows de PR

Após a prova canônica, os workflows V2 componentes foram reclassificados como diagnósticos manuais (`workflow_dispatch`). Eles continuam disponíveis para investigação específica, mas não compõem artifacts automaticamente nem são autoridade de publicação.

No HEAD de consolidação operacional:

```text
head_sha: 163e8af70bde993201f28e502f1cdf422b615ab7
```

os únicos workflows automáticos de PR são:

```text
CI
V2 Gate 4 Evergreen Contract
```

Validação desse HEAD:

```text
CI                              run 33494102926  success
V2 Gate 4 Evergreen Contract    run 33494103382  success
```

A Full Generation permanece a única prova integrada de dados/publicação e é disparada no branch quando mudam as famílias estáveis do caminho canônico (`api/v2/**`, `api/sources/**`, referências e dependências cobertas pelo workflow).

Isso elimina o antigo mesh de dezenas de workflows paralelos que podiam restaurar artifacts de execuções diferentes.

## 8. Papel dos workflows

### Automáticos

```text
CI
→ lint + suíte de regressão

V2 Gate 4 Evergreen Contract
→ valida contrato estrutural do DAG e publication readiness

V2 Gate 4 Full Generation Proof
→ prova integrada, same-generation, para alterações relevantes do caminho canônico
```

### Manuais

Os demais workflows `V2 ...` de Foundation, Classification, Lifecycle, Eligibility, Financeiro, Conduta, cross-pillar, assessment, leaderboards, perfis e experimentos permanecem como ferramentas diagnósticas. Seus artifacts não podem substituir a Full Generation como evidência de publicação.

## 9. Produção e cutover

O fechamento deste Gate **não altera `main`, o widget v1 ou o cron de produção**.

O mecanismo v1 observado continua separado da v2. O Gate 4 provou a semântica e o mecanismo de instalação/rollback em ambiente de teste; habilitar rotina/cron v2 no HostGator é uma decisão de cutover posterior e deliberada.

Portanto:

```text
Gate 4 build/publication contract = fechado
produção v1 = intocada
cron v2 em produção = não habilitado por este fechamento
PR #1 = Draft
ranking geral = bloqueado
```

## 10. Critério preservado daqui em diante

Qualquer alteração futura que afete o caminho canônico só pode ser aceita como nova referência após uma Full Generation verde no HEAD correspondente.

Um workflow componente verde, isoladamente, não substitui essa prova.
