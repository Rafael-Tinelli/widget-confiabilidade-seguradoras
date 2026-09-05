# §19.7 — Consolidação final R5.3, rollback real e cutover do frontend

- Data da consolidação HostGator: **04/09/2026**
- Auditoria final do branch: **05/09/2026**
- Prova real de rollback e retorno: **05/09/2026**
- Cutover de produção do frontend: **05/09/2026**
- Branch: `refactor/v2-data-foundation`
- PR: **#1 Draft**
- Produção: **frontend R5.3 ativo; automações de dados e sensores desativados**

## Estado consolidado

O staging real deixou de representar o R5 original isoladamente. Três correções objetivas foram aplicadas e aprovadas até formar o candidato final **R5.3**:

1. **R5.1 — navegação/History API**: antes de abrir um perfil, a entrada anterior é normalizada para sua origem semântica (`#comparar=<ids>`, `#lista`, `#explorar` ou `#perfil=<anterior>`). Isso elimina o retorno indevido a uma rota antiga, como o caso APS observado no QA.
2. **R5.2 — hero mobile**: em telas estreitas, a ordem DOM/visual passa a ser **título → busca → explicação/metadados**. No desktop, título/explicação permanecem na coluna esquerda e a busca na direita.
3. **R5.3 — lista mobile**: remove o `flex-basis:350px` vertical herdado do desktop no campo de filtro quando `.rk2-list-tools` passa a coluna.

## Geração pública de dados preservada

As correções são exclusivamente de frontend. A geração aprovada permanece:

```text
Full Generation       #69
run                   33829195597
source head           a2fbded4ecffd30fd6095eefd50a982e27846be7
build_id              v2-gate4-full-33829195597-a1
logical package       be7c1da75a7cbfe14de836c97c2b0ecacb0703eeb89f18ddf647bd80d2bfa502
manifested JSONs      805
physical JSONs        806 (inclui distribution_manifest.json)
searchable profiles   791
ordinary insurers     156
ranking_eligible      0
```

O frontend continua consumindo `/ranking-seguradoras/data/v2/public/` e valida o `distribution_manifest.json` e os hashes dos artefatos. Publicação parcial continua proibida.

## Bytes exatos aprovados no HostGator

```text
ranking-seguradoras/index2.php
54115ed4b91505a6490ae5afb9846375a950e6ee43765991e567e795d28051f9

ranking-seguradoras/assets/ranking-v2.js
ceba67d7de5e037027888f521a6640d7285dcfad341a1b497f0260795b13273f

ranking-seguradoras/assets/ranking-v2.css
616d654007e6d231d3bb9a6fec2f2d62cbd16e3021dde3cafd68325ad1958350
```

Cache-bust final:

```text
ranking-v2.css?v=17
ranking-v2.js?v=16
```

`ranking-seguradoras/tests/r5-3-final-frontend.py` fixa esses hashes para impedir divergência silenciosa entre repositório e staging aprovado.

## QA real consolidado

```text
Infraestrutura HostGator                         PASS
Pacote/dados públicos R5                         PASS
current/symlink/HTTP                             PASS
manifesto e arquivos centrais same-generation   PASS
busca e perfis                                   PASS
identidades complexas / edge cases               PASS
comparação 2–3 entidades                         PASS
share / #comparar / reabertura                   PASS
navegação semântica R5.1                         PASS
fail-closed ao bloquear manifesto                PASS
retry após restabelecimento                      PASS
mobile hero / busca prioritária R5.2             PASS
mobile lista / espaçamento R5.3                  PASS
cooperativa                                      N/A nesta geração
```

Casos discriminantes exercitados: Allianz, Youse, Loovi/LTI, Azos/Excelsior, entidade histórica Itaú, 88I, Aruana e APS.

Observação não bloqueante: Back/Forward e os retornos semânticos restauram entidade/rota/estado corretos, mas a posição exata de rolagem não é reconstruída de forma consistente após renderização assíncrona.

## Auditoria final de drift

O branch foi consolidado em `552f9b2749aea434ad85d18ab070c5f70fec3155` antes do registro documental da prova de rollback. Nesse SHA:

```text
CI #1486                               PASS
V2 Gate 4 Evergreen Contract #326      PASS
pytest                                 538 PASS
PR #1                                  Draft
main                                   intocada
```

A verificação curta no ambiente real, sem repetir a bateria funcional, confirmou:

```text
X-Robots-Tag staging                         noindex, follow
ranking-v2.js HTTP SHA256                    ceba67d7de5e037027888f521a6640d7285dcfad341a1b497f0260795b13273f
ranking-v2.css HTTP SHA256                   616d654007e6d231d3bb9a6fec2f2d62cbd16e3021dde3cafd68325ad1958350
manifest build_id                            v2-gate4-full-33829195597-a1
manifest files_count                         805
manifest package_sha256                      be7c1da75a7cbfe14de836c97c2b0ecacb0703eeb89f18ddf647bd80d2bfa502
DOM data-load-state                          ready
canonical                                    https://sanida.com.br/ranking-seguradoras/
```

O pacote local preservado foi novamente verificado pelo instalador: 806 JSONs físicos, 805 manifestados, 791 perfis e 156 seguradoras ordinárias. Os quatro avisos de correlação com entrada constante continuam conhecidos e não bloqueantes.

| Classe | Drift/risco objetivo | Tratamento |
|---|---|---|
| RESOLVIDO | a primeira migração real não possuía `previous`; faltava provar o retorno pelo backup `public-pre-r5-live` | rollback real exercitado e retorno ao R5 comprovado em 05/09/2026 |
| NECESSÁRIA | README/documentação ainda registravam rollback como pendente | reconciliados com a prova real |
| NECESSÁRIA | o CI não comparava integralmente o candidato de produção com os bytes aprovados nem fazia lint dos PHPs de deployment | guard de derivação exata e três `php -l` já adicionados |
| RECOMENDADA | o roteiro conceitual removia o symlink e movia o diretório legado, criando uma janela evitável | prova executada com troca atômica de symlink, sem mover o backup |
| OPCIONAL | restauração exata de scroll após renderização assíncrona | não executada; rota, entidade e estado já são restaurados e não há impacto material comprovado |
| OPCIONAL | lockfile Node, CSP adicional, monitoramento sintético e matriz ampliada de navegadores | mantidos fora do gate; não justificam mudança especulativa |
| NÃO MEXER | `head-global.php`, v1, `main`, cron, sensores, metodologia e ranking | preservados |

## `head-global.php`: decisão final

O `head-global.php` real do HostGator é compartilhado por muitas páginas e **não foi alterado**. Esta é parte do contrato R5.3:

```text
alterar /PHP/head-global.php no HostGator       PROIBIDO neste cutover
copiar head-global.php do payload do widget     NÃO
```

No staging, a não indexação foi comprovada por `X-Robots-Tag: noindex, follow`. A meta robots global pode permanecer com o default legado; o frontend não injeta segunda meta. O antigo `deployment/production-cutover/PHP/head-global.php` não integra o payload de cutover.

## Candidato de produção — PROMOVIDO

`ranking-seguradoras/deployment/production-cutover/index.php` é derivado mecanicamente do mesmo R5.3 aprovado. As diferenças intencionais são somente o estado de produção: sem `X-Robots-Tag: noindex`, `$page_robots` de produção, URL limpa `/ranking-seguradoras/` e carregamento de `legacy-state-redirects.php`.

Após o fechamento do §19.7, uma autorização explícita e separada promoveu esse candidato para `/home1/sanid210/public_html/ranking-seguradoras/index.php` por troca atômica. O arquivo servido foi reconciliado com o candidato versionado:

```text
production index SHA256  081952849d2d6e3d1bb3bca0334e404db5e9c890f66cd50e4330005566cf51fc
legacy redirects SHA256  f803a83ed600f3e71f1bd25ec137bafc7f046a9d9eadba04f6173d76a754823d
PRODUCTION_CUTOVER_STATUS COMPLETE
```

O registro completo do evento, incluindo backup e smoke pós-cutover, está em `docs/production-frontend-cutover.md`.

## Rollback real — PROVADO

A primeira migração não possuía `previous`. Por isso a prova real não exercitou o rollback normal do instalador por `previous`; exercitou corretamente o caso especial da primeira migração, usando o diretório legado preservado:

```text
/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live
```

O diretório continua preservado e **não deve ser apagado** neste fechamento.

Sequência efetivamente executada:

```text
R5
→ criação/validação de symlink temporário para public-pre-r5-live
→ troca atômica de public para o legado com mv --force --no-target-directory
→ validação do legado pelo caminho público real
→ troca atômica de public de volta para current
→ validação pós-retorno do R5
→ smoke curto do staging
```

Evidência do estado legado servido:

```text
legacy JSONs              519
legacy profiles           505
legacy aggregate SHA256   f3f4e9df6d2105798a49231188069ad01b72c40ee816f4289fa0d96c8d94185d
search_index local SHA256 0c81f7502ec1bbc898280e1bd07ee6722e39f7b580d2d188a5779e38a6d789f7
search_index HTTP SHA256  0c81f7502ec1bbc898280e1bd07ee6722e39f7b580d2d188a5779e38a6d789f7
ROLLBACK_LEGACY_VALIDATION=PASS
```

Portanto o teste não foi apenas uma troca teórica de ponteiro: o `search_index.json` legado foi efetivamente servido por HTTP através do caminho público usado pelo frontend.

Retorno ao R5:

```text
public=/home1/sanid210/sanida-v2-publication/generations/v2-gate4-full-33829195597-a1
current=/home1/sanid210/sanida-v2-publication/generations/v2-gate4-full-33829195597-a1
generation=v2-gate4-full-33829195597-a1
R5_RETURN=PASS
```

Validação pós-retorno:

```text
manifest build_id         v2-gate4-full-33829195597-a1
manifest files_count      805
manifest package_sha256   be7c1da75a7cbfe14de836c97c2b0ecacb0703eeb89f18ddf647bd80d2bfa502
manifest HTTP status      200
manifest local SHA256     4a1e5237257d6965447a801f8ed31850c9e74b7609dfb4888c6b5fd1c304d2fe
manifest HTTP SHA256      4a1e5237257d6965447a801f8ed31850c9e74b7609dfb4888c6b5fd1c304d2fe
HTTP_LOCAL_MATCH          True
R5_POST_RETURN_VALIDATION PASS
```

O smoke mínimo após o retorno também passou: carregamento normal do staging, busca por Allianz e abertura normal do perfil. A bateria completa não foi repetida porque os bytes/dados aprovados permaneceram os mesmos e não surgiu sinal de regressão.

```text
ROLLBACK_PROVADO_NO_AMBIENTE_REAL = true
```

## Gates

```text
reopen_methodology_without_concrete_bug = false
frontend_may_recompute_methodology = false
general_score_allowed = false
general_ranking_allowed = false
PRODUCTION_CUTOVER_STATUS = COMPLETE
production_cutover_authorized = consumed
V2_PRODUCTION_AUTOMATION_ENABLED = false
V2_HOSTGATOR_DEPLOY_ENABLED = false
V2_MARKET_SENSOR_AUTOMATION_ENABLED = false
market_sensor_production_enabled = false
SSH_TRUST_HARDENING_IN_CODE = PASS
DEDICATED_HOSTGATOR_ACTIONS_KEY_LOGIN = PASS
V2_HOSTGATOR_SECRETS_CONFIGURED = true
V2_HOSTGATOR_VARIABLES_CONFIGURED = true
GITHUB_ACTIONS_SSH_RUNTIME_VALIDATION = NOT_EXECUTED
```

READY não significava autorização implícita. O gate foi consumido apenas após autorização explícita separada para o frontend. O evento concluído não autorizou merge, alteração de `main`, cron, publicador automático ou sensores.

## Matriz final do gate

```text
GITHUB R5.3 CONSOLIDADO                PASS
CI                                     PASS
EVERGREEN CONTRACT                     PASS
HOSTGATOR STAGING                      PASS
GITHUB = HOSTGATOR                     PASS
DATA GENERATION INTEGRITY              PASS
SEO/CANONICAL/ROBOTS                   PASS
HISTORY / SHARE / ROUTES               PASS
MOBILE                                 PASS
FAIL-CLOSED / RETRY                    PASS
CUTOVER PAYLOAD                        PASS
ROLLBACK REAL                          PASS
R5 RETURN AFTER ROLLBACK               PASS
PRODUCTION FRONTEND CUTOVER             PASS
PRODUCTION HTTP / CANONICAL / ROBOTS    PASS
PRODUCTION SEARCH / PROFILE             PASS
PRODUCTION COMPARISON / HISTORY         PASS
PRODUCTION MOBILE SMOKE                 PASS
FRONTEND ROLLBACK AVAILABLE             PASS
DOCUMENTAÇÃO                           PASS
```

Estado formal:

```text
HOSTGATOR_STAGING_R5_3 = PASS
FRONTEND_CONSOLIDATED_IN_REPOSITORY = YES
SECTION_19_7_STATUS = CLOSED
READY_FOR_CUTOVER = YES
ROLLBACK_PROVADO_NO_AMBIENTE_REAL = true
PRODUCTION_CUTOVER_STATUS = COMPLETE
production_cutover_authorized = consumed
PRODUCTION_FRONTEND_R5_3 = PASS
V2_PRODUCTION_AUTOMATION_ENABLED = false
V2_HOSTGATOR_DEPLOY_ENABLED = false
V2_MARKET_SENSOR_AUTOMATION_ENABLED = false
market_sensor_production_enabled = false
SSH_TRUST_HARDENING_IN_CODE = PASS
DEDICATED_HOSTGATOR_ACTIONS_KEY_LOGIN = PASS
V2_HOSTGATOR_SECRETS_CONFIGURED = true
V2_HOSTGATOR_VARIABLES_CONFIGURED = true
GITHUB_ACTIONS_SSH_RUNTIME_VALIDATION = NOT_EXECUTED
```
