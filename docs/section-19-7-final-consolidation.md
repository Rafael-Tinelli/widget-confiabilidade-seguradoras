# §19.7 — Consolidação final R5.3 após QA HostGator

Data: **04/09/2026**  
Branch: `refactor/v2-data-foundation`  
PR: **#1 Draft**  
Produção: **não autorizada**

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

## `head-global.php`: decisão final

O `head-global.php` real do HostGator é compartilhado por muitas páginas e **não foi alterado**. Esta é parte do contrato R5.3:

```text
alterar /PHP/head-global.php no HostGator       PROIBIDO neste cutover
copiar head-global.php do payload do widget     NÃO
```

No staging, a não indexação foi comprovada por `X-Robots-Tag: noindex, follow`. A meta robots global pode permanecer com o default legado; o frontend não injeta segunda meta. O antigo `deployment/production-cutover/PHP/head-global.php` deixa de integrar o payload de cutover.

## Candidato de produção

`ranking-seguradoras/deployment/production-cutover/index.php` é derivado mecanicamente do mesmo R5.3 aprovado. As diferenças intencionais são somente o estado de produção: sem `X-Robots-Tag: noindex`, `$page_robots` de produção, URL limpa `/ranking-seguradoras/` e carregamento de `legacy-state-redirects.php`.

Esse candidato **não foi instalado**.

## Rollback: blocker operacional remanescente

O diretório anterior foi preservado em:

```text
/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live
```

Existe também backup privado verificado. Como a primeira publicação não tinha `previous`, o primeiro rollback exige remover somente o symlink `public`, restaurar `public-pre-r5-live` como `public`, validar 519 JSON / 505 profiles e o hash agregado histórico, e então retornar ao symlink R5.3.

A integridade do backup foi comprovada, mas essa reversão **ainda não foi executada após a migração real**:

```text
ROLLBACK_PROVADO_NO_AMBIENTE_REAL = false
```

## Gates

```text
reopen_methodology_without_concrete_bug = false
frontend_may_recompute_methodology = false
general_score_allowed = false
general_ranking_allowed = false
production_cutover_authorized = false
market_sensor_production_enabled = false
```

Para remover o último blocker do §19.7: CI do commit consolidado verde → prova reversível de rollback no HostGator → retorno imediato ao R5.3 → smoke/hash curto.

Até isso ocorrer:

```text
HOSTGATOR_STAGING_R5_3 = PASS
FRONTEND_CONSOLIDATED_IN_REPOSITORY = YES
READY_FOR_CUTOVER = NO
reason = rollback real ainda não provado
production_cutover_authorized = false
```
