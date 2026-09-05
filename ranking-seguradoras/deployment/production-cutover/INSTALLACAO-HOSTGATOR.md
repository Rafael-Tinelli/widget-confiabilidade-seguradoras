# R5.3 — staging, cutover e rollback no HostGator

Este documento descreve o estado efetivamente validado em 04–05/09/2026. **Não autoriza produção.**

## Regra crítica: não alterar o head global

O arquivo compartilhado `/home1/sanid210/public_html/PHP/head-global.php` não faz parte deste cutover e não deve ser substituído. O staging aprovado usou o head global existente.

A não indexação do staging é garantida por `X-Robots-Tag: noindex, follow` emitido por `index2.php`. É aceitável que a meta robots do head compartilhado permaneça com o default global; não criar segunda meta robots.

## Frontend R5.3 aprovado

```text
ranking-seguradoras/index2.php
54115ed4b91505a6490ae5afb9846375a950e6ee43765991e567e795d28051f9

ranking-seguradoras/assets/ranking-v2.js
ceba67d7de5e037027888f521a6640d7285dcfad341a1b497f0260795b13273f

ranking-seguradoras/assets/ranking-v2.css
616d654007e6d231d3bb9a6fec2f2d62cbd16e3021dde3cafd68325ad1958350
```

Assets: `ranking-v2.css?v=17` e `ranking-v2.js?v=16`.

## Dados públicos aprovados

```text
publication root  /home1/sanid210/sanida-v2-publication
build_id          v2-gate4-full-33829195597-a1
package_sha256    be7c1da75a7cbfe14de836c97c2b0ecacb0703eeb89f18ddf647bd80d2bfa502
```

`/home1/sanid210/public_html/ranking-seguradoras/data/v2/public` deve continuar apontando para `/home1/sanid210/sanida-v2-publication/current`. Não sincronizar JSONs individualmente.

## Smoke do staging

O QA funcional completo do R5.3 já foi executado. Após a prova real de rollback, foi repetido somente o smoke mínimo necessário: carregamento normal do widget, busca por Allianz e abertura normal do perfil. A validação pós-retorno também confirmou o manifesto servido por HTTP idêntico ao arquivo local da geração restaurada.

Não repetir toda a bateria discriminante quando os hashes acima forem idênticos e não houver sinal objetivo de regressão.

## Prova real de rollback — CONCLUÍDA

Estado legado preservado:

```text
/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live
```

A primeira reversão foi especial porque a primeira instalação não tinha `previous`. Portanto, **não afirmar que o rollback normal do instalador por `previous` foi exercitado**. A prova real usou o diretório legado preservado acima e troca atômica do ponteiro `public` no mesmo filesystem.

Sequência efetivamente executada:

```text
R5
→ preflight de tipos/destinos/contagens
→ hash agregado do legado
→ symlink temporário para public-pre-r5-live
→ troca atômica de public para o legado com mv --force --no-target-directory
→ validação local + HTTP através do caminho public
→ troca atômica de public de volta para current
→ validação do R5 restaurado
→ smoke curto do staging
```

Evidência do legado efetivamente servido:

```text
legacy JSONs              519
legacy profiles           505
legacy aggregate SHA256   f3f4e9df6d2105798a49231188069ad01b72c40ee816f4289fa0d96c8d94185d
search_index local SHA256 0c81f7502ec1bbc898280e1bd07ee6722e39f7b580d2d188a5779e38a6d789f7
search_index HTTP SHA256  0c81f7502ec1bbc898280e1bd07ee6722e39f7b580d2d188a5779e38a6d789f7
ROLLBACK_LEGACY_VALIDATION=PASS
```

Retorno ao R5:

```text
public=/home1/sanid210/sanida-v2-publication/generations/v2-gate4-full-33829195597-a1
current=/home1/sanid210/sanida-v2-publication/generations/v2-gate4-full-33829195597-a1
generation=v2-gate4-full-33829195597-a1
R5_RETURN=PASS
```

Validação pós-retorno:

```text
build_id              v2-gate4-full-33829195597-a1
files_count           805
package_sha256        be7c1da75a7cbfe14de836c97c2b0ecacb0703eeb89f18ddf647bd80d2bfa502
manifest HTTP status  200
manifest local SHA256 4a1e5237257d6965447a801f8ed31850c9e74b7609dfb4888c6b5fd1c304d2fe
manifest HTTP SHA256  4a1e5237257d6965447a801f8ed31850c9e74b7609dfb4888c6b5fd1c304d2fe
HTTP_LOCAL_MATCH      True
```

O diretório `public-pre-r5-live` continua sendo backup operacional da primeira migração e **não deve ser apagado** neste fechamento.

```text
ROLLBACK_PROVADO_NO_AMBIENTE_REAL = true
```

## Cutover de frontend — somente após autorização explícita

O candidato permanece separado. Quando e somente quando houver autorização posterior, o mapa de instalação será:

| Origem versionada | Destino HostGator |
|---|---|
| `ranking-seguradoras/deployment/production-cutover/index.php` | `/home1/sanid210/public_html/ranking-seguradoras/index.php` |
| `ranking-seguradoras/deployment/production-cutover/legacy-state-redirects.php` | `/home1/sanid210/public_html/ranking-seguradoras/deployment/production-cutover/legacy-state-redirects.php` |

O segundo destino é o caminho exato exigido pelo `require` do candidato. O `ranking-v2.js` e o `ranking-v2.css` de produção devem continuar sendo os mesmos bytes aprovados no staging; não há cópia diferente de assets no payload de cutover.

**Não pertence ao cutover:** `PHP/head-global.php`.

## Não fazer

```text
NÃO alterar /PHP/head-global.php
NÃO instalar cutover antes de autorização
NÃO substituir dados JSON um a um
NÃO apontar public para diretório parcial
NÃO ativar cron/publicador/sensores por consequência do frontend
NÃO apagar v1/backup/legacy antes da prova pós-cutover e janela de rollback
```

Estado atual:

```text
staging R5.3                         PASS
QA funcional                         PASS
fail-closed/retry                    PASS
mobile                               PASS
rollback real pós-migração           PASS
R5 return after rollback             PASS
READY FOR CUTOVER                    YES
ROLLBACK_PROVADO_NO_AMBIENTE_REAL    true
production_cutover_authorized        false
market_sensor_production_enabled     false
```

**READY FOR CUTOVER ≠ CUTOVER AUTORIZADO.** Este estado significa apenas que o candidato está tecnicamente pronto para uma decisão posterior; nenhuma ação de produção é autorizada por este documento.
