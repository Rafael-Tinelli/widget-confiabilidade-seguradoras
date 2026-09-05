# R5.3 — staging, cutover e rollback no HostGator

Este documento descreve o estado efetivamente validado em 04/09/2026. **Não autoriza produção.**

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

Confirmar somente: HTTP 200; `X-Robots-Tag: noindex, follow`; canonical de produção; `data-load-state=ready`; busca abre perfil; comparação simples funciona; em 375×667 a busca vem antes da explicação; lista mobile não tem o gap do `flex-basis`.

Não repetir toda a bateria discriminante quando os hashes acima forem idênticos.

## Prova de rollback ainda obrigatória

Estado legado preservado:

```text
/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live
```

A primeira reversão é especial porque a primeira instalação não tinha `previous`. Em janela controlada:

```text
1. confirmar public -> current R5;
2. remover somente o symlink public;
3. renomear public-pre-r5-live -> public;
4. validar 519 JSON e 505 profiles;
5. validar hash agregado histórico;
6. reverter: public -> public-pre-r5-live;
7. recriar public como symlink para /home1/sanid210/sanida-v2-publication/current;
8. validar manifesto/build_id/hash/HTTP;
9. smoke curto do frontend.
```

Não executar essa prova sem acompanhamento deliberado.

## Cutover de frontend — somente após autorização explícita

O candidato é `ranking-seguradoras/deployment/production-cutover/index.php`, junto de `legacy-state-redirects.php`. O `ranking-v2.js` e o `ranking-v2.css` de produção devem ser os mesmos bytes aprovados no staging.

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
staging R5.3                    PASS
QA funcional                    PASS
fail-closed/retry               PASS
mobile                          PASS
rollback real pós-migração      PENDENTE
READY FOR CUTOVER               NO
production_cutover_authorized   false
```
