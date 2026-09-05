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

## Prova de rollback ainda obrigatória — executar uma etapa por vez

Estado legado preservado:

```text
/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live
```

A primeira reversão é especial porque a primeira instalação não tinha `previous`.
Nenhum comando de cutover de frontend pertence a esta prova.

### Etapa 1 — preflight somente leitura

**OBJETIVO**

Confirmar tipos, destinos e contagens antes de criar ou trocar qualquer ponteiro.

**COMANDO EXATO**

```bash
set -euo pipefail
SANIDA_PUBLIC_DATA=/home1/sanid210/public_html/ranking-seguradoras/data/v2/public
SANIDA_LEGACY_DATA=/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live
SANIDA_CURRENT=/home1/sanid210/sanida-v2-publication/current
test -L "$SANIDA_PUBLIC_DATA"
test -d "$SANIDA_LEGACY_DATA" && test ! -L "$SANIDA_LEGACY_DATA"
test -L "$SANIDA_CURRENT"
test "$(readlink -f "$SANIDA_PUBLIC_DATA")" = "$(readlink -f "$SANIDA_CURRENT")"
command mv --help | grep -F -- '--no-target-directory' >/dev/null
printf 'public=%s\ncurrent=%s\nlegacy=%s\njson=%s\nprofiles=%s\n' \
  "$(readlink -f "$SANIDA_PUBLIC_DATA")" \
  "$(readlink -f "$SANIDA_CURRENT")" \
  "$(readlink -f "$SANIDA_LEGACY_DATA")" \
  "$(find "$SANIDA_LEGACY_DATA" -type f -name '*.json' | wc -l)" \
  "$(find "$SANIDA_LEGACY_DATA/profiles" -maxdepth 1 -type f -name '*.json' | wc -l)"
```

**RESULTADO ESPERADO**

- `public` e `current` resolvem para a mesma geração R5;
- `legacy` resolve para `public-pre-r5-live`;
- `json=519`;
- `profiles=505`;
- nenhum texto de erro e código de saída zero.

**PARE AQUI**

Não crie symlink temporário e não altere `public`. Registre a saída antes da
próxima etapa.

### Etapas seguintes — não executar sem conferir a etapa anterior

Depois do preflight, cada ação será fornecida separadamente no mesmo formato. A
prova usará um symlink irmão temporário e `mv --no-target-directory` no mesmo
filesystem para trocar o ponteiro de forma atômica. Isso evita a janela entre
`unlink public` e `mv public-pre-r5-live public`, mantém o diretório legado no
lugar e torna o retorno ao R5 igualmente atômico.

Sequência controlada prevista:

```text
1. capturar o hash agregado dos 519 JSONs legados;
2. preparar e validar um symlink temporário para public-pre-r5-live;
3. trocar public atomicamente para o legado;
4. validar contagens, hash e HTTP do legado;
5. trocar public atomicamente de volta para current;
6. validar manifesto/build_id/hash/HTTP do R5.3;
7. executar somente o smoke curto do frontend.
```

Se qualquer guarda falhar, parar sem tentar corrigir caminhos por presunção. Não
usar `rm`, não mover o diretório legado e não deixar `public` apontando para o
legado entre sessões.

## Cutover de frontend — somente após autorização explícita

O candidato permanece separado. Quando e somente quando houver autorização, o
mapa de instalação será:

| Origem versionada | Destino HostGator |
|---|---|
| `ranking-seguradoras/deployment/production-cutover/index.php` | `/home1/sanid210/public_html/ranking-seguradoras/index.php` |
| `ranking-seguradoras/deployment/production-cutover/legacy-state-redirects.php` | `/home1/sanid210/public_html/ranking-seguradoras/deployment/production-cutover/legacy-state-redirects.php` |

O segundo destino é o caminho exato exigido pelo `require` do candidato. O
`ranking-v2.js` e o `ranking-v2.css` de produção devem continuar sendo os mesmos
bytes aprovados no staging; não há cópia diferente de assets no payload de
cutover.

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
