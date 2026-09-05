# Cutover de produção do frontend R5.3

- Data: **05/09/2026**
- URL: `https://sanida.com.br/ranking-seguradoras/`
- Branch de origem: `refactor/v2-data-foundation`
- PR: **#1 Draft**
- HEAD conferido antes do cutover: `80df1c62bebd0c802922b8fa6dc7fd51edaf5d70`
- Merge em `main`: **não executado**

## Escopo consumido

A autorização foi restrita à troca da superfície v1 pelo frontend R5.3 aprovado. Não incluiu geração ou publicação automática de dados, cron, sensores evergreen, telemetria, alteração de metodologia, merge em `main` nem mudança do `head-global.php` compartilhado.

```text
FRONTEND_PRODUCTION_CUTOVER = AUTHORIZED_AND_CONSUMED
AUTOMATIC_GENERATION = NOT_AUTHORIZED
AUTOMATIC_HOSTGATOR_PUBLICATION = NOT_AUTHORIZED
MARKET_SENSORS = NOT_AUTHORIZED
HOSTGATOR_CRON_JOBS = NOT_AUTHORIZED
```

## Bytes promovidos

O `index.php` de produção foi promovido por troca atômica a partir do candidato derivado mecanicamente do mesmo R5.3 aprovado no staging.

```text
/home1/sanid210/public_html/ranking-seguradoras/index.php
081952849d2d6e3d1bb3bca0334e404db5e9c890f66cd50e4330005566cf51fc

/home1/sanid210/public_html/ranking-seguradoras/deployment/production-cutover/legacy-state-redirects.php
f803a83ed600f3e71f1bd25ec137bafc7f046a9d9eadba04f6173d76a754823d
```

Os assets continuaram sendo exatamente os bytes aprovados:

```text
ranking-seguradoras/assets/ranking-v2.js
ceba67d7de5e037027888f521a6640d7285dcfad341a1b497f0260795b13273f

ranking-seguradoras/assets/ranking-v2.css
616d654007e6d231d3bb9a6fec2f2d62cbd16e3021dde3cafd68325ad1958350
```

Cache-bust preservado: `ranking-v2.css?v=17` e `ranking-v2.js?v=16`.

## Backup do frontend anterior

Antes da troca, o `index.php` v1 foi copiado para fora do `public_html`, com modo `600`, sintaxe PHP validada e hash reconciliado:

```text
/home1/sanid210/sanida-frontend-backups/ranking-seguradoras/index.php.pre-r5-3.20260905T132240Z
9b2995a2925824f7b638daec62355c93f1b49a9169698caa10724a5bebd6ac28
```

```text
FRONTEND_ROLLBACK_AVAILABLE = YES
```

O backup não deve ser apagado até uma decisão explícita de aposentadoria da v1.

## Dados preservados

O cutover não regenerou nem reinstalou os dados. Os dois ponteiros permaneceram resolvendo para a geração já aprovada:

```text
public/current target  /home1/sanid210/sanida-v2-publication/generations/v2-gate4-full-33829195597-a1
build_id               v2-gate4-full-33829195597-a1
files_count            805
package_sha256         be7c1da75a7cbfe14de836c97c2b0ecacb0703eeb89f18ddf647bd80d2bfa502
manifest SHA256        4a1e5237257d6965447a801f8ed31850c9e74b7609dfb4888c6b5fd1c304d2fe
```

O diretório da primeira migração continua preservado:

`/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live`

Ele contém 519 JSONs e 505 perfis, conforme a prova anterior. Essa disponibilidade não deve ser descrita como prova do rollback normal por `previous`; o caso já exercitado foi o caso especial da primeira migração via `public-pre-r5-live`.

```text
DATA_ROLLBACK_AVAILABLE = YES
```

## Validações pós-cutover

```text
PRODUCTION_FRONTEND_R5_3          PASS
PRODUCTION_HTTP                   PASS
PRODUCTION_CANONICAL              PASS
PRODUCTION_ROBOTS                 PASS
PRODUCTION_DATA_GENERATION        v2-gate4-full-33829195597-a1
PRODUCTION_MANIFEST_INTEGRITY     PASS
PRODUCTION_SEARCH                 PASS
PRODUCTION_PROFILE                PASS
PRODUCTION_COMPARISON             PASS
PRODUCTION_DEEP_LINK              PASS
PRODUCTION_HISTORY                PASS
PRODUCTION_MOBILE_SMOKE           PASS
LEGACY_STATE_REDIRECTS            PASS
FRONTEND_ROLLBACK_AVAILABLE       YES
DATA_ROLLBACK_AVAILABLE           YES
FINAL_SERVER_RECONCILIATION       PASS
```

Os parâmetros legados `q`, `perfil` e `comparar` retornaram `301` para estados por fragmento na URL limpa. O carregamento retornou `data-load-state=ready`; busca e perfil da Allianz, comparação, compartilhamento/deep-link e retorno por History/Back passaram.

Os avisos observados no painel *Issues* do navegador eram recomendações não bloqueantes ligadas ao `main.js` global e a dimensões de imagens lazy-loaded. Não houve erro de console ou regressão objetiva que justificasse reabrir o R5.3.

## Invariantes preservados

```text
PR_1_STATUS = DRAFT
MAIN_MERGED = false
HEAD_GLOBAL_CHANGED = false
V1_BACKUP_PRESERVED = true
PUBLIC_PRE_R5_LIVE_PRESERVED = true
V2_PRODUCTION_AUTOMATION_ENABLED = false
V2_HOSTGATOR_DEPLOY_ENABLED = false
V2_MARKET_SENSOR_AUTOMATION_ENABLED = false
market_sensor_production_enabled = false
SSH_TRUST_HARDENING_IN_CODE = PASS
DEDICATED_HOSTGATOR_ACTIONS_KEY_LOGIN = PASS
V2_HOSTGATOR_SECRETS_CONFIGURED = true
V2_HOSTGATOR_VARIABLES_CONFIGURED = true
GITHUB_ACTIONS_SSH_RUNTIME_VALIDATION = NOT_EXECUTED
REPOSITORY_SSH_PRIVATE_KEY_EXPOSURE = false
REPOSITORY_KNOWN_HOSTS_MATERIAL = false
```

O publicador prepara chave e `known_hosts` somente no `RUNNER_TEMP`, valida chave privada, sintaxe e vínculo exato do host/porta, usa `StrictHostKeyChecking=yes`, `IdentitiesOnly=yes` e confiança limitada ao arquivo efêmero, e remove o material com `if: always()`. Uma chave dedicada foi instalada no HostGator com encaminhamentos e PTY desabilitados; seu login não interativo foi provado. Os quatro secrets SSH e as variables operacionais foram configurados no GitHub, sem versionar seus valores. Os três gates de habilitação permaneceram explicitamente `false`.

## Estado formal

```text
SECTION_19_7_STATUS = CLOSED
READY_FOR_CUTOVER = YES
PRODUCTION_CUTOVER_STATUS = COMPLETE
production_cutover_authorized = consumed
PRODUCTION_FRONTEND_R5_3 = PASS
```

O hardening da confiança SSH e o provisionamento dos secrets/variables estão concluídos, sem material sensível versionado. Como o workflow só se torna operacional a partir do branch default e os gates continuam desligados, a leitura desses secrets por um runner do GitHub ainda não foi executada. O próximo gate é operacional e separado: avaliar explicitamente o merge, executar uma Full Generation em `main` e só então autorizar — em decisão própria — uma publicação manual controlada. Este registro não autoriza essas ações nem qualquer habilitação recorrente.
