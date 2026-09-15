# FASE C — Automação de Produção — Fechamento

Data: 07/09/2026.  
Atualização operacional de cadência: 15/09/2026.

Status: **FECHADA — automação recorrente de geração e publicação HostGator comprovada ponta a ponta**.

## Escopo preservado

A FASE C ficou restrita à geração V2 recorrente e publicação automática no HostGator. Não houve ativação de FASE D, market sensors, GSC, telemetry ou SQLite; não houve alteração de metodologia, ranking, scoring ou frontend.

## Estado operacional final

```text
V2_PRODUCTION_AUTOMATION_ENABLED=true
V2_HOSTGATOR_DEPLOY_ENABLED=true
V2_MARKET_SENSOR_AUTOMATION_ENABLED=false
```

Cadência permanente:

```text
V2 Production Generation Schedule
janela principal:   cron 17 9 * * 1  — segunda-feira 09:17 UTC
janela de retomada: cron 17 6 * * 4  — quinta-feira 06:17 UTC
```

A segunda janela foi adicionada em 15/09/2026 depois de observar indisponibilidade intermitente de fontes oficiais: `BaseCompleta.zip` da SUSEP chegou a funcionar lentamente em uma geração e retornar HTTP 500 na geração seguinte; o WebDAV da Receita encerrou conexão sem resposta. A janela adicional usa exatamente a mesma Full Generation, o mesmo contrato `fresh/stale/unavailable` e os mesmos caches validados. Não há TTL novo, aging policy, mudança metodológica ou tratamento especial do dado: a finalidade é somente oferecer às fontes oficiais uma segunda oportunidade semanal em dia e horário diferentes.

O Consumer.gov continua sendo tentado naturalmente em cada Full Generation. Sua indisponibilidade observada por falha de resolução DNS não foi tratada como simples sobrecarga de horário.

Os horários de materialização do `schedule` no GitHub Actions são best-effort; durante a prova foram observados atrasos de horas. O requisito operacional continua sendo atualização recorrente confiável, não SLA de minuto exato.

## Prova C1

Foi comprovado um `event=schedule` real disparando a Full Generation canônica em `main`, com geração íntegra e sem publicação quando `V2_HOSTGATOR_DEPLOY_ENABLED=false`.

Durante C1 também foi identificado e corrigido o encadeamento `GITHUB_TOKEN -> workflow_dispatch -> workflow_run`: como um `workflow_dispatch` criado por `GITHUB_TOKEN` não materializa o `workflow_run` subsequente da forma inicialmente esperada, o scheduler passou a aguardar a Full Generation exata e publicar um handoff determinístico. O publisher recebe então o `workflow_run` do scheduler e revalida o run fonte, build, head SHA e artifact antes de qualquer SSH.

## Prova C2 ponta a ponta

Cadeia automática real:

```text
V2 Production Generation Schedule
run 34160240336
 event=schedule
 main
 success
    ↓
V2 Gate 4 Full Generation Proof
run 34160260594
 actor=github-actions[bot]
 main
 success
    ↓
exact handoff + exact artifact
    ↓
V2 HostGator Public Package Sync
run 34161886490
 event=workflow_run
 success
    ↓
HostGator atomic install
    ↓
public HTTP manifest match
```

Full Generation:

```text
build_id=v2-gate4-full-34160260594-a1
source_head_sha=6c4b4e654dbb5ff652949798fc1d3f062e94cfa5
stage_count=25
watchdog_blocking_drift=0
ranking_eligible=0
conduct_status=conduct_methodology_closed_for_signal_design
financial_publication_audit_status=financial_publication_chain_verified
financial_reference_period=202606
package_sha256=47d0acfce4402e2754282184d1b1e2728978cc74d95265138ad6817c572aeeb2
```

Artifact exato:

```text
name=v2-gate4-full-generation-34160260594-a1
artifact_id=10032854463
artifact_digest=sha256:28ef14fe2a96564ad3b31a5ebb937a8566b339929bbc5477f3eb436d825a3ad9
```

Fontes e segurança:

```text
SUSEP TLS verification                 PASS
BaseCompleta hard deadline 900s        PASS
BaseCompleta current fetch ~642.9s     PASS
Consumer.gov validated cache fallback  PASS
no cross-source stitching              PRESERVED
pinned SSH known_hosts                  PASS
StrictHostKeyChecking=yes               PASS
```

Instalação HostGator registrada pelo workflow:

```text
current=v2-gate4-full-34160260594-a1
previous=v2-gate4-full-34009832860-a1
package_sha256=47d0acfce4402e2754282184d1b1e2728978cc74d95265138ad6817c572aeeb2
```

O manifesto servido por HTTP foi consultado depois da publicação e retornou exatamente:

```text
build_id=v2-gate4-full-34160260594-a1
source_head_sha=6c4b4e654dbb5ff652949798fc1d3f062e94cfa5
package_sha256=47d0acfce4402e2754282184d1b1e2728978cc74d95265138ad6817c572aeeb2
```

## Critérios de fechamento

```text
C1_WEEKLY_GENERATION_ENABLED              PASS
SECOND_WEEKLY_RECOVERY_WINDOW             PASS
SCHEDULED_EVENT_PROOF                     PASS
FULL_GENERATION_FROM_MAIN                 PASS
SOURCE_TIMING                             PASS
SUSEP_TLS                                 PASS
SUSEP_HARD_DEADLINES                      PASS
VALIDATED_CACHE_FALLBACK                  PASS
METHODOLOGY_CONTRACTS                     PASS
RANKING_ELIGIBLE_ZERO                     PASS
AUTO_PUBLICATION_ENABLED                  PASS
AUTOMATIC_WORKFLOW_RUN_PUBLICATION        PASS
EXACT_SOURCE_ARTIFACT_VALIDATION          PASS
PINNED_SSH_TRUST                          PASS
ATOMIC_HOSTGATOR_INSTALL                  PASS
PUBLIC_HTTP_MANIFEST_MATCH                PASS
ROLLBACK_CAPABILITY                       AVAILABLE
LEGACY_V1_SCHEDULES                       OFF
MARKET_SENSOR_AUTOMATION                  OFF
COMPLAINT_SOURCE_WATCH                    ON
HOSTGATOR_CRON                            NONE
```

## Limites preservados para a FASE D

A FASE C não ativa nem integra:

- market identity sensors;
- GSC;
- unknown query telemetry;
- SQLite;
- alterações de frontend relacionadas a telemetry;
- BDR/SusepCon;
- mudanças metodológicas de ranking/scoring/Conduta.

Esses itens permanecem fora deste fechamento e devem ser tratados separadamente na FASE D.
