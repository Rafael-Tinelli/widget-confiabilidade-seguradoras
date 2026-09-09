# Housekeeping pós-cutover — Fases C e D

Status: **FECHADO**.

Data original: 07/09/2026 (BRT).  
Reconciliação final do remake: 09/09/2026 (BRT).

Este documento registra a limpeza de repositório posterior ao fechamento operacional das Fases C e D. Ele não reabre metodologia, frontend, Conduta, publicação ou contratos públicos.

## 1. Autoridade operacional atual

Para o estado atual de produção, os documentos de fechamento e o README corrente prevalecem sobre fotografias anteriores do projeto:

- `README.md` — contrato operacional corrente;
- `docs/phase-c-production-automation-closeout.md` — geração/publicação automáticas;
- `docs/phase-d-evergreen-closeout.md` — descoberta evergreen em produção;
- este documento — housekeeping e encerramento de migração.

Documentos anteriores que registram flags como `false`, produção ainda não ativada, cutover ainda não autorizado ou sensores apenas preparados devem ser lidos como **snapshots históricos da etapa em que foram escritos**, e não como o estado operacional atual.

O README já foi reconciliado e agora declara explicitamente a v2 como ativa em produção e `main` como branch canônico.

Estado operacional consolidado:

```text
V2_PRODUCTION_AUTOMATION_ENABLED=true
V2_HOSTGATOR_DEPLOY_ENABLED=true
V2_MARKET_SENSOR_AUTOMATION_ENABLED=true

PRODUCTION_FRONTEND_R5_3=PASS
WEEKLY_FULL_GENERATION=ACTIVE
AUTO_HOSTGATOR_PUBLICATION=ACTIVE
MARKET_SENSORS=ACTIVE
GSC=OPTIONAL_NOT_CONFIGURED
HOSTGATOR_CRON=NONE
```

Os guardrails metodológicos permanecem inalterados:

```text
general_score_allowed=false
general_ranking_allowed=false
ranking_eligible=0
assertion_effect=none
score_effect=none
complaint_transfer_effect=none
automatic_registry_mutation=forbidden
```

## 2. Bridge de cache de migração removido

O workflow `.github/workflows/v2-main-source-cache-seed.yml` existia apenas para semear o cache inicial de `main` a partir de um run confiável do antigo branch `refactor/v2-data-foundation`.

Ele exigia explicitamente como origem:

```text
head_branch = refactor/v2-data-foundation
path = .github/workflows/tmp-export-v2-validated-source-cache.yml
```

Após o cutover e as provas de Full Generation em `main`, esse bridge deixou de fazer parte do caminho operacional permanente. Mantê-lo aumentaria superfície de manutenção e conservaria uma dependência artificial do branch de migração.

Por isso o housekeeping removeu:

- `.github/workflows/v2-main-source-cache-seed.yml`;
- `tests/test_v2_main_source_cache_seed.py`.

A remoção não altera a geração semanal, os caches normais da Full Generation nem os fallbacks validados já existentes.

## 3. Triggers do branch de migração aposentados

Depois da consolidação de `main`, não há razão operacional para workflows manterem gatilhos de `push` restritos ao antigo branch `refactor/v2-data-foundation`.

Na reconciliação final foram retirados esses triggers mortos de:

- `.github/workflows/v2-gate4-full-generation-proof.yml`;
- `.github/workflows/v2-gate4-conduct-cache-bootstrap.yml`;
- `.github/workflows/v2-gate4-source-snapshot-integration.yml`.

Os workflows continuam disponíveis por `workflow_dispatch` quando essa execução explícita fizer sentido. A automação semanal de produção permanece separada e continua despachando a Full Generation canônica em `main`.

## 4. Evergreen — arquitetura final

A Fase D terminou com:

```text
SUSEP licensed delta      ACTIVE
SUSEP Sandbox delta       ACTIVE
widget unknown queries    ACTIVE
GSC                       OPTIONAL / NOT CONFIGURED
```

A telemetria de busca desconhecida usa SQLite privado fora de `public_html`. O snapshot agregado usado pelo GitHub é lido por SSH pinado e SQLite em modo read-only.

A antiga tentativa de snapshot administrativo por HTTP/Cloudflare não faz parte da arquitetura final. A regra Cloudflare dedicada `Evergreen snapshot GitHub [SKIP CUSTOM]` foi removida do ambiente de produção após a prova SSH.

Os secrets GitHub abaixo também são obsoletos para o workflow atual e podem ser removidos da configuração do repositório sem alteração de código:

```text
V2_UNKNOWN_QUERY_SNAPSHOT_URL
V2_UNKNOWN_QUERY_SNAPSHOT_TOKEN
```

A existência ou exclusão desses secrets não pode ser verificada por leitura do repositório. Se ainda existirem em **Settings → Secrets and variables → Actions**, podem ser removidos. O token local do endpoint no HostGator pode permanecer para uso administrativo local; ele não é necessário para a coleta automática do sensor via SSH.

## 5. Documentação histórica

Não se deve reescrever retrospectivamente documentos de auditoria/fechamento apenas para fazer todas as fotografias históricas mostrarem o estado atual.

Exemplos de arquivos que podem conter estados antigos de flags e ainda assim continuam válidos como registro histórico:

- `docs/production-frontend-cutover.md`;
- `docs/section-19-7-final-consolidation.md`;
- `docs/section-19-3-19-4-evergreen-hostgator-publication.md`;
- `docs/emerging-market-discovery-audit.md`;
- `docs/emerging-market-identity-coverage.md`.

Quando a pergunta for **"qual é o estado atual?"**, usar o README corrente, os closeouts C/D e este housekeeping, não a fotografia histórica isolada.

## 6. Legado V1 preservado deliberadamente

A limpeza final do remake não transforma preservação histórica em obrigação de apagar tudo o que contém `v1` ou `legacy`.

Continuam deliberadamente preservados, quando ainda existentes:

- builders e artefatos V1 necessários à reprodução histórica;
- workflows V1 sem recorrência automática, disponíveis apenas para recuperação/forense explícita;
- `widget-ui/dist/` e testes correspondentes enquanto úteis à reprodução do estado anterior;
- `verify_extraction.sh` como ferramenta manual de legado;
- diagnósticos e experimentos V2 que documentam por que decisões metodológicas foram aceitas ou rejeitadas.

Uma eventual **purga do legado** é outra decisão de manutenção e não é requisito para declarar o remake encerrado.

## 7. Branches

`main` é a única autoridade canônica de desenvolvimento e produção após o cutover.

Branches temporários de correção ou housekeeping devem ser removidos depois do merge dos seus PRs. A exclusão de refs é uma ação de repositório separada do conteúdo versionado e não altera o estado funcional da aplicação.

## 8. Correção final da auditoria TOOL-01

Em 09/09/2026, o PR #26 incorporou as últimas correções localizadas identificadas pela auditoria pós-remake:

- N01 — semântica visual de Conduta no comparador;
- N02 — recuperação coerente após troca de geração;
- N05 — apresentação de razões junto à fronteira `1,00`.

Essas correções não alteraram metodologia, fontes, elegibilidade, score, ranking, geração, sensores ou publicação. O CI pós-merge ficou verde.

## 9. Fora de escopo

Este housekeeping não autoriza:

- alteração de score, ranking ou metodologia;
- mudança de Conduta;
- mudança estrutural do frontend R5.3;
- mudança de publicação HostGator;
- reativação de cron HostGator;
- reativação recorrente de writers V1;
- integração BDR/SusepCon;
- ativação de GSC;
- relaxamento de TLS, `known_hosts`, deadlines ou fallback validado.

Conclusão:

```text
POST_CUTOVER_REPOSITORY_HOUSEKEEPING = CLOSED
MIGRATION_CACHE_BRIDGE = RETIRED
MIGRATION_BRANCH_TRIGGERS = RETIRED
CURRENT_OPERATIONAL_AUTHORITY = README + PHASE_C_CLOSEOUT + PHASE_D_CLOSEOUT
LEGACY_PURGE_REQUIRED_FOR_REMAKE_CLOSE = false
METHODOLOGY_CHANGED = false
FRONTEND_ARCHITECTURE_CHANGED = false
PUBLICATION_CHANGED = false
```
