# Housekeeping pós-cutover — Fases C e D

Status: **pronto para merge após CI**.

Data: 07/09/2026 (BRT).

Este documento registra a limpeza de repositório posterior ao fechamento operacional das Fases C e D. Ele não reabre metodologia, frontend, Conduta, publicação ou contratos públicos.

## 1. Autoridade operacional atual

Para o estado atual de produção, os documentos de fechamento prevalecem sobre fotografias anteriores do projeto:

- `docs/phase-c-production-automation-closeout.md` — geração/publicação automáticas;
- `docs/phase-d-evergreen-closeout.md` — descoberta evergreen em produção.

Documentos anteriores que registram flags como `false`, produção ainda não ativada, cutover ainda não autorizado ou sensores apenas preparados devem ser lidos como **snapshots históricos da etapa em que foram escritos**, e não como o estado operacional atual.

Isso inclui o bloco de status pré-cutover ainda presente no topo do `README.md`. Até uma futura revisão editorial integral do README, esse bloco não deve prevalecer sobre os closeouts C/D nem sobre este housekeeping para descrever o runtime atual.

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

Por isso este housekeeping remove:

- `.github/workflows/v2-main-source-cache-seed.yml`;
- `tests/test_v2_main_source_cache_seed.py`.

A remoção não altera a geração semanal, os caches normais da Full Generation nem os fallbacks validados já existentes.

## 3. Evergreen — arquitetura final

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

O token local do endpoint no HostGator pode permanecer para uso administrativo local; ele não é necessário para a coleta automática do sensor via SSH.

## 4. Documentação histórica

Não se deve reescrever retrospectivamente documentos de auditoria/fechamento apenas para fazer todas as fotografias históricas mostrarem o estado atual.

Exemplos de arquivos que podem conter estados antigos de flags e ainda assim continuam válidos como registro histórico:

- `README.md` — especialmente o bloco de status pré-cutover;
- `docs/production-frontend-cutover.md`;
- `docs/section-19-7-final-consolidation.md`;
- `docs/section-19-3-19-4-evergreen-hostgator-publication.md`;
- `docs/emerging-market-discovery-audit.md`;
- `docs/emerging-market-identity-coverage.md`.

Quando a pergunta for **"qual é o estado atual?"**, usar os closeouts C/D e este housekeeping, não a fotografia histórica isolada.

## 5. Branches

Branches temporários de diagnóstico, cutover, Phase C e Phase D podem ser removidos depois que seus PRs/commits relevantes estiverem materializados em `main`.

A limpeza de branches é deliberadamente separada deste PR para que a exclusão de refs não seja confundida com mudança de conteúdo versionado.

O antigo `refactor/v2-data-foundation` também deve ser tratado como branch de migração concluída. Antes de removê-lo, basta confirmar que não há commit material não incorporado a `main`; o antigo workflow `tmp-export-v2-validated-source-cache.yml` não deve ser promovido como infraestrutura permanente.

## 6. Fora de escopo

Este housekeeping não autoriza:

- alteração de score, ranking ou metodologia;
- mudança de Conduta;
- mudança do frontend R5.3;
- mudança de publicação HostGator;
- reativação de cron HostGator;
- reativação de writers V1;
- integração BDR/SusepCon;
- ativação de GSC;
- relaxamento de TLS, `known_hosts`, deadlines ou fallback validado.

Conclusão:

```text
POST_CUTOVER_REPOSITORY_HOUSEKEEPING = DOCUMENTED
MIGRATION_CACHE_BRIDGE = RETIRED
CURRENT_OPERATIONAL_AUTHORITY = PHASE_C_CLOSEOUT + PHASE_D_CLOSEOUT
METHODOLOGY_CHANGED = false
FRONTEND_CHANGED = false
PUBLICATION_CHANGED = false
```
