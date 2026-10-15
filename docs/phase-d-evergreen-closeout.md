# Fase D — Evergreen — fechamento operacional

Status: **FECHADA / ATIVA EM PRODUÇÃO**.

Data do fechamento operacional: 07/09/2026 (BRT).

Este documento é o registro operacional mais recente da camada de descoberta evergreen. Quando houver conflito com fotografias históricas anteriores em `README.md`, `docs/emerging-market-discovery-audit.md`, `docs/emerging-market-identity-coverage.md`, `docs/production-frontend-cutover.md`, `docs/section-19-7-final-consolidation.md` ou `docs/phase-c-production-automation-closeout.md`, este closeout prevalece apenas para o **estado operacional atual da Fase D**. Os contratos metodológicos históricos não são reabertos.

## 1. Escopo fechado

A Fase D ativa a descoberta observacional de identidades emergentes sem autorizar inferência pública automática.

Fontes operacionais:

```text
SUSEP licensed delta      ACTIVE
SUSEP Sandbox delta       ACTIVE
widget unknown queries    ACTIVE
GSC                       OPTIONAL / NOT CONFIGURED
```

Decisão explícita de fechamento:

```text
GSC_REQUIRED_FOR_EVERGREEN = false
```

O Search Console permanece compatível com o workflow, porém deliberadamente não é requisito para a operação evergreen. A ausência de GSC deve permanecer representada como `gsc_query = unavailable`, sem degradar SUSEP, Sandbox ou telemetria do widget.

## 2. Invariantes preservados

A ativação não altera os limites metodológicos do v2:

```text
assertion_effect = none
score_effect = none
complaint_transfer_effect = none
automatic_registry_mutation = forbidden
blocking = false
```

Portanto:

- detectar automaticamente não significa afirmar automaticamente;
- marca não é entidade legal por presunção;
- grupo econômico não é sucessão;
- `risk_carrier` não autoriza transferência de reclamações;
- fuzzy/similaridade não resolve identidade;
- candidato não regulatório ambíguo exige revisão source-backed;
- `general_score_allowed = false` e `general_ranking_allowed = false` permanecem inalterados;
- `ranking_eligible = 0` continua sendo contrato de segurança, salvo futura mudança metodológica formal e separada.

## 3. Telemetria do widget em produção

O frontend R5.3 registra somente buscas elegíveis com catálogo pronto e zero resultados.

Cadeia operacional:

```text
browser
→ endpoint same-origin
→ query normalizada
→ SQLite privado fora de public_html
```

Persistência mínima:

```text
normalized_query
first_seen
last_seen
last_seen_day
count
distinct_day_count
```

Não são persistidos IP, cookie, sessão, e-mail, CPF ou identificador pessoal.

Prova funcional realizada em produção:

```text
KNOWN_QUERY_NOT_RECORDED     PASS
UNKNOWN_QUERY_RECORDED       PASS
COUNT_AGGREGATION            PASS
DISTINCT_DAY_COUNT           PASS
SYNTHETIC_TEST_CLEANUP       PASS
FRONTEND_TELEMETRY_R5_3      PASS
```

O registro sintético de prova foi removido após o teste.

## 4. Snapshot administrativo — arquitetura final

A tentativa inicial de leitura HTTP pelo GitHub Actions encontrou proteção do Cloudflare Bot Fight Mode. O BFM do plano Free foi preservado por decisão operacional.

A arquitetura definitiva não reduz a proteção pública e não depende de exceção Cloudflare:

```text
visitante/widget POST
→ Cloudflare + BFM + regras normais
→ endpoint PHP
→ SQLite privado

GitHub market sensor
→ SSH HostGator autenticado
→ known_hosts pinado
→ PHP remoto somente leitura
→ SQLite SQLITE3_OPEN_READONLY
→ JSON agregado
```

Propriedades de segurança do workflow:

```text
BatchMode=yes
IdentitiesOnly=yes
StrictHostKeyChecking=yes
GlobalKnownHostsFile=/dev/null
UpdateHostKeys=no
SQLITE3_OPEN_READONLY
```

O sensor não faz `scp`, não envia arquivo ao HostGator e não modifica o SQLite. O material SSH efêmero é criado com permissões restritas no runner e removido ao final da etapa.

O JSON aceito exige:

```text
artifact = v2_widget_unknown_market_query_snapshot
privacy_contract = aggregated_query_only_no_personal_or_session_identifiers
queries = array
```

## 5. Prova manual integrada da rota final

Workflow:

```text
V2 Emerging Market Identity Sensors
run 34174115622
```

Resultado auditado nos logs:

```text
PINNED_SSH                     PASS
STRICT_HOST_KEY_CHECKING       PASS
SQLITE_READONLY                PASS
HOSTGATOR_AGGREGATE_SNAPSHOT   PASS
SNAPSHOT_CONTRACT_VALIDATION   PASS
widget_unknown_search          fresh
susep_licensed_delta           fresh
susep_sandbox_delta            fresh
gsc_query                      unavailable
```

Linha decisiva do run:

```text
HostGator aggregate snapshot collected over pinned SSH: 0 normalized queries
```

Zero consultas era o estado correto naquele instante: o registro sintético já havia sido removido e não havia buscas desconhecidas acumuladas.

## 6. Ativação de produção

Estado operacional confirmado no fechamento:

```text
V2_PRODUCTION_AUTOMATION_ENABLED=true
V2_HOSTGATOR_DEPLOY_ENABLED=true
V2_MARKET_SENSOR_AUTOMATION_ENABLED=true
```

O workflow evergreen continua gated por `V2_MARKET_SENSOR_AUTOMATION_ENABLED == 'true'` e consome uma Full Generation exata; não procura uma geração arbitrária como autoridade.

## 7. Prova automática

Após a ativação, uma Full Generation em `main` acionou automaticamente a camada evergreen por `workflow_run`.

A issue singleton operacional foi atualizada pelo proprietário correto da fila:

```text
[v2] Relationship review queue
issue #4
Source Full Generation: 34174762916
```

Estado registrado na issue após a cadeia automática:

```text
Current review candidates       66
Emerging-market candidates       0
Blocking registry drift          0

gsc_query                        unavailable
susep_licensed_delta             fresh
susep_sandbox_delta              fresh
widget_unknown_search            fresh
```

Isso prova simultaneamente que:

1. a Full Generation exata em `main` foi aceita como autoridade do ciclo;
2. o workflow de sensores assumiu a atualização automática da issue quando a flag de mercado está ativa;
3. SUSEP licenciadas, Sandbox e telemetria do widget chegaram como `fresh`;
4. GSC ausente permaneceu explicitamente `unavailable` e não bloqueou o ciclo;
5. nenhum candidate emergente foi inventado quando não havia observação suficiente;
6. o watchdog preservou `blocking registry drift = 0`.

Quando `V2_MARKET_SENSOR_AUTOMATION_ENABLED == true`, o workflow `V2 Relationship Review Queue` deixa de ser o escritor automático da issue singleton. O workflow evergreen passa a ser o proprietário porque renderiza o superset de relações + market candidates. O workflow relacional antigo permanece disponível manualmente para diagnóstico.

## 7.1. Lifecycle regulatório do Sandbox

A descoberta regulatória preserva identidade e histórico separadamente do estado
corrente. Uma entidade conhecida não é removida apenas porque deixou de aparecer na
fotografia atual do Sandbox.

Invariantes:

```text
absence_from_current_sandbox != delete_identity
sandbox_history_is_append_only = true
current_regulatory_state != regulatory_history
stale_source_must_not_override_conflicting_fresh_source
```

O ledger persistente do Sandbox é transportado no cache validado do Gate 4 e registra,
por CNPJ exato, a última fotografia confiável e eventos como entrada observada,
alterações de status/prazo/modalidades/prorrogação e saída da fonte corrente.

Quando uma entidade deixa a fotografia corrente do Sandbox e o mesmo CNPJ aparece na
fonte corrente de entidades licenciadas como seguradora, a transição é materializada
como candidata determinística `sandbox_graduated_to_ordinary_license`, preservando a
participação Sandbox no histórico. Ausência sem essa evidência não é rotulada
automaticamente como encerramento ou descontinuação.

Se a fonte Sandbox estiver `stale` e conflitar com evidência licenciada `fresh`, o
Gate 4 falha fechado: um snapshot antigo não pode mais prevalecer silenciosamente na
classificação regulatória corrente.

## 8. Threshold da busca desconhecida

Contrato atual:

```text
widget_min_count = 2
widget_min_distinct_days = 2
```

Semântica:

```text
1 ocorrência / abaixo do threshold
→ observed

>= 2 ocorrências em >= 2 dias distintos
→ review_required
```

Uma observação nunca cria automaticamente uma identidade canônica.

## 9. GSC deliberadamente opcional

O collector GSC permanece implementado para eventual uso futuro, mas não integra o conjunto mínimo necessário ao Evergreen em produção.

Razões operacionais:

- SUSEP licenciadas fornece o melhor sinal de nova entidade regulada;
- Sandbox fornece sinal direto de participante regulatório emergente;
- buscas sem resultado no widget fornecem demanda diretamente ligada ao catálogo da ferramenta;
- GSC adicionaria credenciais/IAM/WIF e acoplamento com SEO sem ser necessário para o objetivo atual.

Decisão:

```text
GSC_PRODUCTION_CONFIGURATION = NOT_REQUIRED
GSC_SENSOR_STATUS_WHEN_UNCONFIGURED = unavailable
PHASE_D_CLOSURE_BLOCKED_BY_GSC = false
```

A decisão pode ser revisitada futuramente apenas se houver evidência de ganho de cobertura suficiente para justificar a complexidade.

## 10. Limites de manutenção

O target continua sendo zero manutenção periódica de descoberta, não zero intervenção humana em qualquer evento.

Manutenção excepcional pode continuar necessária se:

- SUSEP alterar schema, HTML, API ou taxonomia;
- HostGator alterar SSH, PHP, SQLite ou paths privados;
- uma credencial SSH for rotacionada;
- surgir tipo regulatório não modelado;
- um candidate ambíguo exigir investigação source-backed;
- a telemetria pública sofrer regressão objetiva.

Esses casos são exceções observáveis, não curadoria semanal.

## 11. Limpeza operacional pós-migração

A leitura do snapshot pelo workflow não usa mais:

```text
V2_UNKNOWN_QUERY_SNAPSHOT_URL
V2_UNKNOWN_QUERY_SNAPSHOT_TOKEN
```

como secrets do GitHub Actions. O endpoint PHP pode manter seu token administrativo local no HostGator para uso administrativo, mas cópias desses valores no GitHub são obsoletas para a arquitetura final.

A regra Cloudflare dedicada `Evergreen snapshot GitHub [SKIP CUSTOM]` também é obsoleta, porque o sensor não consulta mais o snapshot por HTTP público. Removê-la recupera um slot de Custom Rules sem afetar a operação do Evergreen.

Essas duas remoções são **higiene operacional**, não dependências funcionais da cadeia que já foi provada.

## 12. Estado final

```text
PHASE_D_EVERGREEN_STATUS                CLOSED
PRODUCTION_SENSORS_CONFIGURED           true
PRODUCTION_SENSORS_ENABLED              true
SUSEP_LICENSED_SENSOR                   PASS
SUSEP_SANDBOX_SENSOR                    PASS
FRONTEND_UNKNOWN_QUERY_TELEMETRY        PASS
SQLITE_PRIVATE_STORAGE                  PASS
PINNED_SSH_SNAPSHOT                     PASS
SQLITE_READONLY_SNAPSHOT                PASS
SNAPSHOT_PRIVACY_CONTRACT               PASS
AUTOMATIC_WORKFLOW_RUN_TRIGGER          PASS
SINGLETON_REVIEW_QUEUE_OWNERSHIP        PASS
AUTOMATIC_NON_REGULATED_ASSERTION       FORBIDDEN
AUTOMATIC_REGISTRY_MUTATION             FORBIDDEN
GSC                                     OPTIONAL_NOT_CONFIGURED
ROUTINE_DISCOVERY_MAINTENANCE_TARGET    ZERO
```

Conclusão:

> A Fase D está operacionalmente fechada. O sistema detecta mudanças regulatórias e demanda desconhecida sem transformar detecção em afirmação. SUSEP licenciadas, Sandbox e telemetria do próprio widget formam o conjunto ativo de sensores. O snapshot privado é lido pelo GitHub via SSH pinado e SQLite somente leitura. GSC permanece opcional. Ambiguidades continuam exigindo revisão humana source-backed por projeto.
