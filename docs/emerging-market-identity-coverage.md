# Cobertura evergreen de identidades emergentes de mercado

Status: **implementação preparada no branch `refactor/v2-data-foundation`; produção não ativada**.

Esta camada existe para reduzir a distância entre `exists in the market` e `detected` sem enfraquecer o modelo conservador de identidade/relationships do v2.

O contrato é:

```text
sensor
→ observação
→ candidate
→ evidência
→ revisão/resolução quando necessária
→ registro canônico
→ evergreen
```

Nunca:

```text
sensor
→ inferência automática
→ identidade/relação pública
```

## 1. Problema e precedente Azos

Azos demonstrou que uma identidade comercial relevante pode não aparecer em nenhuma das fontes que alimentavam o universo canônico. Isso não era erro de matching: era ausência de cobertura de descoberta.

O caso também consolidou a separação:

```text
market identity != legal entity of the risk carrier
```

Azos pode ser publicada como `insurtech_platform`, com pessoa jurídica/CNPJ próprios, enquanto a Companhia Excelsior de Seguros permanece o `risk_carrier` documentado. Não há herança de assessment nem transferência de reclamações.

A nova camada trata uma busca por uma identidade ainda desconhecida como **observação de demanda**, e não como prova de existência ou relação.

## 2. Invariantes preservados

Todo candidate emergente carrega:

```text
assertion_effect = none
score_effect = none
complaint_transfer_effect = none
automatic_registry_mutation = forbidden
blocking = false
```

Também permanecem válidos:

- `brand != legal entity`;
- `group != succession`;
- `risk carrier != complaint transfer`;
- Sandbox não entra no benchmark ordinário;
- cooperativa de seguros não entra no benchmark ordinário por presunção;
- fuzzy pode sugerir proximidade, nunca resolver identidade;
- sensor de demanda indisponível não invalida Gate 4;
- `missing`, `malformed`, `unavailable` e zero não são intercambiáveis.

## 3. Arquitetura implementada

```text
                   ┌─ SUSEP licensed delta
                   ├─ SUSEP Sandbox delta
                   ├─ Consumer.gov / watchdog existente
MARKET SENSORS ────┤
                   ├─ widget unknown searches
                   └─ GSC unresolved queries
                            │
                            ▼
                 market_identity_observations.py
                            │
                            ▼
               market identity candidate registry
                            │
                assertion_effect = none
                            │
                            ▼
              merge_market_identity_watchdog.py
                            │
                            ▼
             relationship watchdog review artifact
                            │
                            ▼
             singleton GitHub review queue / issue
                            │
                            ▼
                source-backed investigation
                            │
                            ▼
               verified canonical registry
```

Arquivos centrais:

- `api/v2/market_identity_observations.py`;
- `api/v2/build_market_identity_candidates.py`;
- `api/v2/merge_market_identity_watchdog.py`;
- `api/v2/render_relationship_review_queue.py`;
- `.github/workflows/v2-emerging-market-identity-sensors.yml`;
- `ops/hostgator/unknown-market-query-endpoint.php`;
- `tools/fetch_gsc_market_queries.py`;
- `widget-ui/src/unknownMarketTelemetry.js`.

Os snapshots brutos dos sensores de demanda não são versionados no Git. O workflow produz artifacts compactos e retidos por prazo limitado.

## 4. Sensor regulatório — entidades licenciadas SUSEP

O sensor possui função de delta determinístico baseada em FIP/CNPJ e distingue:

```text
new_regulated_entity
regulated_name_change
regulated_status_change
```

Uma nova entidade licenciada gera candidate forte de revisão, mas **não** é automaticamente classificada como `ordinary_insurer` apenas porque apareceu.

Separadamente, o pipeline canônico já preserva uma proteção importante: a taxonomia oficial lida em `api/sources/susep_licensed.py` falha fechado se a SUSEP introduzir um código de tipo ainda não modelado. Isso evita que uma categoria nova seja silenciosamente descartada ou reinterpretada.

A baseline temporal do sensor é operacional e observacional: o workflow reaproveita o snapshot regulatório compacto do último run bem-sucedido do **próprio sensor**, enquanto a autoridade canônica da revisão continua sendo a Full Generation exata que disparou o run. Essa baseline não autoriza afirmação pública.

## 5. Sensor Sandbox SUSEP

O delta Sandbox usa CNPJ exato e produz:

```text
new_sandbox_participant
```

O participante continua semanticamente separado do universo ordinário. Não há promotion para assessment/ranking por presença no Sandbox.

O refresh oficial já existente (`update-susep-sandbox.yml`) continua independente. A nova camada não substitui essa coleta nem muda sua cadência.

## 6. Cooperativas de seguros

Em 6 de maio de 2026, a SUSEP informou a entrada em vigor da Resolução CNSP nº 492, de 4 de maio de 2026, que estabelece normas gerais para sociedades cooperativas de seguros:

https://www.gov.br/susep/pt-br/central-de-conteudos/noticias/2026/maio/publicadas-as-normas-que-regulamentam-protecao-patrimonial-mutualista-e-cooperativas-de-seguro

O v2 passa a reconhecer o subtipo:

```text
insurance_cooperative
```

Semântica pública:

```text
query_state = insurance_cooperative
public label = Sociedade cooperativa de seguros
filter_bucket = other
assessment = not_applicable
ordinary assessment = false
ordinary ranking = false
```

A identidade permanece pesquisável.

### 6.1. Ponte de classificação atual

A consulta de entidades licenciadas atualmente consumida ainda não expõe, no contrato observado, um código machine-readable dedicado a cooperativas. Por isso foi implementada uma ponte estreita baseada **somente no nome legal oficial retornado pela fonte**, exigindo simultaneamente a forma `COOPERATIVA` e a finalidade `SEGURO/SEGUROS`.

Essa ponte não é uma autoridade universal nem usa marketing/fuzzy/allowlist. Se a SUSEP introduzir um tipo oficial novo, o adapter continuará falhando fechado até que o código seja modelado explicitamente.

Assim:

```text
fonte atual tipa genericamente como insurer + nome legal inequívoco
→ subtype insurance_cooperative
→ pesquisável
→ fora do comparator ordinário
```

Mas:

```text
novo código/taxonomia SUSEP desconhecido
→ source taxonomy changed
→ fail closed
→ revisão do adapter
```

## 7. Busca sem resultado no widget

A implementação preparada usa um endpoint PHP same-origin e um SQLite pequeno no HostGator.

Frontend:

```text
query normalizada
+ zero resultados
+ debounce
+ endpoint same-origin explicitamente configurado
→ POST {"query": "valor normalizado"}
```

O hook permanece desligado enquanto `VITE_UNKNOWN_MARKET_QUERY_ENDPOINT` não existir no build.

Ele também:

- recusa endpoint de outro origin;
- usa `credentials: omit`;
- não lê cookies/localStorage/sessionStorage;
- não envia CNPJ de usuário, e-mail, CPF, IP ou identificador de sessão;
- não faz busca externa síncrona;
- falha sem afetar a experiência de consulta.

### 7.1. Persistência mínima

O endpoint grava somente:

```text
normalized_query
first_seen
last_seen
last_seen_day
count
distinct_day_count
```

Não grava:

```text
IP
cookie
user id
session id
e-mail
CPF
```

O snapshot administrativo GET é protegido por Bearer token e devolve apenas agregados.

O arquivo SQLite deve ficar fora de `public_html`.

### 7.2. Threshold

Defaults atuais, configuráveis no builder:

```text
widget_min_count = 2
widget_min_distinct_days = 2
```

Justificativa: duas ocorrências em dois dias distintos reduzem a promoção de typo isolado sem exigir qualquer identificador pessoal ou de sessão.

Estados:

```text
abaixo do threshold  → observed
atinge threshold     → review_required
```

Uma ocorrência única continua disponível no artifact observacional, mas não precisa gerar trabalho operacional imediato.

## 8. Google Search Console

O collector usa a Search Analytics API oficial:

https://developers.google.com/webmaster-tools/v1/searchanalytics/query

Contrato:

- propriedade Search Console explicitamente configurada;
- filtro `page = canonical exata da página do ranking`;
- dimensão `query`;
- `type = web`;
- `dataState = final`;
- janela padrão de 28 dias;
- paginação até o limite oficial de 25.000 linhas por request.

O Google documenta que a API pode retornar apenas as linhas superiores e não garante a totalidade das consultas; portanto GSC é sensor de demanda, não prova negativa de existência.

Defaults de promoção:

```text
gsc_min_impressions = 5
OU
gsc_min_clicks = 1
```

São configuráveis e só promovem `review_required`. Nenhuma query GSC cria identidade/relação.

### 8.1. Autenticação preparada

O workflow usa Workload Identity Federation + service account por padrão, via `google-github-actions/auth@v3`, com escopo:

```text
https://www.googleapis.com/auth/webmasters.readonly
```

Configuração necessária antes de ativar:

Secrets:

```text
GSC_WIF_PROVIDER
GSC_SERVICE_ACCOUNT
V2_UNKNOWN_QUERY_SNAPSHOT_URL
V2_UNKNOWN_QUERY_SNAPSHOT_TOKEN
```

Variables:

```text
GSC_PROPERTY
GSC_RANKING_PAGE_URL
V2_MARKET_SENSOR_AUTOMATION_ENABLED
```

A service account precisa ter acesso de leitura à propriedade correspondente no Search Console e permissão de impersonation pela Workload Identity Federation.

Nenhuma credencial é armazenada no código ou em artifact.

## 9. Fusão de sinais

A fusão só ocorre quando existe uma âncora determinística idêntica.

Exemplos:

```text
widget: "nova marca"
GSC:    "nova marca"
→ mesmo candidate_key
→ observations preservadas por source
```

Mas:

```text
"nova marca"
"nova marca seguros"
→ candidates separados
```

Similaridade pode ser apresentada futuramente como `possible_same_candidate`, mas não pode alterar o lifecycle para `resolved_existing_identity` sem uma chave/evidência determinística.

## 10. Candidate lifecycle

Estados suportados pelo contrato:

```text
observed
review_required
resolved_existing_identity
verified_new_identity
dismissed_noise
dismissed_non_market_query
```

A implementação automática desta etapa só produz `observed` e `review_required`. Os demais estados pertencem à resolução/revisão source-backed e não são inferidos pelos sensores.

O histórico necessário pode permanecer no artifact/issue de revisão; não foi criado um banco canônico adicional nem uma trilha de commits por observação.

## 11. Corretoras

Corretora detectada não é falso positivo por definição. Após verificação source-backed, `market_identity.kind = broker` pode ser publicada na camada de identidade de mercado.

Semântica obrigatória:

```text
market_role = broker
assessment inheritance = forbidden
assessment = not_applicable
relationship to insurer by distribution alone = forbidden
complaint transfer = forbidden
```

Vender produtos de uma seguradora não cria `brand_of`, `risk_carrier` ou sucessão.

A resolução deve consultar CNPJ/Receita, cadastro SUSEP de corretores, site oficial e documentação primária conforme o caso.

## 12. Insurtechs/plataformas

Não há cadastro regulatório exaustivo de “insurtech”. Por isso a descoberta é prioritariamente orientada por demanda:

```text
unknown widget query + GSC
→ candidate
→ investigação
```

A investigação pode concluir, por evidência, que a identidade é plataforma tecnológica, corretora, MGA/distribuidora, marca, seguradora regulada ou Sandbox. O marketing nunca decide essa classe.

Azos permanece caso regressivo de referência.

## 13. Relação com o relationship watchdog e a fila única

`merge_market_identity_watchdog.py` recebe o `relationship_watchdog.json` de uma Full Generation exata e o candidate registry observacional. Antes da fusão valida novamente:

```text
assertion_effect = none
score_effect = none
complaint_transfer_effect = none
automatic_registry_mutation = forbidden
blocking = false
```

O resultado continua com artifact `v2_relationship_watchdog`, mas acrescenta:

- `candidate_domain = emerging_market_identity`;
- provenance/sources;
- `market_sensor_status`;
- contagens de `market_identity_review_count` e `observed_only`.

`render_relationship_review_queue.py` mostra as fontes no Markdown e mantém compatibilidade com a issue singleton:

```text
[v2] Relationship review queue
```

Não é criada uma issue por termo/query.

## 14. Gate 4 e falhas dos sensores

Gate 4 continua independente dos sensores de demanda.

```text
GSC unavailable
widget telemetry unavailable
→ sensor_status explícito
→ Gate 4 permanece válido
```

O workflow de sensores consome **uma Full Generation exata** e não procura “latest successful Full Generation” como autoridade.

O workflow automático também permanece gated por:

```text
V2_MARKET_SENSOR_AUTOMATION_ENABLED == true
```

Esta variável não foi habilitada nesta implementação.

O workflow pode ser executado manualmente contra um run exato para prova no branch. A atualização da issue é restrita a source run em `main`, e no modo manual exige opt-in `update_issue=true`.

## 15. Cadência e gap temporal

A camada não altera o cron preparado da Full Generation (`segunda-feira 09:17 UTC`).

Target:

```text
nova entidade regulada / Sandbox / cooperativa
→ próxima coleta regulatória + ciclo de sensor/geração
→ ordem de grandeza semanal
```

Para identidade não regulatória:

```text
primeira busca relevante no widget ou GSC
→ observed
recorrência suficiente
→ review_required
```

Collection cadence e canonical generation cadence continuam conceitos separados.

## 16. Crescimento do repositório

Não são versionados:

- logs brutos de busca;
- exportações GSC periódicas;
- SQLite de produção;
- candidate por arquivo;
- snapshot por consulta;
- commits por observação.

Artifacts do workflow:

1. `v2-emerging-market-identity-<run>-a<attempt>` — candidate registry, watchdog aumentado e Markdown de revisão;
2. `v2-market-sensor-state` — somente baseline regulatória compacta necessária ao próximo delta.

Retenção atual: 30 dias.

## 17. Sensores deliberadamente fora desta etapa

Não foram adicionados como sensores principais:

- Open Insurance;
- INPI Classe 36;
- crawler amplo da web;
- crawler sistemático de sites de seguradoras;
- redes sociais;
- app stores.

A relação custo/ruído/cobertura não justifica incorporá-los antes de medir a cobertura da rede regulatória + demanda real.

## 18. Limitações conhecidas

1. O endpoint PHP está apenas preparado; nenhuma instalação no HostGator foi feita.
2. `VITE_UNKNOWN_MARKET_QUERY_ENDPOINT` não foi configurado em produção.
3. O workflow automático está preparado, mas `V2_MARKET_SENSOR_AUTOMATION_ENABLED` permanece desligado.
4. WIF/service account e acesso Search Console ainda precisam ser provisionados antes da coleta GSC real.
5. A primeira execução do sensor regulatório não possui baseline anterior; nela o delta fica `unavailable` e a baseline é criada para o run seguinte.
6. A consulta licenciada SUSEP observada ainda não oferece tipo machine-readable próprio para cooperativa; a ponte pelo nome legal oficial é deliberadamente estreita e fail-closed diante de nova taxonomia.
7. Sensores de demanda medem procura observada, não cobertura exaustiva do mercado.

Essas limitações são explícitas e não são convertidas em evidência negativa.

## 19. Critério de fechamento operacional

A arquitetura/código fecha o objetivo de cobertura evergreen quando, após autorização e configuração operacional:

```text
new regulated entity → detectable
new Sandbox participant → detectable
new cooperative → searchable and outside ordinary benchmark
unknown widget market query → observable
unknown relevant GSC query → observable
broker → publishable as broker without carrier inference
candidate → no assertion/score/complaint transfer/registry mutation
verified relation → source-backed resolution remains mandatory
```

O cutover/ativação em produção é uma decisão separada e não faz parte deste documento.
