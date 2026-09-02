# Auditoria da competência de descoberta evergreen de identidades de mercado

Status: **fechada em arquitetura e código no branch `refactor/v2-data-foundation`; ativação operacional/produção não autorizada**.

Esta auditoria consolida uma competência distinta do evergreen de dados/publicação: a capacidade de o widget perceber que **uma empresa, marca ou identidade de mercado nova passou a existir ou passou a ser procurada**, sem depender de uma revisão periódica manual do catálogo.

Documento de arquitetura relacionado:

`docs/emerging-market-identity-coverage.md`

## 1. Pergunta auditada

A pergunta não é apenas:

> os dados das empresas já conhecidas se atualizam sozinhos?

Ela passa a incluir:

> o sistema consegue perceber que surgiu uma empresa/identidade nova que ainda não conhece?

Essas duas competências são diferentes.

O target correto é:

```text
nova identidade relevante
→ observação automática por fonte regulatória ou demanda real
→ candidate não autoritativo
→ revisão source-backed quando necessária
→ registry canônico
→ evergreen daí em diante
```

Nunca:

```text
sensor
→ inferência automática
→ identidade/relação pública
```

## 2. Definição correta de "zero manutenção"

`zero manutenção` não significa `zero intervenção humana em qualquer hipótese`.

A arquitetura diferencia:

### 2.1. Zero manutenção rotineira de descoberta

Após configuração/ativação operacional, não deve existir uma tarefa recorrente do tipo:

```text
alguém lembrar de procurar novas seguradoras/marcas
alguém exportar GSC manualmente
alguém comparar listas SUSEP à mão
alguém revisar semanalmente o mercado
```

Os sensores devem executar automaticamente e tornar exceções visíveis.

Esse objetivo está implementado em arquitetura/código.

### 2.2. Zero intervenção humana para resolução de identidade

Esse **não** é um objetivo do v2.

Para fatos ambíguos como:

- insurtech vs corretora;
- plataforma vs marca;
- MGA/distribuidora;
- `risk_carrier`;
- sucessão;
- transferência de carteira;
- relação comercial que não equivale a identidade;

uma revisão source-backed continua deliberadamente necessária.

Isso não é manutenção rotineira: é tratamento de exceção material.

## 3. Resultado por classe de entidade

| Classe | Descoberta | Entrada pública | Intervenção humana esperada |
|---|---|---|---|
| nova seguradora licenciada | automática por SUSEP licenciadas | automática quando a taxonomia conhecida permite classificação | nenhuma para existir/pesquisar; apenas exceções regulatórias |
| novo participante Sandbox | automática por fonte Sandbox | automática, separado do benchmark ordinário | somente ambiguidade/fonte alterada |
| nova cooperativa de seguros | automática sob taxonomia atual + nome legal oficial inequívoco | automática como `insurance_cooperative`, fora do ordinary benchmark | necessária se SUSEP mudar a taxonomia ou o contrato não for inequívoco |
| nova insurtech/plataforma | automática quando houver demanda observável no widget/GSC | não automática | revisão source-backed para definir papel e relações |
| nova marca | automática quando houver demanda observável | não automática | revisão source-backed |
| corretora | automática quando houver demanda observável | não automática | revisão source-backed para publicar corretamente como broker |
| novo tipo regulatório SUSEP desconhecido | detecção por mudança de taxonomia | fail-closed | necessária para modelar a nova categoria |

## 4. Nova seguradora: competência mais forte

A fonte `api/sources/susep_licensed.py` consome o cadastro oficial de entidades licenciadas e valida a própria taxonomia do formulário.

`apply_licensed_classification()` não depende de a empresa já existir no SES: FIPs presentes na fonte licenciada e ausentes do inventário-base são materializados por `_entity_from_licensed()`.

Logo:

```text
nova FIP licenciada
→ materialização da identidade regulatória
→ active_licensed
→ perfil/searchability conforme contrato público
```

A ausência inicial de evidência financeira/Conduta não deve esconder a empresa; apenas limita seu assessment.

Isso reduz o gap técnico de uma nova seguradora ao ciclo de coleta/geração depois que a SUSEP a publica na fonte consumida.

## 5. Sandbox

Participantes Sandbox são resolvidos por CNPJ exato. Quando ainda não existem no inventário regulatório principal, a arquitetura possui materialização própria de identidade Sandbox.

Sandbox permanece:

```text
pesquisável
≠ ordinary insurer
≠ ordinary benchmark
≠ assessment inheritance
```

O sensor temporal adicional detecta novos CNPJs entre snapshots.

## 6. Cooperativas

O subtipo `insurance_cooperative` foi criado explicitamente.

Sob o contrato observado hoje, a SUSEP ainda não expõe um código machine-readable próprio para cooperativas na fonte licenciada consumida. A ponte atual só classifica quando o **nome legal oficial** combina inequivocamente forma cooperativa e finalidade de seguro.

Se a SUSEP introduzir um novo código/tipo:

```text
unknown source taxonomy
→ fetch fail-closed
→ revisão explícita
```

Portanto a arquitetura prefere uma interrupção visível a classificar uma categoria nova silenciosamente como seguradora ordinária.

## 7. Insurtechs, plataformas, marcas e corretoras

Não há uma fonte regulatória exaustiva capaz de listar todas as identidades comerciais relevantes do mercado.

Por isso a descoberta não regulatória é orientada por demanda:

```text
busca sem resultado no widget
+
query GSC da página
→ observation
→ threshold
→ review_required
```

Azos é o caso regressivo de referência: antes de existir no registry, uma busca recorrente por `Azos` deve gerar candidate; depois de materializada canonicamente, a mesma query deixa de ser desconhecida.

### Limite inevitável

Uma insurtech que:

- não aparece em fonte regulatória observada;
- nunca é procurada no widget;
- não produz query observável no GSC;

pode permanecer desconhecida.

Isso é uma limitação consciente. O sistema não pretende varrer toda a internet; pretende combinar **reconhecimento regulatório + relevância demonstrada por demanda real**.

## 8. GSC

O collector usa a Search Analytics API com:

- página canônica explicitamente configurada;
- dimensão `query`;
- dados finais;
- janela padrão de 28 dias;
- autenticação WIF/service account;
- escopo somente leitura.

GSC é complementar, não exaustivo. Uma marca totalmente ausente do conteúdo da página pode não gerar impressão para essa página; portanto GSC não substitui a telemetria de busca interna.

## 9. Busca sem resultado

Depois de instalado/configurado, o frontend envia somente a query normalizada quando:

```text
ready
+ resultCount == 0
+ query elegível
+ endpoint same-origin configurado
```

O endpoint agrega contagens/dias em SQLite sem IP, cookie ou sessão.

Threshold default:

```text
count >= 2
AND
distinct_day_count >= 2
→ review_required
```

Uma ocorrência única permanece observacional.

## 10. Proteções contra falsos positivos

Todo candidate preserva:

```text
assertion_effect = none
score_effect = none
complaint_transfer_effect = none
automatic_registry_mutation = forbidden
blocking = false
```

A fusão automática só ocorre por âncora determinística idêntica.

Fuzzy/similaridade nunca promove uma identidade.

Assim, o sistema pode aceitar sensores relativamente amplos sem permitir que ruído vire afirmação pública.

## 11. Segurança de entradas observacionais

O endpoint HostGator usa statements preparados; texto de consulta nunca é concatenado em SQL executável.

A normalização elimina markup/pontuação antes da persistência da busca interna.

Como defesa adicional, a review queue também trata valores observacionais como texto não confiável e neutraliza HTML/Markdown antes de renderizá-los em GitHub Markdown. Isso cobre inclusive queries GSC, que podem chegar do lado externo sem passar pelo normalizador do frontend.

O objetivo não é presumir que usuários normais pesquisarão e-mail/CPF ou payloads de ataque. É garantir que um endpoint público de telemetria continue inerte mesmo diante de tráfego não normal.

## 12. Automação operacional

Workflow preparado:

`.github/workflows/v2-emerging-market-identity-sensors.yml`

Fluxo:

```text
Full Generation exata em main conclui success
+
V2_MARKET_SENSOR_AUTOMATION_ENABLED == true
→ restaura baseline observacional do próprio sensor
→ coleta SUSEP licenciadas/Sandbox
→ coleta GSC se configurado
→ coleta snapshot agregado do HostGator se configurado
→ constrói candidate registry
→ funde no relationship watchdog
→ atualiza a issue singleton de revisão
→ publica artifacts compactos
→ salva próxima baseline
```

Nenhum `latest successful Full Generation` é usado como autoridade de publicação/revisão.

## 13. Manutenção residual real

Depois do setup de produção, a expectativa é **zero manutenção periódica**, mas não ausência absoluta de eventos de manutenção.

Manutenção/evento pode ser necessária se:

1. SUSEP mudar HTML/API/schema/taxonomia;
2. Google alterar autenticação/API Search Console;
3. a URL canônica da página mudar;
4. HostGator mudar ambiente/paths;
5. surgir uma categoria regulatória nova;
6. um candidate não regulado exigir resolução humana;
7. credenciais/secrets forem rotacionados.

Esses são eventos excepcionais e observáveis, não curadoria semanal do mercado.

## 14. Gap temporal esperado

Com automações habilitadas no desenho atual:

```text
seguradora/Sandbox/cooperativa sob contrato conhecido
→ fonte oficial publica
→ próximo ciclo semanal
```

Para mercado não regulatório:

```text
primeira demanda no widget/GSC
→ observed
recorrência/threshold
→ próximo ciclo de sensor
→ review_required
```

O sistema não promete detectar uma empresa não regulada antes que exista algum sinal observável de demanda.

## 15. Prova integrada disponível

A Full Generation posterior à implementação da camada regulatória/cooperativa terminou com sucesso:

```text
workflow   V2 Gate 4 Full Generation Proof #66
run        33666689092
head       1a14bab84c85ba88919f5e5b2e6e8221e6cbd212
build_id   v2-gate4-full-33666689092-a1
conclusion success
stage_count 25
watchdog_blocking_drift 0
ranking_eligible 0
```

Artifact:

```text
id      9861836318
name    v2-gate4-full-generation-33666689092-a1
digest  sha256:f45ce9b8b1cb8bbd7ec09734bad77189e2ded4b1aa0ce8aeee43f717de84bff8
```

Snapshot público desse run:

```text
lifecycle_entities                         777
brands                                      14
profiles                                   791
search_entries                             791
ordinary_current_insurer_profiles          156
ordinary_profiles_with_assessment_payload  156
sandbox_entity_profiles                     12
special_purpose_insurer_profiles             3
insurance_cooperative_profiles               0
ranking_eligible                              0
```

`insurance_cooperative_profiles = 0` é fotografia da fonte nessa geração, não ausência de suporte à categoria.

## 16. Estado de fechamento

```text
DISCOVERY_ARCHITECTURE_STATUS = CLOSED
DISCOVERY_CODE_STATUS = CLOSED
ROUTINE_DISCOVERY_MAINTENANCE_TARGET = ZERO_AFTER_SETUP
AUTOMATIC_NON_REGULATED_IDENTITY_ASSERTION = FORBIDDEN
HUMAN_SOURCE_BACKED_EXCEPTION_REVIEW = REQUIRED_WHEN_AMBIGUOUS
PRODUCTION_SENSORS_CONFIGURED = false
PRODUCTION_SENSORS_ENABLED = false
PRODUCTION_CUTOVER_AUTHORIZED = false
```

Conclusão:

> O widget adquiriu uma competência própria de descoberta evergreen. Para entidades reguladas sob taxonomia conhecida, a descoberta e materialização podem ser integralmente automáticas. Para identidades comerciais não reguladas, a descoberta pode ser automática e de manutenção rotineira próxima de zero, mas a resolução canônica permanece deliberadamente humana/source-backed para impedir falsos positivos.
