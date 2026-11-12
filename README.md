# Ranking de Seguradoras Sanida — Pipeline de Dados e Metodologia v2

> **Arquitetura operacional:** v2 com frontend R5.3, duas janelas semanais de geração, publicação automática HostGator e descoberta evergreen, condicionadas às variáveis de ativação dos respectivos workflows.  
> **Branch canônico:** `main`.  
> **Consumidor.gov:** aquisição observacional de `finalizadas` separada da Conduta e da publicação; adquirir um ZIP não atualiza o pacote público.  
> **Ranking geral:** bloqueado por contrato (`general_ranking_allowed = false`, `ranking_eligible = 0`).  
> **Score geral:** proibido (`general_score_allowed = false`).  
> **Frontend:** PHP/HTML/CSS/JS consome contratos públicos prontos; **não recalcula metodologia**.  
> **GSC:** compatível com a arquitetura, porém deliberadamente **opcional e não configurado**.  
> **Cron HostGator:** nenhum. Toda a automação de produção ocorre no GitHub Actions.

Este README é o **contrato operacional corrente do projeto**. Documentos de auditoria e fechamento anteriores continuam válidos como fotografias históricas, mas não devem prevalecer sobre este README nem sobre os closeouts das Fases C e D quando a pergunta for sobre o runtime atual.

Arquitetura e calendários revisados em 12/11/2026 (BRT). As ativações abaixo foram registradas nos closeouts de produção; não substituem a conferência das variáveis, dos runs e do manifesto efetivamente servido (§13.1):

```text
MAIN_BRANCH                              main
PRODUCTION_FRONTEND_R5_3                PASS
V2_PRODUCTION_AUTOMATION_ENABLED        true
V2_HOSTGATOR_DEPLOY_ENABLED             true
V2_MARKET_SENSOR_AUTOMATION_ENABLED     true
FULL_GENERATION_WINDOWS                MON_09:17_UTC / THU_06:17_UTC
AUTO_HOSTGATOR_PUBLICATION              ACTIVE
MARKET_SENSORS                          ACTIVE
WIDGET_UNKNOWN_QUERY_TELEMETRY          ACTIVE
SQLITE_PRIVATE                          ACTIVE
EVERGREEN_SNAPSHOT                      PINNED_SSH_READONLY
GSC                                     OPTIONAL_NOT_CONFIGURED
COMPLAINT_SOURCE_WATCH                  ACTIVE
FINALIZADAS_OBSERVER                    DAYS_10_16_11:27_UTC
FINALIZADAS_CONDUCT_INTEGRATION          NOT_ENABLED
HOSTGATOR_CRON                          NONE
GENERAL_SCORE_ALLOWED                   false
GENERAL_RANKING_ALLOWED                 false
RANKING_ELIGIBLE                        0
```

## Documentação operacional principal

Para o estado atual de produção, ler primeiro:

- `docs/phase-c-production-automation-closeout.md` — fechamento da geração/publicação; calendário corrente no §10;
- `docs/phase-d-evergreen-closeout.md` — sensores, telemetria, SQLite e snapshot SSH;
- `docs/post-cutover-repository-housekeeping.md` — limpeza pós-cutover e autoridade documental atual;
- `docs/production-frontend-cutover.md` — cutover do frontend R5.3;
- `ranking-seguradoras/deployment/production-cutover/INSTALLACAO-HOSTGATOR.md` — instalação e rollback.
- [Aquisição observacional de finalizadas](docs/finalizadas-observational.md) — fonte JSON, layouts, armazenamento, revisões e limites de reutilização.

Contratos metodológicos e técnicos centrais:

- `docs/section-19-1-methodology-data-audit-closure.md`;
- `docs/section-19-2-frontend-information-audit.md`;
- `docs/section-19-3-19-4-evergreen-hostgator-publication.md`;
- `docs/section-19-5-repository-cleanup.md`;
- `docs/section-19-7-final-consolidation.md`;
- `docs/emerging-market-identity-coverage.md`;
- `docs/emerging-market-discovery-audit.md`;
- `docs/financial-methodology-closure.md`;
- `docs/conduct-methodology-closure.md`;
- `docs/cross-pillar-calibration-stage-1.md`;
- `docs/cross-pillar-architecture-stage-2.md`;
- `docs/cross-pillar-assessment-semantic-contract.md`;
- `docs/assessment-eligibility-contract.md`;
- `docs/ranking-eligibility-preflight.md`;
- `docs/exploratory-leaderboards-contract.md`;
- `docs/public-search-profile-contract.md`.

---

# 1. Objetivo do produto

A ferramenta pública deve ajudar o consumidor a responder, em linguagem progressiva:

1. **Quem é a empresa ou marca pesquisada?**
2. **É seguradora autorizada, participante Sandbox, cooperativa de seguros, marca, plataforma, corretora, entidade histórica ou outro tipo de empresa?**
3. **Existe seguradora por trás da marca ou relação societária relevante?**
4. **Os sinais financeiros disponíveis mostram algum alerta material?**
5. **As reclamações parecem altas ou baixas em relação ao tamanho da operação quando essa comparação é realmente possível?**
6. **Há limitações que impedem uma conclusão conjunta?**
7. **Quais números, períodos, fontes e regras sustentam a resposta?**

A progressão pública obrigatória é:

```text
resposta rápida
→ sinais em português comum
→ identidade e relações relevantes
→ comparação visual
→ números técnicos
→ fontes, período e limites
→ metodologia completa
```

O consumidor não deve precisar conhecer `PLA/CMR`, `ILT`, `observed/expected`, `risk_carrier`, códigos internos ou nomes de estados do backend para entender a primeira resposta.

---

# 2. Princípio metodológico central

A ordem permanece:

```text
identidade correta
→ classificação correta
→ lifecycle e relationships
→ elegibilidade
→ evidência
→ período correto
→ comparabilidade
→ calibração
→ avaliação
→ eventual ordenação permitida
```

Regra de projeto:

> **identidade correta → dado correto → período correto → comparação correta → conclusão útil → explicação transparente.**

É proibido produzir conclusão primeiro e procurar justificativa depois.

Guardrails permanentes:

```text
general_score_allowed = false
general_ranking_allowed = false
ranking_eligible = 0
null != 0
missing != zero
brand != legal_entity
group != succession
risk_carrier_relation != complaint_transfer
fuzzy_search != identity_resolution
```

Nenhuma ativação operacional de C ou D alterou esses contratos.

---

# 3. Escopo do repositório

O repositório é responsável por:

- ingestão e cache de fontes;
- normalização;
- identidade canônica;
- classificação regulatória;
- lifecycle jurídico;
- relações de marca, sucessão, grupo e `risk_carrier`;
- descoberta observacional de novas identidades de mercado;
- elegibilidade;
- evidência financeira;
- evidência de Conduta;
- reconciliação entre sujeito de reclamação e exposição;
- comparabilidade;
- contratos semânticos;
- auditoria e invariantes;
- testes;
- geração dos JSONs públicos;
- workflows, artifacts e publicação;
- telemetria observacional de busca sem resultado.

O frontend público fica fora da metodologia. Sua função é pesquisar, navegar, formatar e renderizar contratos já fechados.

```text
php_may_recompute_methodology = false
js_may_recompute_methodology  = false
```

PHP/JS não podem:

- recalcular PLA/CMR ou ILT;
- recalcular `observed/expected`;
- transferir reclamações entre marca, subject e carrier;
- decidir identidade por fuzzy matching;
- criar score;
- criar ranking geral;
- inferir sucessão ou aquisição;
- transformar `null` em `0`;
- decidir missingness;
- inventar desempate.

A telemetria do frontend é exceção **operacional**, não metodológica: registra apenas consultas elegíveis sem resultado para observação futura.

---

# 4. Arquitetura de produção

```text
FONTES OFICIAIS / PÚBLICAS
    │
    ├── SUSEP / SES
    ├── SUSEP — licenciadas
    ├── SUSEP — regimes especiais
    ├── SUSEP — Sandbox
    ├── Receita Federal — lifecycle/CNPJ
    └── Consumer.gov Base Completa / último core validado quando necessário
    │
    ▼
COLETA + CACHE + VALIDAÇÃO
    │
    ▼
IDENTIDADE / CLASSIFICAÇÃO / LIFECYCLE
    │
    ▼
RELATIONSHIPS / MARCAS / RISK CARRIERS
    │
    ▼
ELEGIBILIDADE REGULATÓRIA
    │
    ▼
EVIDÊNCIA FINANCEIRA + CONDUTA
    │
    ▼
COMPARABILIDADE / RECONCILIAÇÃO
    │
    ▼
CONTRATOS SEMÂNTICOS
    │
    ▼
PACOTE PÚBLICO VERSIONADO
    │
    ▼
HOSTGATOR — instalação atômica current/previous
    │
    ▼
PHP + CSS + JS R5.3
```

O pacote público é produzido pelo backend. O frontend apenas o consome.

O observador de `finalizadas` tem fluxo separado: JSON oficial → download → validação/agregado experimental → artefato Actions. Não há ligação desse artefato ao Gate 4 ou ao HostGator.

---

# 5. Fontes de reclamações e Conduta

A camada de Conduta permanece conservadora.

Regras permanentes:

```text
no_cross_source_stitching = true
absence_is_not_zero = true
zero_complaints_is_not_automatic_favorability = true
```

Situação operacional das fontes:

- SusepCon/BDR não substituem a fonte de Conduta sem retomada pública consumível e contrato próprio; alterações na página monitorada são sinais para investigação, não prova de uma nova base utilizável;
- BDR/SUSEP existe como obrigação de envio regulatório, mas não é integrado ao pipeline sem base pública adequada;
- a fonte corrente canônica de Consumer.gov é a área oficial direta de Dados Abertos em `www.consumidor.gov.br/pages/dadosabertos/externo/`;
- o CKAN legado em `dados.mj.gov.br` não é dependência corrente do Gate 4 e permanece apenas como sinal de recuperação/forense no source watch;
- a aquisição produtiva exige publicações explicitamente identificadas como Base Completa; o arquivo `finalizadas_YYYY-MM.zip` não é um substituto automaticamente autorizado;
- Consumer.gov pode usar `validated_cache_fallback` quando a fonte oficial direta estiver indisponível ou não puder ser validada;
- fallback validado e explicitamente `stale` não é, por si só, falha metodológica;
- esse fallback preserva a evidência anterior e sua data; não comprova atualização efetiva da janela;
- o workflow `V2 Complaint Source Watch` monitora recuperação de fontes de forma independente da geração/publicação.

Nenhuma fonte nova deve ser costurada com outra apenas para preencher ausência.

## 5.1. Leitura, cache e janela de Base Completa

A infraestrutura da PR #52 escolhe o encoding antes de emitir linhas, verifica SHA-256 do cache bruto, valida temporários antes de promover arquivos e seleciona uma janela completa por manifesto/objetos imutáveis. Os leitores compartilham `api/v2/consumer_gov_window.py`, sem descobrir a janela ativa por um `glob` de meses avulsos. O fallback V2 restaura a seleção e os objetos verificados.

O legado versionado conserva 2025-07 a 2026-06 como janela declarada. Os testes de paridade em `tests/test_consumer_gov_repo_evidence.py` comparam agregados e mensais preservados; não certificam um CSV oficial ausente. O contrato de **novas** aquisições Base Completa exige competência por abertura. Essa exigência do código não comprova qual foi o evento temporal dos registros históricos já agregados.

O fechamento de Conduta conserva prêmio direto de seguros como exposição operacional, alinhamento mensal e de população, critérios de comparabilidade e satisfação apenas contextual. Não autoriza score geral, remediação inferida ou transferência de reclamações entre marca e seguradora. Ver [contrato de Conduta](docs/conduct-methodology-closure.md).

## 5.2. Observador de finalizadas

O workflow `.github/workflows/v2-finalizadas-observational.yml` consulta o [JSON oficial de publicações](https://www.consumidor.gov.br/pages/publicacao/externo/publicacoes.json?indicadorTipoPublicacao=2), identifica códigos divulgados e baixa os dois meses mais recentes conhecidos. Não usa HTML, Selenium, cookies do navegador ou IDs adivinhados.

O parser reconhece dois layouts de 19 colunas: português com `S/N` e `snake_case` com `Sim/Não`; valida a competência por **data de finalização**. Ausência de resolução em reclamação avaliada é preservada e sinalizada para revisão, sem imputação.

| Camada | Uso atual |
|---|---|
| `.github/finalizadas-publications.json` | URL/hash fixados de outubro/2026; não contém o corpus. |
| `runner.temp/finalizadas-observational` | ZIPs, agregados experimentais, validações e manifesto temporários. |
| Artefato `finalizadas-observational-<run>-a<attempt>` | Evidência experimental com retenção de 30 dias; pode conter saídas parciais de run falho. |
| Manifesto do último sucesso em `main` | Recuperado por tentativa para comparar revisões; **não evita novo download/processamento** e pode estar indisponível. |
| Conduta / pacote público / HostGator | Não consomem essas saídas. Integração não habilitada. |

Agenda: dias **10 e 16**, **11h27 UTC / 08h27 Brasília**. O horário é uma janela de observação, não garantia de publicação mensal pelo órgão. `workflow_dispatch` também está disponível; qualquer execução continua experimental.

As próximas etapas são tornar a retenção/reutilização verificável e demonstrar equivalência histórica de população/competência antes de propor integração. Data, tamanho ou código inalterado não provam que o conteúdo remoto permaneceu igual. Detalhes e provas de setembro/outubro: [documentação do observador](docs/finalizadas-observational.md).

---

# 6. Pilar econômico-financeiro

Dimensões principais:

| Dimensão | Referência | Papel |
|---|---|---|
| Capital regulatório | PLA/CMR | eixo principal |
| Liquidez | ILT | eixo principal |
| Liquidez corrente | ILC | diagnóstico complementar |
| Operação | ICA/IC | contexto longitudinal |
| Rentabilidade | ILPL | diagnóstico apenas |

Não há score financeiro numérico.

## 6.1. Regra crítica de PLA/CMR

A fonte `Ses_pl_margem.csv` preserva, entre outros:

```text
plajustado  → pla_adjusted
NovoPla     → new_pla
CMR         → cmr
```

A regra vigente é:

```text
PLA/CMR = new_pla / cmr
```

`pla_adjusted` é evidência intermediária e não pode substituir silenciosamente `new_pla`.

Consequências:

- `new_pla` ausente → métrica indisponível;
- CMR zero → evidência inutilizável, não desempenho zero;
- o frontend não recalcula essa razão;
- qualquer mudança futura exige revisão metodológica formal.

---

# 7. Identidade, busca e relações

Para registros SUSEP/SES:

```text
entity_id = fip:XXXXXX
```

Pessoa jurídica:

```text
legal_entity_id = cnpj:<CNPJ>
```

A busca pública pode localizar:

- seguradora autorizada;
- participante Sandbox;
- sociedade cooperativa de seguros;
- marca;
- insurtech/plataforma;
- corretora;
- previdência;
- capitalização;
- ressegurador;
- entidade histórica;
- entidade em regime especial;
- outras identidades materializadas.

Ser pesquisável não significa ser elegível para assessment ordinário.

Guardrails:

```text
brand != legal_entity
same_group != succession
same_group != acquisition
same_group != joint_venture
risk_carrier_relation != complaint_transfer
```

Fuzzy matching pode ordenar candidatos, nunca decidir identidade.

---

# 8. Contrato público de busca e perfil

Contrato canônico:

`docs/public-search-profile-contract.md`

Arquivos principais:

```text
data/derived/v2/public/search_index.json
data/derived/v2/public/profile_manifest.json
data/derived/v2/public/profiles/*.json
```

`search_index.json` resolve busca e desambiguação. `profile_manifest.json` resolve perfil → arquivo. Cada perfil contém identidade, relações, sinais disponíveis, semântica de ausência e contexto suficiente para o frontend não inventar lógica.

Semântica obrigatória:

```text
null != 0
```

O frontend deve respeitar `availability`, `public_use`, `zero_semantics` e `meaning`.

Números de entidades/perfis registrados em documentos antigos são snapshots históricos; o estado corrente deve ser lido no artifact da Full Generation correspondente.

---

# 9. Frontend R5.3 em produção

O frontend R5.3 está ativo em produção e foi reconciliado por bytes/hash, HTTP, SEO e smoke funcional.

Arquivos principais:

```text
ranking-seguradoras/index2.php
ranking-seguradoras/assets/ranking-v2.js
ranking-seguradoras/assets/ranking-v2.css
```

Princípios:

- resposta humana antes do jargão;
- comparação lado a lado sem vencedor artificial;
- perfis adaptativos por tipo de identidade;
- ausência e incomparabilidade explicitadas;
- metodologia fora do JavaScript;
- estado funcional compartilhável sem criar inventário massivo de páginas SEO.

A v1 e `public-pre-r5-live` permanecem preservados como capacidade de rollback conforme documentação de cutover.

`/PHP/head-global.php` não faz parte do pipeline metodológico e não deve ser alterado por housekeeping operacional deste projeto.

---

# 10. Fase C — geração e publicação automáticas

Status: **FECHADA / ATIVA EM PRODUÇÃO**.

Cadência permanente:

```text
V2 Production Generation Schedule
cron: 17 9 * * 1   — segunda-feira 09:17 UTC / 06:17 Brasília
cron: 17 6 * * 4   — quinta-feira 06:17 UTC / 03:17 Brasília
```

A materialização do evento `schedule` no GitHub Actions é best-effort. As duas janelas dão nova oportunidade de aquisição na semana; sucesso do job não prova que todas as fontes estejam atualizadas.

Cadeia:

```text
schedule
→ V2 Gate 4 Full Generation Proof em main
→ validação da geração exata
→ handoff determinístico
→ V2 HostGator Public Package Sync
→ artifact exato daquele run
→ validação build_id/source_head_sha/package_sha256
→ SSH com known_hosts pinado
→ instalação atômica current/previous
→ manifesto público HTTP
```

Guardrails de segurança:

```text
SUSEP TLS verification                 REQUIRED
SUSEP BaseCompleta wall-clock deadline 900s
validated cache fallback               PRESERVED
no cross-source stitching              PRESERVED
StrictHostKeyChecking                   yes
pinned known_hosts                      REQUIRED
rollback capability                    AVAILABLE
HostGator cron                          NONE
legacy recurring V1 writers            OFF
```

Workflows principais:

```text
.github/workflows/v2-production-generation-schedule.yml
.github/workflows/v2-gate4-full-generation-proof.yml
.github/workflows/v2-hostgator-publication.yml
```

A publicação automática também aceita a conclusão bem-sucedida de `V2 Gate 4 Full Generation Proof` em `main`, se `V2_HOSTGATOR_DEPLOY_ENABLED == true`. Portanto, disparar esse workflow não é um ensaio isolado. O observador de `finalizadas` não integra a lista de gatilhos de publicação. O limite de 900s acima é da fonte SUSEP; não é um prazo geral para Consumer.gov ou Receita.

---

# 11. Fase D — Evergreen

Status: **FECHADA / ATIVA EM PRODUÇÃO**.

Fontes operacionais:

```text
SUSEP licensed delta      ACTIVE
SUSEP Sandbox delta       ACTIVE
widget unknown queries    ACTIVE
GSC                       OPTIONAL / NOT CONFIGURED
```

Decisão explícita:

```text
GSC_REQUIRED_FOR_EVERGREEN = false
```

Detectar automaticamente não significa afirmar automaticamente.

Todo candidate emergente preserva:

```text
assertion_effect = none
score_effect = none
complaint_transfer_effect = none
automatic_registry_mutation = forbidden
blocking = false
```

A fusão automática só pode usar âncora determinística adequada. Similaridade não resolve identidade.

## 11.1. Telemetria de busca sem resultado

O frontend registra somente consultas elegíveis quando:

```text
catálogo pronto
+ zero resultados
+ query elegível
+ endpoint same-origin configurado
```

Cadeia:

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

Threshold atual:

```text
widget_min_count = 2
widget_min_distinct_days = 2
```

Abaixo do threshold, a observação pode permanecer `observed`; ao atingir o threshold, pode virar `review_required`. Isso não cria identidade pública.

## 11.2. Snapshot agregado do HostGator

A leitura administrativa final não usa exceção Cloudflare.

```text
visitante/widget POST
→ Cloudflare + Bot Fight Mode + regras normais
→ endpoint PHP
→ SQLite privado

GitHub market sensor
→ SSH HostGator autenticado
→ known_hosts pinado
→ PHP remoto somente leitura
→ SQLite SQLITE3_OPEN_READONLY
→ JSON agregado
```

Propriedades obrigatórias:

```text
BatchMode=yes
IdentitiesOnly=yes
StrictHostKeyChecking=yes
GlobalKnownHostsFile=/dev/null
UpdateHostKeys=no
SQLITE3_OPEN_READONLY
```

O workflow não faz `scp` do SQLite, não altera o banco e remove o material SSH efêmero ao final.

Workflow principal:

```text
.github/workflows/v2-emerging-market-identity-sensors.yml
```

Quando `V2_MARKET_SENSOR_AUTOMATION_ENABLED == true`, esse workflow assume a atualização automática da issue singleton de revisão porque renderiza o superset de relações + candidates emergentes.

---

# 12. GSC

Google Search Console **não é requisito de produção** para o Evergreen.

Estado esperado sem configuração:

```text
gsc_query = unavailable
```

Isso não degrada:

- SUSEP licensed delta;
- SUSEP Sandbox delta;
- telemetria do widget;
- Full Generation;
- publicação HostGator;
- Gate 4.

A integração pode ser retomada no futuro se houver benefício operacional demonstrado. Até lá, não se deve criar dependência de WIF/service account apenas para completar um sensor opcional.

---

# 13. HostGator e rollback

Estruturas operacionais principais:

```text
publication root:
/home1/sanid210/sanida-v2-publication

public package root:
/home1/sanid210/public_html/ranking-seguradoras/data/v2/public

backup preservado:
/home1/sanid210/public_html/ranking-seguradoras/data/v2/public-pre-r5-live

evergreen private root:
/home1/sanid210/sanida-v2-evergreen
```

O HostGator não é scheduler da v2. Não criar cron para geração/publicação.

A publicação usa geração inteira e troca atômica entre `current` e `previous`; o rollback não deve ser removido durante limpezas de repositório ou deploy.

## 13.1. Como conferir os dados efetivamente publicados

1. Consulte o [manifesto público](https://sanida.com.br/ranking-seguradoras/data/v2/public/distribution_manifest.json) e registre `build_id`, `source_head_sha` e `package_sha256`.
2. Localize o run de Full Generation e o artefato **exatos** dessa geração; confira `source_lineage.json` e `conduct_source_lineage.json`, incluindo `state`, `freshness_method`, `fetched_at` e a janela de dados.
3. Confira período e disponibilidade no perfil/contrato público correspondente. A data do run ou do deploy não substitui o período de referência da fonte.
4. Para uma cópia local do pacote, `python -m api.v2.install_public_generation verify <diretorio-do-pacote>` verifica hashes e conjunto de arquivos sem instalar. Validação de bytes não substitui reconciliação semântica.

Um artefato do observador, uma CI verde ou a existência do ZIP oficial não demonstram que setembro/outubro já entraram na Conduta. Se o artefato da geração expirou, declare a limitação e use a evidência preservada da geração; não suponha atualização.

---

# 14. Workflows operacionais

Camada permanente de produção:

```text
V2 Production Generation Schedule
V2 Gate 4 Full Generation Proof
V2 HostGator Public Package Sync
V2 Emerging Market Identity Sensors
V2 Complaint Source Watch
V2 Relationship Review Queue
```

Os writers antigos permanecem sem recorrência automática e só podem ser usados deliberadamente para recuperação/forense quando ainda existirem:

```text
Refresh data (manual legacy only)
refresh-receita-lifecycle.yml
update-susep-sandbox.yml
```

Não restaurar schedules antigos.

Workflows experimentais/diagnósticos preservados não são autoridade de produção apenas por existirem no repositório.

O `Finalizadas Observational Acquisition` é recorrente, mas **experimental**: apenas `contents: read`/`actions: read`, saídas em `runner.temp`, sem chamada de Receita, Gate 4 ou deploy. Seu artefato de 30 dias não é cache persistente de produção.

---

# 15. Validações obrigatórias

O pipeline deve falhar diante de situações como:

- `entity_id` duplicado;
- CNPJ incompatível duplicado;
- marca apontando para entidade inexistente;
- referência pública apontando para perfil inexistente;
- SSPE recebendo semântica de seguradora ordinária;
- Sandbox vazando para benchmark ordinário;
- cooperativa vazando para benchmark ordinário por presunção;
- previdência/capitalização vazando para exposição de seguros;
- reclamações transferidas silenciosamente de subject para carrier;
- pressão calculada sem denominador comparável;
- fonte sem período;
- valores não finitos;
- queda anormal de cobertura;
- alteração inesperada de schema;
- alteração desconhecida da taxonomia SUSEP;
- `null` convertido em zero;
- score produzido por artifact que proíbe scoring;
- subset incompleto apresentado como ranking integral;
- frontend reconstruindo lógica do backend;
- `PLA/CMR` calculado com campo diferente de `new_pla`;
- fallback silencioso de `new_pla` para `pla_adjusted`;
- reconstrução do pacote final misturando artifacts de gerações diferentes;
- candidate evergreen com `assertion_effect` diferente de `none`;
- candidate evergreen alterando score, reclamações ou registry;
- fuzzy resolvendo automaticamente identidade emergente.

Sensores de demanda indisponíveis não devem invalidar Gate 4; sua ausência precisa aparecer explicitamente como estado do sensor.

---

# 16. O que não fazer

Não:

- reabrir score para facilitar UI;
- escolher peso Financeiro × Conduta arbitrariamente;
- criar ranking geral para satisfazer palavra-chave;
- premiar excesso de PLA/CMR ou ILT sem novo contrato;
- tratar ausência como zero;
- tratar incomparabilidade como neutralidade;
- misturar seguros, previdência e capitalização;
- transferir reclamações por semelhança de nome;
- incluir Sandbox no baseline ordinário;
- incluir cooperativa no baseline ordinário por presunção;
- aplicar fuzzy matching decisório;
- permitir PHP/JS corrigir backend;
- transformar demanda desconhecida em identidade pública automaticamente;
- permitir candidate observacional mutar o registry;
- publicar artifact de feature branch ou run falho;
- ignorar hashes de pacote/manifesto;
- relaxar TLS ou `known_hosts` para fazer coleta passar;
- remover o hard deadline da BaseCompleta sem evidência concreta;
- reativar cron HostGator;
- reativar writers V1 recorrentes;
- apagar `public-pre-r5-live`;
- integrar BDR/SusepCon sem fonte pública consumível e contrato próprio;
- transformar GSC opcional em dependência sem benefício demonstrado.

---

# 17. Histórico de consolidação

Os §§19.1–19.7 foram a trilha de fechamento que levou ao estado atual. Documentos dessa etapa podem dizer corretamente, **naquele momento**, que `main`, publicação ou sensores ainda não estavam autorizados.

Essas afirmações são snapshots históricos e não descrevem mais o runtime.

Sequência operacional concluída:

```text
§19.1 metodologia/dados                  CLOSED
§19.2 frontend information audit         CLOSED
§19.3 evergreen architecture             CLOSED
§19.4 HostGator publication architecture CLOSED
§19.5 repository cleanup                 CLOSED
§19.6 frontend/UX/SEO                    CLOSED
§19.7 final consolidation                CLOSED
FRONTEND R5.3 CUTOVER                    COMPLETE
FASE C PRODUCTION AUTOMATION             CLOSED / ACTIVE
FASE D EVERGREEN                         CLOSED / ACTIVE
POST-CUTOVER HOUSEKEEPING                COMPLETE
```

O antigo branch `refactor/v2-data-foundation` e branches temporários das fases de implantação não são branches canônicos de desenvolvimento após o cutover. O branch de produção e referência corrente é `main`.

---

# 18. Critério de manutenção futura

Áreas fechadas não devem ser reabertas apenas porque outra abordagem seria possível.

Mudanças posteriores devem responder a pelo menos uma destas condições:

- bug objetivo;
- regressão;
- mudança de fonte oficial;
- mudança de schema/taxonomia;
- quebra de contrato público;
- necessidade operacional concreta;
- nova evidência metodológica suficiente para um processo formal separado.

O objetivo de manutenção é preservar:

- automação;
- cache;
- snapshots;
- rastreabilidade;
- busca rápida;
- utilidade ao consumidor;
- segurança de publicação;
- separação entre detecção automática e afirmação pública.

---

## Licença e uso

Este repositório é mantido pela Sanida Corretora de Seguros.

Dados de terceiros permanecem sujeitos às condições, limitações e responsabilidades de suas respectivas fontes.

A metodologia Sanida é uma interpretação própria de dados públicos e não deve ser apresentada como nota, classificação ou certificação oficial da SUSEP.
