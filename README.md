# Ranking de Seguradoras Sanida — Pipeline de Dados e Metodologia v2

> **Status do projeto:** fundação metodológica concluída em Draft; início da fase de **consolidação, auditoria e operacionalização evergreen**.  
> **Branch de trabalho:** `refactor/v2-data-foundation`.  
> **PR:** #1 permanece **Draft**. Não fazer merge em `main` antes do fechamento da consolidação.  
> **Produto principal:** consulta de identidade + avaliação semântica individual + comparação lado a lado; leaderboards unidimensionais e coleções semânticas são exploração secundária.  
> **Ranking geral:** continua bloqueado (`ranking_eligible = 0`).  
> **Frontend:** PHP/HTML/CSS/JS no HostGator consome contratos públicos prontos; **não recalcula metodologia**.  
> **Marco crítico de 29/08/2026:** corrigido o numerador de `PLA/CMR`. O pipeline deve usar `NovoPla` (`new_pla`) como PLA prudencial final; `plajustado` (`pla_adjusted`) permanece somente como evidência intermediária da fonte. Artifacts gerados antes dessa correção não devem ser tratados como atuais para capital, assessment combinado ou leaderboards dependentes de capital.

Este README é o **contrato operacional do projeto**. A partir daqui, o objetivo deixa de ser abrir novas frentes metodológicas e passa a ser consolidar o que já foi construído, eliminar inconsistências, automatizar publicação e garantir que o frontend use corretamente os contratos existentes.

Documentação principal:

- `docs/financial-methodology-closure.md`;
- `docs/conduct-methodology-closure.md`;
- `docs/cross-pillar-calibration-stage-1.md`;
- `docs/cross-pillar-architecture-stage-2.md`;
- `docs/cross-pillar-assessment-semantic-contract.md`;
- `docs/assessment-eligibility-contract.md`;
- `docs/ranking-eligibility-preflight.md`;
- `docs/exploratory-leaderboards-contract.md`;
- `docs/public-search-profile-contract.md`.

Regras marcadas como **EXPERIMENTAL**, **DIAGNÓSTICO**, **LEGACY**, **INVALIDATED** ou **PENDENTE** nunca podem vazar silenciosamente para o frontend público.

---

# 1. Objetivo do produto

A ferramenta pública deve ajudar o consumidor a responder, em linguagem progressiva:

1. **Quem é a empresa ou marca pesquisada?**
2. **É seguradora autorizada, participante Sandbox, marca, entidade histórica ou outro tipo de empresa?**
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

# 2. Princípio central

A ordem metodológica permanece:

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

---

# 3. Escopo do repositório

O repositório é responsável por:

- ingestão e cache de fontes;
- normalização;
- identidade canônica;
- classificação regulatória;
- lifecycle jurídico;
- relações de marca, sucessão, grupo e `risk_carrier`;
- elegibilidade;
- evidência financeira;
- evidência de Conduta;
- reconciliação entre sujeito de reclamação e exposição;
- comparabilidade;
- contratos semânticos;
- auditoria e invariantes;
- testes;
- geração dos JSONs públicos;
- workflows e artifacts.

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

---

# 4. Arquitetura-alvo

```text
FONTES OFICIAIS / PÚBLICAS
    │
    ├── SUSEP / SES
    ├── SUSEP — licenciadas
    ├── SUSEP — regimes especiais
    ├── SUSEP — Sandbox
    ├── Receita Federal — lifecycle/CNPJ
    ├── BDR / SusepCon quando publicamente consumível
    └── Consumer.gov
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
JSONs PÚBLICOS
    │
    ▼
PHP + CSS + JS
```

---

# 5. Identidade, busca e relações

## 5.1. Identidade antes da avaliação

Para registros SUSEP/SES:

```text
entity_id = fip:XXXXXX
```

Pessoa jurídica:

```text
legal_entity_id = cnpj:<CNPJ>
```

A Receita Federal complementa lifecycle cadastral; não concede licença nem substitui a SUSEP.

## 5.2. Busca mais ampla que assessment

A busca pública pode localizar:

- seguradora autorizada;
- participante Sandbox;
- marca;
- plataforma;
- corretora;
- previdência;
- capitalização;
- ressegurador;
- entidade histórica;
- entidade em regime especial;
- outras identidades materializadas.

Ser pesquisável não significa ser elegível para assessment ordinário.

## 5.3. Fuzzy matching

```text
fuzzy search → ordenar candidatos
fuzzy search ≠ decidir identidade
```

## 5.4. Relações

O backend pode preservar:

- `incorporated_into`;
- sucessores;
- grupo econômico observado;
- marca;
- `brand_of`;
- `risk_carrier`;
- relações especiais de subject/carrier/portfólio.

Guardrails:

```text
brand ≠ legal_entity
same_group ≠ succession
same_group ≠ acquisition
same_group ≠ joint_venture
risk_carrier_relation ≠ complaint_transfer
```

Casos públicos importantes já materializados:

- Youse ↔ Caixa Seguradora;
- Loovi ↔ LTI Seguros;
- HDI Seguros × HDI Global no grupo TALANX, mantendo identidades distintas;
- Zurich com necessidade de reconciliação temporal;
- Bradesco Seguros com divisão de carteira;
- entidades históricas com sucessão explícita quando documentada.

---

# 6. Contrato público de busca e perfil

Contrato canônico:

`docs/public-search-profile-contract.md`

Arquivos:

```text
data/derived/v2/public/search_index.json
data/derived/v2/public/profile_manifest.json
data/derived/v2/public/profiles/*.json
```

No build que fechou esse contrato em 28/08/2026:

```text
entidades lifecycle                               492
marcas verificadas                                 13
perfis públicos                                   505
entradas de busca                                 505
seguradoras ordinárias atuais no lifecycle        159
com payload de assessment daquele snapshot        157
participantes Sandbox                              12
```

Esses números são snapshot, não constantes metodológicas.

`search_index.json` resolve busca e desambiguação. `profile_manifest.json` resolve perfil → arquivo. Cada arquivo de `profiles/` contém identidade, relações, sinais disponíveis, semântica de ausência e contexto suficiente para o frontend não inventar lógica.

### Semântica obrigatória

```text
null ≠ 0
```

Exemplo Youse:

- reclamações podem existir;
- `expected_complaints` pode ser `null`;
- `pressure_ratio` pode ser `null`;
- prêmio bruto zero pode existir na fonte;
- esse zero pode ser explicitamente marcado como **não exibível como tamanho da operação**.

O frontend deve respeitar `availability`, `public_use`, `zero_semantics` e `meaning`.

---

# 7. Pilar econômico-financeiro

## 7.1. Arquitetura

| Dimensão | Referência | Papel |
|---|---|---|
| Capital regulatório | PLA/CMR | eixo principal |
| Liquidez | ILT | eixo principal |
| Liquidez corrente | ILC | diagnóstico complementar |
| Operação | ICA/IC | contexto longitudinal |
| Rentabilidade | ILPL | diagnóstico apenas |

Não há score financeiro numérico.

## 7.2. Correção crítica de PLA/CMR — 29/08/2026

Foi identificado um erro de interpretação de campo no pipeline de capital.

A fonte `Ses_pl_margem.csv` preserva, entre outros:

```text
plajustado  → normalizado como pla_adjusted
NovoPla     → normalizado como new_pla
CMR         → cmr
```

O pipeline antigo calculava:

```text
pla_adjusted / CMR
```

Isso utilizava um valor intermediário e gerava falsos alertas prudenciais em casos materiais, inclusive Porto Seguro.

A regra corrigida é:

```text
PLA/CMR = new_pla / cmr
```

Contrato atual no código:

```text
CAPITAL_PLA_SOURCE_FIELD = "new_pla"
CAPITAL_PLA_RAW_INTERMEDIATE_FIELD = "pla_adjusted"
```

Consequências obrigatórias:

- `new_pla` é o numerador prudencial do PLA/CMR;
- `pla_adjusted` continua preservado como evidência intermediária/bruta;
- **não existe fallback silencioso** de `new_pla` para `pla_adjusted`;
- se `new_pla` estiver ausente, a métrica fica indisponível;
- CMR zero continua sendo evidência inutilizável, não desempenho zero;
- a seleção da competência financeira madura deve usar a mesma semântica de derivabilidade de `new_pla / CMR`.

Versões após a correção:

```text
FINANCIAL_EVIDENCE_VERSION = 2.0-draft-evidence-profile-3
MATURITY_POLICY_VERSION     = 2.0-draft-financial-period-maturity-2
```

Testes regressivos foram adicionados para impedir retorno ao numerador antigo.

### Regra de invalidação

Artifacts anteriores a essa correção que contenham:

- sinal de capital;
- assessment conjunto dependente de capital;
- coleções financeiras;
- leaderboard `highest_pla_cmr_ratio`;
- semântica pública derivada do capital;

**devem ser regenerados antes de qualquer publicação considerada atual.**

Contagens históricas como “14 seguradoras abaixo do CMR” pertencem ao build anterior e ficam **invalidada(s) para uso corrente** até rebuild integral da cadeia.

## 7.3. Competência financeira madura

```text
última competência observada ≠ última competência madura
```

A política escolhe a competência comum mais recente com cobertura suficiente de capital derivável e alinhamento com as demais fontes financeiras.

A maturidade não pode usar semântica diferente da métrica final: após a correção, derivabilidade de capital significa `new_pla` disponível + `CMR > 0`.

## 7.4. Capital

Estados:

```text
capital_below_cmr
capital_meets_or_exceeds_cmr
capital_signal_unavailable
```

Regras:

- `PLA/CMR < 1`: insuficiência observada frente ao CMR na competência analisada;
- `PLA/CMR >= 1`: requisito observado atendido;
- não afirmar insolvência automaticamente;
- excesso de capital não gera recompensa linear;
- histórico curto altera confiança, não desempenho.

## 7.5. Liquidez

ILT é o indicador principal. ILC é diagnóstico complementar.

A referência `1,0` do ILT é paridade aritmética, **não threshold prudencial oficial da SUSEP**.

```text
ilt_below_arithmetic_parity
ilt_at_or_above_arithmetic_parity
ilt_signal_unavailable
```

Capital e liquidez são não compensatórios: um não apaga materialmente o outro.

## 7.6. Operação

ICA/IC permanecem contexto longitudinal:

```text
balanced_persistent
improved
recent_pressure
persistent_pressure
indeterminate
```

Não formam terceiro score financeiro.

---

# 8. Pilar de Conduta

Pergunta central:

> **Há reclamações acima do que seria esperado para o tamanho comparável da operação, e esse sinal é suficientemente robusto e persistente?**

O contrato de Conduta não responde simplesmente “quem tem mais reclamações”.

## 8.1. Cascata de fontes

```text
P1  BDR / SusepCon atual e publicamente consumível
 ↓
P2  Consumer.gov Base Completa autêntica + SES
 ↓
P3  Consumer.gov core preservado + SES
 ↓
P0  evidência indisponível
```

Regra:

```text
series_policy = no_cross_source_stitching
```

Fontes diferentes não são costuradas em uma falsa série longitudinal.

## 8.2. Exposição

A exposição econômica aprovada para seguros é:

```text
source                         Ses_seguros.csv
approved denominator           premio_direto
diagnostic companion           premio_ganho
```

Previdência e capitalização não entram no denominador de seguros.

## 8.3. Evidência observada não é pressão comparável

```text
conduct_evidence_state ≠ pressure_comparability_state
```

Uma empresa pode ter reclamações observadas e ainda não permitir cálculo proporcional seguro.

## 8.4. Subject, marca e carrier

```text
consumer-facing subject
↔ relação documentada
↔ risk carrier(s)
```

Relações fornecem contexto; não transferem automaticamente reclamações ou produção.

### Youse

A relação Youse → Caixa é documentada, mas a produção total da Caixa não pode ser usada automaticamente como denominador das reclamações da Youse.

### Loovi / LTI

A evidência Consumer.gov é atribuída à LTI Seguros. O perfil da marca Loovi pode apresentar esse contexto explicando a relação, sem transformar a marca em pessoa jurídica avaliada.

## 8.5. Estatística e tempo

O contrato distingue:

- acima do esperado;
- abaixo do esperado;
- sem diferença clara;
- conclusão sensível ao denominador;
- histórico temporal insuficiente;
- pressão não comparável;
- persistência;
- tendência;
- satisfação sample-aware.

Resposta/finalização no Consumer.gov não prova resolução.

---

# 9. Sandbox

Sandbox continua fora do benchmark ordinário.

```text
ordinary_ranking_effect = none
```

Participantes Sandbox podem ser pesquisados e receber contexto próprio de identidade e Conduta.

Loovi ↔ LTI é o caso público mais desenvolvido no contrato atual.

Sandbox não herda assessment ordinário e não contamina baseline ordinário.

---

# 10. Avaliação entre pilares

A arquitetura combinada é:

```text
noncompensatory_state_matrix_with_adverse_qualifiers
```

Não existe média ponderada entre Financeiro e Conduta.

A avaliação conjunta deve dizer **onde** existe sinal adverso e onde a evidência está incompleta, sem criar uma taxa de câmbio artificial entre capital, liquidez e reclamações.

Estados públicos podem incluir:

```text
no_current_core_adverse_signal
conduct_pressure_only
liquidity_pressure_only
liquidity_and_conduct_pressure
capital_shortfall_without_conduct_pressure
capital_shortfall_and_conduct_pressure
evidence_incomplete_for_joint_assessment
```

### Atenção após correção do PLA

As **categorias de capital e as contagens de estados combinados precisam ser regeneradas** com `new_pla / CMR` antes de serem tratadas como snapshot atual.

O desenho semântico continua válido; as contagens concretas anteriores podem mudar.

---

# 11. Assessment e ranking

Os gates permanecem separados:

```text
regulatory_universe_eligible
→ assessment_eligible
→ ranking_eligible
```

`assessment_eligible` significa que há evidência suficiente e comparável para publicar a avaliação conjunta daquela versão. Não é selo de qualidade.

`ranking_eligible` permanece `0` para ranking geral.

A existência de leaderboards unidimensionais não abre ranking composto.

Regra dos leaderboards:

> **a própria métrica deve definir a ordem.**

Exemplos autorizados pelo contrato exploratório:

```text
largest_by_direct_premium
highest_pla_cmr_ratio
highest_ilt
lowest_conduct_pressure_ratio
highest_conduct_pressure_ratio
```

O leaderboard de PLA/CMR deve ser reconstruído após a correção de `new_pla`.

Coleções semânticas permanecem `ordered = false`.

Não suportados:

```text
ranking_geral
mais_popular
emergente_promissora
consagrada_exemplar
crescimento_de_premio
```

---

# 12. JSONs públicos

Existem duas famílias complementares.

## 12.1. Busca/perfil

```text
public/search_index.json
public/profile_manifest.json
public/profiles/*.json
```

## 12.2. Exploração/comparação

```text
public/insurer_explorer.json
public/explore_index.json
public/leaderboards/*.json
public/collections/*.json
```

No servidor da Sanida, o frontend deve consumir a estrutura consolidada em:

```text
/ranking-seguradoras/data/v2/public/
```

A fase de consolidação deve eliminar o processo manual de baixar e mesclar artifacts sempre que possível.

---

# 13. Frontend público

A direção atual da página é:

```text
Consulta e comparação de seguradoras
```

Fluxo principal:

```text
buscar nome conhecido pelo usuário
→ confirmar identidade
→ resposta rápida
→ relações importantes
→ sinais centrais em bom português
→ comparar 2–4 seguradoras
→ explorar lista/rankings por critério
→ metodologia sob demanda
```

Princípios de UX:

- usuário deve manter sensação de controle;
- busca deve ser fácil de refazer;
- navegação não pode sobrepor header global;
- elementos internos não devem usar tags genéricas que colidam com CSS global (`header`, `footer`, `article`) quando classes neutras resolvem melhor;
- nenhuma bandeja/sticky deve competir com o menu da Sanida sem necessidade funcional inequívoca;
- desktop e mobile devem possuir composições próprias quando a comparação em matriz não couber;
- complexidade técnica entra progressivamente;
- cor deve comunicar função e semântica, não ornamentação.

### Semântica visual

Direção:

```text
azul   → navegação/informação neutra
verde  → sinal favorável naquele critério
vermelho → alerta material naquele critério
âmbar   → cautela/atenção
cinza   → ausência, inconclusão ou neutralidade
```

### Comparador

Desktop: critérios na coluna esquerda e seguradoras nas colunas seguintes.

Leitura visual permitida:

```text
✓  sem alerta naquele critério
✕  alerta
!  atenção/cautela
≈  sem diferença clara
—  sem conclusão segura
```

Mobile: reorganizar por seguradora, sem esmagar matriz horizontal.

---

# 14. SEO e linguagem pública

SEO orienta a arquitetura sem sacrificar a experiência.

Território semântico prioritário da página:

- consulta e comparação de seguradoras;
- seguradoras autorizadas pela SUSEP;
- lista de seguradoras;
- seguradoras confiáveis;
- maiores seguradoras do Brasil;
- rankings por critérios objetivos;
- consulta de identidade/regulação.

Evitar disputar intencionalmente termos específicos de ramos de seguro quando existirem páginas próprias, reduzindo risco de canibalização.

Headings públicos devem permanecer naturais, por exemplo:

```text
Consulta e comparação de seguradoras
Lista de seguradoras autorizadas pela SUSEP
Maiores seguradoras do Brasil e outros rankings por critérios objetivos
Como saber se uma seguradora é confiável?
```

Termos de bastidor como `v2`, `active_licensed`, `ordinary`, `snapshot`, `lifecycle` e nomes internos de states não devem aparecer ao consumidor sem necessidade explicativa.

---

# 15. Evergreen e zero manutenção

A meta operacional é **zero manutenção editorial/técnica recorrente, ou o mais próximo possível disso**.

Princípios:

1. fonte oficial/pública sempre que possível;
2. cache validado;
3. fallback sem mudança de semântica;
4. workflow reproduzível;
5. artifacts públicos versionados;
6. publicação automática ou sincronização automática para o HostGator;
7. validação antes de promoção;
8. rollback simples;
9. nenhuma curadoria periódica de números que possa ser derivada automaticamente;
10. curadoria apenas para fatos relacionais realmente não automatizáveis, como algumas marcas/sucessões documentadas.

A fase de consolidação deve transformar a atualização em algo semelhante a:

```text
fonte muda
→ workflow coleta/cacheia
→ valida schemas/invariantes
→ reconstrói contratos
→ valida regressões
→ publica artifact público aprovado
→ HostGator sincroniza
→ troca atômica da versão ativa
→ frontend consome
```

Nunca:

```text
workflow parcial
→ JSON incompleto sobrescreve produção
```

---

# 16. Workflows e dependências

Workflows relevantes incluem:

- `CI`;
- `V2 Foundation Validation`;
- `V2 Classification Validation`;
- `V2 Lifecycle Relationships Validation`;
- `V2 Eligibility Validation`;
- `V2 Financial Evidence Validation`;
- `V2 Liquidity Experiment`;
- `V2 Operating Experiment`;
- `V2 Financial Methodology Closure`;
- `V2 Consumer.gov Conduct Evidence`;
- `V2 Receita Consumer.gov Identity Experiment`;
- `V2 Conduct Comparative Preflight`;
- `V2 Conduct Coverage Reconciliation`;
- `V2 Conduct Comparative Calibration v2`;
- `V2 Conduct Credibility Diagnostic`;
- `V2 Conduct Portfolio Mix Diagnostic`;
- `V2 Conduct Methodology Closure`;
- `V2 Sandbox Brand Conduct Evidence`;
- `V2 Cross-Pillar Calibration Stage 1`;
- `V2 Cross-Pillar Coverage Audit`;
- `V2 Cross-Pillar Architecture Stage 2`;
- `V2 Cross-Pillar Assessment Semantic Contract`;
- `V2 Assessment Eligibility Contract`;
- `V2 Ranking Eligibility Preflight`;
- `V2 Exploratory Leaderboards Contract`;
- `V2 Public Search Profile Contract`.

A consolidação deve revisar dependências e gatilhos para evitar dezenas de workflows redundantes executando por qualquer alteração de documentação ou frontend.

Também deve separar claramente:

```text
produção necessária
vs
diagnóstico permanente
vs
experimento histórico/manual
```

---

# 17. Validações obrigatórias

O pipeline deve falhar diante de situações como:

- `entity_id` duplicado;
- CNPJ incompatível duplicado;
- marca apontando para entidade inexistente;
- Sandbox vazando para benchmark ordinário;
- previdência/capitalização vazando para exposição de seguros;
- reclamações transferidas silenciosamente de subject para carrier;
- pressão calculada sem denominador comparável;
- fonte sem período;
- valores não finitos;
- queda anormal de cobertura;
- alteração inesperada de schema;
- `null` convertido em zero;
- zero bruto exibido com semântica errada;
- score produzido por artifact que proíbe scoring;
- pilar ausente tratado como neutralidade;
- subset incompleto apresentado como ranking integral;
- avaliação incompleta escondendo alerta disponível;
- frontend reconstruindo lógica de backend;
- `PLA/CMR` calculado com campo diferente de `new_pla`;
- fallback silencioso de `new_pla` para `pla_adjusted`;
- competência madura calculada com regra de capital diferente da usada no assessment;
- artifact público dependente de capital gerado com versão anterior do Financial Evidence.

---

# 18. Correção de capital — arquivos alterados

A correção de 29/08/2026 foi aplicada somente no branch de trabalho e manteve `main` intacta.

Arquivos centrais:

```text
api/v2/financial_evidence.py
api/v2/financial_periods.py
tests/test_v2_financial_evidence.py
tests/test_v2_financial_evidence_inventory.py
tests/test_v2_financial_periods.py
tests/test_v2_susep_financial_evidence.py
```

O conjunto de testes inclui regressão onde `plajustado` indicaria falsamente razão abaixo de 1 enquanto `NovoPla` mostra requisito atendido. A regra correta deve vencer.

---

# 19. Fase atual — CONSOLIDAÇÃO DO WIDGET

A próxima fase oficial do projeto é:

```text
consolidation_audit_evergreen_publication_and_frontend_polish
```

Objetivos obrigatórios:

## 19.1. Auditoria geral de metodologia e dados

- revisar fórmulas desde a fonte até o JSON público;
- confrontar amostras relevantes com dados brutos;
- testar casos intuitivamente suspeitos;
- procurar erros de unidade, campo, sinal, período, denominador e missingness;
- verificar consistência entre documentação, código, testes e artifacts;
- adicionar regressões para todo bug real encontrado.

A descoberta `plajustado` × `NovoPla` é o precedente: aparência de plausibilidade não substitui auditoria da semântica da fonte.

## 19.2. Auditoria do potencial informacional do frontend

Verificar se a página efetivamente usa:

- identidade;
- aliases;
- lifecycle;
- sucessões;
- grupos;
- marcas;
- risk carriers;
- relações de Conduta;
- Sandbox Conduct;
- sinais financeiros;
- sinais de reclamações;
- períodos;
- comparabilidade;
- confiança;
- limites;
- leaderboards;
- coleções.

Nenhuma informação útil deve ficar presa no repo por falta de contrato público; se faltar payload, ampliar o backend em vez de reconstruir lógica no JS.

## 19.3. Evergreen / zero manutenção

- revisar fontes e caches;
- definir dependências reais entre workflows;
- eliminar necessidade de download manual de artifacts;
- criar publicação/sincronização segura para o HostGator;
- manter staging/versão anterior para rollback;
- documentar recuperação quando fonte oficial estiver temporariamente fora do ar.

## 19.4. Cron jobs e publicação

Definir mecanismo para que `/ranking-seguradoras/data/v2/public/` receba em sincronia somente um **pacote público validado**.

O cron não deve tentar adivinhar que dois artifacts independentes terminaram ao mesmo tempo. A consolidação deve preferir um pacote de distribuição único ou um manifest/versionamento que permita atualização atômica.

## 19.5. Limpeza do repositório

Classificar arquivos como:

```text
KEEP — produção/fundação atual
DIAGNOSTIC — auditoria ainda útil
LEGACY — histórico reprodutível
DELETE — redundante/obsoleto/temporário
```

Revisar especialmente:

- workflows temporários;
- scripts de sincronização antigos;
- artifacts experimentais;
- código v1 que não é mais referência;
- helpers duplicados;
- documentação contraditória;
- builders substituídos;
- testes de experimentos invalidados.

Excluir somente depois de confirmar que nenhuma dependência atual os usa.

## 19.6. Revisão do frontend e SEO

Comparar o frontend atual com o widget anterior para recuperar somente o que era realmente positivo em:

- busca;
- clareza;
- feedback de interação;
- densidade de informação;
- navegação;
- capacidade de descoberta;
- semântica SEO.

Não restaurar:

- score antigo;
- ranking geral antigo;
- lógica metodológica em React/JS;
- linguagem enganosa;
- excesso visual.

A prioridade é experiência do usuário; SEO orienta títulos, headings, arquitetura semântica e cobertura de intenção sem degradar o português.

---

# 20. O que não fazer

Não:

- reabrir score para “facilitar” UI;
- escolher peso Financeiro × Conduta arbitrariamente;
- usar ranking geral para satisfazer palavra-chave;
- premiar excesso de PLA/CMR ou ILT sem novo contrato;
- tratar ausência como zero;
- tratar incomparabilidade como neutralidade;
- misturar seguros, previdência e capitalização;
- transferir reclamações por semelhança de nome;
- incluir Sandbox no baseline ordinário;
- aplicar fuzzy matching decisório;
- permitir PHP/JS corrigir backend;
- publicar artifacts antigos após mudança metodológica;
- manter processo manual só porque funciona em teste;
- apagar código experimental antes de confirmar dependências;
- alterar `main` antes do encerramento da consolidação.

---

# 21. Critério de sucesso da consolidação

A fase termina quando:

- fórmulas críticas foram auditadas da fonte à tela;
- regressões cobrem bugs materiais conhecidos;
- artifacts dependentes do novo PLA foram reconstruídos e validados;
- o frontend apresenta corretamente identidade, relações, avaliação e limites;
- comparação funciona em desktop e mobile sem sobreposição de elementos;
- linguagem pública está limpa de jargão interno desnecessário;
- SEO e UX foram revisados em conjunto;
- a publicação dos JSONs é automática, versionada e atômica;
- cron jobs têm dependência clara e falha segura;
- arquivos experimentais/legacy estão classificados e limpos;
- documentação não contém “próximo gate” já encerrado nem snapshots contraditórios;
- `main` recebe somente o que estiver auditado e pronto para produção.

---

# 22. Diretriz final

A v2 não existe para reproduzir a v1 com outros números.

Ela deve preservar o que a engenharia anterior fazia bem:

- automação;
- cache;
- snapshots;
- rastreabilidade;
- busca rápida;
- utilidade ao consumidor.

E eliminar o que era frágil:

- universo mal delimitado;
- matching tratado como verdade;
- score sobre dados incompletos;
- proxies excessivos;
- mistura de pessoa jurídica, marca e carrier;
- mistura de domínios de produto;
- frontend recalculando metodologia;
- falsa precisão de uma ordem total;
- jargão antes da resposta humana;
- processos manuais permanentes;
- artifacts experimentais vazando para produto.

O próximo estágio é:

```text
AUDITAR
→ RECONSTRUIR ARTIFACTS AFETADOS
→ CONSOLIDAR WORKFLOWS
→ AUTOMATIZAR PUBLICAÇÃO
→ AUDITAR FRONTEND
→ POLIR UX/SEO
→ LIMPAR LEGACY/EXPERIMENTOS
→ VALIDAR STAGING
→ SOMENTE DEPOIS PREPARAR MERGE/PRODUÇÃO
```

---

## Licença e uso

Este repositório é mantido pela Sanida Corretora de Seguros.

Dados de terceiros permanecem sujeitos às condições, limitações e responsabilidades de suas respectivas fontes.

A metodologia Sanida é uma interpretação própria de dados públicos e não deve ser apresentada como nota, classificação ou certificação oficial da SUSEP.
