# Ranking de Seguradoras Sanida — Pipeline de Dados e Metodologia v2

> **Status do projeto:** refatoração metodológica e arquitetural em andamento.  
> **Branch de trabalho:** `refactor/v2-data-foundation`.  
> **PR:** #1 permanece **Draft**.  
> **Marco atual (2026-08-26):** identidade, classificação regulatória, lifecycle jurídico, relationships, elegibilidade formal, evidência financeira e de Conduta estão implementados em draft. Os contratos de sinal **Financeiro** e **Conduta** estão fechados sem score. A calibração entre pilares concluiu **Stage 1, auditoria de cobertura, Stage 2 e o contrato semântico de avaliação**. A matriz não compensatória está formalizada para linguagem pública: **85/157** seguradoras possuem suporte semântico para avaliação conjunta completa e **72/157** permanecem como avaliação conjunta incompleta, preservando os sinais disponíveis de cada pilar. Isso **não abre** `assessment_eligible` nem `ranking_eligible`; ambos continuam em `0`.  
> **Próximo gate:** `assessment_eligibility_contract`. A ferramenta só poderá abrir elegibilidade formal de avaliação após separar, de maneira executável, completude semântica, confiança/evidência e condições de publicação. Ranking permanece bloqueado.  
> **Regra de segurança:** nada nesta branch deve ser tratado como score ou ranking final enquanto os gates formais de avaliação, representatividade e ranking não forem concluídos.

Este README é o **contrato de projeto**, o guia de implementação da v2 e o registro das decisões metodológicas já tomadas. Os fechamentos e calibrações específicas estão documentados em:

- `docs/financial-methodology-closure.md`;
- `docs/conduct-methodology-closure.md`;
- `docs/cross-pillar-calibration-stage-1.md`;
- `docs/cross-pillar-architecture-stage-2.md`;
- `docs/cross-pillar-assessment-contract-preflight.md`;
- `docs/cross-pillar-assessment-semantic-contract.md`.

Regras marcadas como **EM CALIBRAÇÃO**, **EXPERIMENTAL** ou **PENDENTE** não podem ser convertidas silenciosamente em scoring.

---

## 1. Objetivo

O projeto mantém a camada de dados e inteligência da ferramenta pública da Sanida para consulta de seguradoras, marcas, participantes do Sandbox e outras entidades relacionadas ao mercado de seguros.

A ferramenta deve ajudar o consumidor a responder perguntas como:

- **Esta seguradora é confiável?**
- **Ela apresenta sinais financeiros compatíveis com sustentar seus compromissos?**
- **Há atrito com consumidores acima do que seria esperado para seu porte e perfil de operação?**
- **Os problemas parecem episódicos ou persistentes?**
- **O nome pesquisado é realmente uma seguradora?**
- **Qual é a entidade regulada por trás de determinada marca?**
- **A empresa opera em regime ordinário, Sandbox ou outra condição regulatória?**

A complexidade deve ficar no backend. A apresentação pública deve ser progressiva:

```text
resposta rápida
    ↓
justificativa resumida
    ↓
indicadores
    ↓
fontes e períodos
    ↓
metodologia completa
```

---

## 2. Princípio central

A v2 segue esta ordem:

```text
identidade correta
→ classificação correta
→ lifecycle e relationships
→ elegibilidade
→ evidência
→ comparabilidade
→ calibração
→ avaliação
→ ranking
```

A ordem inversa — produzir nota primeiro e procurar justificativa depois — não é aceita.

A regra de projeto permanece:

> **identidade correta → dado correto → período correto → comparação correta → conclusão útil → explicação transparente.**

---

## 3. Escopo do repositório

O repositório é responsável por:

- ingestão e cache de fontes;
- normalização;
- identidade canônica;
- classificação regulatória;
- lifecycle jurídico;
- relações de marca, sucessão e grupo;
- elegibilidade;
- evidência financeira e de Conduta;
- experimentos metodológicos;
- auditoria;
- testes;
- artifacts/JSONs derivados;
- futura publicação da API v2.

O frontend público não pertence à arquitetura-alvo do repositório. A página da Sanida será construída em PHP/HTML/CSS/JavaScript e deverá receber dados semanticamente prontos.

O frontend **não pode reconstruir**:

- score;
- ranking;
- matching;
- classificação regulatória;
- elegibilidade;
- regras de relacionamento;
- fórmulas metodológicas.

---

## 4. Arquitetura-alvo

```text
FONTES OFICIAIS / PÚBLICAS
    │
    ├── SUSEP / SES
    ├── SUSEP — licenciadas
    ├── SUSEP — regimes especiais
    ├── SUSEP — Sandbox
    ├── Receita Federal — CNPJ
    ├── BDR / SusepCon
    └── Consumidor.gov
    │
    ▼
COLETA + CACHE + VALIDAÇÃO
    │
    ▼
IDENTIDADE / CLASSIFICAÇÃO / LIFECYCLE
    │
    ▼
RELATIONSHIPS E MARCAS
    │
    ▼
ELEGIBILIDADE
    │
    ▼
EVIDÊNCIA FINANCEIRA + EVIDÊNCIA DE CONDUTA
    │
    ▼
COMPARABILIDADE
    │
    ▼
CALIBRAÇÃO
    │
    ▼
AVALIAÇÃO
    │
    ▼
RANKING
    │
    ▼
JSONs PÚBLICOS v2
```

---

## 5. Princípios metodológicos aprovados

### 5.1. Identidade vem antes da nota

Nome comercial não é chave primária.

Para registros regulatórios SUSEP/SES:

```text
entity_id = fip:XXXXXX
```

O CNPJ identifica a pessoa jurídica:

```text
legal_entity_id = cnpj:<CNPJ>
```

A Receita Federal funciona como dimensão jurídica/cadastral separada e não sobrescreve o status regulatório da SUSEP.

### 5.2. A busca é ampla; o ranking é restrito

A busca pode localizar:

- seguradora ordinária;
- participante Sandbox;
- marca;
- plataforma;
- corretora;
- ressegurador;
- previdência;
- capitalização;
- entidade histórica;
- entidade em regime especial;
- outras entidades identificáveis.

Isso não significa que todas possam receber nota ou disputar o ranking ordinário.

### 5.3. Dado ausente nunca vale zero

```text
ausência de dado ≠ desempenho ruim
```

### 5.4. Marcas não herdam score

Uma marca pode resolver para um risk carrier, mas a marca não se transforma na pessoa jurídica avaliada.

### 5.5. Fuzzy matching não decide identidade

Fuzzy matching e heurísticas podem localizar candidatos. A decisão final exige evidência determinística ou documental.

### 5.6. Porte não gera mérito por si só

Prêmio, patrimônio, market share e tamanho de grupo podem contextualizar a empresa, mas não geram pontos automaticamente.

### 5.7. Ranking é consequência

```text
indicadores
→ avaliação
→ elegibilidade
→ coorte
→ ordenação
→ posição
```

---

## 6. Universo regulatório e elegibilidade

A v2 separa três gates:

```text
regulatory_universe_eligible
→ assessment_eligible
→ ranking_eligible
```

Snapshot atual da branch:

```text
identidades materializadas                 ~490
seguradoras ordinárias atuais                157
regulatory_universe_eligible                 157
assessment_eligible                            0
ranking_eligible                               0
Sandbox no universo ordinário                  0
regimes especiais no universo ordinário        0
```

**157 não é constante metodológica.** É o snapshot atual derivado das fontes. Uma nova seguradora licenciada pode entrar no universo regulatório antes de possuir histórico suficiente; nesse caso, os gates posteriores permanecem fechados.

---

## 7. Classificação regulatória

A fonte corrente de licenciadas é o serviço oficial SUSEP.

A classificação distingue, entre outros:

- `insurer`;
- `open_pension_entity`;
- `capitalization_company`;
- resseguradores;
- Sandbox;
- regimes especiais.

`LISTAEMPRESAS.csv` funciona como ponte auxiliar entre FIP, CNPJ e nome. Não define sozinho o universo atual.

Presença em fluxo SES significa presença de dado/atividade, não necessariamente licença atual.

---

## 8. Lifecycle jurídico — Receita Federal

A Receita é usada para cross-check cadastral da pessoa jurídica.

Regra:

```text
SUSEP  → licença, tipo, regime e status regulatório
Receita → situação cadastral/lifecycle jurídico
```

Contradições materiais devem interromper o pipeline para investigação.

A validação bulk já resolveu integralmente o conjunto-alvo utilizado na fundação da v2. O snapshot filtrado evita armazenar o cadastro nacional inteiro.

---

## 9. Relationships, grupos e marcas

`incorporated_into` só é materializado com evidência explícita.

Grupo econômico ou semelhança de nome **não prova sucessão**.

O backend preserva:

- sucessões verificadas;
- cadeia de sucessores;
- grupos econômicos observados;
- marcas;
- relações `risk_carrier`;
- contextos de consulta.

Buckets genéricos de grupo como `INDEPENDENTE` ou `OUTROS GRUPOS` não criam falsa relação societária.

---

# PILAR ECONÔMICO-FINANCEIRO

## 10. Arquitetura financeira

A pergunta é:

> **A seguradora apresenta sinais financeiros e operacionais compatíveis com a capacidade de honrar compromissos e sustentar sua atividade?**

O contrato de sinal Financeiro está fechado em `docs/financial-methodology-closure.md`.

| Dimensão | Referência | Papel |
|---|---|---|
| Capital regulatório | PLA/CMR | eixo principal |
| Liquidez | ILT | eixo principal |
| Liquidez corrente | ILC | diagnóstico complementar |
| Operação | ICA/IC | filme longitudinal |
| Rentabilidade | ILPL | diagnóstico apenas |

Não há score interno, média ponderada ou bônus crescente por magnitude.

---

## 11. Competência financeira madura

A v2 distingue:

```text
última competência observada
!=
última competência financeira madura
```

Diagnóstico real de PLA/CMR derivável:

```text
202601  148
202602  151
202603  153
202604  155
202605  155  ← competência madura
202606  130  ← observada, porém imatura
```

A competência selecionada foi `2026-05`.

No período maduro:

```text
PLA/CMR derivável   155/157
ILC derivável       156/157
ILT derivável       156/157
```

CMR zero, ausência ou evidência inutilizável não viram desempenho zero.

---

## 12. Capital regulatório — PLA/CMR

Decisões fechadas:

- PLA/CMR < 1 significa insuficiência de capital observada frente ao CMR na competência de referência;
- PLA/CMR >= 1 atende ao requisito observado;
- magnitude acima de 1 não gera mérito linear ou tiers positivos arbitrários;
- histórico curto afeta confiança, não desempenho;
- porte absoluto não gera pontos.

Estados:

```text
capital_below_cmr
capital_meets_or_exceeds_cmr
capital_signal_unavailable
```

Não afirmar insolvência automaticamente a partir de PLA/CMR < 1.

---

## 13. Liquidez — ILT e ILC

A investigação selecionou:

- **ILT** como principal referência de liquidez;
- **ILC** como diagnóstico complementar.

ILT mostrou estabilidade temporal superior e informação parcialmente distinta de PLA/CMR.

A referência `1,0` do ILT é paridade aritmética, não threshold prudencial oficial da SUSEP.

Estados:

```text
ilt_below_arithmetic_parity
ilt_at_or_above_arithmetic_parity
ilt_signal_unavailable
```

Valores extremos não recebem recompensa crescente.

---

## 14. Combinação Financeira e filme operacional

Capital e liquidez usam lógica não compensatória:

```text
capital_requirement_shortfall_observed
capital_requirement_met_with_liquidity_pressure
core_indicators_without_current_shortfall
core_financial_signal_unavailable
```

Capital excedente não apaga pressão de liquidez; liquidez alta não apaga insuficiência de capital.

ICA/IC permanecem filme longitudinal:

```text
balanced_persistent
improved
recent_pressure
persistent_pressure
indeterminate
```

O filme operacional **não é um terceiro score financeiro bruto** e não sobrescreve capital/liquidez.

---

## 15. Resultado do fechamento Financeiro

```text
core_indicators_without_current_shortfall              120
capital_requirement_met_with_liquidity_pressure         21
capital_requirement_shortfall_observed                  14
core_financial_signal_unavailable                        2
```

Confiança:

```text
established_core_history    143
limited_core_history         12
insufficient_core_evidence    2
```

Filme operacional:

```text
balanced_persistent 90
improved            13
recent_pressure      7
persistent_pressure 14
indeterminate       33
```

Workflow `V2 Financial Methodology Closure`, run `33003042769`: Ruff verde, 3/3 testes, build real, fronteiras e artifact verdes (artifact `9619421688`).

Esses estados são contratos de evidência e linguagem, não score nem ranking.

---

# PILAR DE CONDUTA

## 16. Pergunta central e fechamento

Conduta não responde:

> “Qual empresa tem mais reclamações?”

A pergunta correta é:

> **Esta seguradora apresenta atrito com consumidores acima do que seria esperado para o tamanho e o perfil de sua operação? E esse sinal é robusto e persistente?**

O contrato de sinal de Conduta está fechado em `docs/conduct-methodology-closure.md`, sem score numérico.

A lógica responde em camadas:

1. há reclamações demais para o tamanho da operação?;
2. há evidência suficiente para confiar na diferença?;
3. o sinal é persistente ou episódico?;
4. há melhora, deterioração ou nenhuma mudança clara?;
5. o mix de carteira exige cautela?;
6. a satisfação possui amostra suficiente para interpretação?

Remediação **não é inferida** com o P3 atual, porque resposta/finalização não provam solução e o denominador avaliado não está preservado com robustez suficiente.

---

## 17. Cascata de fontes de Conduta

Implementado em:

`api/v2/conduct_source_cascade.py`

Prioridade:

```text
P1  BDR / SusepCon atual e publicamente consumível
 ↓
P2  Consumer.gov Base Completa autêntica + SES
 ↓
P3  Consumer.gov core preservado + SES
 ↓
P0  evidência de Conduta indisponível
```

Uma fonte só passa pelo gate se for:

- atual;
- pública;
- estruturada;
- consumível;
- com cobertura suficiente.

Regra obrigatória:

```text
series_policy = no_cross_source_stitching
```

Séries de fontes diferentes não podem ser costuradas como se fossem uma única série longitudinal.

---

## 18. Estado real das fontes de Conduta

### 18.1. P1 — BDR / SusepCon

BDR existe e recebe dados em 2026, mas isso não prova disponibilidade pública dos bytes brutos para análise automatizada.

O SusepCon público observado permanece congelado no 4º trimestre de 2025.

Enquanto a fonte não for atual, pública, estruturada e consumível, P1 não pode assumir o pipeline.

### 18.2. P2 — Consumer.gov Base Completa

A Base Completa foi processada historicamente e é mais rica que os arquivos de reclamações finalizadas.

O host antigo ficou indisponível.

Arquivos `finalizadas_YYYY-MM.zip` **não podem substituir silenciosamente a Base Completa**, pois representam população diferente.

Logo P2 permanece indisponível enquanto os bytes autênticos da Base Completa não forem recuperáveis.

### 18.3. P3 — Consumer.gov core preservado + SES

É a estratégia prática atual.

O período preservado usado na investigação é:

```text
2025-07 → 2026-06
```

O core conserva por mês:

- reclamações;
- respondidas;
- finalizadas;
- campos de resolução que realmente existam no agregado;
- quantidade de avaliações de satisfação;
- soma das notas.

Ele não inventa denominadores ausentes.

---

## 19. Identidade Consumer.gov

A fundação de identidade já foi levada a alto nível de resolução sem fuzzy decisório.

Experimento de referência sobre 90.332 reclamações:

```text
matched_current_insurer   82.423
outside_current_universe   5.872
ambiguous                  1.995
unresolved                    42
```

Os 42 registros não resolvidos permanecem sem atribuição.

Regras:

- CNPJ e evidência determinística têm prioridade;
- Receita pode sustentar exclusão inequívoca de não-seguradora;
- Receita não concede licença;
- corretora, plataforma, varejista ou canal não transfere reclamações automaticamente para uma seguradora;
- wrappers multiempresa e homônimos permanecem ambíguos quando necessário.

---

## 20. Filme Consumer.gov

A branch preserva filme longitudinal de 12 meses sem score.

Sinais disponíveis no contrato de Conduta incluem:

- cobertura temporal;
- pressão anual normalizada;
- credibilidade estatística;
- persistência;
- tendência;
- satisfação sample-aware;
- contexto de mix de carteira.

Alta taxa de resposta ou finalização **não equivale automaticamente a boa resolução**.

A interpretação deve evitar inferências de intenção.

---

## 21. Erro metodológico identificado e invalidado

A primeira tentativa de `Conduct Comparative Calibration` combinou:

```text
prêmio direto de seguros
+
contribuições de previdência privada
```

Esse desenho foi **invalidado**.

PGBL, VGBL e Previdência Tradicional são tratados neste projeto como **previdência privada**, não como produção de seguros para o ranking de seguradoras.

Capitalização também permanece fora do denominador de seguros.

O workflow antigo foi preservado somente para reprodutibilidade histórica:

`V2 Conduct Comparative Calibration (legacy invalidated)`

Ele não pode ser usado para:

- score;
- ranking;
- elegibilidade;
- calibração atual.

---

## 22. Exposição econômica de seguros — regra fechada

O reader atual é:

`api/sources/susep_insurance_exposure.py`

Ele lê **somente**:

`Ses_seguros.csv`

Campos mínimos:

```text
damesano
coenti
coramo
premio_direto
premio_ganho
```

Contrato:

```text
exposure_domain = insurance_only
approved_operational_denominator = insurance_premium_direct
diagnostic_companion = insurance_premium_earned
```

A aprovação de `premio_direto` é restrita a `direct_one_to_one_candidate`. Se a conclusão muda materialmente quando `premio_ganho` é usado como lente diagnóstica, a conclusão direcional fica bloqueada.

Explicitamente excluídos:

```text
Ses_Contrib_Benef.csv  → previdência privada
Ses_Dados_Cap.csv      → capitalização
```

Prêmio não é número de clientes ou de apólices.

---

## 23. Conduta observada ≠ pressão comparável

Uma mudança arquitetural importante foi separar:

```text
conduct_evidence_state
```

de:

```text
pressure_comparability_state
```

Uma empresa pode continuar pesquisável e ter evidência de Conduta mesmo quando não é possível normalizar suas reclamações por uma exposição comparável.

Isso evita que grandes marcas desapareçam do widget apenas porque marca, provider Consumer.gov e risk carrier não coincidem 1:1.

---

## 24. Reconciliation audit — resultado real

Artifact:

`v2_conduct_coverage_reconciliation`

Workflow:

`V2 Conduct Coverage Reconciliation`

Resultado real:

```text
universo regulatório                              157

Conduta observada                                 127
sem reclamações observadas                         30

candidatas a pressão 1:1 por prêmio direto        103
pressão indisponível por enquanto                  54
```

Estados de cobertura do widget:

```text
conduct_observed_pressure_candidate                83
conduct_observed_pressure_unavailable              44
no_observed_complaints_pressure_candidate          20
identity_financial_context_pressure_unavailable    10
```

Os 157 permanecem materializados. A indisponibilidade da pressão não apaga a entidade do produto.

---

## 25. Principais motivos de pressão indisponível

Diagnóstico real:

```text
hybrid_insurance_pension_requires_product_numerator          26
no_current_insurance_activity_observed                       11
no_current_insurance_activity_observed_pension_activity       5
negative_direct_premium_requires_accounting_review             3
shared_consumer_subject_requires_product_split                 2
consumer_subject_single_carrier_exposure_not_brand_specific    1
multi_carrier_subject_requires_product_split                   1
portfolio_transfer_requires_temporal_reconciliation            1
portfolio_transfer_counterparty_requires_temporal_reconciliation 1
shared_exposure_with_external_consumer_subject                 1
runoff_pressure_not_applicable                                 1
no_positive_insurance_premium_observed                         1
```

Esses estados não são “exclusões do widget”. São **rotas de reconciliação**.

---

## 26. Seguradoras híbridas seguros + previdência

No P3 atual, o provider Consumer.gov pode trazer reclamações da pessoa jurídica sem taxonomia de produto suficiente.

Se a entidade opera simultaneamente:

```text
seguros + previdência
```

não é metodologicamente válido dividir todas as reclamações apenas pelo prêmio de seguros.

Estado:

```text
hybrid_insurance_pension_requires_product_numerator
```

Isso afeta players relevantes.

A solução não é somar contribuições de previdência ao denominador. A solução é recuperar, quando possível, **numerador separado por produto**.

---

## 27. Consumer-facing subject e risk carrier

A unidade de análise deixou de pressupor que:

```text
provider Consumer.gov
=
pessoa jurídica pesquisada
=
risk carrier
=
entidade que recebe a produção
```

Quando necessário, o projeto separa:

```text
consumer-facing subject
↔ relacionamento documentado
↔ risk carrier(s)
```

Essas relações fornecem contexto e rotas de recuperação; **não transferem reclamações nem criam exposição automaticamente**.

Registro:

`data/reference/v2/conduct_subject_relationships.json`

---

## 28. Casos especiais documentados

### 28.1. Youse → Caixa Seguradora

A Youse possui relação documentada com Caixa Seguradora como risk carrier.

Política atual:

```text
brand_specific_exposure_required
```

As reclamações da Youse não são divididas automaticamente pelo prêmio total da Caixa.

A Caixa também não pode parecer artificialmente melhor se parte das reclamações do negócio estiver registrada no subject Youse.

### 28.2. Zurich Brasil → Zurich Minas

A transferência integral de carteira teve efeito em `2026-04-01`.

Estado:

```text
temporal_reconciliation_required
```

Reclamações e produção precisam ser reconciliadas temporalmente antes de qualquer pressão comparativa.

### 28.3. Bradesco Seguros → Auto/RE + Vida

A carteira da Bradesco Seguros foi dividida entre Bradesco Auto/RE e Bradesco Vida e Previdência.

Reclamações genéricas de Bradesco Seguros não podem ser rateadas sem separação de produto.

### 28.4. Seguradora Líder / DPVAT

O caso é tratado como `runoff`.

Pressão corrente por prêmio não é aplicável da mesma forma que em uma seguradora ordinária em produção normal.

---

# SANDBOX E MARCAS

## 29. Sandbox permanece fora do ranking ordinário

Participantes Sandbox podem ser pesquisados e ter contexto regulatório e de Conduta.

Eles não entram:

- nas 157 seguradoras ordinárias;
- no baseline ordinário;
- no score ordinário;
- no ranking ordinário.

Regra:

```text
ordinary_ranking_effect = none
```

---

## 30. Conduta Sandbox

Workflow:

`V2 Sandbox Brand Conduct Evidence`

A camada preserva Conduta de participantes Sandbox que apareçam no Consumer.gov, sem transferir seus dados para seguradoras ordinárias.

Execução real:

```text
participantes Sandbox materializados             12
com reclamações observadas                        6
reclamações Sandbox resolvidas no artifact     1.510
contextos de marca verificados                     1
```

Nada desse artifact pode produzir score ou `pressure_ratio`.

---

## 31. Loovi ↔ LTI Seguros

Caso implementado:

```text
brand:loovi
→ risk_carrier
→ LTI Seguros S.A.
```

LTI:

```text
CNPJ 47.006.254/0001-80
regime = sandbox
```

A marca Loovi existe tanto no registro específico Sandbox quanto no registro canônico de marcas da v2.

Aliases verificados incluem:

- Loovi Seguros;
- Loovi Technology.

A relação não transforma Loovi em seguradora nem transfere score.

---

## 32. Evidência real LTI / contexto Loovi

Período:

```text
2025-07 → 2026-06
```

Consumer.gov preservado para LTI:

```text
reclamações                   1.329
respondidas                   1.286
taxa de resposta             96,76%
finalizadas                   1.329
taxa de finalização          100,0%
avaliações de satisfação       619
satisfação média             ~2,645/5
meses com reclamações         12/12
```

Satisfação:

```text
primeira metade   ~2,664  (n=292)
segunda metade    ~2,627  (n=327)
direção           stable
```

O provider original é:

```text
LTI Seguros
```

Portanto a linguagem pública deve preservar a atribuição:

> O Consumer.gov registra reclamações contra a LTI Seguros, seguradora Sandbox vinculada aos seguros comercializados pela Loovi.

Não afirmar automaticamente:

> “A Loovi teve 1.329 reclamações.”

---

## 33. Guardrails de marca

O resolvedor deve distinguir marca genérica da entidade securitária específica.

Exemplos:

```text
Sicoob  ≠ Sicoob Seguradora de Vida e Previdência S.A.
Crefisa ≠ Crefisa Seguros S.A.
Loovi   ≠ LTI Seguros S.A.
```

Nome genérico sozinho não pode virar carrier por similaridade textual.

---

# COMPARABILIDADE DE CONDUTA

## 34. RppA como precedente conceitual

O SusepCon utiliza a ideia de reclamações ponderadas pela arrecadação.

Isso sustenta:

```text
observed complaints / expected complaints
```

ou, de forma equivalente:

```text
complaint share / premium share
```

Mas isso é **pressão relativa de reclamações**, não percentual de clientes que reclamam.

---

## 35. Population alignment e alinhamento temporal

A população de reclamações e exposição deve ser idêntica em cada baseline comparável.

Regra:

```text
complaints_and_exposure_same_entities_only
```

Além disso, o esperado anual é soma dos esperados mensais:

```text
expected_m = reclamações_mercado_m × prêmio_entidade_m / prêmio_mercado_m
expected_12m = Σ expected_m
observed_12m = Σ reclamações apenas nos meses comparáveis
```

Reclamações de meses sem exposição comparável permanecem como evidência, mas não são forçadas para a razão normalizada daquele mês.

---

## 36. Pequenas amostras e credibilidade — decisão fechada

Razão bruta não é conclusão.

A pressão `observed / expected` usa intervalo exato e proteção contra múltiplas comparações no universo comparável.

Decisão atual:

```text
shrinkage = não selecionado
Empirical Bayes = não selecionado
```

A função desta camada é bloquear afirmações frágeis, não produzir uma magnitude suavizada para score.

---

## 37. Mix de carteira — investigação encerrada nesta etapa

O SES preserva `coramo` e prêmio direto por ramo.

O diagnóstico real encontrou associação positiva fraca entre distância de carteira e diferença de pressão e mostrou que peers realmente próximos são escassos.

Decisão:

```text
portfolio_adjustment = false
peer_groups_selected = false
distance_threshold_selected = false
```

`coramo` permanece contexto e diagnóstico de sensibilidade. Ausência de peer nunca significa neutralidade.

---

## 38. Persistência, tendência, satisfação e remediação

O contrato distingue:

- pressão persistente;
- pressão episódica/esparsa;
- melhora;
- deterioração;
- ausência de diferença clara;
- evidência temporal insuficiente.

Uma conclusão anual exige ao menos 9 meses comparáveis em 12. Persistência exige repetição da direção com evidência em pelo menos metade dos meses comparáveis. Tendência compara as duas metades da janela.

Satisfação é sample-aware e não corrige a incidência de reclamações.

Remediação não é estabelecida pelo P3 atual.

---

# CALIBRAÇÃO ENTRE PILARES

## 39. Stage 1 — medir ordem antes de escolher pesos

Financeiro e Conduta possuem contratos fechados, mas não scores internos.

O Stage 1 responde:

> **O que os dois pilares conseguem afirmar juntos sem permitir que um sinal forte esconda um problema material ou que ausência de evidência vire neutralidade?**

Artifact:

`v2_cross_pillar_calibration_diagnostic`

Workflow:

`V2 Cross-Pillar Calibration Stage 1`

Contrato completo:

`docs/cross-pillar-calibration-stage-1.md`

Resultado de cobertura conjunta:

```text
universo regulatório                    157
conclusão central nos dois pilares       85
conclusão conjunta ainda indisponível    72
```

Os 72 não são tratados como neutros nem recebem imputação.

---

## 40. Coordenadas ordinais — diagnóstico apenas

Para estudar ordenabilidade sem fabricar score:

```text
Financeiro
F0 = sem insuficiência central atual
F1 = capital atendido + pressão de liquidez
F2 = insuficiência de capital observada

Conduta
C0 = abaixo do esperado OU sem diferença clara
C1 = acima do esperado com evidência suficiente
```

`below_expected` e `not_distinguishable` permanecem juntos em `C0` porque menos reclamações que o esperado não prova atendimento superior.

Entre as 85 conclusivas:

```text
F0|C0   46
F0|C1   14
F1|C0    8
F1|C1    8
F2|C0    5
F2|C1    4
```

Todos os seis grupos possuem empates.

---

## 41. Ordenabilidade e Pareto

Pareto é usado como auditoria de dominância, não como ranking público.

```text
fronteira 1   46
fronteira 2   22
fronteira 3   13
fronteira 4    4
```

A primeira fronteira contém 54,1% das 85 empresas conclusivas.

Entre 3.570 pares:

```text
estritamente ordenáveis    60,22%
empatados                   33,56%
incomparáveis                6,22%
```

Conclusão:

> **os contratos fechados não determinam uma ordem total 1–85.**

Uma ordem total exigiria nova regra normativa de mérito ou prioridade entre pilares.

---

## 42. Stage 2 — arquitetura de avaliação concluída

O Stage 2 comparou arquiteturas sem transformar preferência normativa em matemática.

Candidato líder validado:

```text
noncompensatory_state_matrix_with_adverse_qualifiers
```

Estados atuais:

```text
no_current_core_adverse_signal                 46
conduct_pressure_only                          14
liquidity_pressure_only                         8
liquidity_and_conduct_pressure                  8
capital_shortfall_without_conduct_pressure      5
capital_shortfall_and_conduct_pressure          4
evidence_incomplete_for_joint_assessment       72
```

Decisões:

```text
continuous_weighted_score_selected          false
lexicographic_total_order_selected           false
pareto_front_number_selected_as_public_tier  false
capital_gate_total_order_selected            false
```

O Stage 2 confirmou 222 pares de trade-off normativo. Portanto, uma ordem total não emerge automaticamente dos contratos fechados.

A matriz é não compensatória: identifica **onde** existe sinal adverso, sem criar taxa de câmbio entre capital, liquidez e Conduta.

---

## 43. Auditoria de representatividade

Workflow:

`V2 Cross-Pillar Coverage Audit`

Universo 12m:

```text
prêmio direto positivo   R$ 210,502 bilhões
reclamações mapeadas            82.423
```

As 85 com conclusão conjunta representam:

```text
69,94% do prêmio direto positivo
54,40% das reclamações
```

As 72 sem conclusão conjunta representam:

```text
30,06% do prêmio direto positivo
45,60% das reclamações
```

Portanto:

> **a população atual de 85 não pode ser chamada de ranking integral do mercado.**

O principal gargalo é a não comparabilidade estrutural de Conduta.

Os 54 casos não comparáveis concentram:

```text
28,37% do prêmio positivo
44,37% das reclamações
```

As 12 empresas com cobertura temporal insuficiente representam apenas cerca de 0,04% do prêmio positivo e as cinco sensíveis ao denominador cerca de 1,65%.

---

## 44. Prioridade de recuperação de cobertura

Rotas de maior materialidade atual:

```text
híbridas seguros + previdência
26 entidades → 16,14% do prêmio / 28,55% das reclamações

shared Consumer subject / product split
2 entidades → 6,92% do prêmio / 10,09% das reclamações

transferência Zurich
contraparte → ~3,92% do prêmio

shared exposure com subject externo
~1,40% do prêmio
```

Resolver esses casos não garante conclusão de pressão; apenas recupera comparabilidade potencial para que os gates estatísticos possam então ser aplicados corretamente.

Somente as dez maiores entidades sem conclusão conjunta concentram aproximadamente 25,92% de todo o prêmio positivo do universo.

---

## 45. Contrato semântico de avaliação — FECHADO

O preflight de linguagem pública foi transformado em contrato executável e validado contra as 157 seguradoras.

Artifact:

```text
v2_cross_pillar_assessment_semantic_contract
```

Workflow:

```text
V2 Cross-Pillar Assessment Semantic Contract
```

Pergunta humana:

> **O que os sinais disponíveis permitem dizer ao consumidor sobre esta seguradora, em linguagem útil, sem transformar evidência parcial em garantia?**

Ordem pública obrigatória:

```text
leitura geral
→ sinais encontrados
→ por que isso importa
→ qualificadores
→ números e metodologia
→ limites da conclusão
→ confiança e cobertura
```

Classes públicas atuais:

```text
Leitura central favorável      46
Atenção                        30
Alerta prudencial               9
Avaliação conjunta incompleta  72
TOTAL                          157
```

Para as 85 entidades com os dois núcleos conclusivos, existe **suporte semântico para avaliação pública individual conjunta**.

Isso não equivale a elegibilidade formal:

```text
semantic_public_assessment_supported  85
assessment_eligible                    0
ranking_eligible                       0
```

A distinção é obrigatória.

### 45.1. Avaliação incompleta não esconde sinal disponível

Entre as 72 avaliações conjuntas incompletas, o contrato encontrou:

```text
5 com alerta prudencial de capital
5 com atenção à liquidez
```

Logo:

```text
joint assessment incomplete != suppress available pillar evidence
```

A interface deve informar a incompletude conjunta **e** preservar qualquer sinal utilizável de Financeiro ou Conduta.

### 45.2. Qualificadores de Conduta

Persistência e tendência públicas só podem aparecer quando a conclusão anual final é:

```text
above_expected_with_sufficient_evidence
```

Se a pressão final não está acima do esperado:

```text
persistence qualifier = null
trend qualifier       = null
```

Isso impede frases contraditórias como “sem pressão acima do esperado” acompanhada de chip “pressão piorando”.

### 45.3. Detalhe sem bônus

`below_expected` e `not_distinguishable` continuam juntos em `C0` para a matriz adversa, mas permanecem semanticamente distintos no cartão de Conduta.

Menos reclamações que o esperado pode ser descrito como resultado favorável **para esse indicador**, sem virar bônus de qualidade ou score.

---

# CONFIANÇA, SCORE E RANKING

## 46. Score e gates continuam bloqueados

Atualmente:

```text
semantic_public_assessment_supported = 85
assessment_eligible                  = 0
ranking_eligible                     = 0
```

O contrato semântico **não** abre gates formais.

A avaliação geral depende de:

```text
Financeiro
+
Conduta
+
qualidade/confiança da evidência
+
comparabilidade
+
representatividade suficiente para a alegação pública feita
```

Não haverá redistribuição silenciosa de peso quando um pilar estiver indisponível.

---

## 47. Confiança da avaliação

`score` e `assessment_confidence` são conceitos diferentes.

Confiança deve considerar, entre outros:

- identidade;
- atualidade;
- cobertura;
- histórico;
- consistência;
- amostra;
- comparabilidade;
- qualidade do relacionamento entre subject e carrier.

Confiança não deve ser usada como desempenho nem para maquiar ausência de metodologia.

---

## 48. Ranking

O ranking final, se a calibração demonstrar que ele é defensável, deve conter apenas entidades:

- corretamente identificadas;
- do universo definido;
- com avaliação completa;
- comparáveis dentro da coorte;
- avaliadas pela mesma versão metodológica;
- acompanhadas de disclosure de cobertura coerente com a alegação pública.

Preferir linguagem como:

> 8ª entre 41 seguradoras elegíveis nesta comparação

e não:

> 8ª melhor seguradora do Brasil

quando a segunda frase não puder ser sustentada.

A população atual de 85 semanticamente completas **não** pode ser descrita como ranking integral do mercado.

---

# FONTES

## 49. Hierarquia de autoridade

```text
FIP / licença / tipo / regime atual       → SUSEP
atividade / produção / financeiro         → SUSEP / SES
CNPJ e situação cadastral                 → Receita Federal
grupo econômico                           → SUSEP / SES
sucessão                                  → relação explicitamente verificada
marca / risk carrier                      → relação verificável
provider Consumer.gov                     → resolução determinística/documentada
Conduta comparativa                       → cascata de fontes + metodologia calibrada
avaliação conjunta                        → contratos fechados + calibração entre pilares
```

---

## 50. Regras de contingência

Cache e fallback existem para disponibilidade operacional, não para mudar a semântica da fonte.

Exemplos:

- timeout isolado da SUSEP não justifica novo substituto metodológico;
- `finalizadas` não substitui Base Completa;
- fonte mais antiga não se torna “atual” só porque está em cache;
- fontes P1/P2/P3 não são costuradas longitudinalmente.

Um timeout transitório do workflow de Classification foi resolvido por rerun sem alteração de metodologia.

---

# WORKFLOWS

## 51. Validações relevantes da v2

A branch possui, entre outros:

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
- `V2 Cross-Pillar Assessment Semantic Contract`.

O antigo:

`V2 Conduct Comparative Calibration (legacy invalidated)`

é somente manual e histórico.

---

## 52. Validação específica da calibração conjunta

### Cross-Pillar Calibration Stage 1

```text
run                  33013794644
Ruff                 verde
testes               3/3
build real           verde
boundaries           verdes
artifact             9623644844
```

### Cross-Pillar Coverage Audit

```text
run                  33014199915
Ruff                 verde
testes               3/3
build real           verde
boundaries           verdes
artifact             9623805832
```

### Cross-Pillar Architecture Stage 2

```text
run                  33015231566
Ruff                 verde
testes               3/3
build real           verde
boundaries           verdes
artifact             9624261105
```

### Cross-Pillar Assessment Semantic Contract

```text
run                  33021494915
job                  98352686525
Ruff                 verde
testes               4/4
build real           verde
universo             157/157
boundaries           verdes
artifact             9626706193
SHA256 ZIP           1504ea96f132eff338bdcea3619884b397353cfa1cea43c42a594b347df6f514
```

Todos preservam score/ranking bloqueados conforme o escopo de cada artifact.

---

# API v2 E PUBLICAÇÃO

## 53. API pública — direção

Contratos públicos candidatos:

```text
/api/v2/meta.json
/api/v2/entities.json
/api/v2/brands.json
/api/v2/rankings.json
```

O schema final ainda não está congelado.

A API pública deve ser enxuta; artifacts de pesquisa e auditoria permanecem internos.

---

## 54. Proveniência

O backend deve preservar:

```text
automatic
derived
curated
unsupported
```

Tudo que altera score, elegibilidade ou situação regulatória deve ser automático ou derivado de fonte sustentável.

Curadoria pode:

- resolver marcas;
- registrar aliases;
- documentar sucessões;
- registrar relationships verificáveis.

Curadoria não pode:

- fabricar licença;
- transferir reclamações sem evidência;
- alterar números financeiros;
- atribuir pressão sem denominador comparável.

---

## 55. Validações obrigatórias

O pipeline deve falhar em situações como:

- `entity_id` duplicado;
- CNPJ incompatível duplicado;
- marca apontando para entidade inexistente;
- Sandbox vazando para ranking ordinário;
- pressão calculada para entidade que falhou no gate;
- previdência/capitalização vazando para exposição de seguros;
- reclamações de subject transferidas silenciosamente para carrier;
- fonte sem período;
- valores não finitos;
- queda anormal de cobertura;
- alteração inesperada de schema;
- score produzido por artifact que proíbe scoring;
- pilar ausente tratado como neutralidade;
- subset incompleto apresentado como ranking integral do mercado;
- avaliação conjunta incompleta ocultando alerta disponível em um pilar;
- qualificador adverso de Conduta exibido quando a conclusão anual não está acima do esperado;
- suporte semântico confundido com `assessment_eligible` formal.

---

# SEQUÊNCIA DE IMPLEMENTAÇÃO

## 56. Estado das fases

### Fundação regulatória — IMPLEMENTADA EM DRAFT

- identidade FIP/CNPJ;
- classificação oficial;
- regimes especiais;
- Sandbox;
- lifecycle Receita;
- grupos;
- relationships;
- marcas;
- elegibilidade regulatória.

### Financeiro — CONTRATO DE SINAL FECHADO

- financial evidence;
- maturidade de competência;
- PLA/CMR;
- ILT/ILC;
- filme operacional;
- ILPL rejeitado como eixo;
- combinação interna não compensatória;
- score interno não definido.

### Conduta — CONTRATO DE SINAL FECHADO

- identidade Consumer.gov;
- core mensal;
- source cascade;
- exposure reader insurance-only;
- reconciliation audit;
- marca/subject/carrier;
- population alignment;
- incerteza;
- persistência;
- tendência;
- mix de carteira contextual;
- satisfação sample-aware;
- remediação não estabelecida;
- score interno não definido.

### Calibração entre pilares — CONTRATO SEMÂNTICO FECHADO

- Stage 1: matriz real, ordenabilidade e Pareto diagnóstico;
- Coverage Audit: representatividade econômica e de reclamações;
- Stage 2: arquitetura não compensatória selecionada como base da avaliação;
- contrato semântico: linguagem pública e guardrails validados nas 157;
- 85 com suporte semântico para avaliação conjunta;
- 72 como avaliação conjunta incompleta, preservando sinais disponíveis;
- full-market ranking bloqueado;
- score e pesos não selecionados.

### Elegibilidade formal de avaliação — PRÓXIMO GATE

`assessment_eligibility_contract` deve decidir quando o suporte semântico se transforma em elegibilidade formal de avaliação.

### Score geral — BLOQUEADO

Só pode ser discutido se uma transformação numérica demonstrar agregar informação defensável além dos estados e sem violar os contratos fechados.

### Ranking — BLOQUEADO

`ranking_eligible` permanece `0`; cobertura e ordenação ainda não sustentam ranking integral de mercado.

### Schema/publicação/frontend — PENDENTES

Nenhuma regra v2 deve ser migrada ao frontend antes dos gates formais correspondentes.

---

## 57. O que não fazer agora

Não:

- escolher 50/50, 60/40 ou qualquer outro peso por conveniência;
- criar `financial_score` ou `conduct_score` apenas para viabilizar média;
- premiar excesso de PLA/CMR sem nova evidência metodológica;
- premiar ILT extremo;
- tratar `below_expected` como prova de melhor atendimento;
- usar confiança como desempenho;
- usar ICA/IC como bônus oculto;
- tratar pilar indisponível como neutro;
- abrir `assessment_eligible` sem o contrato formal de elegibilidade;
- abrir `ranking_eligible`;
- chamar as 85 semanticamente completas de ranking integral do mercado;
- produzir ranking 1–157;
- usar reclamações brutas como nota;
- usar prêmio como número de clientes;
- misturar previdência com seguros;
- misturar capitalização com seguros;
- transferir reclamações entre subject/carrier sem evidência;
- incluir Sandbox no benchmark ordinário;
- reabrir ILPL sem nova justificativa;
- aplicar fuzzy matching decisório;
- implementar scoring no PHP/JS;
- alterar `main`.

---

## 58. Próximo gate — Assessment Eligibility Contract

A próxima investigação deve transformar o suporte semântico já validado em uma política formal de elegibilidade, sem confundi-lo com ranking.

O contrato deve responder:

1. quais requisitos mínimos de identidade, atualidade, comparabilidade e confiança são obrigatórios para `assessment_eligible`?;
2. `joint_core_complete` é condição necessária e suficiente ou ainda exige gates adicionais de evidência?;
3. como tratar histórico financeiro limitado sem transformá-lo em desempenho ruim?;
4. como publicar avaliação conjunta completa quando um qualificador contextual estiver indisponível?;
5. quais falhas tornam a avaliação incompleta, e quais apenas reduzem confiança?;
6. como garantir que alertas materiais continuem visíveis mesmo quando `assessment_eligible = false`?;
7. como versionar elegibilidade quando a metodologia ou a fonte mudar?;
8. como separar `assessment_eligible` de `ranking_eligible` de forma estrutural e testável?;
9. qual disclosure de cobertura deve acompanhar comparações entre avaliadas?;
10. quais condições precisam ser satisfeitas antes de qualquer discussão de ranking público?

O contrato semântico atual já prova que:

```text
semantic_public_assessment_supported = 85
```

mas deliberadamente mantém:

```text
assessment_eligible = 0
ranking_eligible    = 0
```

A próxima etapa deve decidir se e em quais condições o primeiro gate pode ser aberto **sem abrir o segundo**.

---

## 59. Definição de sucesso da v2

A refatoração será considerada bem-sucedida quando:

- o consumidor puder pesquisar entidade ou marca sem conhecer a estrutura jurídica;
- a natureza da entidade for identificada corretamente;
- marcas relevantes forem resolvidas sem virar falsamente seguradoras;
- Sandbox puder ter inteligência própria sem contaminar o universo ordinário;
- seguradoras comparáveis forem avaliadas pela mesma metodologia;
- players difíceis não desapareçam apenas por estrutura societária complexa;
- ausência de dado não vire punição nem neutralidade;
- pressão de reclamações seja ajustada por exposição de forma defensável;
- pequenas amostras não dominem o resultado;
- persistência e tendência sejam distinguidas;
- a avaliação conjunta não esconda uma insuficiência material atrás de outro pilar;
- a cobertura da comparação seja transparente;
- o processo seja auditável;
- o frontend não corrija nem invente lógica;
- atualização editorial rotineira seja mínima;
- score só apareça quando a evidência realmente sustentar uma conclusão.

---

## 60. Diretriz final

A v2 não existe para reproduzir a lógica da v1.

Ela deve preservar o que a engenharia anterior fazia bem:

- automação;
- cache;
- snapshots;
- testes;
- rastreabilidade.

E substituir o que era conceitualmente frágil:

- universo mal delimitado;
- matching tratado como verdade;
- nota sobre dados incompletos;
- proxies excessivos;
- score sem comparabilidade;
- frontend acoplado à metodologia;
- mistura de entidades, marcas e carriers;
- mistura de domínios de produto;
- falsa precisão de uma ordem total quando os sinais só sustentam estados.

A próxima etapa é:

```text
Assessment Eligibility Contract
→ definir requisitos formais de avaliação
→ separar completude, confiança e desempenho
→ preservar alertas em casos inelegíveis
→ abrir assessment_eligible somente quando sustentado
→ manter ranking_eligible independente e bloqueado
→ somente depois discutir coortes, comparação ordinal e eventual ranking
```

---

## Licença e uso

Este repositório é mantido pela Sanida Corretora de Seguros.

Dados de terceiros permanecem sujeitos às condições, limitações e responsabilidades de suas respectivas fontes.

A metodologia Sanida é uma interpretação própria de dados públicos e não deve ser apresentada como nota, classificação ou certificação oficial da SUSEP.
