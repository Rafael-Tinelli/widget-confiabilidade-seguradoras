# Ranking de Seguradoras Sanida — Pipeline de Dados e Metodologia v2

> **Status do projeto:** refatoração metodológica e arquitetural em andamento.  
> **Branch de trabalho:** `refactor/v2-data-foundation`.  
> **PR:** #1 permanece **Draft**.  
> **Marco atual (2026-08-26):** identidade, classificação regulatória, lifecycle jurídico, relationships, elegibilidade formal, evidência financeira e de Conduta estão implementados em draft. Os contratos de sinal **Financeiro** e **Conduta** estão fechados sem score. A próxima etapa metodológica é a calibração entre pilares.  
> **Regra de segurança:** nada nesta branch deve ser tratado como score ou ranking final enquanto a calibração entre pilares e os gates finais de avaliação não forem concluídos.

Este README é o **contrato de projeto**, o guia de implementação da v2 e o registro das decisões metodológicas já tomadas. Os fechamentos específicos estão documentados em:

- `docs/financial-methodology-closure.md`;
- `docs/conduct-methodology-closure.md`.

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

A arquitetura conceitual e o contrato do sinal estão fechados quanto aos eixos internos.

| Dimensão | Referência | Papel |
|---|---|---|
| Capital regulatório | PLA/CMR | eixo principal |
| Liquidez | ILT | eixo principal |
| Liquidez corrente | ILC | diagnóstico complementar |
| Operação | ICA/IC | filme longitudinal contextual |
| Rentabilidade | ILPL | diagnóstico apenas |

Não foi selecionada transformação numérica nem média ponderada interna. O fechamento específico está em `docs/financial-methodology-closure.md`.

---

## 11. Competência financeira madura

A v2 distingue:

```text
última competência observada
!=
última competência financeira madura
```

Diagnóstico real recente de PLA/CMR derivável:

```text
202601  148
202602  151
202603  153
202604  155
202605  155  ← competência madura
202606  132  ← observada, porém imatura no snapshot mais recente
```

A competência selecionada permanece `2026-05`.

No período maduro:

```text
PLA/CMR derivável   155/157
ILC derivável       156/157
ILT derivável       156/157
```

CMR zero, ausência ou evidência inutilizável não viram desempenho zero.

---

## 12. Capital regulatório — PLA/CMR

Decisões fechadas para o sinal:

- PLA/CMR é o eixo principal de capital;
- `1,0` é a fronteira material entre PLA abaixo do CMR e PLA que atende/supera o CMR;
- `PLA/CMR < 1` produz `capital_below_cmr`;
- `PLA/CMR >= 1` produz `capital_meets_or_exceeds_cmr`;
- ausência produz `capital_signal_unavailable`;
- não há escala linear;
- não há faixas positivas arbitrárias acima de `1,0`;
- magnitude maior não gera mérito ilimitado;
- histórico curto afeta confiança, não desempenho;
- porte absoluto não gera pontos.

O valor bruto permanece disponível para transparência e futura calibração entre pilares, mas o contrato do sinal não transforma excesso de capital em bônus crescente.

---

## 13. Liquidez — ILT e ILC

A investigação selecionou:

- **ILT** como principal referência de liquidez;
- **ILC** como diagnóstico complementar.

ILT mostrou estabilidade temporal superior e informação parcialmente distinta de PLA/CMR.

Estados fechados do sinal:

```text
ilt_below_arithmetic_parity
ilt_at_or_above_arithmetic_parity
ilt_signal_unavailable
```

O ponto `1,0` é referência aritmética do ILT, **não threshold prudencial oficial da SUSEP**. Não há transformação contínua selecionada nem tiers positivos acima da paridade. Valores extremos não recebem recompensa linear.

---

## 14. Combinação financeira e filme operacional

Capital e liquidez não são combinados por média ponderada nesta camada.

```text
capital_requirement_shortfall_observed
capital_requirement_met_with_liquidity_pressure
core_indicators_without_current_shortfall
capital_requirement_met_liquidity_unavailable
core_financial_signal_unavailable
```

Regras:

- insuficiência de capital não é apagada por liquidez alta;
- excesso de capital não apaga pressão de liquidez;
- ausência não é imputada;
- `core_indicators_without_current_shortfall` não significa selo de solvência ou segurança financeira.

A operação é observada longitudinalmente:

```text
balanced_persistent
improved
recent_pressure
persistent_pressure
indeterminate
```

ICA é referência principal; IC e componentes explicam a formação do resultado. O filme operacional é contexto longitudinal e **não sobrescreve capital ou liquidez**.

---

## 15. ILPL — investigação encerrada

ILPL foi submetido a investigação fechada e falhou no gate de estabilidade longitudinal exigido.

Decisão:

> **ILPL não entra como componente independente de scoring.**

Pode permanecer como diagnóstico de geração de resultado.

A busca por novos eixos financeiros independentes está encerrada nesta etapa.

### 15.1. Resultado real do fechamento financeiro

Competência madura `2026-05`, universo de 157 seguradoras:

```text
capital_meets_or_exceeds_cmr  141
capital_below_cmr              14
capital_signal_unavailable      2

ilt_at_or_above_arithmetic_parity 127
ilt_below_arithmetic_parity        29
ilt_signal_unavailable              1

core_indicators_without_current_shortfall              120
capital_requirement_met_with_liquidity_pressure         21
capital_requirement_shortfall_observed                  14
core_financial_signal_unavailable                        2
```

Confiança do núcleo:

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

# PRÓXIMO GATE

## 39. Próximo passo — calibração entre pilares

Financeiro e Conduta possuem agora contratos de sinal fechados, sem nota numérica.

O próximo gate é testar como transformar os estados semanticamente válidos dos dois pilares em avaliação geral sem destruir seus guardrails.

Questões centrais:

- é necessário score contínuo ou estados compostos são suficientes?;
- como impedir compensação indevida de sinal material?;
- como tratar pilar indisponível sem redistribuir peso silenciosamente?;
- como separar desempenho de `assessment_confidence`?;
- quais entidades podem tornar-se `assessment_eligible` e, só depois, `ranking_eligible`?;
- se houver score, qual transformação possui interpretação pública defensável?

---

## 40. Contratos fechados antes da calibração conjunta

### Financeiro

```text
PLA/CMR → capital
ILT     → liquidez principal
ILC     → diagnóstico
ICA/IC  → filme operacional contextual
ILPL    → diagnóstico apenas
```

Sem média ponderada interna e sem `financial_score`.

### Conduta

```text
pressão normalizada por exposição
+ credibilidade
+ persistência
+ tendência
+ satisfação sample-aware
+ contexto de mix
```

Sem `conduct_score`.

Os 54 casos sem pressão comparável permanecem preservados com `pressure = null` e motivo explícito.

---

## 41. Rotas paralelas de recuperação dos 54 casos

O fechamento de Conduta não encerra as rotas de recuperação.

- híbridas seguros/previdência → recuperar numerador por produto;
- Zurich → reconciliação temporal;
- Bradesco → separação de produto/carteira;
- Youse/Caixa → exposição específica da marca/subject;
- prêmio direto negativo → revisão contábil;
- nenhuma atividade securitária observada → auditoria do universo operacional;
- run-off → tratamento próprio;
- ausência de prêmio positivo → reconciliação de exposição.

O objetivo é **maximizar cobertura sem inventar atribuições**.

---

# CONFIANÇA, SCORE E RANKING

## 42. Score continua bloqueado

Atualmente:

```text
assessment_eligible = 0
ranking_eligible = 0
```

Os fechamentos de Financeiro e Conduta não abrem automaticamente esses gates.

A futura avaliação geral depende de:

```text
Financeiro
+
Conduta
+
qualidade/confiança da evidência
+
comparabilidade
```

Não haverá redistribuição silenciosa de peso quando um pilar estiver indisponível.

---

## 43. Confiança da avaliação

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

Confiança não deve ser usada para maquiar ausência de metodologia.

---

## 44. Ranking

O ranking final deve conter apenas entidades:

- corretamente identificadas;
- do universo definido;
- com avaliação completa;
- comparáveis dentro da coorte;
- avaliadas pela mesma versão metodológica.

Preferir linguagem como:

> 8ª entre 41 seguradoras elegíveis nesta comparação

e não:

> 8ª melhor seguradora do Brasil

quando a segunda frase não puder ser sustentada.

---

# FONTES

## 45. Hierarquia de autoridade

```text
FIP / licença / tipo / regime atual       → SUSEP
atividade / produção / financeiro         → SUSEP / SES
CNPJ e situação cadastral                 → Receita Federal
grupo econômico                           → SUSEP / SES
sucessão                                  → relação explicitamente verificada
marca / risk carrier                      → relação verificável
provider Consumer.gov                     → resolução determinística/documentada
Conduta comparativa                       → cascata de fontes + metodologia calibrada
```

---

## 46. Regras de contingência

Cache e fallback existem para disponibilidade operacional, não para mudar a semântica da fonte.

Exemplos:

- timeout isolado da SUSEP não justifica novo substituto metodológico;
- `finalizadas` não substitui Base Completa;
- fonte mais antiga não se torna “atual” só porque está em cache;
- fontes P1/P2/P3 não são costuradas longitudinalmente.

Um timeout transitório do workflow de Classification foi resolvido por rerun sem alteração de metodologia.

---

# WORKFLOWS

## 47. Validações relevantes da v2

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
- `V2 Sandbox Brand Conduct Evidence`.

O antigo:

`V2 Conduct Comparative Calibration (legacy invalidated)`

é somente manual e histórico.

---

## 48. Estado de validação alcançado

Os dois contratos de sinal possuem workflows próprios e execução real verde.

### Financial Methodology Closure

```text
Ruff                 ok
testes direcionados  3/3
build real            ok
157 entidades         preservadas
boundaries            ok
artifact upload       ok
```

### Conduct Methodology Closure

```text
Ruff                 ok
testes direcionados  3/3
build real            ok
157 entidades         preservadas
103 comparáveis       preservadas
54 não comparáveis    preservadas
boundaries            ok
artifact upload       ok
```

Esses workflows validam contratos de sinal, não score ou ranking.

---

# API v2 E PUBLICAÇÃO

## 49. API pública — direção

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

## 50. Proveniência

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

## 51. Validações obrigatórias

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
- score produzido por artifact que proíbe scoring.

---

# SEQUÊNCIA DE IMPLEMENTAÇÃO

## 52. Estado das fases

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
- combinação não compensatória capital/liquidez;
- confiança separada de desempenho;
- linguagem permitida/proibida;
- score numérico ainda proibido.

### Conduta — CONTRATO DE SINAL FECHADO

- identidade Consumer.gov;
- core mensal;
- source cascade;
- exposure reader insurance-only;
- reconciliation audit;
- marca/subject/carrier;
- Sandbox Conduct;
- population alignment mensal;
- credibilidade;
- persistência e tendência;
- mix como contexto;
- satisfação sample-aware;
- remediação não inferida;
- score numérico ainda proibido.

### Calibração entre pilares — PRÓXIMA ETAPA

Financeiro e Conduta serão combinados apenas depois de estudar compatibilidade semântica, regras de não compensação e `assessment_confidence`.

### Schema/publicação/frontend — PENDENTES

Nenhuma regra v2 deve ser migrada ao frontend antes da avaliação composta.

---

## 53. O que não fazer agora

Não:

- criar `financial_score` ou `conduct_score` por conveniência;
- definir pesos entre pilares antes da calibração conjunta;
- produzir ranking 1–157;
- usar reclamações brutas como nota;
- usar prêmio como número de clientes;
- misturar previdência com seguros;
- misturar capitalização com seguros;
- substituir Base Completa por `finalizadas`;
- transferir reclamações Youse → Caixa automaticamente;
- ratear reclamações Bradesco sem produto;
- ignorar transferência temporal Zurich;
- incluir Sandbox no benchmark ordinário;
- dar score à Loovi;
- reabrir ILPL;
- adicionar novos eixos financeiros sem nova justificativa;
- transformar PLA/CMR ou ILT em bônus linear;
- permitir que capital alto apague pressão de liquidez;
- permitir que liquidez alta apague insuficiência de capital;
- aplicar fuzzy matching decisório;
- implementar scoring no PHP/JS;
- alterar `main`.

---

## 54. Critério alcançado para o fechamento de Conduta

O contrato de Conduta demonstrou ser possível:

1. normalizar reclamações pela escala operacional nas entidades comparáveis;
2. estudar mix de carteira sem forçar coortes artificiais;
3. controlar pequenas amostras;
4. separar evento episódico de persistência;
5. medir tendência;
6. incorporar satisfação com amostra;
7. lidar explicitamente com subject/carrier divergentes;
8. preservar os players importantes mesmo sem pressão comparável;
9. explicar a métrica ao consumidor sem extrapolar a evidência.

Quando esses requisitos falham em uma entidade específica, a conclusão correspondente permanece `null`/indisponível em vez de ser inventada.

---

## 55. Definição de sucesso da v2

A refatoração será considerada bem-sucedida quando:

- o consumidor puder pesquisar entidade ou marca sem conhecer a estrutura jurídica;
- a natureza da entidade for identificada corretamente;
- marcas relevantes como Loovi forem resolvidas sem virar falsamente seguradoras;
- Sandbox puder ter inteligência própria sem contaminar o ranking ordinário;
- seguradoras comparáveis forem avaliadas pela mesma metodologia;
- players difíceis não desapareçam apenas por estrutura societária complexa;
- ausência de dado não vire punição;
- pressão de reclamações seja ajustada por exposição de forma defensável;
- pequenas amostras não dominem o resultado;
- persistência e trajetória sejam distinguidas;
- os sinais financeiros centrais não permitam compensação silenciosa de insuficiências;
- o processo seja auditável;
- o frontend não corrija nem invente lógica;
- atualização editorial rotineira seja mínima;
- score só apareça quando a evidência realmente sustentar uma conclusão.

---

## 56. Diretriz final

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
- mistura de domínios de produto.

A próxima etapa é:

```text
Financial Signal Contract  ─┐
                            ├→ calibração entre pilares
Conduct Signal Contract   ──┘
                              ↓
                        avaliação composta
                              ↓
                  assessment confidence final
                              ↓
                      elegibilidade/ranking
```

Somente a calibração conjunta poderá decidir se uma nota numérica é necessária e, se for, qual transformação e quais pesos são defensáveis.

---

## Licença e uso

Este repositório é mantido pela Sanida Corretora de Seguros.

Dados de terceiros permanecem sujeitos às condições, limitações e responsabilidades de suas respectivas fontes.

A metodologia Sanida é uma interpretação própria de dados públicos e não deve ser apresentada como nota, classificação ou certificação oficial da SUSEP.
