# Fechamento formal — §19.1 Auditoria geral de metodologia e dados

Status: **FECHADO em 01/09/2026**.

Este documento formaliza o encerramento do item **19.1 — Auditoria geral de metodologia e dados** da fase de consolidação da v2.

O objetivo desta etapa não era recalibrar a metodologia porque uma distribuição parecesse surpreendente. O objetivo era verificar se a cadeia implementada preserva corretamente a semântica das fontes e dos contratos aprovados, da ingestão ao JSON público.

## 1. Escopo auditado

A auditoria percorreu, conforme aplicável:

```text
fonte oficial/pública
→ download/cache
→ parsing e normalização
→ identidade/classificação/lifecycle
→ período e população
→ campo e unidade
→ sinal e missingness
→ denominador
→ fórmula
→ artifact intermediário
→ contrato semântico
→ elegibilidade
→ explorer/leaderboards/collections
→ perfil público
→ pacote Gate 4
```

Foram auditados especialmente:

- identidade, lifecycle, marcas, grupos, sucessões e `risk_carrier`;
- universo regulatório e exclusões de escopo;
- ingestão e evidência financeira SES;
- capital `new_pla / CMR`;
- ILT/ILC e contexto operacional;
- exposição de seguros usada em Conduta;
- alinhamento mensal de reclamações observadas/esperadas;
- credibilidade, persistência, tendência e sensibilidade de denominador;
- missingness, zeros e valores não finitos;
- reconciliação entre Financeiro e Conduta;
- assessment individual e ranking preflight;
- leaderboards unidimensionais e coleções;
- contrato público de busca/perfis;
- lineage, freshness/cache e pacote público de uma única geração.

## 2. Regra de investigação utilizada

Quando um resultado parecia estranho, a sequência de auditoria foi:

```text
source
→ field
→ parsing
→ sign
→ unit
→ period
→ denominator
→ missingness
→ formula
```

Somente depois disso seria admissível discutir nova metodologia.

Essa regra impediu que bugs de ingestão ou alinhamento fossem confundidos com necessidade de recalibração.

## 3. Principais erros factuais encontrados e corrigidos

### 3.1. Capital: numerador PLA/CMR

Foi consolidada a correção crítica:

```text
PLA/CMR = new_pla / cmr
```

`pla_adjusted` permanece evidência intermediária e **não pode ser fallback**.

A derivabilidade usada na seleção da competência financeira madura foi alinhada à mesma semântica da métrica final.

### 3.2. Ingestão financeira SES

A fronteira da fonte foi endurecida para falhar fechado diante de:

- linha CSV estruturalmente malformada;
- FIP, `damesano` ou `CMPID` inválido/fracionário;
- competência que não seja `AAAAMM` válida;
- número não vazio malformado;
- valor não finito;
- alias acidental de identificador por remoção de caracteres;
- componente financeiro conhecido no quadro oficial incorreto.

Missing legítimo continua distinto de valor malformado. Negativos e zeros são preservados como fatos da fonte.

### 3.3. Conduta: pressão anual temporalmente alinhada

A pressão anual passou a obedecer ao contrato mensal:

```text
expected_m = market_complaints_m × entity_premium_m / market_premium_m
expected_12m = Σ expected_m
observed_12m = Σ observed_m apenas nos mesmos meses comparáveis
pressure_12m = observed_12m / expected_12m
```

Foram removidas recomputações anuais incompatíveis em Credibility e Portfolio Mix.

### 3.4. Conduta: missingness e zero

Foram separados explicitamente:

- prêmio ausente;
- prêmio zero;
- prêmio negativo;
- mercado sem reclamações no mês;
- reclamação zero;
- linha com ambos os prêmios ausentes;
- valor malformado.

`market_complaints = 0` não cria baseline neutro: o mês fica indisponível para pressão normalizada.

### 3.5. Períodos e contadores

Reconciliation, Calibration, Closure e consumidores correlatos passaram a rejeitar:

- meses fora de ordem;
- lacunas em janelas que se declaram consecutivas;
- duplicatas incompatíveis;
- contadores fracionários silenciosamente truncados.

### 3.6. Contrato público de perfis

Foi identificado que o Gate 4 canônico ainda executava o builder-base de perfis, enquanto a finalização regulatória de SSPE e o validator público existiam fora do caminho efetivamente empacotado.

Correção:

- Gate 4 passa pela finalização regulatória pública;
- SSPE permanece `entity_type = insurer`, mas recebe subtype público explícito;
- SSPE continua fora do assessment/ranking ordinário;
- o validator público é executado no caminho canônico;
- referências como `target_profile_id`, `risk_carrier_profile_id` e `successor_profile_id` não podem apontar para perfil inexistente.

### 3.7. Política operacional obsoleta

O `assessment_eligibility_contract` ainda descrevia restauração de `latest successful artifacts`, herança do Gate 3.

O contrato foi alinhado ao Gate 4 fechado:

```text
same-generation workspace
same build_id
no cross-run artifact reconstruction
```

## 4. Invariantes confirmados

A auditoria não encontrou motivo para reabrir os contratos metodológicos centrais.

Permanecem obrigatórios:

```text
null ≠ 0
missing ≠ zero
absence_of_evidence ≠ adverse_evidence
brand ≠ legal_entity
group ≠ succession
risk_carrier_relation ≠ complaint_transfer
new_pla ≠ pla_adjusted fallback
Sandbox ∉ ordinary benchmark
SSPE ∉ ordinary assessment/ranking
frontend ≠ methodology engine
ranking_eligible = 0
```

Também permanece vedado:

- score geral;
- média Financeiro × Conduta;
- ranking total do mercado;
- desempate de mérito inventado;
- transformar leaderboard unidimensional em ranking geral;
- atribuir performance negativa por histórico curto ou missingness.

## 5. Provas de fechamento

### Código funcional auditado

O conjunto final de correções transversais chegou ao commit:

```text
7993dbabd1cf3cd21181c88d072aed4ce5573538
```

### CI

```text
status = success
```

### Evergreen Contract

```text
status = success
```

### Full Generation Proof #49

```text
workflow  = V2 Gate 4 Full Generation Proof
run       = 33567550092
run_number = 49
head      = 7993dbabd1cf3cd21181c88d072aed4ce5573538
status    = completed
conclusion = success
```

Artifact final:

```text
id     = 9824434275
name   = v2-gate4-full-generation-33567550092-a1
sha256 = d0ccb6ce274542015431ae9fde0084c12941d1983ce06527bf6872e442244431
```

Essa execução completou com sucesso:

- DAG integral em uma única geração;
- validação de lineage e fronteiras metodológicas;
- geração do contrato público regulatório fechado;
- validação do pacote público;
- verificação de install e rollback exatos;
- persistência dos caches validados;
- upload do proof final.

## 6. Snapshot operacional após a auditoria

As contagens abaixo são fotografia da geração, não constantes metodológicas:

```text
regulatory_universe                   156
conduct_comparable                    101
conduct_not_comparable                 55
assessment_eligible                    82
ranking_eligible                        0
```

Os documentos metodológicos específicos foram reconciliados com a geração integrada atual. Snapshots históricos explicitamente datados continuam sendo histórico e serão classificados/limpos no §19.5 quando apropriado.

## 7. Critério de encerramento

O §19.1 é considerado fechado porque:

- fórmulas críticas foram auditadas da fonte ao JSON público;
- amostras e casos suspeitos foram confrontados com a cadeia real;
- bugs factuais encontrados foram corrigidos na origem adequada;
- regressões foram adicionadas para impedir retorno dos erros;
- missingness, zero, sinal, unidade, período e denominador possuem contratos explícitos;
- documentação corrente foi reconciliada com artifacts atuais;
- o Gate 4 canônico empacota e valida a mesma semântica auditada;
- CI, Evergreen e Full Generation Proof encerraram verdes.

Portanto:

```text
SECTION_19_1_STATUS = CLOSED
methodology_recalibration_required = false
ranking_gate_opened = false
production_cutover_authorized_by_19_1 = false
```

## 8. Próximo item

O próximo item da consolidação é:

```text
§19.2 — Auditoria do potencial informacional do frontend
```

O §19.2 deve verificar se o frontend aproveita corretamente a informação já disponível nos contratos públicos. Se faltar payload necessário, o backend pode ser ampliado; o frontend não deve reconstruir metodologia.
