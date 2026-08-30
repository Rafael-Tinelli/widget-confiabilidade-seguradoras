# Calibração entre pilares — Stage 1

Status: **fechada como diagnóstico estrutural; score, pesos e ranking continuam proibidos**.

Este estágio junta os contratos já fechados de Financeiro e Conduta para responder uma pergunta limitada: **quanto de uma leitura conjunta decorre diretamente das evidências aprovadas, sem inventar compensações entre pilares?**

## Contrato estrutural

O Stage 1 não cria mérito cardinal. Ele usa coordenadas ordinais de adversidade apenas para diagnosticar ordenabilidade:

```text
Financeiro
F0 = core_indicators_without_current_shortfall
F1 = capital_requirement_met_with_liquidity_pressure
F2 = capital_requirement_shortfall_observed

Conduta
C0 = below_expected_with_sufficient_evidence
     OU not_distinguishable_from_expected
C1 = above_expected_with_sufficient_evidence
```

`below_expected` permanece em C0 porque menos reclamações que o esperado, isoladamente, não prova atendimento superior.

Regras permanentes:

- ausência de evidência nunca vira C0/F0;
- Financeiro e Conduta não se compensam;
- histórico e contexto operacional qualificam evidência, não criam bônus;
- nenhum peso Financeiro × Conduta é inferido dos dados;
- Pareto é diagnóstico de dominância, não ranking público;
- a população é derivada dos artifacts regulatórios e dos contratos dos pilares, nunca de um número fixo.

## Matriz não compensatória

Quando os dois pilares são conclusivos, a combinação produz estados semânticos. O conjunto de estados possíveis é estrutural; a quantidade de entidades em cada estado é uma fotografia do mercado.

```text
F0|C0  nenhum sinal adverso central
F0|C1  pressão de Conduta
F1|C0  pressão de liquidez
F1|C1  pressão de liquidez + Conduta
F2|C0  insuficiência de capital sem pressão de Conduta
F2|C1  insuficiência de capital + pressão de Conduta
```

Se um estado tiver zero entidades em uma execução, ele continua fazendo parte do vocabulário metodológico.

## Cobertura e ordenabilidade

O Stage 1 mede separadamente:

1. quantas entidades têm núcleo conjunto conclusivo;
2. quantas ficam incompletas por evidência;
3. quantos pares são dominados, empatados ou incomparáveis;
4. quanto do prêmio positivo e das reclamações está coberto pela população conclusiva.

Nenhuma dessas quantidades é um threshold de aceitação codificado. Elas são diagnósticos da execução.

A existência de empates ou pares incomparáveis demonstra que os contratos não determinam uma ordem total. Resolver esses pares exigiria uma prioridade normativa adicional — por exemplo, uma ordem lexicográfica ou uma taxa de troca entre Financeiro e Conduta — e essa escolha não é feita neste estágio.

## Snapshot validado — 30/08/2026

Baseline regulatório do Gate 2 e artifacts usados pelo Gate 3:

```text
universo regulatório                         156
joint_core_conclusive                         85
joint_core_incomplete                         71
```

Matriz observada entre as 85 conclusivas:

```text
F0|C0  48
F0|C1  15
F1|C0  11
F1|C1  10
F2|C0   0
F2|C1   1
```

Diagnóstico par a par:

```text
pares possíveis             3.570
estritamente comparáveis    2.072
empatados                    1.333
incomparáveis                  165
```

Cobertura da população conjunta conclusiva:

```text
entidades                         85 / 156 = 54,49%
parcela do prêmio direto positivo          69,94%
parcela das reclamações                    54,40%
```

Prontidão das 71 incompletas na leitura semântica atual:

```text
Conduta não comparável com segurança       52
cobertura temporal insuficiente             11
sensibilidade ao denominador                 5
núcleo Financeiro incompleto                 3
```

Esses números **não são regras do Stage 1**. Entradas, saídas, amadurecimento de histórico e melhoria de reconciliação podem alterá-los sem mudança metodológica.

## Decisão metodológica

O Stage 1 sustenta:

```text
continuous weighted score     não selecionado
lexicographic total order      não selecionada
Pareto como ranking público    não selecionado
matriz não compensatória       compatível com os contratos
ranking geral                  não suportado
```

A principal conclusão é negativa e importante: **não existe base para escolher 50/50, 60/40 ou qualquer outro peso apenas porque os dados estão disponíveis**.

## Implementação

```text
api/v2/build_cross_pillar_calibration_diagnostic.py
api/v2/build_cross_pillar_coverage_audit.py
tests/test_v2_cross_pillar_calibration_diagnostic.py
tests/test_v2_cross_pillar_coverage_audit.py
.github/workflows/v2-cross-pillar-calibration-stage-1.yml
.github/workflows/v2-cross-pillar-coverage-audit.yml
```

Artifacts:

```text
data/derived/v2/cross_pillar_calibration_diagnostic.json
data/derived/v2/cross_pillar_coverage_audit.json
```

Os builders reconciliam conjuntos de `entity_id`; diferenças de população falham de forma explícita. Os workflows core do Gate 2 usam restauração same-head para evitar mistura silenciosa de gerações.

## Próximo estágio

O Stage 2 testa arquiteturas de avaliação sem abrir score ou ranking. A matriz não compensatória segue como candidata principal porque preserva o domínio exato do alerta e mantém missingness fora de desempenho.
