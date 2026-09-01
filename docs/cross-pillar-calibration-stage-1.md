# Calibração entre pilares — Stage 1

Status: **fechada como diagnóstico estrutural; score, pesos e ranking continuam proibidos**.

O Stage 1 junta os contratos fechados de Financeiro e Conduta para medir quanto de uma leitura conjunta decorre diretamente das evidências aprovadas, sem inventar compensações entre pilares.

## Contrato estrutural

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
- a população é derivada dos artifacts, nunca de número fixo.

## Matriz não compensatória

```text
F0|C0  nenhum sinal adverso central
F0|C1  pressão de Conduta
F1|C0  pressão de liquidez
F1|C1  pressão de liquidez + Conduta
F2|C0  insuficiência de capital sem pressão de Conduta
F2|C1  insuficiência de capital + pressão de Conduta
```

Um estado com zero entidades continua fazendo parte do vocabulário metodológico.

## Cobertura e ordenabilidade

O Stage 1 mede população conclusiva/incompleta, pares comparáveis/empatados/incomparáveis e cobertura econômica. Nenhuma dessas quantidades é threshold de aceitação.

Pares incomparáveis demonstram que os contratos não determinam uma ordem total. Resolver esses pares exigiria prioridade normativa adicional, que não foi selecionada.

## Snapshot integrado corrente

Na geração integrada de 01/09/2026 usada na auditoria §19.1:

```text
universo regulatório                         156
joint_core_conclusive                         82
joint_core_incomplete                         74
```

Matriz observada entre as 82 conclusivas:

```text
F0|C0  45
F0|C1  14
F1|C0  11
F1|C1  10
F2|C0   0
F2|C1   2
```

Diagnóstico de ordenação:

```text
pares possíveis             3.321
estritamente comparáveis    1.985
empatados                    1.182
incomparáveis                  154
```

Cobertura:

```text
entidades                         82 / 156 = 52,56%
parcela do prêmio direto positivo          69,69%
parcela das reclamações                    54,27%
```

Prontidão das 74 incompletas:

```text
Conduta não comparável com segurança       51
cobertura temporal insuficiente             11
sensibilidade ao denominador                 5
núcleo Financeiro incompleto                 7
```

Esses números são fotografia da geração, não regras do Stage 1.

## Decisão metodológica

```text
continuous weighted score     não selecionado
lexicographic total order      não selecionada
Pareto como ranking público    não selecionado
matriz não compensatória       compatível com os contratos
ranking geral                  não suportado
```

Não existe base para escolher 50/50, 60/40 ou qualquer outro peso apenas porque os dados estão disponíveis.

## Operação canônica

Stage 1 e os estágios seguintes são executados pelo Gate 4 no mesmo workspace/build. Restauração de `latest successful` de outra geração não é autoridade de publicação.

## Implementação

```text
api/v2/build_cross_pillar_calibration_diagnostic.py
api/v2/build_cross_pillar_coverage_audit.py
tests/test_v2_cross_pillar_calibration_diagnostic.py
tests/test_v2_cross_pillar_coverage_audit.py
```

Artifacts:

```text
data/derived/v2/cross_pillar_calibration_diagnostic.json
data/derived/v2/cross_pillar_coverage_audit.json
```

## Encadeamento já concluído

O Stage 2, o contrato semântico, Assessment Eligibility e Ranking Preflight já foram fechados separadamente. Nenhum deles abriu score ou ranking geral.
