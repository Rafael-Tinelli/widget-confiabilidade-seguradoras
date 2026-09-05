# Contrato semântico de avaliação entre pilares — v2

Status: **fechado; score e ranking continuam proibidos**.

Este contrato transforma os estados do Stage 2 em linguagem pública executável. A pergunta é:

> **O que a evidência disponível permite dizer ao consumidor sem transformar resultado parcial em garantia, bônus ou ranking?**

## Hierarquia pública

```text
1. leitura geral
2. sinais encontrados
3. por que isso importa
4. qualificadores
5. números e metodologia
6. limites
7. confiança e cobertura
```

Resultado favorável pode ser reconhecido dentro do escopo analisado; alerta material não pode ser compensado; ausência de evidência nunca vira neutralidade.

## Classes públicas

```text
favorable_reading
attention
prudential_warning
evidence_incomplete
```

São classes semânticas, não notas e não posições de ranking.

## Estados completos

```text
no_current_core_adverse_signal
→ Leitura central favorável

conduct_pressure_only
→ Atenção à Conduta

liquidity_pressure_only
→ Atenção à liquidez

liquidity_and_conduct_pressure
→ Atenção em liquidez e Conduta

capital_shortfall_without_conduct_pressure
→ Alerta prudencial de capital

capital_shortfall_and_conduct_pressure
→ Alerta de capital e Conduta
```

Nenhum título autoriza inferir preço, qualidade de cobertura, solvência futura ou superioridade global.

## Avaliação conjunta incompleta

Quando o núcleo conjunto não está completo:

```text
public_class = evidence_incomplete
```

Isso significa limitação de evidência, não desempenho ruim nem resultado neutro. Sinais disponíveis de um pilar continuam visíveis e não podem ser escondidos pela incompletude do outro.

## Conduta: detalhe sem bônus

O cartão preserva a diferença entre:

```text
acima do esperado
abaixo do esperado
sem diferença clara
não comparável
cobertura temporal insuficiente
sensível ao denominador
```

`below_expected` não vira bônus de qualidade. Persistência e tendência adversas só qualificam conclusão anual `above_expected_with_sufficient_evidence`.

## Assessment completeness

Cada entidade do universo regulatório comparável recebe:

```text
joint_core_complete
ou
joint_core_incomplete
```

O contrato semântico não abre sozinho `assessment_eligible` e nunca abre `ranking_eligible`.

## Snapshot integrado corrente

Na geração integrada de 01/09/2026 usada na auditoria §19.1:

```text
regulatory_universe                    156
semantic_public_assessment_supported    82
joint_core_incomplete                    74
```

Classes observadas:

```text
favorable_reading     45
attention              35
prudential_warning      2
evidence_incomplete    74
```

Estados observados:

```text
no_current_core_adverse_signal                 45
conduct_pressure_only                          14
liquidity_pressure_only                        11
liquidity_and_conduct_pressure                 10
capital_shortfall_without_conduct_pressure      0
capital_shortfall_and_conduct_pressure          2
evidence_incomplete_for_joint_assessment       74
```

Detalhe de Conduta observado:

```text
abaixo do esperado                    41
acima do esperado                     26
sem diferença clara                   18
não comparável com segurança          55
cobertura temporal insuficiente       11
sensível ao denominador                5
```

Esses valores são fotografia da geração.

## Guardrails executáveis

- Stage 1 e Stage 2 reconciliam população e IDs;
- avaliação completa exige estado de matriz;
- avaliação incompleta não recebe estado conjunto como se fosse completa;
- sinais disponíveis não são suprimidos por missingness de outro pilar;
- qualificadores adversos de Conduta não vazam para conclusão não adversa;
- capital shortfall não pode ser escondido do alerta público;
- nenhum score ou ranking é criado;
- nenhuma contagem de classe é constante metodológica.

## Implementação

```text
api/v2/build_cross_pillar_assessment_semantic_contract.py
tests/test_v2_cross_pillar_assessment_semantic_contract.py
```

Artifact:

```text
data/derived/v2/cross_pillar_assessment_semantic_contract.json
```

## Encadeamento já concluído

O `assessment_eligibility_contract` aplica o gate formal de avaliabilidade individual. Esse contrato já está fechado e mantém o ranking em gate separado.
