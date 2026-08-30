# Contrato semântico de avaliação entre pilares — v2

Status: **fechado; score e ranking continuam proibidos**.

Este contrato transforma os estados do Stage 2 em linguagem pública executável. A pergunta é:

> **O que a evidência disponível permite dizer ao consumidor sem transformar resultado parcial em garantia, bônus ou ranking?**

## Hierarquia pública

A apresentação deve seguir:

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

O contrato usa quatro classes:

```text
favorable_reading
attention
prudential_warning
evidence_incomplete
```

Elas são classes semânticas, não notas e não posições de ranking.

A quantidade de entidades em cada classe é derivada do snapshot e pode mudar.

## Estados completos

Mapeamento estrutural:

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

Nenhum desses títulos autoriza inferir preço, qualidade de cobertura, atendimento individual, solvência futura ou superioridade global.

## Avaliação conjunta incompleta

Quando o núcleo conjunto não está completo:

```text
public_class = evidence_incomplete
```

Isso significa limitação de evidência, não desempenho ruim e não resultado neutro.

Causas possíveis incluem:

- Conduta não comparável com segurança;
- cobertura temporal insuficiente;
- sensibilidade ao denominador;
- núcleo Financeiro incompleto;
- outra indisponibilidade explicitamente classificada.

## Evidência disponível não pode ser escondida

`joint_core_incomplete` não suprime um alerta já sustentado por um pilar.

Assim, uma avaliação conjunta incompleta pode carregar, por exemplo:

```text
prudential_capital_warning
liquidity_attention
conduct_attention
```

O usuário deve ver simultaneamente o alerta disponível e a razão pela qual a leitura conjunta não pôde ser fechada.

## Conduta: detalhe sem bônus

O cartão de Conduta preserva a diferença entre:

```text
acima do esperado
abaixo do esperado
sem diferença clara
não comparável
cobertura temporal insuficiente
sensível ao denominador
```

`below_expected` não se transforma em bônus de qualidade.

Persistência e tendência só podem qualificar publicamente uma conclusão anual adversa de Conduta. Se a conclusão final não é `above_expected_with_sufficient_evidence`, qualificadores adversos de persistência/tendência devem ser nulos.

## Assessment completeness

Cada entidade do universo regulatório comparável recebe explicitamente:

```text
joint_core_complete
ou
joint_core_incomplete
```

O contrato semântico pode dizer que uma avaliação individual está semanticamente suportada, mas **não abre sozinho** `assessment_eligible`. Esse gate pertence ao contrato seguinte.

Também não abre `ranking_eligible`.

## Snapshot validado — 30/08/2026

Run:

```text
V2 Cross-Pillar Assessment Semantic Contract
run 33323343824
head 35e509d31de68a9311ede57ac245de6b7d3c0e11
artifact 9735526262
SHA256 ZIP 510617588b5875f32610cf8db25606824fdd2f3bc168129fbf570ee64f9a78b2
```

População:

```text
regulatory_universe                    156
semantic_public_assessment_supported    85
joint_core_incomplete                    71
```

Classes observadas:

```text
favorable_reading     48
attention              36
prudential_warning      1
evidence_incomplete    71
```

Estados observados:

```text
no_current_core_adverse_signal                 48
conduct_pressure_only                          15
liquidity_pressure_only                        11
liquidity_and_conduct_pressure                 10
capital_shortfall_and_conduct_pressure          1
evidence_incomplete_for_joint_assessment       71
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

Alertas disponíveis entre as avaliações incompletas:

```text
liquidity_attention          6
prudential_capital_warning   2
```

Esses valores são fotografia. O contrato exige apenas que alertas disponíveis sejam preservados e que os counts reconciliem com a população correspondente.

## Guardrails executáveis

- Stage 1 e Stage 2 devem reconciliar população e IDs;
- avaliação completa exige estado de matriz;
- avaliação incompleta não recebe estado conjunto como se fosse completa;
- sinais disponíveis não são suprimidos por missingness de outro pilar;
- qualificadores adversos de Conduta não vazam para conclusão não adversa;
- nenhum score é criado;
- nenhum ranking é criado;
- `assessment_eligible` e `ranking_eligible` não são abertos por este artifact;
- nenhuma contagem de classe é tratada como constante metodológica.

## Implementação

```text
api/v2/build_cross_pillar_assessment_semantic_contract.py
tests/test_v2_cross_pillar_assessment_semantic_contract.py
.github/workflows/v2-cross-pillar-assessment-semantic-contract.yml
```

Artifact:

```text
data/derived/v2/cross_pillar_assessment_semantic_contract.json
```

## Próximo gate

`assessment_eligibility_contract` decide quando a base é suficiente para publicar formalmente a avaliação conjunta individual, independentemente de o resultado ser favorável ou adverso.
