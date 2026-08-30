# Contrato de elegibilidade de avaliação — v2

Status: **fechado; assessment individual pode ser aberto quando a evidência satisfaz o contrato; ranking permanece independente e bloqueado**.

## Pergunta do gate

> **Temos base regulatória, comparabilidade e evidência suficientes para publicar uma avaliação conjunta desta seguradora?**

A elegibilidade mede se a avaliação pode ser feita com segurança. Ela **não mede se o resultado é bom ou ruim**.

## Regras permanentes

Para `assessment_eligible = true`:

- a entidade deve pertencer ao universo regulatório ordinário comparável;
- o núcleo conjunto deve estar completo;
- o contrato semântico deve suportar avaliação pública;
- a evidência financeira central deve ser suficiente;
- a conclusão de Conduta deve ser comparável e conclusiva.

Guardrails:

```text
performance_result_used_for_eligibility             false
adverse_result_blocks_assessment                     false
limited_financial_history_blocks_assessment          false
missingness_treated_as_neutral                       false
ranking_gate_independent                             true
```

Histórico financeiro limitado, quando ainda aceito pelo contrato de evidência, é disclosure de confiança e não penalidade de desempenho.

## Estados

```text
eligible_complete_joint_assessment
not_eligible_joint_evidence_incomplete
not_eligible_core_evidence_confidence
```

Uma entidade inelegível não recebe nota zero, posição inferior ou classe negativa. O motivo da ineligibilidade deve permanecer explícito.

## População dinâmica

O workflow não exige 157, 156, 85 ou qualquer outra quantidade fixa.

Invariantes executáveis:

```text
assessment_eligible + assessment_not_eligible = regulatory_universe
ranking_eligible = 0
entity rows = regulatory_universe
eligible rows = assessment_eligible
semantic_supported >= assessment_eligible
```

A distribuição entre classes públicas também é diagnóstico. O único veto semântico estrutural é: `evidence_incomplete` não pode aparecer entre as entidades elegíveis.

## Snapshot validado — 30/08/2026

Run:

```text
V2 Assessment Eligibility Contract
run 33323343812
head 35e509d31de68a9311ede57ac245de6b7d3c0e11
artifact 9735530398
SHA256 ZIP 237a9444bd373302758c0e1f7f0d9642f30ef0ac6ba2870a83a6a896db2d4d4d
```

População:

```text
regulatory_universe                    156
semantic_public_assessment_supported    85
assessment_eligible                     85
assessment_not_eligible                 71
ranking_eligible                         0
```

Classes entre as 85 elegíveis:

```text
favorable_reading     48
attention              36
prudential_warning      1
```

Razões principais entre as 71 não elegíveis:

```text
conduta_nao_comparavel_com_seguranca       52
conduta_com_cobertura_temporal_insuficiente 11
conduta_sensivel_ao_denominador              5
financeiro_central_incompleto                3
```

Todas as 85 elegíveis têm, no snapshot atual, `historico_estabelecido` como confiança financeira central. Isso é resultado da execução, não requisito de contagem.

## O que este gate não autoriza

`assessment_eligible = true` não significa:

- seguradora boa;
- seguradora recomendada;
- ranking elegível;
- posição relativa;
- ausência de alertas;
- garantia futura.

Uma avaliação adversa pode ser perfeitamente elegível se a evidência for suficiente.

## Implementação

```text
api/v2/build_assessment_eligibility_contract.py
tests/test_v2_assessment_eligibility_contract.py
.github/workflows/v2-assessment-eligibility-contract.yml
```

Artifact:

```text
data/derived/v2/assessment_eligibility_contract.json
```

## Operação atual

Neste Gate 3, o workflow restaura artifacts bem-sucedidos da branch e os builders reconciliam população/IDs, falhando quando gerações incompatíveis são misturadas. A orquestração transversal por build ID/same-head para toda a cadeia é assunto do Gate 4.

## Próximo gate

O `ranking_eligibility_preflight` pergunta separadamente se existe escopo e regra de ordenação defensáveis para chamar o produto de ranking.
