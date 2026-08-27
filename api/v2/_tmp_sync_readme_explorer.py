from pathlib import Path

path = Path("README.md")
text = path.read_text(encoding="utf-8")

replacements = [
    (
        "> **Marco atual (2026-08-26):** identidade, classificação regulatória, lifecycle jurídico, relationships, elegibilidade formal, evidência financeira e de Conduta estão implementados em draft. Os contratos de sinal **Financeiro** e **Conduta** estão fechados sem score. A calibração entre pilares concluiu **Stage 1, auditoria de cobertura, Stage 2 e o contrato semântico de avaliação**. A matriz não compensatória está formalizada para linguagem pública: **85/157** seguradoras possuem suporte semântico para avaliação conjunta completa e **72/157** permanecem como avaliação conjunta incompleta, preservando os sinais disponíveis de cada pilar. Isso **não abre** `assessment_eligible` nem `ranking_eligible`; ambos continuam em `0`.  \n> **Próximo gate:** `assessment_eligibility_contract`. A ferramenta só poderá abrir elegibilidade formal de avaliação após separar, de maneira executável, completude semântica, confiança/evidência e condições de publicação. Ranking permanece bloqueado.  ",
        "> **Marco atual (2026-08-27):** os contratos de sinal **Financeiro** e **Conduta**, a calibração entre pilares, o contrato semântico público, o **Assessment Eligibility Contract**, o **Ranking Eligibility Preflight** e o **Exploratory Leaderboards Contract** estão fechados. **85/157** seguradoras possuem avaliação conjunta elegível; **72/157** permanecem com avaliação conjunta incompleta, preservando os sinais disponíveis. A ferramenta é prioritariamente um **comparador/avaliação semântica**. Cinco leaderboards unidimensionais e cinco coleções semânticas estão autorizados como exploração secundária. `ranking_eligible` permanece **0** e o ranking geral continua bloqueado.  \n> **Próximo estágio de produto:** `public_api_json_packaging_and_frontend_php_integration`. O backend gera semântica, métricas, coleções e leaderboards; o PHP no HostGator somente renderiza os JSONs e não recompõe a metodologia.  ",
    ),
    (
        "- `docs/cross-pillar-assessment-semantic-contract.md`.\n",
        "- `docs/cross-pillar-assessment-semantic-contract.md`;\n- `docs/assessment-eligibility-contract.md`;\n- `docs/ranking-eligibility-preflight.md`;\n- `docs/exploratory-leaderboards-contract.md`.\n",
    ),
    (
        "identidades materializadas                 ~490\nseguradoras ordinárias atuais                157\nregulatory_universe_eligible                 157\nassessment_eligible                            0\nranking_eligible                               0\nSandbox no universo ordinário                  0\nregimes especiais no universo ordinário        0\n",
        "identidades materializadas                 ~490\nseguradoras ordinárias atuais                157\nregulatory_universe_eligible                 157\nsemantic_public_assessment_supported          85\nassessment_eligible                           85\nassessment_not_eligible                       72\nranking_preflight_candidates                  85\nranking_eligible                               0\nleaderboards unidimensionais públicos          5\ncoleções semânticas públicas                   5\nSandbox no universo ordinário                  0\nregimes especiais no universo ordinário        0\n",
    ),
    (
        "Isso não equivale a elegibilidade formal:\n\n```text\nsemantic_public_assessment_supported  85\nassessment_eligible                    0\nranking_eligible                       0\n```\n\nA distinção é obrigatória.\n",
        "O suporte semântico antecede o gate formal. O `Assessment Eligibility Contract` já confirmou:\n\n```text\nsemantic_public_assessment_supported  85\nassessment_eligible                   85\nassessment_not_eligible               72\nranking_eligible                       0\n```\n\nA distinção continua obrigatória: `assessment_eligible` autoriza publicar a avaliação conjunta; não é selo de qualidade e não abre ranking geral.\n",
    ),
    (
        "## 46. Score e gates continuam bloqueados\n\nAtualmente:\n\n```text\nsemantic_public_assessment_supported = 85\nassessment_eligible                  = 0\nranking_eligible                     = 0\n```\n\nO contrato semântico **não** abre gates formais.\n",
        "## 46. Avaliação aberta; score e ranking geral continuam bloqueados\n\nAtualmente:\n\n```text\nsemantic_public_assessment_supported = 85\nassessment_eligible                  = 85\nassessment_not_eligible              = 72\nranking_preflight_candidates         = 85\nranking_eligible                     = 0\n```\n\nO contrato semântico não abriu gates sozinho. O `Assessment Eligibility Contract` abriu formalmente a avaliação para 85; o `Ranking Eligibility Preflight` manteve a ordem total bloqueada; o `Exploratory Leaderboards Contract` abriu somente rankings unidimensionais e coleções explicitamente estreitas.\n",
    ),
    (
        "A população atual de 85 semanticamente completas **não** pode ser descrita como ranking integral do mercado.\n",
        "A população atual de 85 avaliáveis **não** pode ser descrita como ranking integral do mercado nem como ordem total 1–85. O `Ranking Eligibility Preflight` encontrou 1.198 pares empatados e 222 incomparáveis entre 3.570 pares. Isso não impede leaderboards em que uma única métrica declarada define literalmente a ordem, desde que o título não seja convertido em ‘melhor seguradora’.\n",
    ),
    (
        "- `V2 Cross-Pillar Assessment Semantic Contract`.\n",
        "- `V2 Cross-Pillar Assessment Semantic Contract`;\n- `V2 Assessment Eligibility Contract`;\n- `V2 Ranking Eligibility Preflight`;\n- `V2 Exploratory Leaderboards Contract`.\n",
    ),
]

for old, new in replacements:
    count = text.count(old)
    if count != 1:
        raise SystemExit(
            f"README sync expected exactly one match, found {count}: {old[:90]!r}"
        )
    text = text.replace(old, new)

marker = "\n# FONTES\n"
if text.count(marker) != 1:
    raise SystemExit("README exploration insertion marker must occur once")

section = r'''
# EXPLORAÇÃO PÚBLICA E COMPARADOR

## Produto principal: avaliação semântica + comparação lado a lado

A direção pública prioritária da v2 é:

```text
busca por seguradora
→ avaliação semântica individual
→ comparação de 2–4 seguradoras nos mesmos eixos
→ exploração por leaderboards e coleções específicas
```

A comparação lado a lado não cria vencedor automático e não calcula score composto.

## Exploratory Leaderboards Contract — FECHADO

Regra:

> **um leaderboard numérico só existe quando a própria métrica define a ordem; conceitos compostos permanecem coleções semânticas ou não suportados.**

Leaderboards públicos autorizados:

```text
largest_by_direct_premium            132 candidatas
highest_pla_cmr_ratio                155 candidatas
highest_ilt                          156 candidatas
lowest_conduct_pressure_ratio         41 candidatas
highest_conduct_pressure_ratio        26 candidatas
```

Todos usam `competition rank`. Empates permanecem empates; não existe desempate secundário de mérito.

Coleções públicas autorizadas:

```text
financial_core_without_current_adverse_signal   120
favorable_joint_assessment                       46
favorable_with_below_expected_conduct             33
conduct_improving_but_still_adverse                4
conduct_persistent_above_expected                 20
```

Coleções são `ordered = false`.

Conceitos atualmente bloqueados:

```text
mais_popular                not_supported
emergente_promissora        not_supported
consagrada_exemplar         not_supported
crescimento_de_premio       not_supported
ranking_geral               not_supported
```

`financeiro_mais_em_dia` é traduzido apenas como coleção semântica de ausência de sinal financeiro central adverso; não como Top 10.

## JSONs públicos para o HostGator

```text
data/derived/v2/exploratory_leaderboards_contract.json
data/derived/v2/public/insurer_explorer.json
data/derived/v2/public/explore_index.json
data/derived/v2/public/leaderboards/*.json
data/derived/v2/public/collections/*.json
```

Regra de integração:

```text
php_may_recompute_methodology = false
```

O PHP carrega, busca, filtra e renderiza. Não recalcula indicadores, não inventa score, não converte coleção em ranking e não declara vencedor geral.

Validação real:

```text
V2 Exploratory Leaderboards Contract
run                     33040347388
job                     98412282069
Ruff                    verde
testes                  7/7
build real              verde
boundaries              verdes
artifact                9633622703
SHA256 ZIP              ebedc4ea8d10959ab3dbb01000d923e4f57d1cb2db960dfbec7ff54a93598905
arquivos públicos       12
ranking_eligible         0
```

Próximo estágio:

```text
public_api_json_packaging_and_frontend_php_integration
```

---
'''

text = text.replace(marker, "\n" + section + marker, 1)
path.write_text(text, encoding="utf-8")
