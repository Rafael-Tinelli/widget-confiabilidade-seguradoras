# §19.2 — Auditoria do potencial informacional do frontend

Status: **EM ANDAMENTO**.

Início: 01/09/2026.

Este documento registra a auditoria entre o que a v2 já sabe, o que o pacote público efetivamente expõe e o que o frontend versionado realmente consome.

O objetivo do §19.2 não é redesenhar UX/SEO. Essa revisão pertence ao §19.6. Aqui a pergunta é mais básica e contratual:

> **A informação metodologicamente aprovada chega ao frontend sem ser perdida, reinterpretada ou recalculada?**

## 1. Regra de auditoria

Cada capacidade recebe uma destas classificações:

```text
PUBLIC_READY   — backend já publica informação suficiente para renderização
PUBLIC_PARTIAL — existe informação útil, mas o contrato público ainda está incompleto
FRONTEND_USED  — frontend consome o contrato v2 corretamente
FRONTEND_GAP   — contrato v2 existe, mas a UI atual não o usa
BACKEND_GAP    — informação útil permanece presa em artifact interno e deve ser projetada ao público
```

Regra permanente:

```text
frontend_gap -> corrigir wiring/renderização
backend_gap  -> ampliar payload público
nunca        -> recomputar metodologia em React/PHP/JS
```

## 2. Baseline empírico usado na auditoria

A inspeção parte da Full Generation Proof #49:

```text
run        33567550092
head       7993dbabd1cf3cd21181c88d072aed4ce5573538
artifact   9824434275
sha256     d0ccb6ce274542015431ae9fde0084c12941d1983ce06527bf6872e442244431
conclusion success
```

Fotografia do pacote público:

```text
profiles                                      790
entity profiles                               777
brand profiles                                 13
ordinary current insurer profiles             156
assessment available                           82
assessment incomplete                          74
sandbox entity profiles                        12
special purpose insurer profiles                3
entities with legal lifecycle                 312
entities with economic-group context          179
entities with explicit successor                5
entities with incoming brands                  13
entities with direct relationships               9
entities with Conduct reconciliation             9
entities with Sandbox Conduct                  12
explorer entities in >=1 leaderboard           47
explorer entities in >=1 semantic collection  131
```

Esses números são snapshot, não constantes metodológicas.

## 3. Diagnóstico do frontend versionado

O frontend atual em `widget-ui/src/` ainda é a implementação v1.

### `App.jsx`

Estado observado:

```text
API principal       /api/v1/insurers.json
ordenação padrão    score_desc
rótulo              Melhor Nota
ordenação adicional Maior Faturamento
copy                solvência + reputação + Open Insurance + nota
```

Ele não lê atualmente:

```text
/ranking-seguradoras/data/v2/public/search_index.json
/ranking-seguradoras/data/v2/public/profile_manifest.json
/ranking-seguradoras/data/v2/public/insurer_explorer.json
/ranking-seguradoras/data/v2/public/explore_index.json
/ranking-seguradoras/data/v2/public/profiles/*.json
/ranking-seguradoras/data/v2/public/leaderboards/*.json
/ranking-seguradoras/data/v2/public/collections/*.json
```

### `InsurerCard.jsx`

A superfície ainda representa a arquitetura antiga:

```text
Nota
Solvência
Reputação
Open Insurance
```

Também infere participação em Open Insurance por score quando a flag não está disponível. Isso não pertence ao contrato metodológico v2.

### `InsurerScoreModal.jsx`

O modal ainda trabalha com pesos e contribuições da nota v1 e contém fallback de recomposição no React.

Mesmo que o backend seja tratado como autoridade quando disponível, o fallback continua incompatível com a v2 porque:

```text
score composto               proibido
peso Financeiro x Conduta    não selecionado
frontend recomputa método    proibido
Open Insurance como pilar    não faz parte do contrato v2 fechado
```

Conclusão da baseline:

> **o gargalo principal do §19.2 é uma migração de contrato de dados, não falta generalizada de informação no backend.**

## 4. Matriz backend público → frontend

| Informação exigida pelo §19.2 | Onde já existe na v2 | Estado do payload | Frontend atual | Decisão |
|---|---|---|---|---|
| identidade | `search_index.json` + `profiles/*.json` | PUBLIC_READY | FRONTEND_GAP | migrar busca/perfil para v2 |
| aliases | `search_index.entries[].aliases` | PUBLIC_PARTIAL | FRONTEND_GAP | marcas possuem aliases; investigar aliases de entidades antes de ampliar |
| lifecycle | `profile.lifecycle.legal_lifecycle` | PUBLIC_READY | FRONTEND_GAP | renderizar contexto, sem inferência |
| sucessões | `profile.lifecycle.successor_*` + `direct_relationships` | PUBLIC_READY | FRONTEND_GAP | permitir navegação ao sucessor |
| grupos | `profile.relationship_context.economic_group` | PUBLIC_READY | FRONTEND_GAP | mostrar como contexto, nunca como sucessão |
| marcas | perfis `profile_kind=brand` + `relationship_context.brands` | PUBLIC_READY | FRONTEND_GAP | busca deve encontrar marca e preservar identidade separada |
| risk carriers | `brand.relationships` / `relationship_context.brands` | PUBLIC_READY | FRONTEND_GAP | renderizar relação verificada |
| relações de Conduta | `relationship_context.conduct_reconciliation` | PUBLIC_READY | FRONTEND_GAP | mostrar reconciliação e limite de atribuição |
| Sandbox Conduct | `sandbox_conduct` / `sandbox_conduct_context` | PUBLIC_READY | FRONTEND_GAP | superfície própria, fora do benchmark ordinário |
| sinais financeiros | `assessment.financial` / `explorer.financial` | PUBLIC_READY | FRONTEND_GAP | renderizar estado + valor + significado |
| sinais de reclamações | `assessment.conduct` / `explorer.conduct` | PUBLIC_READY | FRONTEND_GAP | renderizar estado + observada/esperada quando disponível |
| períodos | `financial.reference_period`; janela mensal existe no artifact de Conduta | PUBLIC_PARTIAL | FRONTEND_GAP | **BACKEND_GAP: publicar janela de Conduta explicitamente** |
| comparabilidade | `assessment.conduct.comparability_state` | PUBLIC_READY | FRONTEND_GAP | nunca inferir a partir de números brutos |
| confiança | `assessment.financial.evidence_confidence` | PUBLIC_READY | FRONTEND_GAP | disclosure, não penalidade |
| limites | `assessment.mandatory_limit` + `profile.limits` + metric `meaning/public_use/zero_semantics` | PUBLIC_READY | FRONTEND_GAP | renderizar progressivamente |
| leaderboards | `explore_index.json` + `leaderboards/*.json` | PUBLIC_READY | FRONTEND_GAP | somente métrica declarada; nunca relabelar como ranking geral |
| coleções | `explore_index.json` + `collections/*.json` | PUBLIC_READY | FRONTEND_GAP | manter não ordenadas |

Resumo:

```text
17 capacidades auditadas
15 PUBLIC_READY
 2 PUBLIC_PARTIAL
17 FRONTEND_GAP em relação ao contrato v2
```

Os dois `PUBLIC_PARTIAL` não têm a mesma gravidade:

1. **período de Conduta** — gap confirmado; a janela está nos artifacts internos e precisa ser materializada no contrato público;
2. **aliases de entidades** — o contrato público possui aliases para marcas, mas nenhuma das 777 entidades do snapshot possui aliases no `search_index`; antes de inventar aliases, é necessário verificar se existe fonte canônica upstream que os sustente.

## 5. Janela de Conduta — gap público confirmado

Na geração #49, as 101 entidades comparáveis possuem a mesma janela mensal preservada:

```text
start_month 2025-07
end_month   2026-06
months      12
```

O perfil público informa hoje apenas `comparable_months`; não informa explicitamente o início/fim da janela.

O frontend não deve deduzir a janela a partir de `generated_at`, competência financeira ou contagem de meses.

Decisão:

```text
publicar conduct.reference_window no backend
frontend apenas renderiza
```

A regra deve continuar válida também para entidades não comparáveis, pois as reclamações observadas pertencem à mesma janela preservada do snapshot de Conduta.

## 6. Arquitetura de consumo recomendada para a migração v2

A migração deve evitar carregar 790 perfis completos de uma vez.

Fluxo recomendado:

```text
1. search_index.json
   -> busca, aliases, CNPJ, FIP, tipo e profile_path

2. profiles/<id>.json sob demanda
   -> resposta individual completa
   -> identidade, lifecycle, relações, assessment, Sandbox, limites

3. insurer_explorer.json
   -> lista/comparação das 156 seguradoras ordinárias
   -> estados semânticos e métricas já calculados

4. explore_index.json
   -> registry de leaderboards/coleções e caveats

5. leaderboards/*.json / collections/*.json sob demanda
   -> exploração secundária
```

`profile_manifest.json` permanece disponível para resolução/validação de paths quando necessário.

## 7. O que pode ser reaproveitado do frontend atual

Sem entrar ainda em UX/SEO do §19.6, podem ser preservados como infraestrutura neutra:

- shell React/Vite;
- campo de busca;
- paginação, se ainda fizer sentido após a migração;
- tratamento de loading/erro;
- integração com header WordPress;
- acessibilidade básica de teclado/modal;
- componentes visuais genéricos que não carreguem semântica v1.

Não devem ser tratados como fundação metodológica reutilizável:

```text
score_desc
Melhor Nota
Nota
solvencyScore/reputationScore/innovationScore
weights/contributions
Open Insurance como terceiro pilar
fallback de score no React
inferência de participação OPIN por score
```

## 8. Próximas ações do §19.2

Ordem de execução:

```text
A. materializar janela de Conduta no payload público
B. criar camada de acesso v2 no widget sem lógica metodológica
C. migrar busca para search_index/profile_path
D. migrar perfil individual para profiles/*.json
E. migrar lista/comparação para insurer_explorer.json
F. ligar leaderboards e coleções ao explore_index
G. auditar aliases de entidades upstream
H. executar regressão: nenhum score/peso/metodologia v1 no caminho ativo
I. fechar matriz 17/17 e formalizar §19.2
```

## 9. Limites desta etapa

O §19.2 não autoriza:

- escolher nova estética;
- reabrir SEO;
- definir layout final mobile/desktop;
- restaurar score antigo;
- inventar ordem default de mérito;
- mudar metodologia para facilitar o frontend.

Esses assuntos pertencem a outras etapas, sobretudo §19.6 para polimento de frontend/SEO.
