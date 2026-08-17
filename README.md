# Ranking de Seguradoras Sanida — Pipeline de Dados e Metodologia v2

> **Status do projeto:** refatoração metodológica e arquitetural em andamento.  
> Este README funciona, nesta fase, como **contrato de projeto, guia de implementação e registro das decisões já tomadas**.  
> Regras marcadas como **EM VALIDAÇÃO** não devem ser tratadas como metodologia definitiva nem incorporadas ao scoring sem os testes previstos neste documento.

---

## 1. Objetivo

O projeto mantém a camada de dados e inteligência que alimenta a ferramenta pública da Sanida para consulta de seguradoras, empresas em regimes regulatórios específicos, marcas e outras entidades relacionadas ao mercado de seguros.

A ferramenta pública deve responder rapidamente às dúvidas que motivam a maior parte das consultas do usuário:

- **Esta seguradora é confiável?**
- **Ela apresenta bons sinais institucionais?**
- **Como ela se compara a outras seguradoras realmente comparáveis?**
- **Por que recebeu essa avaliação?**
- **O nome pesquisado é realmente uma seguradora?**
- **Quem é a entidade regulada por trás de uma marca?**
- **A empresa está em regime ordinário, Sandbox ou outra condição regulatória identificável?**

A complexidade metodológica deve existir **por trás da resposta**, não antes dela.

O usuário não deve precisar compreender o funcionamento do SES, PLA, CMR, SusepCon, matching de entidades ou fórmulas contábeis para obter uma resposta inicial útil.

A transparência deve ser progressiva:

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

## 2. Princípio central do produto

O projeto não existe para produzir uma tabela tecnicamente sofisticada. Ele existe para **ajudar o consumidor a tomar uma decisão melhor**.

A inteligência deve, portanto, seguir esta ordem:

```text
1. identificar corretamente o que foi pesquisado
2. entender o status e o papel da entidade
3. decidir se ela pode ser avaliada
4. decidir com quem ela pode ser comparada
5. calcular os indicadores permitidos pelas fontes
6. produzir uma avaliação
7. explicar o resultado
8. somente então posicioná-la em um ranking, quando aplicável
```

A ordem inversa — calcular uma nota primeiro e tentar explicar depois — não é aceita na v2.

---

## 3. Escopo do repositório

### 3.1. Responsabilidade do GitHub

A partir da v2, este repositório será responsável por:

- coleta de fontes;
- cache e contingência das fontes;
- normalização;
- resolução de identidade;
- classificação das entidades;
- relações entre marcas e entidades jurídicas;
- consolidação de status regulatório;
- cálculo de indicadores;
- aplicação da metodologia;
- elegibilidade para avaliação;
- elegibilidade para ranking;
- formação das coortes comparáveis;
- geração dos rankings;
- validações;
- auditoria;
- snapshots históricos;
- testes automatizados;
- geração e publicação dos JSONs consumidos pelo site.

O repositório deve terminar na **camada de dados**.

### 3.2. Fora do escopo do GitHub

A v2 **não terá responsabilidade pelo frontend da página pública**.

Devem sair progressivamente deste repositório:

- React;
- Vite;
- HTML da página;
- componentes visuais;
- CSS público;
- modais;
- cards;
- textos editoriais;
- H1/H2;
- `<title>`;
- meta description;
- canonical;
- dados estruturados de página;
- conteúdo de SEO;
- regras de apresentação;
- tradução dos códigos internos em frases de interface.

A apresentação será construída diretamente no ambiente da Sanida com:

```text
PHP
HTML
CSS
JavaScript
```

O PHP deverá receber dados semanticamente prontos.

**Nenhuma regra de scoring, matching, elegibilidade ou classificação regulatória poderá ser reconstruída no PHP ou no JavaScript.**

---

## 4. Arquitetura-alvo

```text
FONTES OFICIAIS / PÚBLICAS
          │
          ├── SUSEP / SES
          ├── licenciamentos e cadastros SUSEP
          ├── Sandbox SUSEP
          ├── SusepCon / BDR, quando utilizável
          ├── Open Insurance
          └── Consumidor.gov, quando aplicável
          │
          ▼
COLETA + CACHE + VALIDAÇÃO DE FONTE
          │
          ▼
NORMALIZAÇÃO
          │
          ▼
IDENTIDADE CANÔNICA
          │
          ├── entidade jurídica
          ├── CNPJ
          ├── código FIP/SUSEP
          ├── tipo
          ├── situação
          ├── regime
          ├── grupo econômico
          └── relações com marcas
          │
          ▼
RESOLUÇÃO DE ENTIDADES E MARCAS
          │
          ▼
INDICADORES
          │
          ▼
ELEGIBILIDADE
          │
          ├── avaliação
          ├── ranking
          └── coorte comparável
          │
          ▼
METODOLOGIA v2
          │
          ▼
VALIDAÇÃO
          │
          ▼
JSONs PÚBLICOS
          │
══════════╪════════════════════════════════════
          │
          ▼
SANIDA.COM.BR
          │
          ├── PHP / HTML / SEO
          ├── CSS
          └── JavaScript de interação
```

---

## 5. O que a ferramenta avalia — e o que não avalia

### 5.1. Território principal

A ferramenta pretende avaliar **sinais institucionais de confiabilidade da seguradora**, usando dados públicos e metodologia própria, documentada e reproduzível.

Uma avaliação positiva significa que os indicadores utilizados pela metodologia apresentam sinais favoráveis dentro do universo comparável.

Ela **não significa**, isoladamente, que:

- qualquer produto daquela seguradora é adequado ao cliente;
- a seguradora tem o menor preço;
- determinada cobertura é melhor;
- determinada apólice é mais ampla;
- uma indenização específica será paga;
- a empresa é “a melhor” para todo tipo de seguro.

### 5.2. Ramos de atuação

Os **ramos de atuação** podem ser informados na ficha da seguradora quando for possível apresentá-los de maneira simples, correta e útil.

Exemplos de linguagem pública possível:

```text
Principais áreas de atuação:
Automóvel, residencial, empresarial e vida
```

Contudo:

- ramos **não são um eixo principal do widget**;
- a ferramenta não será transformada em ranking por intenção comercial como auto, vida, empresarial, garantia etc.;
- páginas especializadas da Sanida continuam sendo responsáveis por explorar essas necessidades, produtos e intenções de busca;
- um ramo não deve receber peso na nota apenas por existir;
- a informação deve servir apenas para ajudar o usuário a compreender o perfil da seguradora.

---

## 6. Princípios metodológicos já aprovados

Os princípios abaixo são **decisões de projeto**.

### 6.1. Identidade vem antes da nota

Uma entidade só pode ser avaliada depois que sua identidade for resolvida com segurança suficiente.

Nome comercial não é chave primária.

Sempre que possível, a identidade deve convergir para:

```text
entity_id
CNPJ
código FIP/SUSEP
razão social
tipo de entidade
status regulatório
```

### 6.2. Coisas diferentes não disputam o mesmo ranking

A busca pública pode localizar:

- seguradora autorizada;
- participante do Sandbox;
- antiga participante do Sandbox;
- marca comercial;
- plataforma;
- intermediário;
- corretora;
- ressegurador;
- entidade de previdência;
- capitalização;
- associação ou entidade de proteção patrimonial mutualista;
- assistência;
- outra entidade identificável.

Isso **não significa que todas sejam elegíveis para uma nota ou para o ranking de seguradoras**.

Regra:

> **A busca é ampla. O ranking é restrito.**

### 6.3. Dado ausente nunca vale zero

Ausência de um indicador não é evidência de desempenho ruim.

É proibido converter automaticamente:

```text
dado ausente → 0 pontos
```

### 6.4. Não haverá redistribuição silenciosa dos pilares principais

Se a metodologia geral exigir dois pilares e um deles não puder ser calculado de maneira válida:

```text
overall_score = null
```

A ferramenta poderá exibir os indicadores disponíveis, mas não deverá transformar automaticamente o pilar restante em 100% da nota sem regra metodológica explícita e previamente aprovada.

### 6.5. Nota e qualidade da evidência são conceitos diferentes

A v2 deverá separar:

```text
score
```

de:

```text
assessment_confidence
```

A nota responde:

> Como se comportaram os indicadores avaliados?

A confiança da avaliação responde:

> Quão robustos e completos são os dados que sustentam essa conclusão?

### 6.6. Ranking é consequência

A posição no ranking nunca entra no cálculo da nota.

Fluxo obrigatório:

```text
indicadores
→ nota
→ elegibilidade
→ coorte
→ ordenação
→ posição
```

### 6.7. Open Insurance não gera pontos de confiabilidade

Participação no Open Insurance pode ser exibida como informação contextual.

Não integra, por princípio, a nota de confiabilidade.

### 6.8. Porte não é sinônimo de confiabilidade

Não conferem pontos por si mesmos:

- prêmio/arrecadação absoluta;
- participação de mercado;
- patrimônio absoluto;
- tamanho do grupo econômico.

Esses dados podem contextualizar o perfil da empresa.

### 6.9. Sinistralidade isolada não mede confiabilidade

Sinistralidade pode ajudar na compreensão econômica da operação, mas não deverá ser interpretada como:

```text
menos sinistros = seguradora melhor
```

Nem deverá ser usada isoladamente como proxy de solvência ou qualidade.

### 6.10. Matching incerto não pode produzir verdade categórica

Matching fuzzy, aliases e heurísticas são ferramentas para localizar candidatos.

Não são autorização para transformar uma hipótese em identidade confirmada.

### 6.11. A metodologia pertence ao backend

O site recebe o resultado.

Não calcula:

- score;
- rating;
- confiança;
- ranking;
- matching;
- situação regulatória;
- elegibilidade.

---

## 7. Resolvedor de identidade

A ferramenta pública deverá aceitar consultas por nome sem exigir que o usuário conheça previamente a natureza jurídica daquilo que está buscando.

Exemplo:

```text
“Nome pesquisado”
       ↓
alias / marca / razão social
       ↓
entidade ou conjunto de entidades relacionadas
       ↓
papel regulatório
       ↓
resposta adequada ao tipo encontrado
```

### 7.1. Modelo conceitual

Uma relação poderá conter, conforme a disponibilidade e a confiabilidade da fonte:

```text
brand_id
entity_id
relationship_type
status
source
confidence
```

Datas de vigência podem ser guardadas quando forem fornecidas de forma estruturada e sustentável pela fonte, mas a experiência pública deve priorizar o **estado atual calculado automaticamente**.

O objetivo operacional é minimizar manutenção editorial.

### 7.2. Marcas não herdam nota

Se uma marca estiver associada a uma seguradora:

```text
Marca X
→ risco assumido pela Seguradora Y
```

a nota pertence à **Seguradora Y**.

A interface poderá mostrar:

> Avaliação da seguradora responsável: 84/100

Não deverá publicar:

> Marca X: 84/100

quando a marca não for a própria entidade avaliada.

---

## 8. Matriz Metodológica v2

### 8.1. Camadas

A v2 será organizada em cinco camadas:

```text
Identidade
→ Elegibilidade
→ Evidências
→ Avaliação
→ Ranking
```

### 8.2. Identidade e contexto

| Informação | Fonte principal | Pontua? | Função |
|---|---|---:|---|
| CNPJ | SUSEP | Não | Identidade |
| Código FIP/SUSEP | SUSEP/SES | Não | Integração |
| Razão social | SUSEP | Não | Identidade |
| Nome de exibição | Derivado/curado | Não | UX |
| Tipo da entidade | SUSEP + normalização | Não | Universo |
| Situação atual | SUSEP | Não | Gate |
| Regime regulatório | SUSEP | Não | Gate |
| Grupo econômico | SUSEP | Não | Contexto |
| Ramos de atuação | SES | Não | Contexto |
| Open Insurance | Fonte oficial | Não | Contexto |
| Marca/alias | Fonte verificável/curadoria | Não | Resolução de busca |

### 8.3. Elegibilidade

A entidade deverá possuir estados independentes.

Exemplo conceitual:

```json
{
  "assessment_eligible": true,
  "ranking_eligible": true,
  "comparison_cohort": "licensed_insurers"
}
```

#### Seguradora ordinária com dados suficientes

```text
avaliação geral: sim
ranking: sim
```

#### Seguradora ordinária com financeiro suficiente e atendimento insuficiente

```text
avaliação financeira: sim
avaliação geral: não
ranking geral: não
```

#### Sandbox

```text
identificação: sim
status regulatório: sim
avaliação geral v2: não, salvo metodologia futura específica
ranking de seguradoras ordinárias: não
```

#### Marca

```text
resolução: sim
nota própria: não
ranking próprio: não
direcionamento à entidade responsável: quando possível
```

---

## 9. Pilares candidatos da avaliação geral

A hipótese de trabalho atual utiliza dois pilares:

```text
1. solidez econômico-financeira
2. atendimento / reclamações
```

A escolha final dos pesos ainda está **EM VALIDAÇÃO**.

### Hipótese inicial

```text
Financeiro:   60%
Atendimento:  40%
```

Esses pesos **não estão aprovados**.

Antes de serem incorporados como regra definitiva, deverão passar por:

- análise de distribuição;
- análise de sensibilidade;
- inspeção de outliers;
- testes em seguradoras conhecidas;
- estabilidade temporal;
- comparação com cenários 50/50, 55/45, 60/40, 65/35 e outros relevantes;
- avaliação do quanto pequenas mudanças de peso alteram o ranking.

Se alterações pequenas de peso provocarem mudanças excessivas de classificação, a metodologia deverá ser revista.

---

## 10. Pilar econômico-financeiro

A versão atual do projeto utiliza um indicador denominado “Solvência” baseado, entre outros fatores, em patrimônio, escala e sinistralidade.

Essa lógica deverá ser **substituída**.

A Base Completa da SUSEP disponibiliza elementos mais apropriados para a análise econômico-financeira e prudencial.

### 10.1. Componentes candidatos

Hipótese inicial:

| Componente | Peso interno candidato | Status |
|---|---:|---|
| Capital regulatório | ~45% | EM VALIDAÇÃO |
| Liquidez | ~30% | EM VALIDAÇÃO |
| Sustentabilidade operacional | ~25% | EM VALIDAÇÃO |

Esses pesos não são definitivos.

### Não entram isoladamente na nota

- sinistralidade;
- prêmio total;
- patrimônio absoluto;
- market share;
- rentabilidade isolada, até validação;
- tamanho da empresa.

---

## 11. Capital regulatório

A principal medida candidata é a relação:

```text
PLA / CMR
```

ou outra expressão equivalente metodologicamente aprovada com base nos dados oficiais.

### 11.1. Princípios aprovados

O indicador não será tratado linearmente.

Não é aceitável inferir:

```text
PLA/CMR 2,0 = duas vezes melhor que PLA/CMR 1,0
```

A transformação deverá respeitar:

- significado regulatório;
- zonas economicamente relevantes;
- ganhos decrescentes;
- saturação;
- histórico;
- estabilidade;
- situações de proximidade ou insuficiência.

### 11.2. EM VALIDAÇÃO

Antes da fórmula definitiva serão testados:

- valor atual;
- média recente;
- mínimo recente;
- volatilidade;
- frequência de aproximação do limite;
- períodos ideais de observação;
- comportamento dos extremos da distribuição;
- situações de CMR zero, negativo, ausente ou não aplicável.

---

## 12. Liquidez

A liquidez será estudada a partir dos campos e conceitos econômico-financeiros compatíveis com a estrutura SUSEP.

### EM VALIDAÇÃO

Será necessário testar:

- índice de liquidez corrente;
- índice de liquidez total;
- eventual combinação;
- redundância estatística com PLA/CMR;
- comportamento por tipo de entidade;
- zonas de adequação;
- saturação;
- estabilidade temporal.

Indicadores altamente redundantes não devem receber pesos independentes que contem a mesma característica duas vezes.

---

## 13. Sustentabilidade operacional

O componente operacional deve medir, na medida permitida pelos dados, o equilíbrio econômico da operação.

O estudo deverá priorizar conceitos mais completos que sinistralidade isolada, como indicadores combinados ou equivalentes compatíveis com a metodologia oficial disponível.

### EM VALIDAÇÃO

Testar:

- índice combinado;
- índice combinado ampliado;
- custos de aquisição;
- despesas administrativas;
- resultado financeiro;
- estabilidade em múltiplos períodos;
- influência do mix operacional;
- outliers;
- comparabilidade.

Nenhum indicador será incluído apenas porque está disponível na base.

---

## 14. Atendimento e reclamações

O objetivo deste pilar é responder, com dados comparáveis:

> Como a empresa se comporta diante de problemas relatados pelos consumidores?

### 14.1. Hierarquia pretendida

Preferência por fonte regulatória oficial que forneça medida comparável e normalizada, especialmente quando houver estrutura adequada do SusepCon/BDR.

O Consumidor.gov poderá funcionar como:

- fonte complementar;
- evidência adicional;
- informação de resolução/satisfação;
- contingência metodológica, se aprovada.

A presença no Consumidor.gov não deve ser automaticamente convertida em verdade sobre uma pessoa jurídica quando a fonte não permitir identidade inequívoca.

### 14.2. EM VALIDAÇÃO

Definir:

- fonte principal automatizável;
- atualização disponível;
- granularidade;
- amostra mínima;
- tratamento de baixa amostra;
- normalização por porte/arrecadação;
- coortes;
- estabilidade;
- substituição/fallback entre SusepCon, BDR e Consumidor.gov;
- efeitos da indisponibilidade temporária da fonte.

---

## 15. Confiança da avaliação

A confiança da avaliação não aumenta nem reduz a nota.

Ela representa a robustez da evidência.

Fatores candidatos:

- identidade confirmada;
- atualidade;
- histórico financeiro suficiente;
- completude;
- consistência temporal;
- qualidade do vínculo entre fontes;
- amostra de reclamações;
- comparabilidade.

Exemplo conceitual:

```json
{
  "score": 84.2,
  "rating": "high",
  "confidence": "high"
}
```

ou:

```json
{
  "score": null,
  "rating": null,
  "confidence": "limited"
}
```

A taxonomia final de confiança está **EM VALIDAÇÃO**.

---

## 16. Classificação verbal da nota

O frontend deverá traduzir o score para linguagem compreensível.

As faixas ainda não estão aprovadas.

Exemplo apenas para teste:

| Score | Rótulo candidato |
|---:|---|
| 85–100 | Confiabilidade muito alta |
| 70–84 | Confiabilidade alta |
| 55–69 | Confiabilidade moderada |
| < 55 | Indicadores exigem atenção |

**Não implementar estas faixas como regra definitiva sem calibração.**

Em especial, a metodologia deverá evitar afirmações categóricas como “não confiável” quando os indicadores não sustentarem esse nível de conclusão.

---

## 17. Ranking

O ranking deverá conter apenas entidades:

- corretamente identificadas;
- pertencentes ao universo definido;
- elegíveis para avaliação;
- com dados suficientes;
- comparáveis dentro da coorte;
- avaliadas pela mesma versão da metodologia.

A apresentação pública deverá preferir linguagem como:

> 8ª entre 41 seguradoras elegíveis nesta comparação

em vez de:

> 8ª melhor seguradora do Brasil

quando a metodologia não sustentar a segunda afirmação.

### 17.1. Não haverá ranking por produto nesta ferramenta

Não é objetivo deste projeto criar rankings independentes de:

- auto;
- vida;
- empresarial;
- garantia;
- residencial;
- viagem;
- outros produtos.

Essas intenções pertencem a páginas editoriais e ferramentas específicas da Sanida.

---

## 18. Schema JSON v2

A v2 deverá utilizar contrato novo e estrito.

A compatibilidade com aliases e estruturas históricas da API v1 **não será objetivo do novo schema**.

A migração deverá ocorrer de forma controlada, mantendo a v1 apenas durante a transição necessária.

### 18.1. Artefatos públicos propostos

```text
/api/v2/meta.json
/api/v2/entities.json
/api/v2/brands.json
/api/v2/rankings.json
```

A quantidade final poderá ser ajustada por razões de desempenho ou tamanho, desde que as responsabilidades permaneçam separadas.

---

## 19. `meta.json`

Responsável pelo estado global da publicação.

Exemplo:

```json
{
  "schema_version": "2.0",
  "methodology_version": "2.0-draft",
  "generated_at": "2026-08-17T03:00:00Z",
  "status": "ok",

  "periods": {
    "financial": "2026-05",
    "complaints": "2025-Q4",
    "licensing": "2026-08-17",
    "sandbox": "2026-08-17"
  },

  "counts": {
    "entities": 0,
    "assessed": 0,
    "ranking_eligible": 0,
    "brands": 0
  },

  "sources": {}
}
```

As datas acima são exemplos de contrato, não valores fixos.

O site poderá usar esse arquivo para informar ao usuário a atualidade das fontes.

---

## 20. `entities.json`

Responsável pelas entidades canônicas e por suas avaliações.

Exemplo de contrato preliminar:

```json
{
  "schema_version": "2.0",
  "entities": [
    {
      "id": "cnpj:12345678000190",

      "identity": {
        "legal_name": "EXEMPLO SEGURADORA S.A.",
        "display_name": "Exemplo Seguros",
        "cnpj": "12345678000190",
        "fip_code": "01234"
      },

      "classification": {
        "entity_type": "insurer",
        "regulatory_regime": "ordinary",
        "regulatory_status": "active",
        "economic_group": "Grupo Exemplo"
      },

      "activities": {
        "summary": [
          "automóvel",
          "residencial",
          "empresarial"
        ]
      },

      "features": {
        "open_insurance": true
      },

      "assessment": {
        "eligible": true,
        "methodology_version": "2.0-draft",

        "overall": {
          "score": 84.2,
          "rating": "high",
          "confidence": "high"
        },

        "financial": {
          "score": 87.1,

          "capital": {
            "score": 91.4,
            "pla": 1850000000,
            "cmr": 930000000,
            "pla_cmr_ratio": 1.989,
            "period": "2026-05"
          },

          "liquidity": {
            "score": 82.3,
            "period": "2026-05"
          },

          "operations": {
            "score": 84.0,
            "period": "2026-05"
          }
        },

        "complaints": {
          "score": 79.8,
          "source_id": "susep_complaints",
          "period": "2025-Q4",
          "comparison_group": "default"
        },

        "reason_codes": [
          "capital_comfortable",
          "capital_stable",
          "complaints_better_than_reference"
        ]
      },

      "ranking": {
        "eligible": true,
        "universe": "licensed_insurers",
        "position": 8,
        "total": 41
      }
    }
  ]
}
```

Os valores, rótulos e estruturas internas de indicadores acima são ilustrativos.

O schema deverá ser fechado somente após a fase de calibração e revisão da necessidade real de cada campo público.

---

## 21. Entidade com avaliação incompleta

Exemplo:

```json
{
  "assessment": {
    "eligible": false,

    "overall": {
      "score": null,
      "rating": null,
      "confidence": "limited"
    },

    "financial": {
      "score": 86.7
    },

    "complaints": {
      "score": null,
      "status": "insufficient_data"
    },

    "reason_codes": [
      "financial_data_available",
      "complaints_insufficient"
    ]
  },

  "ranking": {
    "eligible": false,
    "position": null,
    "reason": "incomplete_assessment"
  }
}
```

O site não deverá deduzir o motivo a partir de combinações de `null`.

O backend deve publicar estados semanticamente claros.

---

## 22. `brands.json`

Marcas não recebem score.

Exemplo:

```json
{
  "schema_version": "2.0",
  "brands": [
    {
      "id": "brand:exemplo",
      "name": "Exemplo",
      "aliases": [
        "Exemplo Seguro",
        "Exemplo Seguros"
      ],
      "classification": "commercial_brand",
      "resolution_status": "resolved",

      "relationships": [
        {
          "type": "risk_carrier",
          "entity_id": "cnpj:12345678000190",
          "status": "current",
          "source_id": "verified_source"
        }
      ]
    }
  ]
}
```

Quando não houver suporte suficiente:

```json
{
  "resolution_status": "unresolved",
  "relationships": []
}
```

Isso é preferível a um vínculo falso criado por similaridade de nomes.

---

## 23. `rankings.json`

O PHP não deve recalcular ranking.

Exemplo:

```json
{
  "schema_version": "2.0",
  "methodology_version": "2.0-draft",

  "rankings": {
    "licensed_insurers": {
      "total": 41,
      "items": [
        {
          "entity_id": "cnpj:11111111000111",
          "position": 1,
          "score": 92.4
        },
        {
          "entity_id": "cnpj:22222222000122",
          "position": 2,
          "score": 91.7
        }
      ]
    }
  }
}
```

---

## 24. Proveniência

A origem dos dados deve ser rastreável.

O JSON público não precisa repetir a descrição completa da fonte dentro de cada entidade.

`meta.json` poderá possuir catálogo semelhante a:

```json
{
  "sources": {
    "susep_pl_margem": {
      "provider": "SUSEP",
      "dataset": "Ses_pl_margem"
    },
    "susep_complaints": {
      "provider": "SUSEP",
      "dataset": "SusepCon/BDR"
    }
  }
}
```

As entidades referenciam `source_id` quando necessário.

---

## 25. Proveniência operacional interna

O pipeline deverá distinguir, internamente, como cada informação foi obtida.

Taxonomia candidata:

```text
automatic
derived
curated
unsupported
```

### Objetivo

- **automatic:** veio diretamente de fonte estruturada;
- **derived:** foi calculado de forma determinística a partir de fonte estruturada;
- **curated:** depende de associação manual/verificada;
- **unsupported:** não há suporte suficiente.

Regra desejada:

> Tudo que altera nota, elegibilidade ou situação regulatória deve ser automático ou derivado de fonte sustentável.

Curadoria poderá auxiliar a resolução de marcas, aliases e apresentação, mas não deverá fabricar situação regulatória ou alterar indicadores financeiros.

---

## 26. O que não deve existir na API pública v2

Não publicar sem necessidade concreta:

- match reports completos;
- candidatos de fuzzy matching;
- score interno de similaridade;
- logs;
- respostas HTTP brutas;
- arquivos originais completos;
- todas as contas do SES;
- estruturas de debug;
- aliases legados da API v1;
- campos duplicados;
- frases editoriais;
- HTML;
- CSS;
- JavaScript;
- texto de interface;
- explicações prontas dependentes de copy.

Exemplo de duplicação que deve desaparecer:

```text
openInsuranceParticipant
open_insurance_participant
opinParticipant
opin_participant
```

Na v2 haverá apenas um conceito canônico, por exemplo:

```json
{
  "open_insurance": true
}
```

---

## 27. `reason_codes`

O backend poderá publicar códigos semânticos que permitam ao site produzir explicações consistentes sem reproduzir a fórmula.

Exemplos preliminares:

```text
capital_comfortable
capital_near_requirement
capital_below_requirement
capital_stable
capital_volatile
liquidity_adequate
operations_balanced
complaints_better_than_reference
complaints_worse_than_reference
complaints_insufficient
financial_data_insufficient
identity_uncertain
not_ranking_eligible
sandbox_regime
```

Os códigos definitivos deverão ser documentados e testados.

O PHP transforma:

```text
capital_comfortable
```

em linguagem pública.

A regra matemática que gerou o código permanece no backend.

---

## 28. Validação obrigatória do build

Um JSON inválido não deve ser publicado.

O pipeline deverá falhar se detectar, entre outros:

- CNPJ duplicado em entidades incompatíveis;
- `entity_id` duplicado;
- marca apontando para entidade inexistente;
- score fora de 0–100;
- `ranking_eligible = true` com score geral ausente;
- posição de ranking para entidade inelegível;
- Sandbox dentro de ranking de autorização ordinária;
- indicador financeiro sem período;
- dado de fonte com período futuro;
- versão de metodologia incompatível;
- `rating` incompatível com o score;
- coorte inexistente;
- ausência de classificação;
- relação regulatória sem fonte mínima exigida;
- valores não finitos;
- queda anormal de cobertura da fonte;
- redução anormal do universo sem confirmação.

Além da validação de schema, deverão existir **sanity checks de dados**.

---

## 29. JSON Schema

A v2 deverá possuir arquivos formais de JSON Schema para validação automática.

Estrutura candidata:

```text
schemas/
├── meta.schema.json
├── entities.schema.json
├── brands.schema.json
└── rankings.schema.json
```

CI e workflows de publicação deverão validar todos os artefatos contra esses schemas antes do commit automatizado.

---

## 30. Auditoria e dados internos

A simplificação da API pública não significa redução da auditabilidade.

O repositório poderá preservar estruturas ricas internamente:

```text
data/
├── raw/
├── normalized/
├── derived/
├── snapshots/
└── audit/
```

Essas estruturas podem conter:

- arquivos de origem;
- snapshots;
- contas intermediárias;
- relatórios de matching;
- divergências;
- métricas de cobertura;
- testes de metodologia;
- explicações de exclusão;
- histórico de alterações.

A API pública deve ser enxuta.

A auditoria interna pode ser detalhada.

---

## 31. Atualização evergreen

O projeto deve minimizar manutenção editorial.

Situações como:

- entrada ou saída do Sandbox;
- mudança de status;
- atualização financeira;
- mudança de ranking;
- alteração de indicador;
- mudança em fonte de reclamações;

devem ser recalculadas automaticamente sempre que a fonte permitir.

Evitar depender de textos manuais como:

> “esta relação era válida até março de 2026”

para representar o estado atual.

Histórico poderá ser preservado internamente e apresentado quando for material, mas a resposta padrão deverá ser derivada do **estado vigente**.

Zero manutenção absoluta não é pressuposto do projeto: endpoints, formatos e regras regulatórias podem mudar.

O objetivo é **quase zero manutenção editorial rotineira**, com manutenção técnica apenas quando a infraestrutura das fontes mudar ou a metodologia for revisada.

---

## 32. Fontes

### 32.1. SUSEP / SES

A SUSEP deve ser a principal referência para:

- identidade regulatória;
- Código FIP;
- CNPJ quando disponível;
- dados financeiros;
- PLA;
- CMR;
- informações contábeis;
- grupos;
- atividades;
- séries históricas;
- demais informações oficiais úteis.

A Base Completa não deve ser interpretada como uma lista pronta de “seguradoras comparáveis”.

O pipeline deverá classificar o universo antes de avaliar.

### 32.2. LISTAEMPRESAS

`LISTAEMPRESAS.csv` é uma fonte auxiliar importante para integração entre códigos, CNPJs e nomes.

Não deverá ser usada isoladamente para definir o universo do ranking.

### 32.3. Sandbox SUSEP

O Sandbox deverá ser integrado ao resolvedor de entidades.

Participantes podem aparecer na busca pública, mas não devem ser misturados automaticamente ao ranking das seguradoras de regime ordinário.

O estado deverá ser atualizado de forma automática sempre que a fonte oficial permitir.

### 32.4. Open Insurance

Informação contextual.

Não compõe a nota de confiabilidade v2.

### 32.5. SusepCon / BDR

Fonte prioritária candidata para o componente regulatório de reclamações, condicionada à disponibilidade, estrutura, periodicidade e automação adequadas.

### 32.6. Consumidor.gov

Fonte pública complementar importante.

A metodologia v2 deverá reduzir a dependência de matching ambíguo marca ↔ pessoa jurídica.

Seu papel definitivo no score ainda está **EM VALIDAÇÃO**.

---

## 33. Estado da API v1

A API v1 e a metodologia atual são consideradas **legadas durante a refatoração**.

A v1 contém decisões que não deverão ser carregadas automaticamente para a v2, entre elas:

- pilar denominado “Solvência” construído por aproximações próprias;
- uso de sinistralidade como parte importante desse pilar;
- score de Open Insurance;
- ausência de reputação podendo gerar contribuição zero;
- aliases redundantes no JSON;
- responsabilidades de apresentação acopladas ao repositório;
- deduplicação defensiva no frontend;
- matching que precisa ser revisto à luz do novo modelo de identidade.

A v1 continuará funcional apenas pelo tempo necessário para construir, validar e migrar a v2.

Não será objetivo manter compatibilidade permanente entre os contratos.

---

## 34. Frontend legado

A pasta atual de frontend poderá permanecer temporariamente durante a transição, mas é considerada **fora da arquitetura-alvo**.

Componentes React/Vite não devem receber novas regras de negócio da v2.

A remoção acontecerá somente quando:

1. API v2 estiver válida;
2. publicação dos JSONs estiver funcionando;
3. página PHP da Sanida consumir a v2;
4. busca e ranking estiverem equivalentes ou superiores;
5. rollback estiver documentado.

---

## 35. Workflows e automação

A infraestrutura operacional existente de:

- cache;
- fallback;
- snapshots;
- CI;
- testes;
- refresh periódico;
- auditoria;
- proteção contra publicação de artefatos inválidos;

deve ser preservada e simplificada quando possível.

A refatoração não parte do pressuposto de reescrever toda a engenharia operacional.

O objetivo é remover complexidade acidental, principalmente:

- frontend no repo;
- aliases;
- compatibilidade histórica desnecessária;
- regras duplicadas;
- schema excessivamente amplo;
- normalizações ambíguas;
- campos de debug em produção.

---

## 36. Sequência de implementação

A ordem abaixo é parte do contrato do projeto.

### Fase 0 — README e contrato

- [x] definir objetivo;
- [x] separar GitHub e frontend;
- [x] estabelecer princípios metodológicos;
- [x] definir estrutura preliminar da API v2;
- [x] registrar pontos ainda em validação.

### Fase 1 — Inventário das fontes

- [ ] mapear arquivos relevantes da Base Completa;
- [ ] mapear chaves;
- [ ] mapear períodos;
- [ ] medir cobertura;
- [ ] documentar entidades presentes por fonte;
- [ ] testar atualização automática;
- [ ] classificar fonte como primária, complementar ou contextual.

**Saída:** artefato de mapeamento de fontes e cobertura.

### Fase 2 — Identidade canônica

- [ ] definir `entity_id`;
- [ ] consolidar Código FIP;
- [ ] consolidar CNPJ;
- [ ] normalizar razão social;
- [ ] classificar `entity_type`;
- [ ] classificar regime regulatório;
- [ ] incorporar grupos econômicos;
- [ ] resolver duplicidades;
- [ ] registrar divergências;
- [ ] criar testes de unicidade.

**Critério de conclusão:** o frontend não precisa deduplicar entidades.

### Fase 3 — Resolvedor de marcas

- [ ] separar marca e entidade jurídica;
- [ ] migrar aliases úteis;
- [ ] classificar origem dos vínculos;
- [ ] reduzir fuzzy matching a geração de candidatos quando necessário;
- [ ] impedir vínculo categórico sem suporte suficiente;
- [ ] integrar Sandbox;
- [ ] testar mudanças de estado;
- [ ] medir quantidade de relações `curated`.

**Meta:** evitar criar dependência de manutenção manual em escala.

### Fase 4 — Matriz quantitativa

- [ ] analisar PLA/CMR;
- [ ] testar janelas temporais;
- [ ] testar saturação;
- [ ] analisar liquidez;
- [ ] medir correlações;
- [ ] estudar indicadores operacionais;
- [ ] estudar reclamações;
- [ ] definir amostras mínimas;
- [ ] testar outliers;
- [ ] testar dados ausentes;
- [ ] testar empresas especializadas;
- [ ] testar estabilidade temporal.

**Somente depois:** definir fórmulas definitivas.

### Fase 5 — Pesos e score

- [ ] testar pesos candidatos;
- [ ] executar análise de sensibilidade;
- [ ] comparar rankings;
- [ ] revisar casos individuais;
- [ ] definir faixas verbais;
- [ ] definir confiança da avaliação;
- [ ] documentar limitações.

**Critério de aprovação:** a metodologia deve permanecer interpretável e razoavelmente estável diante de pequenas alterações de parâmetros.

### Fase 6 — Schema v2 definitivo

- [ ] revisar necessidade real de cada campo;
- [ ] congelar nomes;
- [ ] remover aliases;
- [ ] escrever JSON Schemas;
- [ ] escrever fixtures;
- [ ] validar `reason_codes`;
- [ ] definir versionamento.

### Fase 7 — Builders v2

- [ ] construir normalização;
- [ ] construir identidade;
- [ ] construir brands;
- [ ] construir assessment;
- [ ] construir ranking;
- [ ] gerar `meta.json`;
- [ ] gerar `entities.json`;
- [ ] gerar `brands.json`;
- [ ] gerar `rankings.json`.

### Fase 8 — Testes e auditoria

- [ ] schema;
- [ ] unicidade;
- [ ] integridade referencial;
- [ ] ausência de NaN/Infinity;
- [ ] períodos;
- [ ] cobertura;
- [ ] comparação com snapshots anteriores;
- [ ] regressões de identidade;
- [ ] regressões de score;
- [ ] regressões de ranking;
- [ ] Sandbox;
- [ ] marcas;
- [ ] fonte indisponível;
- [ ] cache;
- [ ] fallback.

### Fase 9 — Publicação dos JSONs

- [ ] adaptar workflow;
- [ ] publicar `/api/v2/`;
- [ ] preservar `/api/v1/` temporariamente;
- [ ] validar consumo em staging;
- [ ] documentar rollback.

### Fase 10 — Frontend no site

Esta fase ocorre fora da responsabilidade estrutural deste repositório.

No site:

- PHP lê v2;
- PHP produz HTML inicial;
- CSS controla apresentação;
- JavaScript controla busca e interação;
- SEO permanece sob controle da página;
- conteúdo editorial permanece separado dos dados.

### Fase 11 — Migração

- [ ] ativar v2 em produção;
- [ ] observar erros;
- [ ] comparar comportamento;
- [ ] remover dependência do bundle legado;
- [ ] remover publicação de assets React/Vite;
- [ ] desativar v1 quando seguro.

### Fase 12 — Limpeza

- [ ] remover `widget-ui`;
- [ ] remover aliases v1 desnecessários;
- [ ] remover código morto;
- [ ] remover compatibilidade temporária;
- [ ] simplificar workflows;
- [ ] revisar dependências.

### Fase 13 — Revisão final deste README

Ao final da refatoração:

- [ ] remover hipóteses rejeitadas;
- [ ] transformar regras aprovadas em documentação definitiva;
- [ ] substituir exemplos provisórios;
- [ ] documentar fórmula final;
- [ ] documentar fontes finais;
- [ ] documentar atualização;
- [ ] documentar schema final;
- [ ] documentar deployment;
- [ ] registrar limitações conhecidas.

---

## 37. Testes quantitativos obrigatórios antes de fechar a metodologia

Nenhum peso ou faixa deve ser aprovado apenas porque “parece razoável”.

Executar, no mínimo:

### Distribuição

- mínimo;
- máximo;
- mediana;
- percentis;
- outliers;
- cobertura.

### Correlação

Identificar componentes redundantes.

### Sensibilidade

Alterar pesos e observar:

- top 10;
- top 20;
- quartis;
- movimentações extremas;
- empresas especializadas;
- empresas pequenas;
- empresas grandes.

### Estabilidade temporal

Recalcular em diferentes períodos.

### Casos-limite

Investigar manualmente entidades que apresentem:

- score muito alto;
- score muito baixo;
- PLA/CMR próximo ao limite;
- ratios extremos;
- baixa amostra de reclamações;
- dados incompletos;
- mudança recente de regime;
- identidade ambígua.

### Sanity check econômico

O algoritmo deve ser matematicamente válido e economicamente interpretável.

Uma fórmula estatisticamente elegante que produz conclusões absurdas deve ser rejeitada.

---

## 38. Critérios para incluir um indicador

Um indicador só entra na nota quando passar por todos os testes:

1. possui fonte sustentável;
2. mede algo relacionado ao conceito pretendido;
3. possui interpretação econômica;
4. é comparável no universo definido;
5. não duplica excessivamente outro indicador;
6. possui tratamento razoável de extremos;
7. possui tratamento de ausência;
8. pode ser atualizado automaticamente;
9. pode ser explicado ao consumidor;
10. melhora a qualidade da conclusão.

A disponibilidade de uma coluna na base **não é razão suficiente para pontuá-la**.

---

## 39. Critérios de simplificação

A refatoração deve reduzir complexidade sem reduzir a capacidade de auditoria.

Preferir:

```text
uma chave canônica
uma regra
uma fonte primária
um significado
```

Evitar:

```text
aliases múltiplos
fallbacks sem semântica
regras reconstruídas em várias camadas
campos duplicados
debug público
heurísticas silenciosas
```

---

## 40. Versionamento

A API e a metodologia deverão ser versionadas separadamente.

Exemplo:

```text
schema_version: 2.0
methodology_version: 2.1
```

Uma alteração de fórmula pode mudar a metodologia sem necessariamente quebrar o contrato JSON.

Uma alteração estrutural incompatível deverá mudar a versão do schema.

Mudanças materiais na metodologia deverão ser registradas em changelog.

---

## 41. Transparência pública

A metodologia deve ser pública e compreensível, mas a primeira tela da ferramenta não deve ser sobrecarregada.

A apresentação ideal deverá permitir níveis progressivos:

```text
Nível 1
resultado

Nível 2
por que recebeu essa avaliação

Nível 3
indicadores e períodos

Nível 4
fontes, fórmula e metodologia
```

Transparência não significa despejar detalhes de implementação no usuário.

---

## 42. Linguagem pública

A ferramenta deverá distinguir cuidadosamente:

```text
seguradora regular
seguradora em Sandbox
marca
plataforma
intermediário
associação
outra entidade
```

Evitar afirmações que ultrapassem os dados.

Preferir:

> apresenta bons sinais institucionais nos indicadores avaliados

a:

> é definitivamente uma boa seguradora para qualquer cliente

Preferir:

> 8ª entre 41 seguradoras elegíveis nesta avaliação

a:

> 8ª melhor seguradora do Brasil

quando o segundo enunciado não puder ser sustentado.

---

## 43. SEO e o repositório

SEO não é responsabilidade deste repositório.

O repo fornece dados.

O site decide:

- páginas indexáveis;
- canonical;
- metadata;
- títulos;
- conteúdo;
- links;
- indexação;
- estrutura editorial;
- integração com outros hubs;
- intenção de busca.

A existência de uma marca no JSON **não implica criação de uma página indexável para ela**.

A ferramenta pode resolver centenas de buscas sem gerar centenas de landing pages.

---

## 44. Princípio editorial relacionado

Conteúdo editorial e ferramenta possuem papéis diferentes.

A ferramenta:

> identifica, verifica, avalia e direciona.

As matérias:

> explicam conceitos, relações, decisões e contextos que justificam tratamento editorial próprio.

O projeto não deverá reproduzir, dentro do JSON, uma segunda camada editorial destinada a capturar consultas.

---

## 45. Definição de sucesso da v2

A refatoração será considerada bem-sucedida quando:

- o usuário conseguir pesquisar uma empresa ou marca sem conhecer sua natureza jurídica;
- o sistema identificar corretamente o tipo da entidade;
- seguradoras comparáveis receberem avaliação coerente;
- entidades incomparáveis não sejam forçadas ao ranking;
- ausência de dados não seja convertida em desempenho ruim;
- a metodologia utilize sinais mais defensáveis;
- o estado atual seja atualizado automaticamente;
- o frontend não precise corrigir dados;
- o frontend não contenha regra de negócio;
- a API pública seja significativamente menor e mais clara;
- cada campo tenha um único significado;
- o processo seja auditável;
- o site tenha controle integral da apresentação e do SEO;
- a manutenção editorial rotineira seja mínima.

---

## 46. Questões metodológicas ainda abertas

Até que sejam testadas, **não considerar como regras definitivas**:

- peso Financeiro × Atendimento;
- subpesos do pilar financeiro;
- fórmula de transformação de PLA/CMR;
- janela temporal do capital;
- fórmula de liquidez;
- indicador operacional final;
- tratamento de empresas com forte especialização;
- fonte definitiva de reclamações;
- amostra mínima;
- normalização das reclamações;
- coortes necessárias;
- cálculo de `assessment_confidence`;
- faixas de rating;
- threshold de elegibilidade;
- `reason_codes` finais;
- quais dados de ramos devem ser resumidos publicamente;
- formato final dos quatro JSONs;
- eventual necessidade de fragmentação por desempenho.

Esses pontos deverão ser resolvidos por análise de dados e testes, não por preferência estética.

---

## 47. Diretriz final desta etapa

A v2 não deve ser construída para preservar a lógica da v1.

Deve preservar o que a engenharia atual faz bem:

- automação;
- cache;
- contingência;
- snapshots;
- testes;
- atualização recorrente;
- rastreabilidade.

E substituir o que se mostrou conceitualmente frágil:

- universo mal delimitado;
- nota aplicada a dados incompletos;
- aproximação de solvência;
- Open Insurance como mérito;
- sinistralidade com interpretação excessiva;
- matching tratado como verdade;
- JSON redundante;
- frontend acoplado ao pipeline.

A regra de projeto é:

> **identidade correta → dado correto → período correto → comparação correta → conclusão útil → explicação transparente.**

---

## Licença e uso

Este repositório é mantido pela Sanida Corretora de Seguros.

Dados de terceiros permanecem sujeitos às condições, limitações e responsabilidades de suas respectivas fontes.

A metodologia Sanida é uma interpretação própria de dados públicos e não deve ser apresentada como nota, classificação ou certificação oficial da SUSEP.
