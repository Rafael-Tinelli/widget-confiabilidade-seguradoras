# Arquitetura (resumo)

Objetivo: gerar um JSON público (API estática) e um widget de confiabilidade de seguradoras, com atualização automática.

## Camadas
1. **ETL (extração)**
   - Fontes: Open Insurance (participantes), SUSEP (registro/ramo), SES (solvência), Consumidor.gov (atendimento/reclamações).
2. **Normalização**
   - Canonicalização por CNPJ (chave primária).
   - Deduplicação, nomes, status, vínculos.
3. **Score**
   - Regras transparentes com pesos.
4. **Publicação**
   - Artefatos em `api/v1/*.json`.
   - Widget consome estes JSONs.

## Artefatos públicos (exemplo)
- `api/v1/participants.json`
- `api/v1/insurers.json`
- `api/v1/score.json`
