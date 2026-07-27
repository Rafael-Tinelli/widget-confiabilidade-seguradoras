# Ranking de Seguradoras — Sanida

Widget e pipeline de dados para consulta e comparação de seguradoras a partir de
fontes públicas. O projeto combina dados financeiros da SUSEP/SES, indicadores
do Consumidor.gov e participação no ecossistema Open Insurance.

A página pública do ranking carrega um bundle React e consulta o arquivo
`/api/v1/insurers.json` no domínio da Sanida.

## Visão geral da arquitetura

```text
Fontes públicas
├── SUSEP / SES
├── Consumidor.gov
└── Diretório Open Insurance
        │
        ▼
Coletores e normalização Python
        │
        ▼
api/build_insurers.py
        │
        ├── api/v1/insurers.json
        └── data/snapshots/insurers_full_*.json.gz
        │
        ▼
widget-ui (React + Vite)
        │
        ├── widget-ui/dist/assets/widget.js
        └── widget-ui/dist/assets/widget.css
        │
        ▼
GitHub / branch main
        │
        ▼
cron do servidor da Sanida
        │
        ├── /public_html/api/v1/insurers.json
        └── /public_html/ranking-seguradoras/assets/
```

O JSON do Sandbox Regulatório da SUSEP é um fluxo separado:

```text
Páginas públicas do Sandbox SUSEP
        │
        ▼
tools/fetch_susep_sandbox.py
        │
        ▼
api/v1/susep-sandbox-participants.json
```

Esse arquivo não participa do cálculo da nota das seguradoras.

## Estrutura principal

```text
api/
├── build_consumidor_gov.py       # baixa e agrega o Consumidor.gov
├── build_insurers.py             # compõe o JSON final do ranking
├── build_json.py                 # snapshot auxiliar de participantes OPIN
├── intelligence.py               # calcula pilares, contribuições e nota
├── matching/                     # matching Consumidor.gov ↔ SUSEP
├── sources/                      # coletores SES, OPIN e Consumidor.gov
│   └── opin_products.py          # endpoints e produtos públicos por CNPJ
└── v1/
    ├── insurers.json
    ├── participants.json
    └── susep-sandbox-participants.json

widget-ui/
├── src/App.jsx
├── src/components/InsurerCard.jsx
├── src/InsurerScoreModal.jsx
└── dist/assets/
    ├── widget.js
    └── widget.css

tools/
└── fetch_susep_sandbox.py

tests/
└── testes de schema, coleta e pontuação
```

## Fontes e contratos

### SUSEP / SES

O coletor SES forma o universo principal de entidades e fornece, quando
disponíveis:

- código SUSEP;
- razão social;
- CNPJ;
- prêmios;
- sinistros;
- patrimônio líquido;
- fontes financeiras encontradas no pacote SES.

O `api/build_insurers.py` exclui registros com sinais claros de corretagem ou
intermediação que não devem integrar o universo de seguradoras.

### Consumidor.gov

O agregado do Consumidor.gov é usado como fonte de reputação. A fonte não
fornece um CNPJ estruturado e confiável para todas as empresas; por isso, o
matching usa nome normalizado, nome fantasia, aliases e demais regras do módulo
`api/matching/consumidor_gov_match.py`.

O pipeline diferencia três situações:

1. não existe registro associado;
2. existe registro associado, mas o pilar não pode ser aplicado;
3. existe registro associado e o pilar é aplicado.

A segunda situação ocorre, por exemplo, quando não há prêmio financeiro positivo
para normalizar a pressão de reclamações.

### Open Insurance

A participação é determinada pela presença do CNPJ no diretório público de
participantes. O backend publica flags binárias por seguradora, entre elas:

```json
{
  "openInsuranceParticipant": true,
  "opinParticipant": true
}
```

O frontend usa essas flags como fonte principal. O score do pilar não deve ser
usado para contradizer uma flag explícita.

O arquivo `api/v1/participants.json`, gerado por `api/build_json.py`, é um
snapshot auxiliar. O frontend não o consulta diretamente, mas o coletor de
produtos Open Insurance pode usá-lo para localizar endpoints públicos antes da
geração de `insurers.json`. O leitor aceita os contratos `data`, `participants`
e `items`; um snapshot vazio não impede a tentativa de download direto.

## Lógica de pontuação

A camada `api/intelligence.py` produz três pilares.

### 1. Solvência

Considera:

- razão entre sinistros e prêmios;
- patrimônio líquido em relação à escala financeira;
- suavização para empresas de baixo volume.

Quando não há prêmio positivo, o loss ratio recebe o status
`insufficient_premiums`. Sinistros negativos recebem o status
`invalid_claims`.

### 2. Reputação

Considera:

- reclamações em relação aos prêmios;
- pressão relativa à média do mercado;
- satisfação;
- proporção de reclamações resolvidas;
- proporção de reclamações respondidas;
- suavização por tamanho da amostra.

A existência de um match não implica aplicação automática do pilar. O backend
publica explicitamente:

```json
{
  "availability": {
    "reputationMatched": true,
    "reputationApplied": false,
    "reputationReason": "insufficient_premiums"
  }
}
```

### 3. Open Insurance

O pilar parte de 60 pontos:

- participante no diretório: mais 20 pontos;
- amplitude de produtos: até mais 20 pontos.

### Pesos

Com o dataset de reputação disponível:

```text
Solvência:       40%
Reputação:       45%
Open Insurance:  15%
```

Se o dataset inteiro de reputação estiver vazio, os pesos passam para:

```text
Solvência:       60%
Reputação:        0%
Open Insurance:  40%
```

Quando apenas uma seguradora possui match não aplicável, a contribuição de
reputação daquela entidade é zero e o peso não é redistribuído.

O backend publica as contribuições efetivamente usadas:

```json
{
  "contributions": {
    "solvency": 27.5,
    "reputation": 0.0,
    "innovation": 12.0,
    "total": 39.5
  }
}
```

`data.score` e `data.contributions` são as fontes canônicas para o frontend. O
modal não deve reconstruir uma regra diferente da usada no backend.

## Contrato resumido de `insurers.json`

```json
{
  "schemaVersion": "1.0.0",
  "generatedAt": "2026-07-27T19:04:50Z",
  "sources": {},
  "meta": {
    "count": 385
  },
  "insurers": [
    {
      "id": "005177",
      "name": "ALLIANZ SEGUROS S.A.",
      "cnpj": "61.573.796/0001-66",
      "cnpjKey": "61573796000166",
      "flags": {},
      "reputation": {},
      "data": {
        "score": 71.55,
        "weights": {},
        "availability": {},
        "contributions": {},
        "componentsDetail": {}
      }
    }
  ]
}
```

## Frontend

O React é montado em `#widget-root`. O `App.jsx`:

1. busca `/api/v1/insurers.json`;
2. remove duplicidades por `id`;
3. pesquisa por nome, código SUSEP e CNPJ com ou sem pontuação;
4. ordena pela nota canônica, nome ou volume de prêmios;
5. pagina 24 entidades por página;
6. abre o modal metodológico ao clicar em um cartão.

O `InsurerCard.jsx` exibe a nota e os três pilares. O
`InsurerScoreModal.jsx` detalha fontes, pesos, contribuições, disponibilidade e
indicadores.

### Build local

```bash
cd widget-ui
npm install
npm run build
```

O Vite gera nomes fixos:

```text
widget-ui/dist/assets/widget.js
widget-ui/dist/assets/widget.css
```

## GitHub Actions

### `Refresh data`

Executa aos domingos às `06:17 UTC` e também por acionamento manual.

Fluxo atual:

1. instala Python e Node.js;
2. gera o snapshot auxiliar OPIN;
3. atualiza o agregado do Consumidor.gov;
4. executa `api.build_insurers`;
5. compila o frontend;
6. valida quantidade mínima e artefatos;
7. executa os testes;
8. remove snapshots antigos;
9. commita os artefatos alterados na branch `main`.

### `CI`

Executa em push e pull request. O lint Ruff é aplicado apenas aos arquivos
Python alterados, e toda a suíte `pytest` é executada.

### `Debug Audit`

Possui dois modos:

- `fast`: audita os arquivos já presentes no commit;
- `full`: reconstrói Consumidor.gov e `insurers.json` antes da auditoria.

No modo `full`, o Debug Audit também executa `api.build_json` para reproduzir
o fluxo do `Refresh data`. Embora o frontend não leia `participants.json`, o
coletor de produtos OPIN pode usá-lo durante a montagem de `insurers.json`.

### `Update SUSEP Sandbox list`

Executa às segundas-feiras às `06:15 UTC` e também manualmente. O workflow:

1. baixa as páginas das edições do Sandbox;
2. extrai somente a coluna explicitamente identificada como empresa,
   participante ou projeto;
3. rejeita linhas de negócio classificadas como participantes;
4. preserva o arquivo anterior quando a coleta é incompleta;
5. valida o JSON;
6. commita e publica apenas quando há alteração.

Falhas de coleta, validação, rebase ou push deixam o workflow vermelho.

## Lógica no servidor da Sanida

A implantação em produção não é feita por WordPress nem pelo navegador. Ela é
realizada por um cron do cPanel que executa um script Bash fora deste
repositório.

### Cron

Configuração observada:

```cron
40 3 * * 0 /bin/bash /home1/sanid210/scripts/update-widget.sh \
  >> /home1/sanid210/logs/widget-update.log 2>&1
```

O cron roda aos domingos, às 03h40 no horário configurado no servidor. O
workflow `Refresh data` começa aproximadamente às 03h17 no horário de Brasília.
Se o workflow do GitHub ainda estiver em execução às 03h40, o servidor poderá
baixar o commit anterior; nesse caso, a atualização seguinte ocorrerá no
próximo cron ou por execução manual do script.

### Script de implantação

Arquivo no servidor:

```text
/home1/sanid210/scripts/update-widget.sh
```

O script atual:

1. ativa `set -euo pipefail`;
2. cria os diretórios de destino, backup e logs;
3. cria um lock em `/tmp/widget-update.lockdir` para evitar duas execuções
   simultâneas;
4. cria um diretório temporário com `mktemp -d`;
5. baixa os artefatos da branch `main` por `raw.githubusercontent.com`;
6. exige HTTP 200 para cada download;
7. verifica se `insurers.json` não está vazio e começa com `{`;
8. cria um backup datado dos arquivos atualmente publicados;
9. instala cada novo arquivo com sufixo `.new`;
10. usa `mv` para substituir cada destino;
11. remove o diretório temporário e o lock ao terminar.

### Artefatos baixados

```text
api/v1/insurers.json
widget-ui/dist/assets/widget.css
widget-ui/dist/assets/widget.js
```

### Destinos em produção

```text
/home1/sanid210/public_html/api/v1/insurers.json
/home1/sanid210/public_html/ranking-seguradoras/assets/widget.css
/home1/sanid210/public_html/ranking-seguradoras/assets/widget.js
```

A página `/ranking-seguradoras/` carrega os dois assets e o bundle consulta:

```text
/api/v1/insurers.json
```

### Backups

Antes da substituição, os arquivos anteriores são copiados para:

```text
/home1/sanid210/.widget-backups/AAAAMMDD-HHMMSS/
```

Para rollback manual, copie os três arquivos do backup escolhido para os
respectivos destinos de produção.

### Logs

Há dois caminhos envolvidos:

```text
/home1/sanid210/logs/widget-update.log
/home1/sanid210/logs/widget-update.test.log
```

O primeiro recebe a saída externa redirecionada pelo cron. O script atual grava
suas mensagens estruturadas de `RUN`, `FETCH`, `ERRO` e `OK` no segundo arquivo.
O histórico analisado registra downloads HTTP 200 e implantações semanais
concluídas.

### Limite atual da implantação

O script do servidor não baixa:

```text
api/v1/susep-sandbox-participants.json
```

Portanto, o workflow do Sandbox pode atualizar o arquivo no GitHub sem atualizar
a cópia em `sanida.com.br`. A publicação desse JSON deve ter um processo próprio
ou ser incluída conscientemente no script do servidor depois que o coletor for
validado.

## Sandbox SUSEP

A lista pública usada pelo coletor não fornece CNPJ. O campo permanece vazio; o
projeto não tenta inferir identificadores por bases externas.

O coletor trata a lista como informação auxiliar e não como parte do ranking de
solvência/reputação. A validação rejeita categorias como `Celular`, `Pets` ou
`Bicicletas` quando aparecem na posição de nome de participante.

## Variáveis de ambiente relevantes

### Consumidor.gov

```text
CG_MONTHS_BACK
CG_FORCE_DOWNLOAD
CG_HTTP_RETRIES
CG_HTTP_BACKOFF
CG_CONNECT_TIMEOUT
CG_READ_TIMEOUT
CG_DOWNLOAD_READ_TIMEOUT
CG_MAX_COMPANY_DROP_PCT
```

### SES

```text
SES_CACHE_DIR
SES_LISTAEMPRESAS_URL
SES_ZIP_URL
SES_ZIP_URL_FALLBACK
SES_ALLOW_INSECURE_SSL
SES_WRITE_AUDIT
SES_AUDIT_DIR
SES_FIN_YEAR_MODE
```

### Open Insurance

```text
OPIN_DIRECTORY_URL
OPIN_PARTICIPANTS_URL
MIN_OPIN_MATCH_FLOOR
STRICT_OPIN_SANITY
```

### Proteções do build

```text
MIN_INSURERS_COUNT
MAX_INSURERS_COUNT
MAX_COUNT_DROP_PCT
WRITE_SNAPSHOT
```

### Sandbox SUSEP

```text
SUSEP_SANDBOX_MIN_ITEMS
SUSEP_SANDBOX_MAX_DROP_PCT
```

## Testes

```bash
python -m pytest -q
```

Testes específicos adicionados:

- parsing da coluna de empresa na 2ª edição do Sandbox;
- bloqueio de linhas de negócio como participantes;
- preservação do JSON anterior em falha de rede;
- reputação encontrada e aplicada;
- reputação encontrada, mas não aplicada por ausência de prêmios;
- ausência de reputação;
- participação OPIN;
- desativação do pilar quando o dataset inteiro está vazio;
- leitura dos contratos `participants` e `data` no snapshot OPIN;
- fallback para download quando o snapshot local está vazio.

## Procedimento de atualização segura

1. alterar arquivos-fonte, nunca o bundle minificado;
2. executar `pytest`;
3. executar o CI;
4. executar `Debug Audit` em modo `full`;
5. conferir `api/v1/insurers.json` e os artefatos do job;
6. executar `npm run build` quando o frontend mudar;
7. aguardar o cron de produção ou executar o script do servidor manualmente;
8. conferir os dois logs e testar a página publicada;
9. usar o backup datado em caso de regressão.

## Arquivos gerados

Não edite manualmente:

```text
api/v1/insurers.json
api/v1/susep-sandbox-participants.json
widget-ui/dist/assets/widget.js
widget-ui/dist/assets/widget.css
data/snapshots/*.json.gz
```

As alterações devem ocorrer nos coletores, na camada de inteligência, nos
componentes React ou nos workflows responsáveis por gerar esses artefatos.
