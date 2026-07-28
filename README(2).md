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
.github/workflows/
├── ci.yml
├── debug-audit.yml
├── refresh-data.yml
└── update-susep-sandbox.yml

api/
├── build_consumidor_gov.py       # baixa e agrega o Consumidor.gov
├── build_insurers.py             # compõe o JSON final do ranking
├── build_json.py                 # snapshot auxiliar de participantes OPIN
├── intelligence.py               # calcula pilares, contribuições e nota
├── matching/                     # matching Consumidor.gov ↔ SUSEP
├── sources/                      # coletores SES, OPIN e Consumidor.gov
│   ├── ses.py
│   ├── opin_participants.py
│   └── opin_products.py
└── v1/
    ├── insurers.json
    ├── participants.json
    └── susep-sandbox-participants.json

data/
├── raw/
│   └── ses/                      # cache operacional das fontes SES
├── derived/                      # agregados, relatórios e auditorias
└── snapshots/                    # snapshots históricos compactados

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
└── testes de schema, coleta, fallback, matching e pontuação
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

A fonte de verdade do universo é `Ses_cias.csv`, contido em
`BaseCompleta.zip`. O arquivo `LISTAEMPRESAS.csv` é usado principalmente para
complementar o mapa de CNPJ por código SUSEP.

O `api/build_insurers.py` exclui registros com sinais claros de corretagem ou
intermediação que não devem integrar o universo de seguradoras.

#### Cache e contingência SES

O workflow `Refresh data` mantém um cache operacional em:

```text
data/raw/ses/
├── LISTAEMPRESAS.csv
└── BaseCompleta.zip
```

Em cada execução, o coletor tenta baixar as fontes atuais. Quando a SUSEP está
temporariamente indisponível:

- `LISTAEMPRESAS.csv` pode ser reutilizado quando o arquivo em cache pode ser
  lido e interpretado;
- `BaseCompleta.zip` só é reutilizado quando passa na validação de assinatura e
  estrutura ZIP;
- a execução continua vermelha quando não existe nem fonte online nem cache
  válido suficiente para montar o universo mínimo de seguradoras.

O workflow usa `actions/cache` para restaurar a versão mais recente disponível e
salvar o cache ao final da coleta. O cache é uma contingência operacional, não
uma nova fonte de verdade.

Antes do commit dos artefatos gerados, o workflow restaura os arquivos
rastreados em `data/raw/ses` para impedir que dados brutos ou alterações de
cache entrem acidentalmente no commit do ranking.

O workflow atual define `SES_KEEP_ZIP=1`, permitindo que o ZIP válido permaneça
disponível para o cache do GitHub Actions.

> Observação: `SES_ALLOW_INSECURE_SSL=1` está habilitado no `Refresh data`
> devido ao comportamento atual do endpoint da SUSEP. Isso desativa a
> verificação TLS apenas para esse coletor e deve ser reavaliado quando o
> endpoint oficial permitir validação normal.

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

#### Fallback do agregado

O `api/build_consumidor_gov.py` tenta atualizar o agregado a partir do CKAN do
Ministério da Justiça. Quando a fonte não resolve DNS ou está indisponível:

- um agregado anterior estruturalmente válido é preservado;
- o build pode continuar com aviso;
- o arquivo válido anterior não é substituído por resultado vazio ou parcial;
- a data real da fonte permanece registrada em `meta.generated_at`.

Quando não existe agregado válido anterior, a falha de atualização é
bloqueante.

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
geração de `insurers.json`.

O leitor de produtos aceita os contratos:

```text
data
participants
items
```

Um snapshot local vazio não impede a tentativa de download direto.

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

Os números e datas acima são apenas exemplos do contrato. O conteúdo efetivo é
regenerado pelo pipeline.

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

Arquivo:

```text
.github/workflows/refresh-data.yml
```

Executa aos domingos às `06:17 UTC` e também por acionamento manual.

Fluxo atual:

1. faz checkout com histórico completo;
2. instala Python 3.11 e Node.js 22;
3. instala dependências Python;
4. restaura o cache persistente das fontes SES;
5. executa diagnóstico não bloqueante das fontes externas;
6. gera o snapshot auxiliar OPIN;
7. tenta atualizar o agregado do Consumidor.gov;
8. preserva o agregado anterior quando a fonte está indisponível e o cache
   derivado é válido;
9. executa `api.build_insurers`;
10. salva o cache SES atualizado;
11. instala as dependências do frontend;
12. compila o bundle React/Vite;
13. executa smoke checks de quantidade mínima e presença do bundle;
14. executa toda a suíte `pytest`;
15. remove snapshots antigos, mantendo os 60 mais recentes;
16. restaura arquivos rastreados da pasta de cache SES;
17. adiciona ao commit apenas `api/v1`, `data/snapshots`, `data/derived` e
    `widget-ui/dist`;
18. cria o commit automatizado;
19. elimina alterações residuais do ambiente de build com
    `git reset --hard HEAD` e `git clean -fd`;
20. faz `fetch` e `rebase` sobre a `main` atual;
21. tenta o push até três vezes.

O workflow usa:

```yaml
concurrency:
  group: refresh-data
  cancel-in-progress: false
```

Isso impede duas execuções simultâneas do próprio `Refresh data`, mas não impede
que outro workflow faça commit na `main`. O rebase antes do push resolve essa
concorrência sem descartar o commit gerado.

### `CI`

Arquivo:

```text
.github/workflows/ci.yml
```

Executa em push e pull request.

O lint Ruff é aplicado somente aos arquivos Python alterados. Toda a suíte
`pytest` é executada em cada rodada, mesmo quando nenhum arquivo Python foi
alterado.

### `Debug Audit`

Arquivo:

```text
.github/workflows/debug-audit.yml
```

Possui dois modos:

- `fast`: audita os arquivos já presentes no commit;
- `full`: tenta atualizar as fontes, reconstrói `insurers.json` e executa a
  auditoria completa.

No modo `full`, o Debug Audit também executa `api.build_json` para reproduzir o
fluxo do `Refresh data`. Embora o frontend não leia `participants.json`, o
coletor de produtos OPIN pode usá-lo durante a montagem de `insurers.json`.

Quando o CKAN do Consumidor.gov está indisponível, o modo `full` pode prosseguir
com um agregado anterior válido, registrando aviso no log.

### `Update SUSEP Sandbox list`

Arquivo:

```text
.github/workflows/update-susep-sandbox.yml
```

Executa às segundas-feiras às `06:15 UTC` e também manualmente.

O workflow:

1. baixa as páginas das edições do Sandbox;
2. extrai somente a coluna explicitamente identificada como empresa,
   participante ou projeto;
3. rejeita linhas de negócio classificadas como participantes;
4. preserva o arquivo anterior quando a coleta é incompleta;
5. valida o JSON;
6. commita e publica somente quando há alteração.

Falhas de coleta, validação, rebase ou push deixam o workflow vermelho.

## Lógica no servidor da Sanida

A implantação em produção não é feita por WordPress nem pelo navegador. Ela é
realizada por um cron do cPanel que executa um script Bash fora deste
repositório.

### Cron

A configuração abaixo é deliberadamente sanitizada. O usuário real da
hospedagem, os caminhos absolutos e demais detalhes operacionais permanecem
somente na documentação privada:

```cron
40 3 * * 0 /bin/bash /home/<usuario>/scripts/update-widget.sh \
  >> /home/<usuario>/logs/widget-update.log 2>&1
```

O cron roda aos domingos, às 03h40 no horário configurado no servidor. O
workflow `Refresh data` começa aproximadamente às 03h17 no horário de Brasília.

O processamento SES pode levar vários minutos. Se o workflow ainda estiver em
execução às 03h40, o servidor poderá baixar o commit anterior. Nesse caso, a
versão nova só chegará à produção no próximo cron ou por execução manual do
script do servidor.

### Script de implantação

Caminho sanitizado:

```text
/home/<usuario>/scripts/update-widget.sh
```

O caminho real e o nome da conta de hospedagem não são publicados neste
repositório.

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

Exemplos sanitizados:

```text
/home/<usuario>/public_html/api/v1/insurers.json
/home/<usuario>/public_html/ranking-seguradoras/assets/widget.css
/home/<usuario>/public_html/ranking-seguradoras/assets/widget.js
```

A página `/ranking-seguradoras/` carrega os dois assets e o bundle consulta:

```text
/api/v1/insurers.json
```

### Backups

Antes da substituição, os arquivos anteriores são copiados para um diretório
privado. Exemplo sanitizado:

```text
/home/<usuario>/.widget-backups/AAAAMMDD-HHMMSS/
```

Para rollback manual, copie os três arquivos do backup escolhido para os
respectivos destinos de produção.

### Logs

Os caminhos abaixo são exemplos sanitizados:

```text
/home/<usuario>/logs/widget-update.log
/home/<usuario>/logs/widget-update.test.log
```

Os nomes e caminhos reais ficam no runbook operacional privado.

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
ou ser incluída conscientemente no script do servidor.

## Sandbox SUSEP

A lista pública usada pelo coletor não fornece CNPJ. O campo permanece vazio; o
projeto não tenta inferir identificadores por bases externas.

O coletor trata a lista como informação auxiliar e não como parte do ranking de
solvência/reputação.

A validação rejeita categorias como:

```text
Celular
Pets
Bicicletas
Caminhões
```

quando aparecem na posição de nome de participante.

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
SES_KEEP_ZIP
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

Execução local:

```bash
python -m pytest -q
```

Testes específicos incluídos:

- parsing da coluna de empresa na 2ª edição do Sandbox;
- bloqueio de linhas de negócio como participantes;
- preservação do JSON anterior em falha de rede;
- reputação encontrada e aplicada;
- reputação encontrada, mas não aplicada por ausência de prêmios;
- ausência de reputação;
- participação OPIN;
- desativação do pilar quando o dataset inteiro está vazio;
- leitura dos contratos `participants` e `data` no snapshot OPIN;
- fallback para download quando o snapshot local está vazio;
- fallback para `LISTAEMPRESAS.csv` quando o ZIP não pode ser baixado;
- uso conjunto dos caches válidos de `LISTAEMPRESAS.csv` e
  `BaseCompleta.zip` quando a rede está indisponível.

## Procedimento de atualização segura

1. alterar arquivos-fonte, nunca o bundle minificado;
2. executar `python -m pytest -q`;
3. executar o CI;
4. executar `Debug Audit` em modo `full`;
5. conferir `api/v1/insurers.json` e os artefatos do job;
6. executar `npm run build` quando o frontend mudar;
7. aguardar o cron de produção ou executar o script do servidor manualmente;
8. conferir os dois logs e testar a página publicada;
9. usar o backup datado em caso de regressão.

## Diagnóstico operacional

### Consumidor.gov indisponível

Sintoma típico:

```text
Could not resolve host: dados.mj.gov.br
```

Com agregado anterior válido, o comportamento esperado é:

```text
CG WARN: atualização online não concluída; preservando agregado válido existente
```

A execução pode prosseguir, mas a data `generated_at` deve ser conferida.

### SUSEP indisponível

O coletor tenta:

1. baixar `LISTAEMPRESAS.csv`;
2. baixar `BaseCompleta.zip` pelo endpoint principal;
3. tentar o endpoint alternativo;
4. usar cache válido quando disponível.

Sem fonte online e sem cache válido, o sanity check deve bloquear a publicação.

### Rebase recusado por árvore suja

O workflow atual:

1. restaura arquivos rastreados de `data/raw/ses`;
2. cria o commit apenas com artefatos publicáveis;
3. executa `git reset --hard HEAD`;
4. executa `git clean -fd`;
5. faz `fetch`, `rebase` e `push`.

Isso evita que resíduos de cache, coletores ou `npm install` impeçam o rebase.

## Arquivos gerados

Não edite manualmente:

```text
api/v1/insurers.json
api/v1/participants.json
api/v1/susep-sandbox-participants.json
widget-ui/dist/assets/widget.js
widget-ui/dist/assets/widget.css
data/snapshots/*.json.gz
```

As alterações devem ocorrer nos coletores, na camada de inteligência, nos
componentes React ou nos workflows responsáveis por gerar esses artefatos.

## Estado esperado de uma rodada saudável

Uma execução completa e saudável deve apresentar, nesta ordem:

1. fontes ou caches válidos carregados;
2. quantidade de seguradoras acima do mínimo;
3. sanity check Open Insurance aprovado;
4. frontend compilado;
5. smoke check aprovado;
6. suíte de testes aprovada;
7. commit criado apenas quando existem alterações;
8. rebase concluído;
9. push concluído.

Avisos de indisponibilidade externa podem aparecer sem comprometer a rodada
quando existe fallback válido e nenhuma proteção de integridade é violada.
