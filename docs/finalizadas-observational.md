# Finalizadas — aquisição observacional (sem publicação)

Esta rotina é **independente** da Base Completa, dos cálculos de Conduta,
do Gate 4, da Receita e do HostGator. Não modifica `data/raw`, `data/derived`,
os arquivos de identidade SUSEP ou o pacote público.

## Operação

Workflow: `.github/workflows/v2-finalizadas-observational.yml`.

Em cada execução, baixa diretamente os ZIPs oficiais cadastrados no JSON
`.github/finalizadas-publications.json`; valida ZIP/CRC/schema de 19 campos,
competência pela **data de finalização**, enums e contagens; conserva
manifestações de hash, agregados `status=experimental` e os ZIPs originais em
artefato de Actions, com retenção de 30 dias. Não publica no HostGator.

A execução agendada ocorre nos dias 8, 15, 22 e 29 de cada mês. Também é
possível usar `workflow_dispatch`, inclusive informando um catálogo JSON
estruturado remoto **em domínio oficial consumidor.gov.br** via `feed_url`.

**Limite importante:** a URL de download inclui um código de publicação não
previsível. A publicação de 2026-10 foi confirmada e cadastrada. Até que um
endpoint oficial estável de catálogo JSON seja identificado, o cadastro da URL
da competência seguinte é um passo manual; nenhuma URL é inventada.
A rotina não depende de Selenium, HTML, scraping ou do portal antigo
`dados.mj.gov.br`.

Exemplo de nova entrada em `.github/finalizadas-publications.json`:

```json
{"month": "2026-11", "url": "https://www.consumidor.gov.br/pages/publicacao/externo/CODIGO_OFICIAL/download"}
```

Substituir `CODIGO_OFICIAL` pelo código numérico real após confirmação no
portal; a rotina rejeita qualquer valor fictício. Para descoberta automática,
é necessário configurar `CG_FINALIZADAS_CATALOG_JSON_URL` com um endpoint
oficial JSON, verificável, cujo contrato seja `{"publications":[{"month":"YYYY-MM","url":"..."}]}`.

## Limitações metodológicas e segurança

- Agregado **não deve ser usado como prova de equivalência** com Base Completa:
  aqui a competência é **finalização**, não abertura.
- Não vincula identidades SUSEP nem altera score; não faz merge de famílias.
- Rejeita downloads fora do host/caminho oficial, redirecionamentos, respostas
  não ZIP, arquivos truncados, colunas inesperadas, mês errado ou inconsistências.
- Hash diferente do catálogo ou do manifesto anterior é uma **revisão observada**,
  explicitamente marcada `requires_manual_review`. Nunca é aprovada para uso
  operacional automaticamente.
- Comparação de revisão depende de artefato anterior ainda disponível no
  GitHub Actions; após expirar a retenção, o hash esperado cadastrado continua
  protegendo apenas os meses que o possuem. Para retenção permanente de
  proveniência, será necessário repositório de evidências separado.
- Não usar esta rotina para executar Full Generation, nem agendá-la com eventos
  `workflow_run` que disparem produção.
