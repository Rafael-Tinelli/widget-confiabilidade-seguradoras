# Finalizadas — aquisição observacional (sem publicação)

Esta rotina é **independente** da Base Completa, dos cálculos de Conduta,
do Gate 4, da Receita e do HostGator. Não modifica `data/raw`, `data/derived`,
os arquivos de identidade SUSEP ou o pacote público.

## Operação

Workflow: `.github/workflows/v2-finalizadas-observational.yml`.

Em cada execução, consulta a lista JSON oficial de publicações e baixa
diretamente os ZIPs dos dois meses mais recentes, identificando os códigos
sem adivinhá-los. O cadastro local fixa a URL e o SHA-256 de outubro/2026
como evidência adicional. A rotina verifica ZIP/CRC e tamanho anunciado,
reconhece explicitamente **dois esquemas oficiais de 19 colunas** e confere
a competência pela **data de finalização** e os contadores por fornecedor.
Gera somente `status=experimental`; o resultado e os ZIPs ficam no artefato
isolado do GitHub Actions (30 dias), nunca no HostGator.

A execução agendada ocorre nos dias 10 e 16 de cada mês, às 11h27 UTC (08h27 em Brasília). Também é
possível usar `workflow_dispatch`, inclusive informando um catálogo JSON
estruturado remoto **em domínio oficial consumidor.gov.br** via `feed_url`.

**Descoberta oficial automática:** o endpoint estruturado foi confirmado em
12/11/2026: `https://www.consumidor.gov.br/pages/publicacao/externo/publicacoes.json?indicadorTipoPublicacao=2`.
Ele entrega uma lista JSON oficial com `nomeArquivo`, `codigo`,
`tamanhoArquivo`, `dataPublicacao` e `dataAlteracao`. A rotina filtra
**somente** `finalizadas_YYYY-MM.zip` não excluídos, constrói o download
direto com o código fornecido e compara os identificadores e hashes com o
manifesto anterior. Não interpreta HTML e não presume os próximos códigos.

A resposta observada em 12/11 tinha 682 publicações e 62 ZIPs
`finalizadas`, com cobertura completa de 2025-01 a 2026-10.

A próxima competência será descoberta automaticamente assim que publicada
nesse JSON. O catálogo foi lido **sem cookies no runner** na prova de
12/11/2026; a execução também baixou e processou setembro e outubro:
https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43311498623

Foram observados dois contratos reais:
- **Setembro/2026:** colunas em português com acentos, datas ISO `YYYY-MM-DD`,
  `Respondida=S/N`; 358.865 registros, 14.116 do segmento. Há **uma**
  avaliação com nota mas sem campo de resolução: registrada como
  `resolution_unknown=1`, `requires_manual_review`, sem imputação.
- **Outubro/2026:** colunas `snake_case`, datas `DD/MM/YYYY`,
  `respondida=Sim/Não`; 414.538 registros, 40.501 do segmento,
  `validated_experimental` sem anomalia de resolução.

O endpoint apresentou também uma falha **504 transitória** em execução
separada; com cabeçalhos XHR, cache-buster e retentativas, a prova posterior
passou. A disponibilidade contínua **não está garantida**: se o serviço
não responder, o job falha fechado, sem sobrescrever a evidência anterior
nem alterar Conduta ou publicação.

O input opcional `feed_url` permite informar apenas a **mesma rota oficial**
com `indicadorTipoPublicacao=2`, e não aceita domínios, hosts ou URLs
genéricas. O catálogo local fixa o SHA-256 de outubro como evidência
histórica adicional.

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
