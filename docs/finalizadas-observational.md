# Finalizadas — aquisição observacional (sem publicação)

Esta rotina é **independente** da Base Completa, dos cálculos de Conduta,
do Gate 4, da Receita e do HostGator. Não modifica `data/raw`, `data/derived`,
os arquivos de identidade SUSEP ou o pacote público.

## Operação

Workflow: `.github/workflows/v2-finalizadas-observational.yml`.

Em cada execução, consulta a lista JSON oficial de publicações e baixa
diretamente os ZIPs dos dois meses mais recentes, identificando os códigos
sem adivinhá-los. O cadastro local fixa a URL e o SHA-256 de outubro/2026
como evidência adicional. A rotina verifica ZIP/CRC e limites de tamanho;
divergência do tamanho anunciado é sinalizada para revisão. Reconhece
explicitamente **dois esquemas oficiais de 19 colunas** e confere
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

A próxima competência pode ser descoberta em uma execução posterior à sua publicação
nesse JSON, desde que o serviço responda e o formato seja reconhecido. O catálogo foi lido **sem cookies no runner** na prova de
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

O catálogo oficial utiliza as retentativas HTTP já existentes e, quando
elas se esgotam por falha transitória (como HTTP 504), faz **uma rodada
adicional** após 10 segundos. Não altera URL, domínio ou contrato do JSON;
a segunda rodada executa uma nova consulta oficial com verificação completa.
Se o serviço continuar indisponível, o job falha **sem usar catálogo histórico
como se fosse atual**. Respostas HTML, redirecionamentos e conflitos de
metadados não são aceitos nem reprocessados por essa recuperação.

O endpoint apresentou também uma falha **504 transitória** em execução
separada; com cabeçalhos XHR, cache-buster e retentativas, a prova posterior
passou. A disponibilidade contínua **não está garantida**: se o serviço
não responder, o job falha sem alterar Conduta ou publicação. Artefatos
de runs anteriores são separados; sua preservação depende da retenção.
Como o upload usa `if: always()`, um run falho pode deixar artefato parcial:
a presença do artefato não comprova validação completa.

O input opcional `feed_url` permite informar apenas a **mesma rota oficial**
com `indicadorTipoPublicacao=2`, e não aceita domínios, hosts ou URLs
genéricas. O catálogo local fixa o SHA-256 de outubro como evidência
histórica adicional.

## Limitações metodológicas e segurança

- Agregado **não deve ser usado como prova de equivalência** com Base Completa:
  aqui a competência é **finalização**. O contrato atual de novas aquisições
  Base Completa exige abertura, mas isso não demonstra o critério temporal
  dos registros históricos já agregados. Essa equivalência permanece pendente.
- Não vincula identidades SUSEP nem altera score; não faz merge de famílias.
- Rejeita downloads fora do host/caminho oficial, redirecionamentos, respostas
  não ZIP, ZIPs truncados, colunas inesperadas, mês errado e inconsistências
  verificadas pelo parser. CRC/hash não comprovam, sozinhos, completude da
  população publicada em um CSV bem formado.
- Hash diferente do catálogo ou do manifesto anterior é uma **revisão observada**,
  explicitamente marcada `requires_manual_review`. Nunca é aprovada para uso
  operacional automaticamente.
- Comparação de revisão depende de artefato anterior ainda disponível no
  GitHub Actions; após expirar a retenção, o hash esperado cadastrado continua
  protegendo apenas os meses que o possuem. Para retenção permanente de
  proveniência, será necessário repositório de evidências separado.
- Não usar esta rotina para executar Full Generation, nem agendá-la com eventos
  `workflow_run` que disparem produção.

## Integridade transacional do output (A07)

A saída observacional é montada integralmente em um diretório temporário
privado **irmão do destino final**, no mesmo sistema de arquivos. ZIPs, CSVs
validados por streaming, agregados, validações e manifesto só são promovidos
por renomeação atômica do diretório depois que todos os meses terminam sem
erro. Na falha ou interrupção antes da promoção, não há um novo bundle aceito;
o destino preexistente **nunca é sobrescrito**.

A barreira de escrita é ancorada no caminho real do módulo/repositório e
também rejeita atalhos de caminho por symlinks, independentemente do CWD.
Esta barreira protege a execução do observador; não pretende ser um mecanismo
de isolamento contra alteração concorrente maliciosa do filesystem.

## Reutilização e retenção: comportamento efetivo

Na correção de recuperação da PR #56, o workflow consulta até 100 runs
bem-sucedidos de `main`, na ordem do retorno da API oficial do GitHub.
Prioriza um **artefato compacto do histórico** (manifesto e proveniência
atrelada a run/attempt/commit, com SHA-256 recomputado). Caso não esteja
disponível, procura o pacote completo legado e confere os hashes do ZIP/CSV,
o CRC e os documentos de validação antes de aceitar a referência. Se o run
mais recente não tiver artefato íntegro, examina os anteriores.

- **Primeira execução sem sucessos anteriores:** `baseline_state=bootstrap`,
  registrado explicitamente; não há evidência de revisão histórica.
- **Referência mais antiga por falta do último artefato:** estado
  `restored_with_gap`. **Todos os meses herdados**, mesmo fora da janela
  atual, ficam pendentes de revisão (`requires_manual_review`) porque o run
  perdido poderia tê-los alterado. O alerta persiste na execução seguinte,
  sem afirmar que o ZIP foi revisado.
- **Há sucessos anteriores, mas nenhum histórico recuperável:** o job
  **falha fechado**. Não inventa nova baseline, não publica e não
  sobrescreve a referência anterior.
- **Restaurado sem lacunas:** `baseline_state=restored`. Pendências
  anteriores são preservadas no manifesto independentemente de estarem
  entre os dois meses processados.

O manifesto compacto acompanha a execução com **90 dias de retenção** e
ocupa apenas alguns KB; o artefato completo, com ZIPs, continua com
**30 dias**. Os dois são apenas evidências observacionais. A nova retenção
reduz o risco de perda entre calendários, mas não fornece armazenamento
permanente: se todos os artefatos expirarem, a recuperação será bloqueada.
O estado compacto só é enviado após uma execução válida e bem-sucedida;
artefatos parciais de falhas continuam inaptos a estabelecer baseline.

A PR de A06 introduz um **reuso local opcional**, nunca um certificado de
que a publicação remota permanece idêntica. O workflow continua consultando
o **JSON oficial atual**, mas, após recuperar o histórico autenticado,
pode buscar o artefato completo da **mesma execução e tentativa**. Confere
o SHA-256 do manifesto com a referência selada, os hashes recomputados do ZIP
e dos dois derivados, a identidade de fonte/competência e a versão do parser.

O reuso só é permitido quando todas as seguintes condições ocorrem:

- Histórico restaurado **sem lacunas**; artefato completo íntegro e ainda
  disponível; nada de fallback silencioso para outro run.
- Catálogo oficial recém-consultado indica a mesma publicação, URL, código,
  tamanho e datas de publicação/alteração. Retiradas, ausência ou revisão
  de fonte bloqueiam o reuso.
- Mesmo parser, hashes íntegros, nenhuma revisão pendente de fonte e
  **menos de sete dias desde o último download real**. O intervalo nunca
  é renovado apenas por reutilizar um ZIP.

O manifesto diferencia `acquisition_mode=current_http_fetch` de
`acquisition_mode=reused_local_verified`. Registra
`source_fetched_at` (último download real) e `catalog_checked_at`
(consulta atual ao catálogo), além de SHA-256 dos derivados. A falta do
artefato, da assinatura/versionamento, a corrupção ou o vencimento do prazo
exige download e reprocessamento da fonte oficial, mantendo os alertas.

**Limite assumido:** sem checksum/ETag remoto independente, uma troca
silenciosa de bytes sem alterar metadados pode ser identificada somente
na próxima revalidação integral, no máximo sete dias depois do download
anterior. Portanto, `reused_local_verified` nunca é chamado de
`remote_unchanged` ou de aquisição atualizada. Indisponibilidade do
catálogo continua fazendo o observador **falhar fechado**, mesmo havendo
ZIP íntegro no histórico.

Não se altera a retenção existente: histórico compacto por 90 dias,
artefato completo por 30 dias. O reuso pode reduzir tráfego de origem e
CPU dentro do intervalo autorizado, mas ainda recupera o artefato do
GitHub e o republica para manter a prova entre runs; não é uma solução
de armazenamento permanente nem integra dados à Conduta.

## Limitações verificadas na auditoria de 12–13/11/2026

- **Achado histórico A02 corrigido na PR #56 (sujeito a CI e merge):**
  pendências de revisão de fonte permanecem sinalizadas em sucessivas
  observações. Alertas de qualidade (como resolução não informada) não
  são classificados como revisões do ZIP; alterações apenas nos metadados
  oficiais também exigem revisão. Falta aprovação manual explícita, fora
  do escopo da PR.
- **Achado histórico A04 corrigido na PR #56 (sujeito a CI e merge):**
  uma publicação fixada ausente ou excluída do feed fica sinalizada,
  sem download. Entradas oficiais da mesma competência com metadados
  conflitantes falham fechado, mesmo com código idêntico.
- Um CSV interrompido entre registros completos, ainda acima do limite mínimo,
  pode passar pelos controles estruturais. SHA-256 identifica os bytes
  adquiridos; não comprova que a origem forneceu a população completa.
- As provas de setembro/outubro ocorreram na branch experimental. A busca
  automática continua filtrando `main` e não as promove por conveniência.
  Artefatos parciais não são tratados como histórico válido. Sem sucessos
  anteriores em `main`, a execução é identificada como bootstrap.

Essas limitações exigem testes específicos antes de implementar reutilização
ou qualquer integração à Conduta. O isolamento produtivo permanece obrigatório.
