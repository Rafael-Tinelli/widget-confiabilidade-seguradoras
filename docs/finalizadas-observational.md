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

O workflow não utiliza os ZIPs recuperados como cache de processamento:
`main()` continua baixando e agregando os dois meses selecionados. Reuso
de dados é uma etapa separada, não implementada aqui. Metadados de
publicação não são certificados de conteúdo idêntico.

Assim, mantendo o código atual e outubro entre os dois meses selecionados,
a execução de 16/11/2026 voltará a baixar e processar outubro. O custo também
inclui o download do artefato anterior quando encontrado. Não existe, neste
workflow, cache persistente do corpus nem integração desses agregados à Conduta.

Uma futura reutilização deve verificar bytes/hash dos objetos restaurados,
versão do parser e estado de revisão, preservar a última evidência válida
e distinguir reuso local de revalidação remota. Sem checksum/validador remoto
confiável, só um novo download permite conferir igualdade byte a byte; uma
política que o adie precisa explicitar esse intervalo de revalidação.

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
