# A09.5 — Prova final de paralelismo: 32 MiB, ordem cruzada e catálogo 2026-11

## Objetivo e premissas

Após o teste GET de 8 MiB ([run #43932721730](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43932721730)), que observou fator 1,612× numa amostra curta, testar uma amostra maior com ordens invertidas **sem alterar o downloader produtivo** nem baixar a base nacional completa.

Usa-se exclusivamente WebDAV oficial de dados abertos, sem API comercial e sem tarifas de consulta. A prova é um diagnóstico isolado, sem impacto em Conduta, Receita/lifecycle, score, fontes já publicadas ou HostGator.

## Limites operacionais

- **32 MiB de leitura por GET**, oito GETs (256 MiB consumidos pela aplicação no máximo), em dois ZIPs oficiais: `Estabelecimentos0.zip` e `Estabelecimentos1.zip`. O servidor pode transmitir alguns bytes adicionais que já estejam nos buffers de rede após o fechamento.
- **Duas conexões simultâneas no máximo**; `requests.Session` independente por thread.
- Ordens: (1) sequencial → paralelo; (2) paralelo → sequencial; cada modo lê os mesmos 32 MiB iniciais de cada arquivo. A sequência cruzada limita, mas não elimina, efeito de horário e aquecimento de cache.
- Envio de GET comum, sem `Range`; respostas HTTP 200, `Content-Length` compatível quando informado, codificação `identity` e assinatura de cabeçalho local ZIP.
- Comparação de **SHA-256 dos prefixos lidos nas quatro transferências de cada partição**, de ETags HTTP e de fingerprints dos metadados oficiais antes/depois.
- Timeout de conexão/leitura por requisição (10/20 s), orçamento de 100 s por GET com progresso e timeout total do job de 15 minutos. Nenhum retry automático.
- Nenhum ZIP bruto ou dado identificável vai para artifacts. Apenas relatório JSON com tempos e metadados não pessoais.

## Completude da publicação 2026-11

A prova exige que o período mais recente descoberto seja **2026-11** e que o catálogo WebDAV contenha:

- dez partições contíguas `Empresas0.zip` a `Empresas9.zip`;
- dez partições contíguas `Estabelecimentos0.zip` a `Estabelecimentos9.zip`;
- `Motivos.zip`, `Cnaes.zip` e `Naturezas.zip`;
- tamanho positivo e ao menos ETag ou Last-Modified por arquivo;
- fingerprint conjunto, fingerprint de lifecycle e estabilidade dos dois entre início e fim.

**Limitação:** isso comprova apenas a **completude do catálogo/metadata**, NÃO a integridade de todos os ZIPs nem a completude das linhas CSV. Os dados integrais continuam submetidos ao fluxo de validação já existente quando houver uma Full Generation necessária.

## Critério pré-definido

Os dois pares serão classificados como **promissores para projeto de prefetch** somente se:
1. Ambos exibirem aceleração **≥ 1,15×** do modo paralelo versus sequencial;
2. Nenhum dos dois arquivos sofrer aumento individual de duração **acima de 2×** dentro do par;
3. Todas as oito leituras de prefixos forem completas, com mesmo SHA-256 por arquivo, HTTP/ETag e fingerprints estáveis;
4. Catálogo oficial de novembro apresentar todos os 23 arquivos obrigatórios por metadados.

Se qualquer verificação de conteúdo, tamanho, ETag ou catálogo falhar, o workflow reprova sem alterar dados. Se todos os controles passarem mas o ganho for insuficiente ou inconsistente, o workflow termina com classificação `mixed_or_unfavorable_no_production_change`; **não recomenda prefetch**.

A ordem cruzada é observacional. Mesmo com ganho em ambas as rodadas, **não será afirmada redução causal de tempo para downloads completos de múltiplos gigabytes** sem uma implementação separada e sua homologação.

## Acionamento

Após PR aprovada e integrada à `main`, GitHub Actions → **V2 A09.5 Final 32MiB Crossover** → Run workflow → branch `main` → confirmação `CROSSOVER`.

O gatilho é exclusivamente manual, não aparece na lista do publicador HostGator, não restaura nem salva caches produtivos e não executa Full Generation. O pacote JSON é mantido por 14 dias.

**Próxima frente somente se o resultado justificar:** projeto de prefetch de duas partições inteiras, com leitor sequencial intacto, validação ZIP/tamanho/SHA-256 e A09.4, fallback serial e teto de espaço em disco. Nenhuma alteração produtiva foi autorizada ou implementada nesta prova.
