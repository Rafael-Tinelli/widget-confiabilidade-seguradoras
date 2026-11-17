# A09.5 — Diagnóstico do Content-Length oficial (sem download dos ZIPs)

## Evidência existente

A prova final [#43942537695](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43942537695) terminou com
`official_size_does_not_match_response`: houve desigualdade entre o tamanho
do recurso no catálogo `PROPFIND` e o `Content-Length` de um GET. O
relatório antigo **não registrou o índice do arquivo ou o valor recebido**.

Na geração integral [#43941197377](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43941197377),
a Receita 2026-11 foi utilizada com sucesso, e a A09.4 registrou todos os dez
ZIPs de Estabelecimentos como `stored` e depois `verified`.
Comprimentos registrados: `Estabelecimentos0.zip` = **2.321.438.760** bytes,
`Estabelecimentos1.zip` = **342.021.474** bytes. Esta evidência **não identifica**
qual GET falhou no teste A09.5 e não explica a causa de eventual discrepância HTTP.

O download produtivo usa `zipfile.is_zipfile`; já a A09.5 reprova a
comparação dos headers *antes de consumir o corpo*. O cache de execução A09.4,
por sua vez, testa tamanho esperado e hash dos arquivos armazenados.

## Nova prova: metadados e cabeçalhos somente

A prova manual `V2 A09.5 Content-Length Diagnostic` executa
`scripts/a095_content_length_diagnostic.py`. Sem baixar nem interpretar
o conteúdo ZIP:

1. Usa a descoberta oficial da Receita e a auditoria já existente de todas
   as 23 entradas do catálogo WebDAV (10 Empresas, 10 Estabelecimentos,
   Motivos, Cnaes, Naturezas), exigindo período 2026-11.
2. Para `Estabelecimentos0.zip` e `Estabelecimentos1.zip`, faz duas
   rodadas sequenciais de `HEAD` e `GET` com `stream=True`, sem qualquer
   chamada para ler o corpo do GET. Fecha a conexão assim que os cabeçalhos
   chegam. **Máximo: quatro HEAD e quatro GET**, sem downloads nacionais,
   concorrência paralela ou retries de GET.
3. Registra apenas índice, rodada, método HTTP, status, tamanhos WebDAV e
   `Content-Length` real, diferenças em bytes, ETag oficial/HTTP,
   Last-Modified HTTP, Content-Encoding/Range, hostname de destino após
   redirecionamentos (sem caminho), códigos dos redirecionamentos e tempos.
4. Repete a leitura do catálogo após cada rodada, registra fingerprint e
   recusa assumir estabilidade quando metadados divergem.
5. Gera JSON contendo resultados, inclusive quando houver divergência, sem
   expor URLs públicas com token, dados de CNPJs, conteúdo HTTP ou ZIPs.
6. Antes do teste, o workflow verifica via GitHub Actions API se a
   Full Generation canônica está ativa e encerra sem acesso à Receita se
   estiver. Não executa Gate 4, não grava cache, não publica no HostGator.

**Limite de rede:** 0 bytes de corpo consumidos pelo script; bibliotecas
HTTP, sockets ou servidores podem transmitir bytes de buffers antes do
fechamento — não se afirma um limite exato de tráfego na rede.

## Interpretação

- `content_length_mismatch_reproduced`: apresenta arquivo, rodada, valores,
  diferença em bytes e ETags. A causa ainda deve ser investigada; **não**
  significa automaticamente ZIP corrompido.
- `official_catalog_changed`: houve alteração de metadados durante a prova;
  priorizar investigação de republicação ou inconsistência na descoberta.
- `header_inconclusive_or_inconsistent`: status não esperado, Content-Length
  ausente/inválido, ETag divergente ou codificação HTTP incompatível.
- `header_match_not_historical_exoneration`: todos os GETs coincidiram
  **neste novo momento**, o que **não desfaz** o resultado anterior.
- Um `HEAD` que responda 405 ou não informe o mesmo comprimento é registrado
  como evidência, mas não substitui o resultado do `GET`.

A próxima decisão sobre prefetch continuará bloqueada até entender a
divergência ou demonstrar que ela é transitória, com evidência.

## Acionamento

Após PR e CI verdes, no GitHub Actions:
**V2 A09.5 Content-Length Diagnostic** → **Run workflow** →
`main` → campo `confirm_probe` = **HEADERS**.

O workflow guarda apenas JSON por 14 dias. Nenhuma Full Generation é
acionada automaticamente.
