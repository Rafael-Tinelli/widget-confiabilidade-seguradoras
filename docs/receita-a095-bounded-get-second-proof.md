# A09.5 — Segunda prova de transporte: GET convencional

## Por que este teste existe

A primeira prova, [run #43912403934](https://github.com/Rafael-Tinelli/widget-confiabilidade-seguradoras/actions/runs/43912403934), identificou HTTP 200 em vez do 206 esperado para o cabeçalho Range. Ela interrompeu o fluxo com segurança, mas não mediu a vazão com uma e duas conexões.

Esta segunda prova utiliza GET convencional, sem Range, e **não altera o downloader produtivo**.

## Escopo e limites

- Mesma origem oficial WebDAV com autenticação pública, metadados via PROPFIND.
- Seleção das duas primeiras partições de Estabelecimentos, com tamanho e fingerprint oficial.
- **Quatro GETs**: um por arquivo em sequência, depois um por arquivo em simultâneo, com no máximo duas conexões ativas.
- Cada resposta HTTP 200 será fechada logo após **8 MiB consumidos** pela aplicação; a soma é **32 MiB de leitura no cliente**. O servidor ou as camadas de rede podem transmitir buffers adicionais, motivo pelo qual não tratamos 32 MiB como limite exato de tráfego no servidor.
- Sem retries automáticos, sem arquivos ZIP persistidos e sem upload de dados brutos.
- Cada sessão HTTP pertence exclusivamente à sua thread; timeout de conexão de 10 s, inatividade de leitura de 20 s e orçamento observado de 100 s por GET; job de até 8 minutos.
- Headers obrigatoriamente compatíveis com ZIP integral: HTTP 200, sem Content-Range, Content-Encoding identity e Content-Length igual ao tamanho informado pela Receita quando disponível.
- Cabeçalho local ZIP `PK\x03\x04`, SHA-256 dos 8 MiB lidos iguais entre modos e fingerprint oficial inalterado antes/depois.
- Saída exclusivamente em JSON sintético de desempenho, sem URLs autenticadas ou conteúdo de CNPJs.

## Como executar

Após CI e merge na main, acionar manualmente **V2 A09.5 Bounded GET Probe**, selecionar branch `main`, inserir `confirm_probe=GET` e executar. O workflow não é observado pelo publicador HostGator, não instala pacotes de produção e não grava caches.

## Como interpretar

O resultado `measured` prova apenas que quatro leituras parciais foram completas e idênticas. O campo `observed_speedup` compara tempos de parede para transferir 16 MiB em série e 16 MiB com duas conexões. É **um indício limitado**, sujeito a aquecimento de cache, variação de rede e desempenho específico dos primeiros bytes.

A ordem é fixa (sequencial antes do paralelo), portanto não se deve afirmar que houve ganho causal permanente sem repetir a prova em condições comparáveis.

Se a medição indicar vazão agregada maior sem respostas de erro, o próximo desenho será um prefetch de no máximo duas partições inteiras, **ainda sem adoção automática**, respeitando memória, disco, hash, metadados e a A09.4.

A prova deliberadamente não faz Full Generation, não adiciona serviços pagos e não mexe na metodologia de Conduta, no ranking ou na publicação.
