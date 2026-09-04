# Instalação segura no HostGator — frontend v2 R5 / §19.7

Data de preparação: 04/09/2026.

Este roteiro separa três decisões diferentes:

1. instalar a correção compatível do `head-global.php` e atualizar **somente o staging** `index2.php`;
2. validar a geração pública v2 de forma íntegra e atômica;
3. fazer o cutover de `index.php` **somente após autorização explícita**.

O pacote entregue contém a etapa 3, mas ela fica em uma pasta marcada
`cutover-NAO-INSTALAR-AINDA`. Não copie essa pasta durante a instalação de staging.

## 1. O que esta instalação não altera

Não remover, renomear, desabilitar ou sobrescrever agora:

- `/public_html/ranking-seguradoras/index.php`;
- `/public_html/ranking-seguradoras/assets/widget.js`;
- `/public_html/ranking-seguradoras/assets/widget.css`;
- cron, workflow ou atualizador v1;
- `main`, symlinks atuais não conferidos ou sensores evergreen;
- arquivos de produção fora dos caminhos enumerados neste roteiro.

Os assets `widget.js` e `widget.css` foram confirmados como dependências da produção
v1. Eles continuam classificados como **LEGACY necessário até o cutover**.

## 2. Pré-requisitos e caminhos

É necessário ter Terminal/SSH com `python3`, `php`, `sha256sum`, `readlink` e
permissão de criar symlink. Descompacte o pacote fora de `public_html`.

Substitua os três caminhos abaixo pelos caminhos absolutos reais da conta:

```bash
set -eu
SANIDA_PUBLIC_HTML=/home/USUARIO/public_html
SANIDA_RELEASE=/home/USUARIO/releases/sanida-rk2-r5
SANIDA_V2_ROOT=/home/USUARIO/sanida-v2-publication
SANIDA_BACKUP=/home/USUARIO/backups/sanida-rk2-before-r5

test -d "$SANIDA_PUBLIC_HTML/ranking-seguradoras"
test -d "$SANIDA_RELEASE/staging"
test -d "$SANIDA_RELEASE/data-package/public"
mkdir -p "$SANIDA_BACKUP/PHP" "$SANIDA_BACKUP/ranking-seguradoras/assets"
```

Antes de copiar qualquer arquivo, confira o ZIP extraído:

```bash
cd "$SANIDA_RELEASE"
sha256sum -c SHA256SUMS
```

O resultado precisa terminar sem `FAILED`.

## 3. Backup recuperável

```bash
cp -a "$SANIDA_PUBLIC_HTML/PHP/head-global.php" \
  "$SANIDA_BACKUP/PHP/head-global.php"
cp -a "$SANIDA_PUBLIC_HTML/ranking-seguradoras/index2.php" \
  "$SANIDA_BACKUP/ranking-seguradoras/index2.php"
cp -a "$SANIDA_PUBLIC_HTML/ranking-seguradoras/assets/ranking-v2.js" \
  "$SANIDA_BACKUP/ranking-seguradoras/assets/ranking-v2.js"
cp -a "$SANIDA_PUBLIC_HTML/ranking-seguradoras/assets/ranking-v2.css" \
  "$SANIDA_BACKUP/ranking-seguradoras/assets/ranking-v2.css"
```

Confirme que os quatro backups existem:

```bash
test -s "$SANIDA_BACKUP/PHP/head-global.php"
test -s "$SANIDA_BACKUP/ranking-seguradoras/index2.php"
test -s "$SANIDA_BACKUP/ranking-seguradoras/assets/ranking-v2.js"
test -s "$SANIDA_BACKUP/ranking-seguradoras/assets/ranking-v2.css"
```

## 4. Validar e instalar a geração pública v2

Primeiro verifique o pacote sem publicar:

```bash
python3 "$SANIDA_RELEASE/tools/install_public_generation.py" verify \
  "$SANIDA_RELEASE/data-package/public"
```

O comando imprime `action: verify`, `build_id` e `package_sha256`. Em seguida,
instale a geração no diretório privado. A troca de `current` é atômica e a
geração anterior, quando já gerenciada por esse instalador, fica em `previous`.

```bash
mkdir -p "$SANIDA_V2_ROOT/generations" "$SANIDA_V2_ROOT/incoming" "$SANIDA_V2_ROOT/tools"
install -m 0600 "$SANIDA_RELEASE/tools/install_public_generation.py" \
  "$SANIDA_V2_ROOT/tools/install_public_generation.py"
python3 "$SANIDA_V2_ROOT/tools/install_public_generation.py" install \
  "$SANIDA_RELEASE/data-package/public" "$SANIDA_V2_ROOT"
python3 "$SANIDA_V2_ROOT/tools/install_public_generation.py" verify \
  "$SANIDA_V2_ROOT/current"
```

### Primeira migração do diretório público para o ponteiro atômico

O caminho público final deve ser um symlink para `current`:

```text
/public_html/ranking-seguradoras/data/v2/public
  -> /home/USUARIO/sanida-v2-publication/current
```

Defina e confira os caminhos, sem sobrescrever nada automaticamente:

```bash
SANIDA_PUBLIC_DATA="$SANIDA_PUBLIC_HTML/ranking-seguradoras/data/v2/public"
test -d "$(dirname "$SANIDA_PUBLIC_DATA")"
```

Se `SANIDA_PUBLIC_DATA` **ainda for um diretório comum**, execute uma única vez:

```bash
test -d "$SANIDA_PUBLIC_DATA"
test ! -L "$SANIDA_PUBLIC_DATA"
mv "$SANIDA_PUBLIC_DATA" "$SANIDA_BACKUP/public-data-v2-before-atomic"
ln -s "$SANIDA_V2_ROOT/current" "$SANIDA_PUBLIC_DATA"
```

Se ele **já for symlink**, não use `mv` nem `ln`; apenas confira o destino:

```bash
test -L "$SANIDA_PUBLIC_DATA"
test "$(readlink -f "$SANIDA_PUBLIC_DATA")" = "$(readlink -f "$SANIDA_V2_ROOT/current")"
```

Validação final do ponteiro:

```bash
test -L "$SANIDA_PUBLIC_DATA"
test "$(readlink -f "$SANIDA_PUBLIC_DATA")" = "$(readlink -f "$SANIDA_V2_ROOT/current")"
python3 "$SANIDA_V2_ROOT/tools/install_public_generation.py" verify \
  "$SANIDA_PUBLIC_DATA"
```

## 5. Instalar somente o staging R5

Valide os PHPs ainda dentro do release:

```bash
php -l "$SANIDA_RELEASE/staging/public_html/PHP/head-global.php"
php -l "$SANIDA_RELEASE/staging/public_html/ranking-seguradoras/index2.php"
```

Copie exatamente estes quatro arquivos:

```bash
install -m 0644 "$SANIDA_RELEASE/staging/public_html/PHP/head-global.php" \
  "$SANIDA_PUBLIC_HTML/PHP/head-global.php"
install -m 0644 "$SANIDA_RELEASE/staging/public_html/ranking-seguradoras/index2.php" \
  "$SANIDA_PUBLIC_HTML/ranking-seguradoras/index2.php"
install -m 0644 "$SANIDA_RELEASE/staging/public_html/ranking-seguradoras/assets/ranking-v2.js" \
  "$SANIDA_PUBLIC_HTML/ranking-seguradoras/assets/ranking-v2.js"
install -m 0644 "$SANIDA_RELEASE/staging/public_html/ranking-seguradoras/assets/ranking-v2.css" \
  "$SANIDA_PUBLIC_HTML/ranking-seguradoras/assets/ranking-v2.css"
```

Valide novamente no destino:

```bash
php -l "$SANIDA_PUBLIC_HTML/PHP/head-global.php"
php -l "$SANIDA_PUBLIC_HTML/ranking-seguradoras/index2.php"
```

O `head-global.php` novo mantém `index, follow, max-image-preview:large` como
default das páginas existentes e permite que o staging solicite `noindex,
follow`. Assim existe apenas uma meta robots no HTML.

## 6. Verificação HTTP obrigatória do staging

O `.htaccess` recebido bloqueia o User-Agent padrão do `curl`; por isso os
comandos usam um User-Agent de navegador.

```bash
curl -fsS -A 'Mozilla/5.0 Sanida-RK2-QA' \
  -D /tmp/sanida-rk2-index2.headers \
  -o /tmp/sanida-rk2-index2.html \
  'https://sanida.com.br/ranking-seguradoras/index2.php'

grep -i '^x-robots-tag: noindex, follow' /tmp/sanida-rk2-index2.headers
grep -F 'ranking-v2.css?v=15' /tmp/sanida-rk2-index2.html
grep -F 'ranking-v2.js?v=15' /tmp/sanida-rk2-index2.html
grep -F '<link rel="canonical" href="https://sanida.com.br/ranking-seguradoras/">' \
  /tmp/sanida-rk2-index2.html
```

Abra o staging no navegador, atualize sem cache e execute no Console:

```js
document.querySelectorAll('meta[name="robots"]').length
document.querySelector('meta[name="robots"]')?.content
document.querySelector('link[rel="canonical"]')?.href
document.querySelector('[data-rk2-root]')?.dataset.loadState
document.querySelector('[data-rk2-root]')?.getAttribute('aria-busy')
```

Resultado esperado, na mesma ordem:

```text
1
"noindex, follow"
"https://sanida.com.br/ranking-seguradoras/"
"ready"
"false"
```

Confira também o manifesto servido:

```bash
curl -fsS -A 'Mozilla/5.0 Sanida-RK2-QA' \
  -o /tmp/sanida-rk2-manifest.json \
  'https://sanida.com.br/ranking-seguradoras/data/v2/public/distribution_manifest.json'
python3 -m json.tool /tmp/sanida-rk2-manifest.json >/dev/null
python3 "$SANIDA_V2_ROOT/tools/install_public_generation.py" verify \
  "$SANIDA_PUBLIC_DATA"
```

## 7. QA funcional antes de qualquer cutover

Execute em desktop e em viewport móvel. Registre PASS/FAIL e evidência:

| Caso | Resultado obrigatório |
|---|---|
| carregamento normal | controles começam bloqueados e ficam disponíveis somente com `loadState=ready` |
| busca simples | resultado correto abre sem erro no Console |
| Allianz | desambiguação permite escolher a entidade correta, sem seleção silenciosa |
| Youse | marca, pessoa jurídica e portador do risco não são colapsados |
| entidade histórica | permanece pesquisável, mas não entra como seguradora ordinária |
| Sandbox/cooperativa | rótulo e exclusão do baseline ordinário são preservados |
| helper técnico | explica PLA/CMR, ILT e reclamações sem criar score |
| comparação | aceita 2–4 seguradoras, preserva ausências e não declara vencedora geral |
| lista e filtros | paginação, busca e filtros não duplicam ou somem registros |
| rankings/coleções | cada arquivo abre; coleção não recebe ordem de melhor/pior |
| compartilhar | fragmentos `#perfil`, `#comparar` e `#consulta` restauram o estado |
| Back/Forward | restaura perfil, comparação, lista e posição de navegação |
| teclado | ordem de Tab é lógica; ao abrir perfil, foco vai ao título do perfil |
| erro de rede | interface mostra erro e botão “Tentar novamente”; dados parciais não viram estado válido |
| SEO staging | uma meta robots `noindex, follow`, header X-Robots noindex e canonical do hub |

Não prossiga se houver erro JavaScript, 404 de JSON, hash divergente, pacote misto,
meta robots duplicada ou comportamento que recomponha score/metodologia no navegador.

## 8. Rollback do staging

Para restaurar somente PHP/CSS/JS:

```bash
cp -a "$SANIDA_BACKUP/PHP/head-global.php" \
  "$SANIDA_PUBLIC_HTML/PHP/head-global.php"
cp -a "$SANIDA_BACKUP/ranking-seguradoras/index2.php" \
  "$SANIDA_PUBLIC_HTML/ranking-seguradoras/index2.php"
cp -a "$SANIDA_BACKUP/ranking-seguradoras/assets/ranking-v2.js" \
  "$SANIDA_PUBLIC_HTML/ranking-seguradoras/assets/ranking-v2.js"
cp -a "$SANIDA_BACKUP/ranking-seguradoras/assets/ranking-v2.css" \
  "$SANIDA_PUBLIC_HTML/ranking-seguradoras/assets/ranking-v2.css"
```

Se já houver uma geração anterior gerenciada pelo instalador:

```bash
python3 "$SANIDA_V2_ROOT/tools/install_public_generation.py" rollback "$SANIDA_V2_ROOT"
```

Na primeira migração, se ainda não existir `previous`, restaure o diretório antigo:

```bash
test -L "$SANIDA_PUBLIC_DATA"
test -d "$SANIDA_BACKUP/public-data-v2-before-atomic"
unlink "$SANIDA_PUBLIC_DATA"
mv "$SANIDA_BACKUP/public-data-v2-before-atomic" "$SANIDA_PUBLIC_DATA"
```

## 9. Cutover — NÃO EXECUTAR AINDA

Só após todos os testes acima passarem e existir autorização explícita:

1. fazer novo backup de `index.php`, `head-global.php`, assets e estado do cron v1;
2. instalar `legacy-state-redirects.php` no caminho incluído no pacote;
3. validar com `php -l` o candidato `index.php` de produção;
4. substituir `index.php` pelo candidato versionado;
5. confirmar que produção tem uma meta `index, follow, max-image-preview:large`,
   canonical do hub e **não** tem `X-Robots-Tag: noindex`;
6. confirmar redirects 301 dos parâmetros legados `q`, `perfil` e `comparar`
   para fragmentos não indexáveis;
7. manter `index2.php` em `noindex` durante a janela de observação;
8. manter assets/atualizador/cron v1 disponíveis para rollback;
9. aposentar o cron v1 apenas depois da estabilização e em mudança separada;
10. ativar cron/publicação v2 e sensores somente em decisões operacionais separadas.

O fechamento técnico do §19.7 não equivale a autorização de merge, cutover, cron
ou sensores.
