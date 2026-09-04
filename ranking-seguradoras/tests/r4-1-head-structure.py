from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
php = (ROOT / 'index2.php').read_text(encoding='utf-8')
production = (
    ROOT / 'deployment' / 'production-cutover' / 'index.php'
).read_text(encoding='utf-8')
head_global = (
    ROOT / 'deployment' / 'production-cutover' / 'PHP' / 'head-global.php'
).read_text(encoding='utf-8')

assert '<!DOCTYPE html>' in php
assert '<html lang="pt-BR">' in php
assert "'/PHP/head-global.php'" not in php  # include uses double-quoted path in current file
assert 'head-global.php' in php
assert "'@type' => 'WebPage'" in php
assert "'@type' => 'WebApplication'" in php
assert "'applicationCategory' => 'FinanceApplication'" in php
assert "'isAccessibleForFree' => true" in php
assert 'application/ld+json' in php
assert '$page_robots = "noindex, follow";' in php
assert "header('X-Robots-Tag: noindex, follow', true);" in php
assert '<meta name="robots"' not in php
assert php.rstrip().endswith('</html>')

# Não repetir o head legado dentro do index2: head-global.php é a autoridade do head.
assert php.count('<head>') == 0
assert php.count('</head>') == 1

# O helper global preserva o default das demais páginas e aceita override por página.
assert head_global.count('<head>') == 1
assert head_global.count('<meta name="robots"') == 1
assert "isset($page_robots)" in head_global
assert "'index, follow, max-image-preview:large'" in head_global
assert 'htmlspecialchars($sanida_page_robots' in head_global

# O candidato de produção troca somente estado, URL compartilhável e redirect legado.
assert "X-Robots-Tag: noindex" not in production
assert '$page_robots = "index, follow, max-image-preview:large";' in production
assert '$rk2_page_url    = "/ranking-seguradoras/";' in production
assert "legacy-state-redirects.php" in production
assert '<meta name="robots"' not in production
assert production.count('<head>') == 0
assert production.count('</head>') == 1

print('R4.1 head structure: PASS')
