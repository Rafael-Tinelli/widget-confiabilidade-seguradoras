from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
php = (ROOT / "index2.php").read_text(encoding="utf-8")
production = (ROOT / "deployment" / "production-cutover" / "index.php").read_text(encoding="utf-8")
deprecated_global_head = ROOT / "deployment" / "production-cutover" / "PHP" / "head-global.php"

assert '<!DOCTYPE html>' in php
assert '<html lang="pt-BR">' in php
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
assert php.count('<head>') == 0
assert php.count('</head>') == 1
assert not deprecated_global_head.exists()
assert "X-Robots-Tag: noindex" not in production
assert '$page_robots = "index, follow, max-image-preview:large";' in production
assert '$rk2_page_url    = "/ranking-seguradoras/";' in production
assert "legacy-state-redirects.php" in production
assert '<meta name="robots"' not in production
assert production.count('<head>') == 0
assert production.count('</head>') == 1
print('R5.3 head structure / global head untouched: PASS')
