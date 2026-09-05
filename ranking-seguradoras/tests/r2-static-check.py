import re
from pathlib import Path

root=Path(__file__).resolve().parents[1]
php=(root/'index2.php').read_text(encoding='utf-8')
js=(root/'assets/ranking-v2.js').read_text(encoding='utf-8')
css=(root/'assets/ranking-v2.css').read_text(encoding='utf-8')

ids=re.findall(r'\bid="([A-Za-z0-9_-]+)"',php)
dups=sorted({x for x in ids if ids.count(x)>1})
assert not dups, f'duplicate HTML ids: {dups}'
html_ids=set(ids)
js_static_ids=set(re.findall(r'\$\("#([A-Za-z0-9_-]+)"\)',js))
missing=sorted(js_static_ids-html_ids)
assert not missing, f'JS references missing static ids: {missing}'

assert 'ranking-v2.css?v=17' in php and 'ranking-v2.js?v=16' in php
assert 'X-Robots-Tag: noindex, follow' in php
assert 'data-history-back' in js
assert 'Voltar à lista de seguradoras' in js
assert 'Voltar à comparação' in js
assert 'routeRequestToken' in js and 'compareRequestToken' in js and 'profileRequestToken' in js
assert js.index('if (route?.kind === "comparar")') < js.index('const view = event?.state?.rk2View'), 'explicit comparison route must precede history snapshot restoration'
assert 'aria-activedescendant' in js and 'aria-selected' in js
assert '.replace(/\\bConduta\\b' not in js, 'generic Conduta rewrite must not return'
assert '.replace(/\\busado pela v2\\b' not in js, 'public copy must come from the backend contract'
assert 'innerHTML = els.searchInput.value' not in js
assert 'innerHTML = els.compareSearch.value' not in js
assert 'loadDistributionManifest' in js and 'sha256Text' in js
assert 'v2_public_distribution_manifest' in js
assert 'profile.profile_id !== profileId' in js
assert js.index('const isCollection = kind === "collection"') < js.index(
    'isCollection ? "v2_public_semantic_collection"'
), 'board artifact selection must happen only after kind classification'
assert 'aria-busy="true"' in php and 'function setCatalogReady(ready)' in js
assert 'data-profile-heading' in js and 'preventScroll: true' in js
assert 'data-reload-app' in js
assert '.rk2-mini-search input:disabled' in css
print('R2 static checks: PASS')
print(f'HTML ids: {len(html_ids)}; JS static id refs: {len(js_static_ids)}')

assert 'Consulte as seguradoras SUSEP e compare sinais de confiabilidade' in php
assert 'Ranking de seguradoras: maiores, capital, liquidez e reclamações' in php
assert 'rk2-trust-answer' in php
assert 'searchParams.set("perfil"' not in js and 'searchParams.set("comparar"' not in js
assert 'function routeHash(kind, value)' in js and 'function parseRouteHash(hash)' in js and 'function comparisonShareURL()' in js and 'function queryShareURL(query)' in js
assert 'legacyRouteFromURL' in js and 'url.searchParams.get("q")' in js
assert 'Estados de aplicação são compartilháveis' in js
print('R3 SEO/routing checks: PASS')

assert 'function technicalHelpFor(profile, key, displayValue)' in js
assert 'data-tech-help-toggle' in js and 'O que é' in js
assert 'Capital disponível ÷ mínimo exigido (PLA/CMR)' in js
assert 'Liquidez total: recursos ÷ compromissos (ILT)' in js
assert 'Capital Mínimo Requerido' in js
assert 'não é um limite prudencial oficial da SUSEP' in js
assert '.rk2-tech-help' in css and '.rk2-tech-help-trigger' in css
print('R3.2 technical helper checks: PASS')
