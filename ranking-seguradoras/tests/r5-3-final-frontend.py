from __future__ import annotations

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
php = (ROOT / "index2.php").read_text(encoding="utf-8")
cutover_root = ROOT / "deployment" / "production-cutover"
production = (cutover_root / "index.php").read_text(encoding="utf-8")
redirects = (cutover_root / "legacy-state-redirects.php").read_text(encoding="utf-8")
css = (ROOT / "assets" / "ranking-v2.css").read_text(encoding="utf-8")
telemetry = (ROOT / "assets" / "unknown-market-telemetry-r5.js").read_text(encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(source: str, old: str, new: str) -> str:
    assert source.count(old) == 1, f"expected one production transform source: {old!r}"
    return source.replace(old, new)


# Pin the exact R5.3 staging bytes that passed HostGator QA on 2026-09-04.
# Phase D is additive in production; TOOL-01 N01/N02/N05 are the approved core JS maintenance delta.
assert sha256(ROOT / "index2.php") == "54115ed4b91505a6490ae5afb9846375a950e6ee43765991e567e795d28051f9"
assert sha256(ROOT / "assets" / "ranking-v2.js") == "56ad825d0a858861d7ab3c611b32ffab59d5d1d8a257d09ef227e19346558929"
assert sha256(ROOT / "assets" / "ranking-v2.css") == "616d654007e6d231d3bb9a6fec2f2d62cbd16e3021dde3cafd68325ad1958350"
assert sha256(ROOT / "assets" / "unknown-market-telemetry-r5.js") == "dd4626e79d91580b04779a39f6f485c56393b9d7bfd613b57961d809a4cdbcf0"

# Mobile hero DOM/visual order: title -> primary search -> explanation.
assert php.count('id="rk2-population"') == 1
assert php.count('id="rk2-search-form"') == 1
assert php.count('class="rk2-hero__explain"') == 1
copy_i = php.index('class="rk2-hero__copy"')
search_i = php.index('class="rk2-search-panel"')
explain_i = php.index('class="rk2-hero__explain"')
context_i = php.index('class="rk2-active-context"')
assert copy_i < search_i < explain_i < context_i

assert '/ranking-seguradoras/assets/ranking-v2.css?v=17' in php
assert '/ranking-seguradoras/assets/ranking-v2.js?v=16' in php
assert '/ranking-seguradoras/assets/ranking-v2.css?v=17' in production
assert '/ranking-seguradoras/assets/ranking-v2.js?v=16' in production
assert '/ranking-seguradoras/assets/unknown-market-telemetry-r5.js?v=1' in production
assert '/ranking-seguradoras/api/v2/unknown-market-query.php' in production
assert 'data-unknown-market-query-endpoint=' in production

# Production remains a mechanical derivative of the exact staging bytes.
# The original five cutover differences are preserved; Phase D adds only three telemetry wiring differences.
expected_production = php
for old, new in (
    (
        "Ranking/Comparador de Seguradoras v2 — §19.7 candidato final de staging",
        "Ranking/Comparador de Seguradoras v2 — §19.7 candidato final de produção",
    ),
    (
        "URL: https://sanida.com.br/ranking-seguradoras/index2.php",
        "URL: https://sanida.com.br/ranking-seguradoras/",
    ),
    (
        "header('X-Robots-Tag: noindex, follow', true);",
        "require __DIR__ . '/deployment/production-cutover/legacy-state-redirects.php';",
    ),
    (
        '$page_robots = "noindex, follow";',
        '$page_robots = "index, follow, max-image-preview:large";',
    ),
    (
        '$rk2_page_url    = "/ranking-seguradoras/index2.php";',
        '$rk2_page_url    = "/ranking-seguradoras/";',
    ),
    (
        '$rk2_page_url    = "/ranking-seguradoras/";',
        '$rk2_page_url    = "/ranking-seguradoras/";\n$rk2_unknown_market_query_endpoint = "/ranking-seguradoras/api/v2/unknown-market-query.php";',
    ),
    (
        '<script src="/ranking-seguradoras/assets/ranking-v2.js?v=16" defer></script>',
        '<script src="/ranking-seguradoras/assets/ranking-v2.js?v=16" defer></script>\n<script src="/ranking-seguradoras/assets/unknown-market-telemetry-r5.js?v=1" defer></script>',
    ),
    (
        '  data-page-url="<?php echo htmlspecialchars($rk2_page_url, ENT_QUOTES, \'UTF-8\'); ?>"\n>',
        '  data-page-url="<?php echo htmlspecialchars($rk2_page_url, ENT_QUOTES, \'UTF-8\'); ?>"\n  data-unknown-market-query-endpoint="<?php echo htmlspecialchars($rk2_unknown_market_query_endpoint, ENT_QUOTES, \'UTF-8\'); ?>"\n>',
    ),
):
    expected_production = replace_once(expected_production, old, new)
assert production == expected_production, "production candidate drifted from approved R5.3 staging + Phase D telemetry wiring"

payload_files = {
    path.relative_to(cutover_root).as_posix()
    for path in cutover_root.rglob("*")
    if path.is_file()
}
assert payload_files == {
    "INSTALLACAO-HOSTGATOR.md",
    "index.php",
    "legacy-state-redirects.php",
}

# Phase D telemetry is observational, same-origin and isolated from methodology/search logic.
assert 'root.dataset.unknownMarketQueryEndpoint' in telemetry
assert 'endpoint.origin !== window.location.origin' in telemetry
assert 'root.dataset.loadState !== "ready"' in telemetry
assert 'suggestions.querySelector("[data-suggestion-index]")' in telemetry
assert 'suggestions.querySelector(".rk2-suggestion-empty")' in telemetry
assert 'JSON.stringify({ query: normalized })' in telemetry
assert 'credentials: "omit"' in telemetry
assert 'keepalive: true' in telemetry
assert '}, 800);' in telemetry
assert "document.cookie" not in telemetry
assert "localStorage" not in telemetry
assert "sessionStorage" not in telemetry

# Legacy HTTP parameters collapse into fragment-only application states.
assert redirects.index("$_GET['perfil']") < redirects.index("$_GET['comparar']") < redirects.index("$_GET['q']")
assert "'/ranking-seguradoras/#' . $rk2_kind . '=' . rawurlencode($rk2_value)" in redirects
assert "header('Location: ' . $rk2_location, true, 301);" in redirects
assert redirects.rstrip().endswith("exit;\n}")

assert '"copy search"' in css
assert '"explain search"' in css
assert '"copy"\n      "search"\n      "explain"' in css
assert '.rk2--result-mode .rk2-hero__explain' in css
assert '.rk2-list-tools .rk2-field{flex:0 0 auto;width:100%}' in css
assert css.count("{") == css.count("}")

print("R5.3 core integrity + Phase D production telemetry wiring: PASS")
