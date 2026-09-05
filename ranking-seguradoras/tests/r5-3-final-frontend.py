from __future__ import annotations

import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
php = (ROOT / "index2.php").read_text(encoding="utf-8")
cutover_root = ROOT / "deployment" / "production-cutover"
production = (cutover_root / "index.php").read_text(encoding="utf-8")
redirects = (cutover_root / "legacy-state-redirects.php").read_text(encoding="utf-8")
css = (ROOT / "assets" / "ranking-v2.css").read_text(encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def replace_once(source: str, old: str, new: str) -> str:
    assert source.count(old) == 1, f"expected one production transform source: {old!r}"
    return source.replace(old, new)


# Pin the exact staging bytes that passed HostGator QA on 2026-09-04.
assert sha256(ROOT / "index2.php") == "54115ed4b91505a6490ae5afb9846375a950e6ee43765991e567e795d28051f9"
assert sha256(ROOT / "assets" / "ranking-v2.js") == "ceba67d7de5e037027888f521a6640d7285dcfad341a1b497f0260795b13273f"
assert sha256(ROOT / "assets" / "ranking-v2.css") == "616d654007e6d231d3bb9a6fec2f2d62cbd16e3021dde3cafd68325ad1958350"

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

# Production must remain a mechanical derivative of the exact staging bytes.
# These are the only five intentional differences in the cutover candidate.
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
):
    expected_production = replace_once(expected_production, old, new)
assert production == expected_production, "production candidate drifted from approved R5.3 staging"

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

print("R5.3 final frontend structure and HostGator byte identity: PASS")
