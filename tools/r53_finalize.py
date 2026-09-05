from pathlib import Path
import hashlib

ROOT = Path('.')
ranking = ROOT / 'ranking-seguradoras'
idx = ranking / 'index2.php'
cssp = ranking / 'assets' / 'ranking-v2.css'


# JS exact R5.1 navigation fix over the R5 source.
jsp = ranking / 'assets' / 'ranking-v2.js'
j = jsp.read_text(encoding='utf-8')
needle = """  function updateURL(profileId, origin = null) {
    try {
      persistCurrentViewState({ force: true });
      const url = pageRouteURL();
"""
replacement = """  function normalizeOriginHistoryEntry(origin) {
    if (!origin?.type) return;

    const url = pageRouteURL();
    let rk2 = null;

    if (origin.type === "comparison") {
      const ids = validComparisonIds((origin.compareIds || state.compareIds).join(","));
      if (ids.length < 2) return;
      url.hash = routeHash("comparar", ids.join(","));
      rk2 = { mode: "comparison" };
    } else if (origin.type === "list") {
      url.hash = "#lista";
      rk2 = { mode: "section", section: "lista" };
    } else if (origin.type === "board") {
      url.hash = "#explorar";
      rk2 = { mode: "section", section: "explorar" };
    } else if (origin.type === "profile" && origin.profileId) {
      url.hash = routeHash("perfil", origin.profileId);
      rk2 = { mode: "profile", profileId: origin.profileId };
    } else {
      return;
    }

    const previous = history.state && typeof history.state === "object" ? history.state : {};
    history.replaceState(
      { ...previous, rk2, rk2View: currentViewSnapshot() },
      "",
      `${url.pathname}${url.search}${url.hash}`,
    );
  }

  function updateURL(profileId, origin = null) {
    try {
      persistCurrentViewState({ force: true });
      if (origin) normalizeOriginHistoryEntry(origin);
      const url = pageRouteURL();
"""
if needle not in j:
    raise SystemExit('JS: updateURL anchor not found')
j = j.replace(needle, replacement, 1)
j = j.replace("""    if (element.closest("#rk2-compare-grid")) return { type: "comparison", label: "Voltar à comparação" };
    if (element.closest("#rk2-list")) return { type: "list", label: "Voltar à lista de seguradoras" };
    if (element.closest("#rk2-result") && state.currentProfileId) return { type: "profile", label: "Voltar ao perfil anterior" };
""", """    if (element.closest("#rk2-compare-grid")) {
      return { type: "comparison", label: "Voltar à comparação", compareIds: [...state.compareIds] };
    }
    if (element.closest("#rk2-list")) return { type: "list", label: "Voltar à lista de seguradoras" };
    if (element.closest("#rk2-result") && state.currentProfileId) {
      return { type: "profile", label: "Voltar ao perfil anterior", profileId: state.currentProfileId };
    }
""", 1)
jsp.write_text(j, encoding='utf-8')

# index2: preserve all R5 content except the already approved R5.2/R5.3 structural changes.
s = idx.read_text(encoding='utf-8')
s = s.replace('/ranking-seguradoras/assets/ranking-v2.css?v=15', '/ranking-seguradoras/assets/ranking-v2.css?v=17')
s = s.replace('/ranking-seguradoras/assets/ranking-v2.js?v=15', '/ranking-seguradoras/assets/ranking-v2.js?v=16')
old_explain = '''        <p class="rk2-hero__lead">\n          Pesquise pelo nome que você conhece. A ferramenta identifica se corresponde a uma seguradora autorizada pela SUSEP,\n          uma marca ou identidade de mercado, uma empresa histórica ou um participante do Sandbox. Depois, separa situação regulatória,\n          capital, liquidez e reclamações para mostrar o que os dados permitem afirmar.\n        </p>\n        <div class="rk2-hero__meta" aria-live="polite">\n          <span id="rk2-population">Carregando catálogo público…</span>\n          <a href="#confiabilidade">Como interpretar a confiabilidade?</a>\n        </div>\n'''
if old_explain not in s:
    raise SystemExit('index2: original hero explanation block not found')
s = s.replace(old_explain, '', 1)
anchor = '''      </div>\n\n      <div class="rk2-active-context" id="rk2-active-context" hidden aria-live="polite">'''
new_explain = '''      </div>\n\n      <div class="rk2-hero__explain">\n        <p class="rk2-hero__lead">\n          Pesquise pelo nome que você conhece. A ferramenta identifica se corresponde a uma seguradora autorizada pela SUSEP,\n          uma marca ou identidade de mercado, uma empresa histórica ou um participante do Sandbox. Depois, separa situação regulatória,\n          capital, liquidez e reclamações para mostrar o que os dados permitem afirmar.\n        </p>\n        <div class="rk2-hero__meta" aria-live="polite">\n          <span id="rk2-population">Carregando catálogo público…</span>\n          <a href="#confiabilidade">Como interpretar a confiabilidade?</a>\n        </div>\n      </div>\n\n      <div class="rk2-active-context" id="rk2-active-context" hidden aria-live="polite">'''
if anchor not in s:
    raise SystemExit('index2: active-context anchor not found')
s = s.replace(anchor, new_explain, 1)
idx.write_text(s, encoding='utf-8')

# CSS exact R5.2/R5.3 diff over R5.
c = cssp.read_text(encoding='utf-8')
c = c.replace('''  grid-template-columns:minmax(0,1.08fr) minmax(360px,.92fr);\n  gap:clamp(36px,5vw,72px);\n  align-items:center;\n}\n\n.rk2-hero__copy{min-width:0}\n''', '''  grid-template-columns:minmax(0,1.08fr) minmax(360px,.92fr);\n  grid-template-areas:\n    "copy search"\n    "explain search";\n  column-gap:clamp(36px,5vw,72px);\n  row-gap:20px;\n  align-items:start;\n}\n\n.rk2-hero__copy{grid-area:copy;min-width:0}\n.rk2-search-panel{grid-area:search;align-self:center}\n.rk2-hero__explain{grid-area:explain;min-width:0}\n''', 1)
c = c.replace('''  max-width:720px;\n  margin:20px 0 0;\n''', '''  max-width:720px;\n  margin:0;\n''', 1)
c = c.replace('''@media (max-width:980px){\n  .rk2-hero__grid{grid-template-columns:1fr;gap:28px}\n  .rk2-hero__copy{text-align:center}\n''', '''@media (max-width:980px){\n  .rk2-hero__grid{\n    grid-template-columns:1fr;\n    grid-template-areas:\n      "copy"\n      "search"\n      "explain";\n    gap:28px;\n  }\n  .rk2-hero__copy,\n  .rk2-hero__explain{text-align:center}\n''', 1)
c = c.replace('''  .rk2-list-tools{align-items:stretch;flex-direction:column}\n  .rk2-field{max-width:none}\n''', '''  .rk2-list-tools{align-items:stretch;flex-direction:column}\n  .rk2-list-tools .rk2-field{flex:0 0 auto;width:100%}\n  .rk2-field{max-width:none}\n''', 1)
c = c.replace('''.rk2--result-mode .rk2-hero__copy,\n.rk2--result-mode .rk2-search-panel{\n''', '''.rk2--result-mode .rk2-hero__copy,\n.rk2--result-mode .rk2-hero__explain,\n.rk2--result-mode .rk2-search-panel{\n''', 1)
cssp.write_text(c, encoding='utf-8')

# Production candidate is derived mechanically from the exact staging file.
prod = s.replace('Ranking/Comparador de Seguradoras v2 — §19.7 candidato final de staging', 'Ranking/Comparador de Seguradoras v2 — §19.7 candidato final de produção')
prod = prod.replace(' * URL: https://sanida.com.br/ranking-seguradoras/index2.php', ' * URL: https://sanida.com.br/ranking-seguradoras/')
prod = prod.replace("header('X-Robots-Tag: noindex, follow', true);", "require __DIR__ . '/deployment/production-cutover/legacy-state-redirects.php';")
prod = prod.replace('$page_robots = "noindex, follow";', '$page_robots = "index, follow, max-image-preview:large";')
prod = prod.replace('$rk2_page_url    = "/ranking-seguradoras/index2.php";', '$rk2_page_url    = "/ranking-seguradoras/";')
(ranking / 'deployment' / 'production-cutover' / 'index.php').write_text(prod, encoding='utf-8')

# Shared global head is deliberately not a widget cutover payload.
head_copy = ranking / 'deployment' / 'production-cutover' / 'PHP' / 'head-global.php'
if head_copy.exists():
    head_copy.unlink()

# Existing static tests must follow the approved asset versions and head policy.
r2 = ranking / 'tests' / 'r2-static-check.py'
t = r2.read_text(encoding='utf-8').replace("assert 'ranking-v2.css?v=15' in php and 'ranking-v2.js?v=15' in php", "assert 'ranking-v2.css?v=17' in php and 'ranking-v2.js?v=16' in php")
r2.write_text(t, encoding='utf-8')
r4css = ranking / 'tests' / 'r4-css-structure.py'
t = r4css.read_text(encoding='utf-8').replace("assert 'ranking-v2.css?v=15' in php and 'ranking-v2.js?v=15' in php", "assert 'ranking-v2.css?v=17' in php and 'ranking-v2.js?v=16' in php").replace("print('R4 CSS/visual structure checks: PASS')", "print('R5.3 CSS/visual structure checks: PASS')")
r4css.write_text(t, encoding='utf-8')

# Replace the head structure test with the final HostGator decision.
(ranking / 'tests' / 'r4-1-head-structure.py').write_text('''from pathlib import Path\n\nROOT = Path(__file__).resolve().parents[1]\nphp = (ROOT / "index2.php").read_text(encoding="utf-8")\nproduction = (ROOT / "deployment" / "production-cutover" / "index.php").read_text(encoding="utf-8")\ndeprecated_global_head = ROOT / "deployment" / "production-cutover" / "PHP" / "head-global.php"\n\nassert '<!DOCTYPE html>' in php\nassert '<html lang="pt-BR">' in php\nassert 'head-global.php' in php\nassert "'@type' => 'WebPage'" in php\nassert "'@type' => 'WebApplication'" in php\nassert "'applicationCategory' => 'FinanceApplication'" in php\nassert "'isAccessibleForFree' => true" in php\nassert 'application/ld+json' in php\nassert '$page_robots = "noindex, follow";' in php\nassert "header('X-Robots-Tag: noindex, follow', true);" in php\nassert '<meta name="robots"' not in php\nassert php.rstrip().endswith('</html>')\nassert php.count('<head>') == 0\nassert php.count('</head>') == 1\nassert not deprecated_global_head.exists()\nassert "X-Robots-Tag: noindex" not in production\nassert '$page_robots = "index, follow, max-image-preview:large";' in production\nassert '$rk2_page_url    = "/ranking-seguradoras/";' in production\nassert "legacy-state-redirects.php" in production\nassert '<meta name="robots"' not in production\nassert production.count('<head>') == 0\nassert production.count('</head>') == 1\nprint('R5.3 head structure / global head untouched: PASS')\n''', encoding='utf-8')

# Ensure CI runs the two final regression guards.
ci = ROOT / '.github' / 'workflows' / 'ci.yml'
y = ci.read_text(encoding='utf-8')
needle = '          python ranking-seguradoras/tests/r4-css-structure.py\n'
if 'r5-3-final-frontend.py' not in y:
    y = y.replace(needle, needle + '          python ranking-seguradoras/tests/r5-3-final-frontend.py\n', 1)
needle2 = '          node ranking-seguradoras/tests/r5-public-integrity.mjs\n'
if 'r5-1-navigation-history.mjs' not in y:
    y = y.replace(needle2, needle2 + '          node ranking-seguradoras/tests/r5-1-navigation-history.mjs\n', 1)
ci.write_text(y, encoding='utf-8')

# Top-level README status line: staging is now proven; rollback remains the blocker.
readme = ROOT / 'README.md'
r = readme.read_text(encoding='utf-8')
old = '> **Status do projeto:** fundação metodológica e revisão de frontend/SEO concluídas em Draft; **§19.1–§19.6 formalmente fechados no branch de trabalho**; **§19.7 implementado no branch e aguardando a prova final no staging HostGator**; recomendação atual **NOT READY** e cutover de produção deliberadamente não autorizado.'
new = '> **Status do projeto:** fundação metodológica e revisão de frontend/SEO concluídas em Draft; **§19.1–§19.6 formalmente fechados no branch de trabalho**; **§19.7 com staging HostGator R5.3 aprovado e consolidado no branch**; rollback real pós-migração ainda pendente; recomendação atual **NOT READY** e cutover de produção deliberadamente não autorizado.'
if old not in r:
    raise SystemExit('README: expected status line not found')
r = r.replace(old, new, 1)
marker = '## 19.7'
if marker in r and 'section-19-7-final-consolidation.md' not in r:
    r = r.replace(marker, marker + '\n\n> **Atualização pós-QA HostGator (04/09/2026):** o staging R5.3 foi aprovado e a consolidação final está registrada em `docs/section-19-7-final-consolidation.md`. O blocker operacional remanescente é a prova real de rollback; produção continua não autorizada.\n', 1)
readme.write_text(r, encoding='utf-8')

expected = {
    ranking / 'index2.php': '54115ed4b91505a6490ae5afb9846375a950e6ee43765991e567e795d28051f9',
    ranking / 'assets' / 'ranking-v2.js': 'ceba67d7de5e037027888f521a6640d7285dcfad341a1b497f0260795b13273f',
    ranking / 'assets' / 'ranking-v2.css': '616d654007e6d231d3bb9a6fec2f2d62cbd16e3021dde3cafd68325ad1958350',
    ranking / 'deployment' / 'production-cutover' / 'index.php': '081952849d2d6e3d1bb3bca0334e404db5e9c890f66cd50e4330005566cf51fc',
}
for path, digest in expected.items():
    got = hashlib.sha256(path.read_bytes()).hexdigest()
    if got != digest:
        raise SystemExit(f'hash mismatch {path}: {got} != {digest}')
print('R5.3 materialization hashes: PASS')