from pathlib import Path
import textwrap

js_path = Path('ranking-seguradoras/assets/ranking-v2.js')
source = js_path.read_text(encoding='utf-8')


def replace_once(old: str, new: str, label: str) -> None:
    global source
    count = source.count(old)
    if count != 1:
        raise SystemExit(f'{label}: expected exactly one match, found {count}')
    source = source.replace(old, new, 1)


replace_once(
    '''  function formatNumber(value, decimals = 2) {\n    const number = finite(value);\n    if (number === null) return "Não disponível";\n    return number.toLocaleString("pt-BR", {\n      minimumFractionDigits: decimals,\n      maximumFractionDigits: decimals,\n    });\n  }\n''',
    '''  function formatNumber(value, decimals = 2) {\n    const number = finite(value);\n    if (number === null) return "Não disponível";\n    return number.toLocaleString("pt-BR", {\n      minimumFractionDigits: decimals,\n      maximumFractionDigits: decimals,\n    });\n  }\n\n  function formatThresholdRatio(value, threshold = 1, decimals = 2) {\n    const number = finite(value);\n    const reference = finite(threshold);\n    if (number === null || reference === null) return "Não disponível";\n\n    const scale = 10 ** decimals;\n    const sameDisplayedBucket = Math.round(number * scale) === Math.round(reference * scale);\n    if (number !== reference && sameDisplayedBucket) {\n      return `${number < reference ? "<" : ">"} ${formatNumber(reference, decimals)}`;\n    }\n    return formatNumber(number, decimals);\n  }\n''',
    'N05 formatter',
)

replace_once(
    '''  async function sha256Text(value) {\n    if (!globalThis.crypto?.subtle || typeof TextEncoder !== "function") {\n      throw new Error("Este navegador não oferece a verificação de integridade exigida.");\n    }\n    const bytes = new TextEncoder().encode(value);\n    return bytesToHex(await globalThis.crypto.subtle.digest("SHA-256", bytes));\n  }\n\n  async function loadDistributionManifest() {\n''',
    '''  async function sha256Text(value) {\n    if (!globalThis.crypto?.subtle || typeof TextEncoder !== "function") {\n      throw new Error("Este navegador não oferece a verificação de integridade exigida.");\n    }\n    const bytes = new TextEncoder().encode(value);\n    return bytesToHex(await globalThis.crypto.subtle.digest("SHA-256", bytes));\n  }\n\n  const GENERATION_COHERENCE_ERROR_CODE = "RK2_GENERATION_COHERENCE";\n\n  function generationCoherenceError(message) {\n    const error = new Error(message);\n    error.code = GENERATION_COHERENCE_ERROR_CODE;\n    return error;\n  }\n\n  function isGenerationCoherenceError(error) {\n    return error?.code === GENERATION_COHERENCE_ERROR_CODE;\n  }\n\n  function recoveryButton(error, retryAttributes, reloadAttributes) {\n    const generationChanged = isGenerationCoherenceError(error);\n    const attributes = generationChanged ? reloadAttributes : retryAttributes;\n    const label = generationChanged ? "Recarregar dados" : "Tentar novamente";\n    return `<button class="rk2-btn rk2-btn--ghost" type="button" ${attributes}>${label}</button>`;\n  }\n\n  async function loadDistributionManifest() {\n''',
    'N02 recovery helpers',
)

replace_once(
    '    if (!expectedDigest) throw new Error("O arquivo solicitado não pertence à geração pública ativa.");',
    '''    if (!expectedDigest) {\n      throw generationCoherenceError("O arquivo solicitado não pertence à geração pública ativa.");\n    }''',
    'N02 missing manifest entry',
)
replace_once(
    '      throw new Error("A verificação de integridade da geração pública falhou.");',
    '      throw generationCoherenceError("A verificação de integridade da geração pública falhou.");',
    'N02 digest mismatch',
)

replace_once(
    '''      els.result.innerHTML = `<div class="rk2-result-shell"><div class="rk2-error"><strong>Não foi possível abrir o perfil.</strong><p>${esc(error.message)}</p><button class="rk2-btn rk2-btn--ghost" type="button" data-retry-profile="${esc(profileId)}">Tentar novamente</button></div></div>`;''',
    '''      const action = recoveryButton(\n        error,\n        `data-retry-profile="${esc(profileId)}"`,\n        `data-reload-profile="${esc(profileId)}"`,\n      );\n      els.result.innerHTML = `<div class="rk2-result-shell"><div class="rk2-error"><strong>Não foi possível abrir o perfil.</strong><p>${esc(error.message)}</p>${action}</div></div>`;''',
    'N02 profile recovery',
)

replace_once(
    '        metric: ratio !== null ? `PLA/CMR ${formatNumber(ratio, 2)}` : "",',
    '        metric: ratio !== null ? `PLA/CMR ${formatThresholdRatio(ratio, 1, 2)}` : "",',
    'N05 PLA/CMR display',
)
replace_once(
    '        metric: ratio !== null ? `ILT ${formatNumber(ratio, 2)}` : "",',
    '        metric: ratio !== null ? `ILT ${formatThresholdRatio(ratio, 1, 2)}` : "",',
    'N05 ILT display',
)

replace_once(
    '''    // Conduta sem diferença clara é informativa, não favorável/adversa.\n    if (stateName === "not_distinguishable_from_expected") {\n      return { symbol: "≈", tone: "neutral", label: "Sem diferença clara" };\n    }\n''',
    '''    // Conduta abaixo do esperado é uma conclusão direcional disponível, mas permanece neutra:\n    // isoladamente, não prova melhor atendimento nem maior qualidade.\n    if (stateName === "below_expected_with_sufficient_evidence") {\n      return { symbol: "↓", tone: "neutral", label: "Reclamações abaixo do esperado; conclusão disponível" };\n    }\n\n    // Conduta sem diferença clara também é informativa, não favorável/adversa.\n    if (stateName === "not_distinguishable_from_expected") {\n      return { symbol: "≈", tone: "neutral", label: "Sem diferença clara" };\n    }\n''',
    'N01 conduct semantics',
)

replace_once(
    '''        <span><b class="rk2-cmp-mark" data-tone="caution">!</b> atenção</span>\n        <span><b class="rk2-cmp-mark" data-tone="neutral">≈</b> sem diferença clara</span>\n        <span><b class="rk2-cmp-mark" data-tone="missing">—</b> sem conclusão</span>''',
    '''        <span><b class="rk2-cmp-mark" data-tone="caution">!</b> atenção</span>\n        <span><b class="rk2-cmp-mark" data-tone="neutral">↓</b> abaixo do esperado</span>\n        <span><b class="rk2-cmp-mark" data-tone="neutral">≈</b> sem diferença clara</span>\n        <span><b class="rk2-cmp-mark" data-tone="missing">—</b> sem conclusão</span>''',
    'N01 legend',
)

replace_once(
    '''          <p>${esc(error.message)}</p>\n          <button class="rk2-btn rk2-btn--ghost" type="button" data-retry-comparison>Tentar novamente</button>\n        </div>''',
    '''          <p>${esc(error.message)}</p>\n          ${recoveryButton(error, "data-retry-comparison", "data-reload-comparison")}\n        </div>''',
    'N02 comparison recovery',
)

replace_once(
    '''      els.boardPanel.innerHTML = `<div class="rk2-error"><strong>Não foi possível carregar esse recorte.</strong><p>${esc(error.message)}</p><button class="rk2-btn rk2-btn--ghost" type="button" data-retry-board data-board-kind="${esc(kind)}" data-board-id="${esc(id)}">Tentar novamente</button></div>`;''',
    '''      const retryAttributes = `data-retry-board data-board-kind="${esc(kind)}" data-board-id="${esc(id)}"`;\n      els.boardPanel.innerHTML = `<div class="rk2-error"><strong>Não foi possível carregar esse recorte.</strong><p>${esc(error.message)}</p>${recoveryButton(error, retryAttributes, "data-reload-board")}</div>`;''',
    'N02 board recovery',
)

replace_once(
    '''      const retryProfile = event.target.closest("[data-retry-profile]");\n      if (retryProfile) {\n        state.profileCache.delete(retryProfile.dataset.retryProfile);\n        openProfile(retryProfile.dataset.retryProfile, { updateHistory: false });\n      }\n\n      const retryComparison = event.target.closest("[data-retry-comparison]");\n      if (retryComparison) {\n        state.compareIds.forEach((id) => state.profileCache.delete(id));\n        renderComparison();\n      }\n\n      const retryBoard = event.target.closest("[data-retry-board]");\n      if (retryBoard) {\n        renderBoard(retryBoard.dataset.boardKind, retryBoard.dataset.boardId, { persist: false });\n      }\n''',
    '''      const reloadProfile = event.target.closest("[data-reload-profile]");\n      if (reloadProfile) {\n        window.location.replace(profileShareURL(reloadProfile.dataset.reloadProfile));\n        return;\n      }\n\n      const reloadComparison = event.target.closest("[data-reload-comparison]");\n      if (reloadComparison) {\n        window.location.replace(comparisonShareURL());\n        return;\n      }\n\n      const reloadBoard = event.target.closest("[data-reload-board]");\n      if (reloadBoard) {\n        persistCurrentViewState({ force: true });\n        window.location.reload();\n        return;\n      }\n\n      const retryProfile = event.target.closest("[data-retry-profile]");\n      if (retryProfile) {\n        state.profileCache.delete(retryProfile.dataset.retryProfile);\n        openProfile(retryProfile.dataset.retryProfile, { updateHistory: false });\n      }\n\n      const retryComparison = event.target.closest("[data-retry-comparison]");\n      if (retryComparison) {\n        state.compareIds.forEach((id) => state.profileCache.delete(id));\n        renderComparison();\n      }\n\n      const retryBoard = event.target.closest("[data-retry-board]");\n      if (retryBoard) {\n        renderBoard(retryBoard.dataset.boardKind, retryBoard.dataset.boardId, { persist: false });\n      }\n''',
    'N02 event handlers',
)

js_path.write_text(source, encoding='utf-8')

test_path = Path('ranking-seguradoras/tests/r6-tool01-audit-regressions.mjs')
test_path.write_text(textwrap.dedent(r'''\
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const widgetRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const jsPath = path.join(widgetRoot, 'assets', 'ranking-v2.js');

const root = {
  dataset: {
    publicBase: '/ranking-seguradoras/data/v2/public',
    pageUrl: '/ranking-seguradoras/',
  },
};
globalThis.document = {
  querySelector(selector) { return selector === '[data-rk2-root]' ? root : null; },
  querySelectorAll() { return []; },
};

let source = fs.readFileSync(jsPath, 'utf8');
source = source.replace(/\n\s*init\(\);\n\}\)\(\);\s*$/, `
  globalThis.__rk2AuditTest = {
    formatThresholdRatio,
    compareVisual,
    generationCoherenceError,
    isGenerationCoherenceError,
    recoveryButton,
  };
})();
`);
vm.runInThisContext(source, { filename: jsPath });

const t = globalThis.__rk2AuditTest;
assert.ok(t, 'audit regression test API was not exposed');

assert.deepEqual(
  t.compareVisual({ state: 'below_expected_with_sufficient_evidence', tone: 'neutral' }),
  {
    symbol: '↓',
    tone: 'neutral',
    label: 'Reclamações abaixo do esperado; conclusão disponível',
  },
);
assert.deepEqual(
  t.compareVisual({ state: 'not_distinguishable_from_expected', tone: 'neutral' }),
  { symbol: '≈', tone: 'neutral', label: 'Sem diferença clara' },
);
assert.deepEqual(
  t.compareVisual({ state: 'pressure_unavailable_not_comparable', tone: 'missing' }),
  { symbol: '—', tone: 'missing', label: 'Sem conclusão segura' },
);

assert.equal(t.formatThresholdRatio(0.9951396368149357, 1, 2), '< 1,00');
assert.equal(t.formatThresholdRatio(1.0049, 1, 2), '> 1,00');
assert.equal(t.formatThresholdRatio(1, 1, 2), '1,00');
assert.equal(t.formatThresholdRatio(0.94, 1, 2), '0,94');

const generationError = t.generationCoherenceError('A verificação de integridade da geração pública falhou.');
assert.equal(t.isGenerationCoherenceError(generationError), true);
assert.equal(t.isGenerationCoherenceError(new Error('Falha de rede')), false);

const generationAction = t.recoveryButton(
  generationError,
  'data-retry-profile="entity:fip:005177"',
  'data-reload-profile="entity:fip:005177"',
);
assert.ok(generationAction.includes('data-reload-profile="entity:fip:005177"'));
assert.ok(generationAction.includes('Recarregar dados'));
assert.ok(!generationAction.includes('data-retry-profile'));

const transientAction = t.recoveryButton(
  new Error('Falha de rede'),
  'data-retry-profile="entity:fip:005177"',
  'data-reload-profile="entity:fip:005177"',
);
assert.ok(transientAction.includes('data-retry-profile="entity:fip:005177"'));
assert.ok(transientAction.includes('Tentar novamente'));
assert.ok(!transientAction.includes('data-reload-profile'));

assert.match(source, /window\.location\.replace\(profileShareURL\(reloadProfile\.dataset\.reloadProfile\)\)/);
assert.match(source, /window\.location\.replace\(comparisonShareURL\(\)\)/);
assert.match(source, /persistCurrentViewState\(\{ force: true \}\);\s*window\.location\.reload\(\)/);

console.log('TOOL-01 audit regressions N01/N02/N05: PASS');
'''), encoding='utf-8')

ci_path = Path('.github/workflows/ci.yml')
ci = ci_path.read_text(encoding='utf-8')
anchor = '          node ranking-seguradoras/tests/r5-1-navigation-history.mjs\n'
addition = anchor + '          node ranking-seguradoras/tests/r6-tool01-audit-regressions.mjs\n'
if addition not in ci:
    if ci.count(anchor) != 1:
        raise SystemExit('CI anchor for regression test not found exactly once')
    ci = ci.replace(anchor, addition, 1)
    ci_path.write_text(ci, encoding='utf-8')
