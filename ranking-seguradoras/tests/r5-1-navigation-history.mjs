import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';

const rankingRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const jsPath = path.join(rankingRoot, 'assets', 'ranking-v2.js');
const root = {
  dataset: {
    publicBase: '/ranking-seguradoras/data/v2/public',
    pageUrl: '/ranking-seguradoras/index2.php',
  },
  classList: { add() {}, remove() {}, contains() { return false; } },
};

globalThis.document = {
  querySelector(selector) { return selector === '[data-rk2-root]' ? root : null; },
  querySelectorAll() { return []; },
};

globalThis.window = {
  location: {
    origin: 'https://sanida.com.br',
    href: 'https://sanida.com.br/ranking-seguradoras/index2.php#perfil=entity%3Afip%3A006777',
    pathname: '/ranking-seguradoras/index2.php',
    search: '',
    hash: '#perfil=entity%3Afip%3A006777',
  },
  clearTimeout() {},
  setTimeout() {},
  addEventListener() {},
  scrollY: 0,
};

globalThis.requestAnimationFrame = (fn) => fn();
Object.defineProperty(globalThis, 'navigator', { value: {}, configurable: true });

const calls = [];
function syncLocation(url) {
  const resolved = new URL(url, window.location.origin);
  window.location.href = resolved.toString();
  window.location.pathname = resolved.pathname;
  window.location.search = resolved.search;
  window.location.hash = resolved.hash;
}

globalThis.history = {
  state: { rk2: { mode: 'profile', profileId: 'entity:fip:006777' } },
  replaceState(state, _title, url) {
    this.state = state;
    calls.push({ method: 'replace', state: structuredClone(state), url });
    syncLocation(url);
  },
  pushState(state, _title, url) {
    this.state = state;
    calls.push({ method: 'push', state: structuredClone(state), url });
    syncLocation(url);
  },
  back() {},
  scrollRestoration: 'auto',
};

let source = fs.readFileSync(jsPath, 'utf8');
source = source.replace(/\n\s*init\(\);\n\}\)\(\);\s*$/, `
  globalThis.__rk2NavigationTest = {
    state, updateURL, originForElement, formatThresholdRatio, compareVisual, generationCoherenceError, isGenerationCoherenceError, recoveryButton
  };
})();
`);
vm.runInThisContext(source, { filename: jsPath });

const t = globalThis.__rk2NavigationTest;
assert.ok(t, 'navigation test API was not exposed');

const ARUANA = 'entity:fip:002119';
const ALLIANZ = 'entity:fip:005177';
const APS = 'entity:fip:006777';
const CHILD = 'entity:fip:004120';
t.state.insurerEntries = [{ profile_id: ARUANA }, { profile_id: ALLIANZ }, { profile_id: CHILD }];

function resetLocation(hash, state) {
  calls.length = 0;
  syncLocation(`/ranking-seguradoras/index2.php${hash}`);
  history.state = state;
}

// Regression: comparison state can coexist with a stale profile hash. Normalize
// the previous history entry before pushing the new profile, so back returns to
// the semantic origin instead of an unrelated older profile.
t.state.compareIds = [ARUANA, ALLIANZ];
resetLocation(`#perfil=${encodeURIComponent(APS)}`, { rk2: { mode: 'profile', profileId: APS } });
t.updateURL(ALLIANZ, { type: 'comparison', label: 'Voltar à comparação', compareIds: [ARUANA, ALLIANZ] });
assert.equal(calls.at(-2).method, 'replace');
assert.equal(calls.at(-2).url, `/ranking-seguradoras/index2.php#comparar=${encodeURIComponent(`${ARUANA},${ALLIANZ}`)}`);
assert.equal(calls.at(-2).state.rk2.mode, 'comparison');
assert.deepEqual(calls.at(-2).state.rk2View.compareIds, [ARUANA, ALLIANZ]);
assert.equal(calls.at(-1).method, 'push');
assert.equal(calls.at(-1).url, `/ranking-seguradoras/index2.php#perfil=${encodeURIComponent(ALLIANZ)}`);
assert.deepEqual(calls.at(-1).state.rk2.origin.compareIds, [ARUANA, ALLIANZ]);

resetLocation(`#perfil=${encodeURIComponent(APS)}`, { rk2: { mode: 'profile', profileId: APS } });
t.updateURL(ALLIANZ, { type: 'list', label: 'Voltar à lista de seguradoras' });
assert.equal(calls.at(-2).url, '/ranking-seguradoras/index2.php#lista');
assert.deepEqual(calls.at(-2).state.rk2, { mode: 'section', section: 'lista' });

resetLocation(`#perfil=${encodeURIComponent(APS)}`, { rk2: { mode: 'profile', profileId: APS } });
t.updateURL(ALLIANZ, { type: 'board', label: 'Voltar para o ranking' });
assert.equal(calls.at(-2).url, '/ranking-seguradoras/index2.php#explorar');
assert.deepEqual(calls.at(-2).state.rk2, { mode: 'section', section: 'explorar' });

resetLocation(`#perfil=${encodeURIComponent(APS)}`, { rk2: { mode: 'profile', profileId: APS } });
t.updateURL(CHILD, { type: 'profile', label: 'Voltar ao perfil anterior', profileId: APS });
assert.equal(calls.at(-2).url, `/ranking-seguradoras/index2.php#perfil=${encodeURIComponent(APS)}`);
assert.deepEqual(calls.at(-2).state.rk2, { mode: 'profile', profileId: APS });
assert.equal(calls.at(-1).url, `/ranking-seguradoras/index2.php#perfil=${encodeURIComponent(CHILD)}`);

t.state.compareIds = [ARUANA, ALLIANZ];
const compareElement = { closest(selector) { return selector === '#rk2-compare-grid' ? {} : null; } };
assert.deepEqual(t.originForElement(compareElement), {
  type: 'comparison',
  label: 'Voltar à comparação',
  compareIds: [ARUANA, ALLIANZ],
});

t.state.currentProfileId = APS;
const profileElement = { closest(selector) { return selector === '#rk2-result' ? {} : null; } };
assert.deepEqual(t.originForElement(profileElement), {
  type: 'profile',
  label: 'Voltar ao perfil anterior',
  profileId: APS,
});


// TOOL-01 audit N01: below-expected conduct remains a neutral but available conclusion.
assert.deepEqual(t.compareVisual({ state: 'below_expected_with_sufficient_evidence', tone: 'neutral' }), {
  symbol: '↓', tone: 'neutral', label: 'Reclamações abaixo do esperado; conclusão disponível',
});
assert.deepEqual(t.compareVisual({ state: 'not_distinguishable_from_expected', tone: 'neutral' }), {
  symbol: '≈', tone: 'neutral', label: 'Sem diferença clara',
});
assert.deepEqual(t.compareVisual({ state: 'pressure_unavailable_not_comparable', tone: 'missing' }), {
  symbol: '—', tone: 'missing', label: 'Sem conclusão segura',
});

// TOOL-01 audit N05: display rounding must not erase which side of the 1.00 boundary a ratio is on.
assert.equal(t.formatThresholdRatio(0.9951396368149357, 1, 2), '< 1,00');
assert.equal(t.formatThresholdRatio(1.0049, 1, 2), '> 1,00');
assert.equal(t.formatThresholdRatio(1, 1, 2), '1,00');
assert.equal(t.formatThresholdRatio(0.94, 1, 2), '0,94');

// TOOL-01 audit N02: generation incoherence gets a full-reload action; transient errors retain same-generation retry.
const generationError = t.generationCoherenceError('A verificação de integridade da geração pública falhou.');
assert.equal(t.isGenerationCoherenceError(generationError), true);
assert.equal(t.isGenerationCoherenceError(new Error('Falha de rede')), false);
const generationAction = t.recoveryButton(generationError, 'data-retry-profile="entity:fip:005177"', 'data-reload-profile="entity:fip:005177"');
assert.ok(generationAction.includes('data-reload-profile="entity:fip:005177"'));
assert.ok(generationAction.includes('Recarregar dados'));
assert.ok(!generationAction.includes('data-retry-profile'));
const transientAction = t.recoveryButton(new Error('Falha de rede'), 'data-retry-profile="entity:fip:005177"', 'data-reload-profile="entity:fip:005177"');
assert.ok(transientAction.includes('data-retry-profile="entity:fip:005177"'));
assert.ok(transientAction.includes('Tentar novamente'));
assert.ok(!transientAction.includes('data-reload-profile'));
assert.match(source, /window\.location\.replace\(profileShareURL\(reloadProfile\.dataset\.reloadProfile\)\)/);
assert.match(source, /window\.location\.replace\(comparisonShareURL\(\)\)/);
assert.match(source, /persistCurrentViewState\(\{ force: true \}\);\s*window\.location\.reload\(\)/);

console.log('R5.1 navigation history regression checks: PASS');