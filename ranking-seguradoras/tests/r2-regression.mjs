import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const widgetRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const jsPath = path.join(widgetRoot, 'assets', 'ranking-v2.js');
const searchPath = process.argv[2] || '/mnt/data/site_r2_test/ranking-seguradoras/data/v2/public/search_index.json';

const root = { dataset: { publicBase: '/ranking-seguradoras/data/v2/public', pageUrl: '/ranking-seguradoras/index2.php' }, classList: { add(){}, remove(){}, contains(){ return false; } } };
globalThis.document = {
  querySelector(selector) { return selector === '[data-rk2-root]' ? root : null; },
  querySelectorAll() { return []; },
};
globalThis.window = {
  location: { origin: 'https://sanida.com.br', href: 'https://sanida.com.br/ranking-seguradoras/index2.php', pathname: '/ranking-seguradoras/index2.php', search: '', hash: '' },
  clearTimeout(){}, setTimeout(){}, addEventListener(){}, scrollY: 0,
};
globalThis.history = { state: null, replaceState(){}, pushState(){}, back(){}, scrollRestoration: 'auto' };
Object.defineProperty(globalThis, 'navigator', { value: {}, configurable: true });
globalThis.requestAnimationFrame = (fn) => fn();

let source = fs.readFileSync(jsPath, 'utf8');
source = source.replace(/\n\s*init\(\);\n\}\)\(\);\s*$/, `\n  globalThis.__rk2Test = {\n    state, normalize, digits, finite, metricValue, publicText, entryTypeLabel,\n    candidateScore, candidatePriority, findCandidates, exactEntriesForQuery,\n    exactEntryForQuery, validComparisonIds, safeHashSelector, routeHash, parseRouteHash,\n    legacyRouteFromURL, profileShareURL, queryShareURL, renderProfile,\n    isMarketIdentity, isOrdinaryInsurer, assessmentTone\n  };\n})();\n`);
vm.runInThisContext(source, { filename: jsPath });

const t = globalThis.__rk2Test;
assert.ok(t, 'test API was not exposed');
const search = JSON.parse(fs.readFileSync(searchPath, 'utf8'));
t.state.entries = search.entries;
t.state.insurerEntries = search.entries.filter((entry) => entry.filter_bucket === 'insurers');

const allianz = t.findCandidates('Allianz');
assert.ok(allianz.length >= 2, 'Allianz should remain disambiguated');
assert.equal(allianz[0].entry.name, 'ALLIANZ SEGUROS S.A.', 'current ordinary insurer should be the first Allianz candidate');
assert.equal(t.exactEntryForQuery('Allianz'), null, 'ambiguous Allianz query must not auto-open a profile');

const tokio = t.findCandidates('tokio marine', t.state.insurerEntries);
assert.equal(tokio[0]?.entry?.profile_id, 'entity:fip:006190', 'Tokio Marine Seguradora should lead insurer-only search');

const valid = t.validComparisonIds('entity:fip:005355,entity:fip:005355,invalid,entity:fip:006190');
assert.deepEqual(valid, ['entity:fip:005355', 'entity:fip:006190'], 'comparison route must deduplicate and reject non-insurer ids');

assert.equal(t.safeHashSelector('#comparar'), '#comparar');
assert.equal(t.safeHashSelector('#lista'), '#lista');
assert.equal(t.safeHashSelector('#%E0%A4%A'), '', 'malformed URI hash must fail closed');
assert.equal(t.safeHashSelector('#<script>'), '', 'unsafe hash selector must fail closed');

assert.equal(t.routeHash('perfil', 'entity:fip:005177'), '#perfil=entity%3Afip%3A005177');
assert.deepEqual(t.parseRouteHash('#perfil=entity%3Afip%3A005177'), { kind: 'perfil', value: 'entity:fip:005177' });
assert.deepEqual(t.parseRouteHash('#consulta=Allianz'), { kind: 'consulta', value: 'Allianz' });
assert.equal(t.parseRouteHash('#comparar'), null, 'plain section anchor must not be treated as application route');
assert.equal(t.profileShareURL('entity:fip:005177'), 'https://sanida.com.br/ranking-seguradoras/index2.php#perfil=entity%3Afip%3A005177');
assert.equal(t.queryShareURL('Loovi'), 'https://sanida.com.br/ranking-seguradoras/index2.php#consulta=Loovi');
assert.deepEqual(t.legacyRouteFromURL(new URL('https://sanida.com.br/ranking-seguradoras/?q=LTI')), { kind: 'consulta', value: 'LTI' });
assert.deepEqual(t.legacyRouteFromURL(new URL('https://sanida.com.br/ranking-seguradoras/?perfil=brand%3Aloovi')), { kind: 'perfil', value: 'brand:loovi' });

assert.equal(t.finite(null), null, 'null must remain missing');
assert.equal(t.finite(0), 0, 'zero must remain a real numeric zero');
assert.equal(t.metricValue({ availability: 'unavailable', value: 0 }), null, 'unavailable metric must not expose a zero value');
assert.equal(t.metricValue({ availability: 'available', value: 0 }), 0, 'available zero must remain zero');

assert.equal(t.publicText('Conduta'), 'Conduta', 'frontend must not rewrite Conduta to reclamações generically');
assert.equal(t.publicText('usado pela v2'), 'usado pela v2', 'frontend must not mask internal backend copy');
assert.equal(t.entryTypeLabel({ market_role_label: 'Insurtech / plataforma de seguros', result_kind: 'brand' }), 'Insurtech / plataforma de seguros');


const publicRoot = path.dirname(searchPath);
function loadProfile(profileId) {
  const entry = t.state.entries.find((row) => row.profile_id === profileId);
  assert.ok(entry, `profile entry missing: ${profileId}`);
  return JSON.parse(fs.readFileSync(path.join(publicRoot, entry.profile_path), 'utf8'));
}

const allianzProfile = loadProfile('entity:fip:005177');
const allianzHtml = t.renderProfile(allianzProfile);
assert.ok(t.isOrdinaryInsurer(allianzProfile));
assert.ok(allianzHtml.includes('data-profile-heading'), 'opened profiles must expose a programmatic focus target');
assert.ok(allianzHtml.includes('Comparar esta seguradora'));
assert.ok(allianzHtml.indexOf('Leitura conjunta') < allianzHtml.indexOf('Identidade e relações relevantes') || !allianzHtml.includes('Identidade e relações relevantes'), 'ordinary insurer should lead with assessment before relationship context');


history.state = { rk2: { mode: 'profile', origin: { type: 'list', label: 'Voltar à lista de seguradoras' } } };
const withImmediateOrigin = t.renderProfile(allianzProfile);
assert.ok(withImmediateOrigin.includes('← Voltar à lista de seguradoras'), 'profile return action must describe the immediate origin');
history.state = { rk2: { mode: 'profile' } };
const withoutOrigin = t.renderProfile(allianzProfile);
assert.ok(!withoutOrigin.includes('data-history-back'), 'profile must not retain an orphan return action without a current origin');

const youseProfile = loadProfile('entity:fip:001121');
const youseHtml = t.renderProfile(youseProfile);
assert.ok(youseHtml.includes('Comparar esta seguradora'), 'Youse remains an ordinary insurer in the current contract');
assert.ok(!youseHtml.includes('Youse é a Caixa') && !youseHtml.includes('YOUSE é a Caixa'), 'Youse must not collapse into Caixa identity');

const looviProfile = loadProfile('brand:loovi');
const looviHtml = t.renderProfile(looviProfile);
assert.ok(!looviHtml.includes('Comparar esta seguradora'), 'brand profile must not inherit ordinary comparison eligibility');
assert.ok(looviHtml.includes('Resposta rápida') || looviHtml.includes('Quem é esta empresa'));

const historicalProfile = loadProfile('entity:fip:003182');
assert.equal(historicalProfile.identity.legal_name, 'ITAU SEGUROS DE AUTO E RESIDÊNCIA S.A.');
const historicalHtml = t.renderProfile(historicalProfile);
assert.ok(historicalHtml.includes('Sucessão'), 'historical incorporated profile must foreground succession context');
assert.ok(!historicalHtml.includes('Comparar esta seguradora'), 'historical entity must not be added as an ordinary comparison company');

const sandboxProfile = loadProfile('entity:cnpj:47006254000180');
assert.equal(sandboxProfile.identity.legal_name, 'LTI Seguros S.A.');
const sandboxHtml = t.renderProfile(sandboxProfile);
assert.ok(sandboxHtml.includes('Sandbox'), 'Sandbox identity must remain explicit');
assert.ok(!sandboxHtml.includes('Comparar esta seguradora'), 'Sandbox profile must not enter ordinary comparison');

const incompleteProfile = loadProfile('entity:fip:004120');
assert.equal(incompleteProfile.identity.legal_name, '88I SEGURADORA DIGITAL S.A.');
const incompleteHtml = t.renderProfile(incompleteProfile);
assert.ok(incompleteHtml.includes('Ainda não há dados suficientes') || incompleteHtml.includes('incomplet'), 'incomplete assessment must remain visibly incomplete');

const warningProfile = loadProfile('entity:fip:002119');
assert.equal(warningProfile.identity.legal_name, 'ARUANA SEGURADORA S. A.');
const warningHtml = t.renderProfile(warningProfile);
assert.ok(t.assessmentTone(warningProfile) === 'adverse', 'prudential warning must receive adverse visual precedence');
assert.ok(!warningHtml.toLowerCase().includes('está quebrando'), 'frontend must not manufacture insolvency language');

const azosProfile = loadProfile('brand:azos');
assert.ok(azosProfile.relationships.some((row) => row.relationship_type === 'risk_carrier' && row.target_profile_id === 'entity:fip:005690'), 'Azos must preserve its verified Excelsior carrier relationship');
const azosHtml = t.renderProfile(azosProfile);
assert.ok(azosHtml.includes('Insurtech / plataforma de seguros'));
assert.ok(azosHtml.includes('Quem é esta empresa'));
assert.ok(!azosHtml.includes('Comparar esta seguradora'), 'market identity must not inherit Excelsior assessment/comparison eligibility');

console.log('R2 + R3 regression checks: PASS');
console.log(`Entries: ${t.state.entries.length}; insurers: ${t.state.insurerEntries.length}; Allianz candidates: ${allianz.length}`);
