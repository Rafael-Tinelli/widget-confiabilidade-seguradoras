import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import assert from 'node:assert/strict';

const widgetRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const jsPath = path.join(widgetRoot, 'assets', 'ranking-v2.js');
const root = { dataset: { publicBase: '/ranking-seguradoras/data/v2/public', pageUrl: '/ranking-seguradoras/index2.php' }, classList: { add(){}, remove(){}, contains(){ return false; } } };

globalThis.document = {
  querySelector(selector) { return selector === '[data-rk2-root]' ? root : null; },
  querySelectorAll() { return []; },
  getElementById() { return null; },
};
globalThis.window = {
  location: { origin: 'https://sanida.com.br', href: 'https://sanida.com.br/ranking-seguradoras/index2.php', pathname: '/ranking-seguradoras/index2.php', search: '', hash: '' },
  clearTimeout(){}, setTimeout(){}, addEventListener(){}, scrollY: 0,
};
globalThis.history = { state: null, replaceState(){}, pushState(){}, back(){}, scrollRestoration: 'auto' };
Object.defineProperty(globalThis, 'navigator', { value: {}, configurable: true });
globalThis.requestAnimationFrame = (fn) => fn();

let source = fs.readFileSync(jsPath, 'utf8');
source = source.replace(/\n\s*init\(\);\n\}\)\(\);\s*$/, `\n  globalThis.__rk2TechTest = { technicalRows, technicalHelpFor, renderTechnical, renderIdentityDetails };\n})();\n`);
vm.runInThisContext(source, { filename: jsPath });

const t = globalThis.__rk2TechTest;
assert.ok(t, 'technical helper test API was not exposed');

const profile = {
  profile_id: 'entity:fip:004383',
  profile_kind: 'entity',
  identity: { name: 'TESTE SEGURADORA S.A.', cnpj: '39999619000197', fip_code: '004383' },
  regulatory: { public_status: 'Autorizada e ativa no cadastro consultado', filter_bucket: 'insurers', regime: 'ordinary' },
  assessment: {
    financial: {
      reference_period: 202605,
      capital: {
        state: 'capital_meets_or_exceeds_cmr',
        plain_language: 'Na competência analisada, o patrimônio ajustado alcança o capital mínimo exigido.',
        technical: { ratio: { value: 2.4110, availability: 'available', public_use: 'displayable' } },
      },
      liquidity: {
        state: 'ilt_below_arithmetic_parity',
        plain_language: 'O indicador de liquidez usado pela metodologia ficou abaixo de sua referência aritmética e merece atenção.',
        technical: { ratio: { value: 0.8168, availability: 'available', public_use: 'displayable' } },
      },
    },
    conduct: {
      state: 'above_expected_with_sufficient_evidence',
      plain_language: 'Há mais reclamações do que esperaríamos para o tamanho da operação nos meses comparáveis.',
      technical: {
        observed_complaints_12m: { value: 83, availability: 'available', public_use: 'displayable' },
        expected_complaints_12m: { value: 67.8, availability: 'available', public_use: 'displayable' },
        observed_expected_ratio: { value: 1.225, availability: 'available', public_use: 'displayable' },
        comparable_months: { value: 12, availability: 'available', public_use: 'displayable' },
      },
    },
    operation_context: {
      insurance_premium_direct_12m: { value: 225000000, availability: 'available', public_use: 'displayable' },
    },
  },
};

const identity = t.renderIdentityDetails(profile);
assert.ok(identity.includes('Identificação e situação na SUSEP'));
assert.ok(identity.includes('CNPJ'));
assert.ok(identity.includes('Código SUSEP'));

const rows = t.technicalRows(profile);
const conductRows = rows.filter((row) => row.group === 'conduct');
assert.equal(conductRows.length, 3, 'Conduct must be condensed into exactly three rows');
assert.deepEqual(conductRows.map((row) => row.key), ['complaints_observed', 'premium_direct_12m', 'complaints_context']);
assert.ok(conductRows[2].value.includes('referência 67,8'));
assert.ok(conductRows[2].value.includes('12 meses'));
assert.ok(!rows.some((row) => row.key === 'complaints_expected'));
assert.ok(!rows.some((row) => row.key === 'complaints_ratio'));
assert.ok(!rows.some((row) => row.key === 'comparable_months'));

const capital = t.technicalHelpFor(profile, 'pla_cmr', '2,4110');
assert.ok(capital.what.includes('Capital Mínimo Requerido'));
assert.ok(capital.importance.includes('margem de proteção'));
assert.ok(capital.interpret.includes('1,0'));
assert.ok(capital.interpret.includes('sinistros'));

const ilt = t.technicalHelpFor(profile, 'ilt', '0,8168');
assert.ok(ilt.what.includes('curto e no longo prazo'));
assert.ok(ilt.importance.includes('obrigações'));
assert.ok(ilt.interpret.includes('não é um limite prudencial oficial'));
assert.ok(!ilt.what.includes('CMPID'));

const premium = t.technicalHelpFor(profile, 'premium_direct_12m', 'R$ 225 mi');
assert.ok(premium.what.includes('não significa recompensa'));
assert.ok(premium.importance.includes('perspectiva'));

const context = t.technicalHelpFor(profile, 'complaints_context', 'referência 67,8 · 12 meses');
assert.ok(context.what.includes('67,8 reclamações'));
assert.ok(context.what.includes('12 meses'));
assert.ok(context.importance.includes('maior ou menor'));
assert.ok(context.interpret.includes('1,225×'));
assert.ok(context.interpret.includes('acima'));

const html = t.renderTechnical(profile);
assert.ok(html.includes('Saúde financeira'));
assert.ok(html.includes('Reclamações e tamanho da operação'));
assert.ok(html.includes('O que é'));
assert.ok(html.includes('Por que importa'));
assert.ok(html.includes('Como interpretar'));
assert.ok(html.includes('Metodologia e fontes'));
assert.ok(html.includes('rk2-data-row__label-text'));
assert.ok(html.includes('rk2-data-row__value'));
assert.ok(!html.includes('rk2-tech-help__title'));
assert.ok(!html.includes('O que este dado diz aqui'));
assert.ok(!html.includes('Por que ausência não aparece como zero?'));
assert.ok(!html.includes('Use o <strong>?</strong>'));
assert.ok(!html.includes('Entender melhor'));
assert.ok(!html.includes('backend'));
assert.ok(!html.includes('CMPID'));
assert.ok(!html.includes('contrato público'));

console.log('R4 technical helper regression checks: PASS');
