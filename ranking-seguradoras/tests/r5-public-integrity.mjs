import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';

const widgetRoot = path.resolve(path.dirname(new URL(import.meta.url).pathname), '..');
const jsPath = path.join(widgetRoot, 'assets', 'ranking-v2.js');
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
    href: 'https://sanida.com.br/ranking-seguradoras/index2.php',
    pathname: '/ranking-seguradoras/index2.php',
    search: '',
    hash: '',
  },
  clearTimeout() {},
  setTimeout() {},
  addEventListener() {},
  scrollY: 0,
};
globalThis.history = {
  state: null,
  replaceState() {},
  pushState() {},
  back() {},
  scrollRestoration: 'auto',
};
Object.defineProperty(globalThis, 'navigator', { value: {}, configurable: true });
Object.defineProperty(globalThis, 'crypto', { value: crypto.webcrypto, configurable: true });
globalThis.requestAnimationFrame = (fn) => fn();

let source = fs.readFileSync(jsPath, 'utf8');
source = source.replace(/\n\s*init\(\);\n\}\)\(\);\s*$/, `
  globalThis.__rk2IntegrityTest = {
    state, safePublicPath, loadDistributionManifest, fetchJSON
  };
})();
`);
vm.runInThisContext(source, { filename: jsPath });

const t = globalThis.__rk2IntegrityTest;
assert.ok(t, 'integrity test API was not exposed');

const files = {
  'search_index.json': JSON.stringify({ artifact: 'v2_public_search_index', entries: [] }),
  'insurer_explorer.json': JSON.stringify({ artifact: 'v2_public_insurer_explorer', entities: [] }),
  'explore_index.json': JSON.stringify({ artifact: 'v2_public_explore_index', leaderboards: [], collections: [] }),
};
const digest = (body) => crypto.createHash('sha256').update(body).digest('hex');
const manifest = {
  artifact: 'v2_public_distribution_manifest',
  build: {
    build_id: 'v2-gate4-full-123-a1',
    source_head_sha: 'a'.repeat(40),
  },
  public_package: {
    files_count: Object.keys(files).length,
    package_sha256: 'b'.repeat(64),
    files: Object.entries(files).map(([filePath, body]) => ({
      path: filePath,
      sha256: digest(body),
      bytes: Buffer.byteLength(body),
    })),
  },
};

let tamperSearch = false;
globalThis.fetch = async (url) => {
  const relative = String(url).split('/data/v2/public/')[1];
  if (relative === 'distribution_manifest.json') {
    return new Response(JSON.stringify(manifest), { status: 200 });
  }
  const body = relative === 'search_index.json' && tamperSearch
    ? `${files[relative]} `
    : files[relative];
  return new Response(body ?? 'not found', { status: body === undefined ? 404 : 200 });
};

await t.loadDistributionManifest();
const search = await t.fetchJSON('search_index.json', 'v2_public_search_index');
assert.deepEqual(search.entries, []);
assert.throws(() => t.safePublicPath('../profiles/secret.json'), /caminho de arquivo inválido/);
assert.throws(() => t.safePublicPath('/profiles/absolute.json'), /caminho de arquivo inválido/);

tamperSearch = true;
await assert.rejects(
  t.fetchJSON('search_index.json', 'v2_public_search_index'),
  /verificação de integridade/,
);

console.log('R5 public package integrity checks: PASS');
