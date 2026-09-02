const DEFAULT_PUBLIC_BASE = '/ranking-seguradoras/data/v2/public';

function trimTrailingSlash(value) {
  return String(value || '').replace(/\/+$/, '');
}

function publicBase() {
  const configured =
    typeof window !== 'undefined' && window.SANIDA_RANKING_V2_PUBLIC_BASE
      ? window.SANIDA_RANKING_V2_PUBLIC_BASE
      : DEFAULT_PUBLIC_BASE;
  return trimTrailingSlash(configured);
}

function safeRelativePath(value) {
  const path = String(value || '').trim().replace(/^\/+/, '');
  if (!path || path.includes('..') || path.includes('\\') || /^[a-z]+:/i.test(path)) {
    throw new Error(`Caminho público v2 inválido: ${value}`);
  }
  return path;
}

function safeId(value) {
  const id = String(value || '').trim();
  if (!/^[a-z0-9][a-z0-9_-]*$/i.test(id)) {
    throw new Error(`Identificador público v2 inválido: ${value}`);
  }
  return id;
}

async function fetchPublicJson(relativePath) {
  const path = safeRelativePath(relativePath);
  const response = await fetch(`${publicBase()}/${path}`, {
    credentials: 'same-origin',
    headers: { Accept: 'application/json' },
  });

  if (!response.ok) {
    throw new Error(`Falha ao carregar contrato público v2 ${path} (${response.status})`);
  }

  const payload = await response.json();
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) {
    throw new Error(`Contrato público v2 inválido em ${path}`);
  }
  return payload;
}

export function loadSearchIndex() {
  return fetchPublicJson('search_index.json');
}

export function loadProfileManifest() {
  return fetchPublicJson('profile_manifest.json');
}

export function loadInsurerExplorer() {
  return fetchPublicJson('insurer_explorer.json');
}

export function loadExploreIndex() {
  return fetchPublicJson('explore_index.json');
}

export function loadDistributionManifest() {
  return fetchPublicJson('distribution_manifest.json');
}

export function loadProfile(profilePath) {
  const path = safeRelativePath(profilePath);
  if (!path.startsWith('profiles/') || !path.endsWith('.json')) {
    throw new Error(`profile_path fora do contrato público: ${profilePath}`);
  }
  return fetchPublicJson(path);
}

export function loadLeaderboard(leaderboardId) {
  return fetchPublicJson(`leaderboards/${safeId(leaderboardId)}.json`);
}

export function loadCollection(collectionId) {
  return fetchPublicJson(`collections/${safeId(collectionId)}.json`);
}

export async function loadPrimaryV2Catalog() {
  const [searchIndex, insurerExplorer, exploreIndex] = await Promise.all([
    loadSearchIndex(),
    loadInsurerExplorer(),
    loadExploreIndex(),
  ]);

  return { searchIndex, insurerExplorer, exploreIndex };
}

export const V2_PUBLIC_BASE_DEFAULT = DEFAULT_PUBLIC_BASE;
