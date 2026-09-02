import React, { useEffect, useLayoutEffect, useMemo, useState } from 'react';
import { ChevronLeft, ChevronRight, Search, ShieldCheck } from 'lucide-react';
import ComparisonPanel from './ComparisonPanel';
import ExplorePanel from './ExplorePanel';
import InsurerProfileModal from './InsurerProfileModal';
import InsurerCard from './components/InsurerCard';
import { loadPrimaryV2Catalog, loadProfile } from './v2Data';

const ITEMS_PER_PAGE = 24;

function normalizeSearch(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLocaleLowerCase('pt-BR')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function entryBucketLabel(entry) {
  if (entry.result_kind === 'brand') return 'Marca';
  const labels = {
    insurers: 'Seguradora',
    sandbox: 'Sandbox',
    historical: 'Histórica',
    other: 'Outra identidade',
  };
  return labels[entry.filter_bucket] || entry.disambiguation || 'Identidade';
}

export default function App() {
  const [catalog, setCatalog] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [query, setQuery] = useState('');
  const [currentPage, setCurrentPage] = useState(1);
  const [selectedProfile, setSelectedProfile] = useState(null);
  const [profileLoading, setProfileLoading] = useState(false);
  const [profileError, setProfileError] = useState('');
  const [compareIds, setCompareIds] = useState([]);

  useEffect(() => {
    let active = true;
    setLoading(true);
    loadPrimaryV2Catalog()
      .then((next) => {
        if (!active) return;
        setCatalog(next);
        setError('');
      })
      .catch((err) => {
        if (!active) return;
        setError(err instanceof Error ? err.message : String(err));
        setCatalog(null);
      })
      .finally(() => {
        if (active) setLoading(false);
      });
    return () => {
      active = false;
    };
  }, []);

  // Preserve only the neutral WordPress-header integration from the previous widget.
  useLayoutEffect(() => {
    const root = document.getElementById('widget-root');
    if (!root) return undefined;
    const header = document.getElementById('menu');
    const adminBar = document.getElementById('wpadminbar');

    const update = () => {
      let bottom = 0;
      if (adminBar) bottom = Math.max(bottom, adminBar.getBoundingClientRect().bottom);
      if (header) bottom = Math.max(bottom, header.getBoundingClientRect().bottom);
      root.style.setProperty('--sanida-sticky-top', `${Math.max(0, Math.round(bottom))}px`);
    };

    update();
    window.addEventListener('scroll', update, { passive: true });
    window.addEventListener('resize', update, { passive: true });
    const observer = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(update) : null;
    if (observer) {
      if (header) observer.observe(header);
      if (adminBar) observer.observe(adminBar);
    }
    return () => {
      window.removeEventListener('scroll', update);
      window.removeEventListener('resize', update);
      observer?.disconnect();
    };
  }, []);

  const searchEntries = catalog?.searchIndex?.entries || [];
  const explorerEntities = catalog?.insurerExplorer?.entities || [];
  const exploreIndex = catalog?.exploreIndex || null;

  const entryByProfileId = useMemo(
    () => new Map(searchEntries.map((entry) => [entry.profile_id, entry])),
    [searchEntries]
  );
  const explorerByEntityId = useMemo(
    () => new Map(explorerEntities.map((entity) => [entity.entity_id, entity])),
    [explorerEntities]
  );

  const ordinaryEntries = useMemo(() => {
    return explorerEntities
      .map((entity) => entryByProfileId.get(`entity:${entity.entity_id}`))
      .filter(Boolean)
      .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'pt-BR'));
  }, [entryByProfileId, explorerEntities]);

  const visibleEntries = useMemo(() => {
    const normalized = normalizeSearch(query);
    if (!normalized) return ordinaryEntries;
    return searchEntries
      .filter((entry) => String(entry.search_text || '').includes(normalized))
      .sort((a, b) => {
        const aStarts = String(a.search_text || '').startsWith(normalized) ? 0 : 1;
        const bStarts = String(b.search_text || '').startsWith(normalized) ? 0 : 1;
        if (aStarts !== bStarts) return aStarts - bStarts;
        return String(a.name || '').localeCompare(String(b.name || ''), 'pt-BR');
      });
  }, [ordinaryEntries, query, searchEntries]);

  useEffect(() => {
    setCurrentPage(1);
  }, [query]);

  const totalPages = Math.max(1, Math.ceil(visibleEntries.length / ITEMS_PER_PAGE));
  const safePage = Math.min(currentPage, totalPages);
  const pageEntries = visibleEntries.slice(
    (safePage - 1) * ITEMS_PER_PAGE,
    safePage * ITEMS_PER_PAGE
  );

  const maxCompare = exploreIndex?.publication_policy?.recommended_max_side_by_side_cards || 4;
  const compareEntities = compareIds.map((id) => explorerByEntityId.get(id)).filter(Boolean);

  const toggleCompare = (entityId) => {
    setCompareIds((current) => {
      if (current.includes(entityId)) return current.filter((id) => id !== entityId);
      if (current.length >= maxCompare) return current;
      return [...current, entityId];
    });
  };

  const openEntry = async (entry) => {
    if (!entry?.profile_path) return;
    setProfileLoading(true);
    setProfileError('');
    try {
      const profile = await loadProfile(entry.profile_path);
      setSelectedProfile(profile);
    } catch (err) {
      setProfileError(err instanceof Error ? err.message : String(err));
      setSelectedProfile(null);
    } finally {
      setProfileLoading(false);
    }
  };

  const openProfileId = (profileId) => {
    const entry = entryByProfileId.get(profileId);
    if (entry) openEntry(entry);
  };

  const openEntityId = (entityId) => openProfileId(`entity:${entityId}`);

  if (loading) {
    return (
      <div className="flex h-64 items-center justify-center text-sm text-slate-500">
        Carregando contratos públicos v2…
      </div>
    );
  }

  if (error) {
    return (
      <div className="mx-auto max-w-3xl rounded-xl border border-red-200 bg-red-50 p-5 text-sm text-red-800">
        <div className="font-semibold">Não foi possível carregar o pacote público v2.</div>
        <div className="mt-1">{error}</div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-white">
      <div className="mx-auto max-w-6xl px-4 pb-6 pt-8">
        <div className="max-w-3xl">
          <div className="mb-3 flex items-center gap-2">
            <ShieldCheck className="h-5 w-5 shrink-0 text-sky-600" />
            <span className="text-sm font-semibold text-sky-700">Consulta e comparação de seguradoras</span>
          </div>
          <h1 className="text-xl font-bold leading-tight text-slate-900 md:text-2xl">
            Consulte seguradoras, marcas e relações verificadas
          </h1>
          <p className="mt-2 text-sm text-slate-600 md:text-base">
            Pesquise uma identidade conhecida, leia os sinais públicos disponíveis e compare seguradoras ordinárias sem nota composta ou ranking geral.
          </p>
        </div>
      </div>

      <div
        className="sticky z-30 border-y border-slate-200 bg-white/95 shadow-sm backdrop-blur"
        style={{ top: 'var(--sanida-sticky-top, 0px)' }}
      >
        <div className="mx-auto flex max-w-6xl flex-col gap-2 px-4 py-3 md:flex-row md:items-center md:justify-between">
          <div className="text-xs text-slate-500">
            {query.trim()
              ? `${visibleEntries.length} identidades encontradas`
              : `${ordinaryEntries.length} seguradoras ordinárias no comparador`}
          </div>
          <div className="relative w-full md:w-96">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Buscar nome, marca, CNPJ ou código SUSEP…"
              className="w-full rounded-lg border border-slate-300 bg-white py-2 pl-9 pr-3 text-sm outline-none focus:border-transparent focus:ring-2 focus:ring-sky-300"
            />
          </div>
        </div>
      </div>

      <main className="bg-slate-50">
        <div className="mx-auto max-w-6xl space-y-6 px-4 py-8">
          {profileLoading ? (
            <div className="rounded-lg border border-slate-200 bg-white p-3 text-sm text-slate-500">
              Carregando perfil…
            </div>
          ) : null}
          {profileError ? (
            <div className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
              {profileError}
            </div>
          ) : null}

          <ComparisonPanel
            entities={compareEntities}
            onRemove={toggleCompare}
            onOpenEntity={openEntityId}
          />

          {pageEntries.length === 0 ? (
            <div className="rounded-xl border border-dashed border-slate-300 bg-white py-16 text-center text-slate-500">
              <Search className="mx-auto mb-3 h-10 w-10 text-slate-300" />
              <div className="font-medium">Nenhuma identidade encontrada</div>
              <div className="mt-1 text-sm">Tente outro nome, marca, CNPJ ou código SUSEP.</div>
            </div>
          ) : (
            <>
              {query.trim() ? (
                <div className="text-xs text-slate-500">
                  A busca inclui seguradoras, marcas, Sandbox, identidades históricas e outras entidades publicadas. Ser pesquisável não significa ser elegível ao assessment ordinário.
                </div>
              ) : null}
              <div className="grid grid-cols-1 gap-5 md:grid-cols-2 lg:grid-cols-3">
                {pageEntries.map((entry) => {
                  const entityId = entry.profile_id?.startsWith('entity:')
                    ? entry.profile_id.slice('entity:'.length)
                    : null;
                  const explorer = entityId ? explorerByEntityId.get(entityId) : null;
                  return (
                    <div key={entry.profile_id}>
                      {query.trim() && !explorer ? (
                        <div className="mb-1 px-1 text-[11px] font-medium uppercase tracking-wide text-slate-400">
                          {entryBucketLabel(entry)}
                        </div>
                      ) : null}
                      <InsurerCard
                        entry={entry}
                        explorer={explorer}
                        onOpen={openEntry}
                        compareSelected={Boolean(entityId && compareIds.includes(entityId))}
                        compareDisabled={compareIds.length >= maxCompare}
                        onToggleCompare={toggleCompare}
                      />
                    </div>
                  );
                })}
              </div>

              {totalPages > 1 ? (
                <div className="flex items-center justify-center gap-4 pt-4">
                  <button
                    type="button"
                    disabled={safePage <= 1}
                    onClick={() => setCurrentPage((page) => Math.max(1, page - 1))}
                    className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    <ChevronLeft className="h-4 w-4" /> Anterior
                  </button>
                  <span className="text-sm text-slate-500">{safePage} de {totalPages}</span>
                  <button
                    type="button"
                    disabled={safePage >= totalPages}
                    onClick={() => setCurrentPage((page) => Math.min(totalPages, page + 1))}
                    className="inline-flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 disabled:cursor-not-allowed disabled:opacity-40"
                  >
                    Próxima <ChevronRight className="h-4 w-4" />
                  </button>
                </div>
              ) : null}
            </>
          )}

          <ExplorePanel exploreIndex={exploreIndex} onOpenEntity={openEntityId} />
        </div>
      </main>

      <InsurerProfileModal
        profile={selectedProfile}
        onClose={() => setSelectedProfile(null)}
        onNavigateProfile={openProfileId}
      />
    </div>
  );
}
