import { useState, useEffect, useMemo } from 'react';
import { Search, ShieldCheck, Award, ChevronLeft, ChevronRight } from 'lucide-react';
import InsurerCard from './components/InsurerCard';
import InsurerScoreModal from './InsurerScoreModal';

const API_URL = `${import.meta.env.BASE_URL}api/v1/insurers.json`;

function App() {
  const [insurers, setInsurers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [sources, setSources] = useState(null);
  const [selectedInsurer, setSelectedInsurer] = useState(null);

  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState('score'); // score (novo) / final_score (legado) / financial_score (legado)
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(24);

  useEffect(() => {
    setLoading(true);

    fetch(API_URL)
      .then(res => res.json())
      .then(data => {
        setSources(data.sources || null);

        const rawList = Array.isArray(data.insurers) ? data.insurers : [];
        setInsurers(rawList);
      })
      .catch(err => {
        console.error('Erro ao carregar insurers.json:', err);
        setInsurers([]);
        setSources(null);
      })
      .finally(() => setLoading(false));
  }, []);

  const filteredInsurers = useMemo(() => {
    if (!searchTerm.trim()) return insurers;

    const q = searchTerm.trim().toLowerCase();
    return insurers.filter(i => {
      const name = (i.name || '').toLowerCase();
      const cnpj = (i.cnpj || '').toLowerCase();
      const id = (i.id || '').toLowerCase();
      return name.includes(q) || cnpj.includes(q) || id.includes(q);
    });
  }, [insurers, searchTerm]);

  const openScoreModal = (insurer) => setSelectedInsurer(insurer);
  const closeScoreModal = () => setSelectedInsurer(null);

  const sortedInsurers = useMemo(() => {
    const list = [...filteredInsurers];

    list.sort((a, b) => {
      const dataA = a?.data || {};
      const dataB = b?.data || {};

      const scoreA = Number(dataA.score ?? dataA.final_score ?? dataA.financial_score) || 0;
      const scoreB = Number(dataB.score ?? dataB.final_score ?? dataB.financial_score) || 0;

      if (sortBy === 'score') return scoreB - scoreA;
      if (sortBy === 'name') return String(a?.name || '').localeCompare(String(b?.name || ''), 'pt-BR');

      return scoreB - scoreA;
    });

    return list;
  }, [filteredInsurers, sortBy]);

  const totalPages = Math.max(1, Math.ceil(sortedInsurers.length / itemsPerPage));
  const safeCurrentPage = Math.min(currentPage, totalPages);

  const paginatedInsurers = useMemo(() => {
    const startIndex = (safeCurrentPage - 1) * itemsPerPage;
    return sortedInsurers.slice(startIndex, startIndex + itemsPerPage);
  }, [sortedInsurers, safeCurrentPage, itemsPerPage]);

  useEffect(() => {
    setCurrentPage(1);
  }, [searchTerm, sortBy]);

  return (
    <div className="min-h-screen bg-slate-50">
      <header className="border-b border-slate-200 bg-white">
        <div className="mx-auto max-w-7xl px-4 py-6">
          <div className="flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
            <div>
              <h1 className="flex items-center gap-2 text-xl font-semibold text-slate-900">
                <ShieldCheck className="h-6 w-6 text-slate-700" />
                Confiabilidade de Seguradoras
              </h1>
              <p className="mt-1 text-sm text-slate-600">
                Score composto por Solvência (SES/SUSEP), Reputação (Consumidor.gov) e Open Insurance (OPIN).
              </p>
            </div>

            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
              <div className="relative">
                <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
                <input
                  type="text"
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  placeholder="Buscar por nome, CNPJ ou SUSEP..."
                  className="w-full rounded-xl border border-slate-200 bg-white py-2 pl-9 pr-3 text-sm text-slate-900 shadow-sm outline-none ring-0 placeholder:text-slate-400 focus:border-slate-300 sm:w-80"
                />
              </div>

              <div className="flex items-center gap-2">
                <Award className="h-4 w-4 text-slate-500" />
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                  className="rounded-xl border border-slate-200 bg-white px-3 py-2 text-sm text-slate-900 shadow-sm outline-none focus:border-slate-300"
                >
                  <option value="score">Ordenar por score</option>
                  <option value="name">Ordenar por nome</option>
                </select>
              </div>
            </div>
          </div>

          <div className="mt-4 text-sm text-slate-600">
            {loading ? (
              <span>Carregando…</span>
            ) : (
              <span>
                {sortedInsurers.length.toLocaleString('pt-BR')} seguradoras encontradas
              </span>
            )}
          </div>
        </div>
      </header>

      <main className="mx-auto max-w-7xl px-4 py-8">
        {loading ? (
          <div className="rounded-2xl bg-white p-8 text-center text-slate-600 ring-1 ring-black/5">
            Carregando lista…
          </div>
        ) : sortedInsurers.length === 0 ? (
          <div className="rounded-2xl bg-white p-8 text-center text-slate-600 ring-1 ring-black/5">
            Nenhuma seguradora encontrada.
          </div>
        ) : (
          <>
            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {paginatedInsurers.map((insurer) => (
                <InsurerCard
                  key={insurer.id}
                  insurer={insurer}
                  onOpenScoreModal={openScoreModal}
                />
              ))}
            </div>

            {/* Paginação */}
            <div className="mt-8 flex items-center justify-between gap-3">
              <button
                type="button"
                onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                disabled={safeCurrentPage <= 1}
                className="inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-900 shadow-sm disabled:cursor-not-allowed disabled:opacity-50"
              >
                <ChevronLeft className="h-4 w-4" />
                Anterior
              </button>

              <div className="text-sm text-slate-600">
                Página <span className="font-semibold text-slate-900">{safeCurrentPage}</span> de{' '}
                <span className="font-semibold text-slate-900">{totalPages}</span>
              </div>

              <button
                type="button"
                onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                disabled={safeCurrentPage >= totalPages}
                className="inline-flex items-center gap-2 rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-900 shadow-sm disabled:cursor-not-allowed disabled:opacity-50"
              >
                Próxima
                <ChevronRight className="h-4 w-4" />
              </button>
            </div>
          </>
        )}

        <InsurerScoreModal
          insurer={selectedInsurer}
          sources={sources}
          onClose={closeScoreModal}
        />
      </main>
    </div>
  );
}

export default App;
