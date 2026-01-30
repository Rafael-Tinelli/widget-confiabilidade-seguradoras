import React, { useLayoutEffect, useMemo, useState, useEffect } from "react";
import InsurerCard from "./components/InsurerCard";
import InsurerScoreModal from "./InsurerScoreModal";
import { Search, SlidersHorizontal, ShieldCheck, ChevronLeft, ChevronRight, Award } from "lucide-react";

const API_URL = '/api/v1/insurers.json'; 

export default function App() {
  const [insurersData, setInsurers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");
  const [sortBy, setSortBy] = useState("score_desc");
  const [selectedInsurer, setSelectedInsurer] = useState(null);
  const [sources, setSources] = useState(null);
  
  // Paginação
  const [currentPage, setCurrentPage] = useState(1);
  const itemsPerPage = 24;

  useEffect(() => {
    setLoading(true);
    fetch(API_URL)
      .then(res => res.json())
      .then(data => {
        const rawList = data.insurers || [];
        // 1. CORREÇÃO DE DUPLICATAS (Usa ID em vez de CNPJ)
        const uniqueList = Array.from(new Map(rawList.map(item => [item.id, item])).values());
        setInsurers(uniqueList);
        setSources(data.sources || null);
        setLoading(false);
      })
      .catch(err => {
        console.error("Erro carregando dados:", err);
        setInsurers([]);
        setLoading(false);
      });
  }, []);

  // --- Sticky abaixo do header do site (WordPress / Sanida) ---
  // Ajusta automaticamente o top do sticky para não sobrepor o header fixo/sticky do site.
  useLayoutEffect(() => {
    const root = document.getElementById("widget-root");
    if (!root) return;

    let raf = 0;
    const updateStickyTop = () => {
      cancelAnimationFrame(raf);
      raf = requestAnimationFrame(() => {
        const candidates = [];

        // WP Admin Bar (quando logado)
        const adminBar = document.getElementById("wpadminbar");
        if (adminBar) candidates.push(adminBar);

        // Headers do site (fora do widget)
        const allHeaders = Array.from(document.querySelectorAll("header"));
        for (const h of allHeaders) {
          if (root.contains(h)) continue; // ignora o header do próprio widget
          candidates.push(h);
        }

        // Calcula o "bottom" máximo dos elementos fixos/sticky que ocupam o topo.
        let offset = 0;
        for (const el of candidates) {
          const cs = window.getComputedStyle(el);
          if (cs.position !== "fixed" && cs.position !== "sticky") continue;
          const r = el.getBoundingClientRect();
          if (r.height < 20) continue;
          if (r.bottom <= 0) continue;
          if (r.top > 100) continue; // só barras realmente no topo
          offset = Math.max(offset, r.bottom);
        }

        root.style.setProperty("--sanida-sticky-top", `${Math.ceil(offset)}px`);
      });
    };

    updateStickyTop();
    window.addEventListener("resize", updateStickyTop, { passive: true });
    window.addEventListener("scroll", updateStickyTop, { passive: true });

    return () => {
      window.removeEventListener("resize", updateStickyTop);
      window.removeEventListener("scroll", updateStickyTop);
      cancelAnimationFrame(raf);
    };
  }, []);

  const insurers = insurersData;

  const filtered = useMemo(() => {
    if (!query) return insurers;
    const lower = query.toLowerCase();
    return insurers.filter((i) => {
      return (
        i.name?.toLowerCase().includes(lower) ||
        i.cnpj?.includes(lower) ||
        i.id?.toLowerCase().includes(lower)
      );
    });
  }, [insurers, query]);

  const sorted = useMemo(() => {
    const list = [...filtered];
    list.sort((a, b) => {
      const dataA = a.data || {};
      const dataB = b.data || {};

      // Compatibilidade: Lê financial_score (novo) ou score (antigo)
      const scoreA = Number(dataA.financial_score) || Number(dataA.score) || 0;
      const scoreB = Number(dataB.financial_score) || Number(dataB.score) || 0;
      
      const premA  = Number(dataA.premiums) || 0;
      const premB  = Number(dataB.premiums) || 0;

      switch (sortBy) {
        case "score_desc":
          if (scoreB !== scoreA) return scoreB - scoreA;
          return premB - premA;
        case "name_asc":
          return (a.name || "").localeCompare(b.name || "");
        case "premiums_desc":
          return premB - premA;
        default:
          return 0;
      }
    });
    return list;
  }, [filtered, sortBy]);

  const totalPages = Math.max(1, Math.ceil(sorted.length / itemsPerPage));
  const safeCurrentPage = Math.min(currentPage, totalPages);

  const paginatedData = useMemo(() => {
    const startIndex = (safeCurrentPage - 1) * itemsPerPage;
    return sorted.slice(startIndex, startIndex + itemsPerPage);
  }, [sorted, safeCurrentPage, itemsPerPage]);

  useEffect(() => {
    setCurrentPage(1);
  }, [query, sortBy]);

  const openScoreModal = (insurer) => setSelectedInsurer(insurer);
  const closeScoreModal = () => setSelectedInsurer(null);

  if (loading) return (
    <div className="flex justify-center items-center h-64">
      <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-[#3498db]"></div>
    </div>
  );

  return (
    <div className="min-h-screen bg-[#f4f5f5]" style={{ paddingTop: "var(--sanida-sticky-top, 0px)" }}>
      <header className="max-w-6xl mx-auto px-4 pt-8 pb-4">
        <div className="flex items-start gap-2">
          <ShieldCheck className="w-6 h-6 text-gray-700 mt-1" />
          <div>
            <h1 className="text-xl md:text-2xl font-bold text-[#1f2937]">
              Confiabilidade de Seguradoras
            </h1>
            <p className="text-sm text-gray-500 mt-1">
              Score composto por Solvência (SES/SUSEP), Reputação (Consumidor.gov) e Open Insurance (OPIN).
            </p>
          </div>
        </div>
      </header>

      {/* Barra sticky do ranking (abaixo do header do site) */}
      <div
        className="sticky z-30 border-b border-gray-200 bg-[#f4f5f5]/95 backdrop-blur"
        style={{ top: "var(--sanida-sticky-top, 0px)" }}
      >
        <div className="max-w-6xl mx-auto px-4 py-4">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-end">
            <div className="relative w-full md:max-w-md">
              <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
              <input
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Buscar por nome, CNPJ ou SUSEP..."
                className="w-full pl-9 pr-3 py-2 rounded-xl border border-gray-200 bg-white text-sm outline-none focus:ring-2 focus:ring-gray-200"
              />
            </div>

            <div className="flex items-center gap-2 w-full md:w-auto md:justify-end">
              <SlidersHorizontal className="w-4 h-4 text-gray-400" />
              <select
                value={sortBy}
                onChange={(e) => setSortBy(e.target.value)}
                className="w-full md:w-auto px-3 py-2 rounded-xl border border-gray-200 bg-white text-sm outline-none focus:ring-2 focus:ring-gray-200"
              >
                <option value="score_desc">Ordenar por score</option>
                <option value="name_asc">Ordenar por nome</option>
                <option value="premiums_desc">Ordenar por prêmios</option>
              </select>
            </div>
          </div>
        </div>
      </div>

      <main className="max-w-6xl mx-auto px-4 py-6">
        <div className="flex items-center justify-between mb-4">
          <p className="text-sm text-gray-500">
            {filtered.length} seguradoras encontradas
          </p>
        </div>

        {paginatedData.length === 0 ? (
          <div className="text-center py-20 text-gray-500">
            Nenhuma entidade encontrada.
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
              {paginatedData.map((insurer) => (
                <InsurerCard
                  key={insurer.id}
                  insurer={insurer}
                  onOpenScoreModal={openScoreModal}
                />
              ))}
            </div>

            {/* Paginação */}
            {totalPages > 1 && (
              <div className="flex justify-center gap-2 mt-8">
                <button
                  onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                  disabled={safeCurrentPage <= 1}
                  className="p-2 rounded-lg border border-gray-300 disabled:opacity-50 hover:bg-gray-100"
                >
                  <ChevronLeft className="w-5 h-5" />
                </button>
                
                <span className="flex items-center px-4 font-bold text-gray-700">
                  {safeCurrentPage} de {totalPages}
                </span>

                <button
                  onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                  disabled={safeCurrentPage >= totalPages}
                  className="p-2 rounded-lg border border-gray-300 disabled:opacity-50 hover:bg-gray-100"
                >
                  <ChevronRight className="w-5 h-5" />
                </button>
              </div>
            )}
          </>
        )}
      </main>

      <InsurerScoreModal
        insurer={selectedInsurer}
        sources={sources}
        onClose={closeScoreModal}
      />
    </div>
  );
}
