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

    const header = document.getElementById("menu");
    const adminBar = document.getElementById("wpadminbar");

    // ----------------------------
    // (A) SAFE padding (estável)
    // ----------------------------
    let safeMax = 0;
    const computeSafe = () =>
      (adminBar ? adminBar.offsetHeight : 0) +
      (header ? header.offsetHeight : 0);
    const setHeaderSafeMax = (force = false) => {
      const v = computeSafe();
      if (force || v > safeMax) {
        safeMax = v;
        root.style.setProperty("--sanida-header-safe", `${safeMax}px`);
      }
    };

    // ----------------------------
    // (B) Sticky top (dinâmico)
    // ----------------------------
    let enabled = false;          // só calcula quando widget está no viewport (ou perto)
    let scheduled = false;
    let rafCommit = 0;
    let lastSticky = Number.NaN;

    const readStickyTop = () => {
      let offset = 0;
      if (adminBar) offset = Math.max(offset, adminBar.getBoundingClientRect().bottom);
      if (header)   offset = Math.max(offset, header.getBoundingClientRect().bottom);
      return Math.max(0, Math.round(offset));
    };

    const commit = () => {
      scheduled = false;
      if (!enabled) return;
      const next = readStickyTop();
      if (!Number.isFinite(lastSticky) || next !== lastSticky) {
        lastSticky = next;
        root.style.setProperty("--sanida-sticky-top", `${next}px`);
      }
    };

    const schedule = () => {
      if (!enabled) return;
      if (scheduled) return;
      scheduled = true;
      cancelAnimationFrame(rafCommit);
      rafCommit = requestAnimationFrame(commit);
    };

    // ----------------------------
    // (C) Range do widget (sem sentinelas)
    // ----------------------------
    let widgetTop = 0;
    let widgetBottom = 0;
    const margin = 240; // tolerância: liga antes de entrar e desliga pouco depois de sair

    const measureRange = () => {
      const y = window.scrollY || window.pageYOffset || 0;
      const rect = root.getBoundingClientRect();
      widgetTop = rect.top + y;
      widgetBottom = widgetTop + root.offsetHeight;
    };

    const isInRange = () => {
      const y = window.scrollY || window.pageYOffset || 0;
      const vh = window.innerHeight || document.documentElement.clientHeight || 0;
      const viewTop = y;
      const viewBottom = y + vh;
      return viewBottom > (widgetTop - margin) && viewTop < (widgetBottom + margin);
    };

    const setEnabled = (nextEnabled) => {
      if (nextEnabled === enabled) return;
      enabled = nextEnabled;
      if (!enabled) {
        lastSticky = Number.NaN;
        scheduled = false;
        cancelAnimationFrame(rafCommit);
        root.style.setProperty("--sanida-sticky-top", "0px");
        return;
      }
      // Entrou em range: garante valor correto imediatamente (evita ficar em 0)
      schedule();
    };

    const onScroll = () => {
      setEnabled(isInRange());
      if (enabled) schedule();
    };

    // ----------------------------
    // (D) Observa mudanças reais sem “observer barulhento”
    // ----------------------------
    const ro = new ResizeObserver(() => {
      // muda conteúdo do widget / header / admin bar
      measureRange();
      setHeaderSafeMax(); // só sobe (max), não causa shift
      onScroll();
    });
    ro.observe(root);
    if (header) ro.observe(header);
    if (adminBar) ro.observe(adminBar);

    // init
    setHeaderSafeMax(true);
    measureRange();
    setEnabled(isInRange());
    schedule();

    window.addEventListener("scroll", onScroll, { passive: true });
   const onResize = () => {
      safeMax = 0;              // resize é o único momento em que o safe pode “recalcular pra baixo”
      setHeaderSafeMax(true);
      measureRange();
      onScroll();
    };
   window.addEventListener("resize", onResize, { passive: true });
    const vv = window.visualViewport;
    if (vv) vv.addEventListener("resize", onResize, { passive: true });

    // settle rápido pra capturar layout tardio (sem MutationObserver global)
    const timers = [200, 700, 1400].map((t) =>
      setTimeout(() => {
        setHeaderSafeMax();
        measureRange();
        onScroll();
      }, t)
    );

    return () => {
      timers.forEach(clearTimeout);
      ro.disconnect();
      window.removeEventListener("scroll", onScroll);
      window.removeEventListener("resize", onResize);
      if (vv) vv.removeEventListener("resize", onResize);
      cancelAnimationFrame(rafCommit);
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
    <div 
      className="min-h-screen bg-[#f4f5f5]"
      // CORREÇÃO: paddingTop garante que o título comece DEPOIS do header fixo do site.
      // Adicionamos +24px de respiro visual.
      style={{ paddingTop: "calc(var(--sanida-header-safe, var(--sanida-sticky-top, 0px)) + 24px)" }}
    >
      {/* HEADER DO WIDGET */}
      {/* Ajuste de padding-top (pt-6) e tamanho do H1 para não brigar com a logo */}
      <div className="max-w-6xl mx-auto px-4 pt-6 pb-6">
        <div className="flex items-start gap-2">
          <ShieldCheck className="w-8 h-8 text-[#3498db] mt-1 shrink-0" />
          <div>
            <h1 className="text-xl md:text-2xl font-bold text-[#1f2937] leading-tight">
              Ranking de Seguradoras (SUSEP): lista, confiabilidade e como consultar
            </h1>
            <p className="text-sm md:text-base text-gray-600 mt-2">
              Score composto por Solvência (SES/SUSEP), Reputação (Consumidor.gov) e Open Insurance (OPIN).
            </p>
          </div>
        </div>
      </div>

      {/* BARRA DE FERRAMENTAS STICKY */}
      {/* Fica grudada logo abaixo do header do site ao rolar */}
      <div
        className="sticky z-30 border-y border-gray-200 bg-[#f4f5f5]/95 backdrop-blur shadow-sm"
        style={{ top: "var(--sanida-sticky-top, 0px)" }}
      >
        <div className="max-w-6xl mx-auto px-4 py-3">
          <div className="flex flex-col md:flex-row gap-3 items-center justify-between">
            
            {/* Contador de resultados: Texto ajustado e apenas visível em Desktop */}
            <div className="hidden md:block text-sm text-gray-500 font-medium whitespace-nowrap">
               {filtered.length} <span className="font-normal">seguradoras encontradas</span>
            </div>

            <div className="flex flex-col sm:flex-row gap-3 w-full md:w-auto">
              {/* Campo de Busca */}
              <div className="relative w-full sm:w-72">
                <Search className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2" />
                <input
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                  placeholder="Buscar nome, CNPJ ou SUSEP..."
                  className="w-full pl-9 pr-3 py-2 rounded-lg border border-gray-300 bg-white text-sm outline-none focus:ring-2 focus:ring-[#3498db] focus:border-transparent transition-all"
                />
              </div>

              {/* Select de Ordenação */}
              <div className="relative w-full sm:w-auto">
                <SlidersHorizontal className="w-4 h-4 text-gray-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
                <select
                  value={sortBy}
                  onChange={(e) => setSortBy(e.target.value)}
                  className="w-full sm:w-auto pl-9 pr-8 py-2 rounded-lg border border-gray-300 bg-white text-sm outline-none focus:ring-2 focus:ring-[#3498db] focus:border-transparent appearance-none cursor-pointer transition-all hover:bg-gray-50"
                >
                  <option value="score_desc">Melhor Nota</option>
                  <option value="name_asc">Nome (A-Z)</option>
                  <option value="premiums_desc">Maior Faturamento</option>
                </select>
                <div className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none border-l border-gray-200 pl-2">
                   <span className="text-[10px] text-gray-400">▼</span>
                </div>
              </div>
            </div>

          </div>
        </div>
      </div>

      <main className="max-w-6xl mx-auto px-4 py-8">
        {/* Contagem removida daqui para evitar duplicidade. Só aparece na barra sticky (Desktop). */}

        {paginatedData.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-20 text-gray-500 bg-white rounded-xl border border-gray-200 border-dashed">
            <Search className="w-12 h-12 text-gray-300 mb-3" />
            <p className="text-lg font-medium">Nenhuma entidade encontrada</p>
            <p className="text-sm">Tente buscar por outro termo ou CNPJ</p>
          </div>
        ) : (
          <>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
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
              <div className="flex justify-center items-center gap-4 mt-12">
                <button
                  onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                  disabled={safeCurrentPage <= 1}
                  className="flex items-center gap-1 px-4 py-2 rounded-lg bg-white border border-gray-200 text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors shadow-sm font-medium text-sm"
                >
                  <ChevronLeft className="w-4 h-4" /> Anterior
                </button>
                
                <span className="text-sm font-medium text-gray-600 bg-white px-4 py-2 rounded-lg border border-gray-100">
                  Página <strong className="text-gray-900">{safeCurrentPage}</strong> de {totalPages}
                </span>

                <button
                  onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                  disabled={safeCurrentPage >= totalPages}
                  className="flex items-center gap-1 px-4 py-2 rounded-lg bg-white border border-gray-200 text-gray-700 hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed transition-colors shadow-sm font-medium text-sm"
                >
                  Próxima <ChevronRight className="w-4 h-4" />
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

