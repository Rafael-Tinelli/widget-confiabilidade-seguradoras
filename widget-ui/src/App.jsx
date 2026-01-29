import React, { useEffect, useMemo, useState } from "react";
import InsurerCard from "./InsurerCard.jsx";
import InsurerScoreModal from "./InsurerScoreModal.jsx";

export default function App() {
  const [meta, setMeta] = useState(null);
  const [insurers, setInsurers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const [searchTerm, setSearchTerm] = useState("");
  const [segment, setSegment] = useState("Todos");
  const [selectedInsurer, setSelectedInsurer] = useState(null);

  useEffect(() => {
    let alive = true;

    async function run() {
      try {
        setLoading(true);
        setError(null);

        const res = await fetch("/api/v1/insurers.json", { cache: "no-store" });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);

        const json = await res.json();
        if (!alive) return;

        setMeta(json?.meta ?? null);
        setInsurers(Array.isArray(json?.insurers) ? json.insurers : []);
      } catch (e) {
        if (!alive) return;
        setError(e?.message ?? "Falha ao carregar dados");
      } finally {
        if (!alive) return;
        setLoading(false);
      }
    }

    run();
    return () => {
      alive = false;
    };
  }, []);

  const segments = useMemo(() => {
    const s = new Set();
    for (const ins of insurers) {
      for (const seg of ins?.segments ?? []) s.add(seg);
    }
    return ["Todos", ...Array.from(s).sort((a, b) => a.localeCompare(b))];
  }, [insurers]);

  const filtered = useMemo(() => {
    const q = searchTerm.trim().toLowerCase();

    const bySeg = (ins) =>
      segment === "Todos" || (ins?.segments ?? []).includes(segment);

    const byQuery = (ins) => {
      if (!q) return true;
      const name = (ins?.name ?? "").toLowerCase();
      const slug = (ins?.slug ?? "").toLowerCase();
      const cnpj = (ins?.cnpj ?? "").toString();
      const susep = (ins?.susep_code ?? "").toString();
      return (
        name.includes(q) ||
        slug.includes(q) ||
        cnpj.includes(q) ||
        susep.includes(q)
      );
    };

    const scoreOf = (ins) => ins?.data?.score;
    return [...insurers]
      .filter(bySeg)
      .filter(byQuery)
      .sort((a, b) => (scoreOf(b) ?? -1) - (scoreOf(a) ?? -1));
  }, [insurers, searchTerm, segment]);

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-6xl px-4 py-6">
        <header className="mb-6 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <h1 className="text-2xl font-semibold">Widget de Confiabilidade</h1>
            <p className="text-sm text-slate-300">
              Clique em um card para ver a matemática e as fontes da nota.
              {meta?.generated_at ? (
                <span className="ml-2 text-slate-400">
                  (dados: {meta.generated_at})
                </span>
              ) : null}
            </p>
          </div>

          <div className="flex flex-col gap-2 md:flex-row md:items-center">
            <input
              className="w-full rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-sm outline-none focus:border-slate-600 md:w-80"
              placeholder="Buscar por nome, SUSEP ou CNPJ…"
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
            />

            <select
              className="rounded-lg border border-slate-800 bg-slate-900 px-3 py-2 text-sm outline-none focus:border-slate-600"
              value={segment}
              onChange={(e) => setSegment(e.target.value)}
            >
              {segments.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>
        </header>

        {loading ? (
          <div className="rounded-xl border border-slate-800 bg-slate-900 p-4 text-sm text-slate-300">
            Carregando seguradoras…
          </div>
        ) : error ? (
          <div className="rounded-xl border border-red-900 bg-red-950/40 p-4 text-sm text-red-200">
            Erro: {error}
          </div>
        ) : (
          <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
            {filtered.map((ins) => (
              <InsurerCard
                key={ins.id}
                insurer={ins}
                onClick={() => setSelectedInsurer(ins)}
              />
            ))}
          </div>
        )}
      </div>

      <InsurerScoreModal
        isOpen={!!selectedInsurer}
        insurer={selectedInsurer}
        meta={meta}
        onClose={() => setSelectedInsurer(null)}
      />
    </div>
  );
}
