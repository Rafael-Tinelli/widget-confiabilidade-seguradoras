import { GitCompareArrows, ShieldCheck } from 'lucide-react';

function formatPeriod(value) {
  const text = String(value || '').replace(/\D/g, '');
  if (text.length !== 6) return value || '—';
  return `${text.slice(4, 6)}/${text.slice(0, 4)}`;
}

function confidenceLabel(value) {
  const labels = {
    established_core_history: 'histórico central estabelecido',
    limited_core_history: 'histórico central limitado',
    insufficient_core_evidence: 'evidência central insuficiente',
  };
  return labels[value] || 'confiança não classificada';
}

function toneClass(publicClass) {
  switch (publicClass) {
    case 'prudential_warning':
      return 'border-red-200 bg-red-50 text-red-900';
    case 'attention':
      return 'border-amber-200 bg-amber-50 text-amber-900';
    case 'favorable_reading':
      return 'border-emerald-200 bg-emerald-50 text-emerald-900';
    default:
      return 'border-slate-200 bg-slate-50 text-slate-800';
  }
}

export default function InsurerCard({
  entry,
  explorer,
  onOpen,
  compareSelected = false,
  compareDisabled = false,
  onToggleCompare,
}) {
  const name = entry?.name || explorer?.display_name || explorer?.legal_name || '—';
  const assessment = explorer?.assessment || null;
  const financial = explorer?.financial || null;
  const conduct = explorer?.conduct || null;
  const isOrdinary = Boolean(explorer);

  return (
    <article className="flex h-full flex-col rounded-2xl bg-white p-5 shadow-sm ring-1 ring-black/5">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <ShieldCheck className="h-4 w-4 shrink-0 text-slate-500" />
            <h3 className="text-base font-semibold leading-tight text-slate-900">{name}</h3>
          </div>
          <p className="mt-1 text-xs text-slate-500">
            {entry?.disambiguation || 'Identidade publicada no contrato v2'}
          </p>
        </div>
      </div>

      {isOrdinary ? (
        <>
          <div className={`mt-4 rounded-xl border p-3 ${toneClass(assessment?.public_class)}`}>
            <div className="text-sm font-semibold">{assessment?.title || 'Leitura conjunta'}</div>
            {assessment?.summary ? <p className="mt-1 text-xs leading-relaxed opacity-90">{assessment.summary}</p> : null}
          </div>

          <div className="mt-4 grid grid-cols-1 gap-2 text-xs text-slate-600">
            <div className="rounded-lg border border-slate-200 p-3">
              <span className="font-medium text-slate-900">Financeiro</span>
              <p className="mt-1 leading-relaxed">
                {financial?.public_interpretation?.headline || 'Sem leitura financeira pública.'}
              </p>
              <div className="mt-1 text-slate-500">
                Competência: {formatPeriod(financial?.reference_period)} · {confidenceLabel(financial?.evidence_confidence)}
              </div>
            </div>

            <div className="rounded-lg border border-slate-200 p-3">
              <span className="font-medium text-slate-900">Conduta</span>
              <p className="mt-1 leading-relaxed">{conduct?.summary || 'Sem resumo público.'}</p>
              {conduct?.reference_window ? (
                <div className="mt-1 text-slate-500">
                  Janela: {conduct.reference_window.start_month} a {conduct.reference_window.end_month}
                </div>
              ) : null}
            </div>
          </div>
        </>
      ) : (
        <div className="mt-4 rounded-xl border border-slate-200 bg-slate-50 p-3 text-sm text-slate-700">
          Este resultado é pesquisável, mas não pertence necessariamente ao universo ordinário de comparação.
        </div>
      )}

      <div className="mt-auto flex flex-wrap items-center gap-2 pt-5">
        <button
          type="button"
          onClick={() => onOpen?.(entry)}
          className="rounded-lg bg-slate-900 px-3 py-2 text-xs font-semibold text-white hover:bg-slate-800"
        >
          Ver perfil
        </button>

        {isOrdinary && onToggleCompare ? (
          <button
            type="button"
            disabled={!compareSelected && compareDisabled}
            onClick={() => onToggleCompare(explorer.entity_id)}
            className={`inline-flex items-center gap-1 rounded-lg border px-3 py-2 text-xs font-semibold transition ${
              compareSelected
                ? 'border-sky-300 bg-sky-50 text-sky-800'
                : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-40'
            }`}
          >
            <GitCompareArrows className="h-3.5 w-3.5" />
            {compareSelected ? 'Remover da comparação' : 'Comparar'}
          </button>
        ) : null}
      </div>
    </article>
  );
}
