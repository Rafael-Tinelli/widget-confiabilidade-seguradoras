import { GitCompareArrows, X } from 'lucide-react';

function formatRatio(value) {
  return typeof value === 'number' && Number.isFinite(value)
    ? value.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : '—';
}

function formatPeriod(value) {
  const text = String(value || '').replace(/\D/g, '');
  return text.length === 6 ? `${text.slice(4, 6)}/${text.slice(0, 4)}` : value || '—';
}

function confidenceLabel(value) {
  const labels = {
    established_core_history: 'Histórico central estabelecido',
    limited_core_history: 'Histórico central limitado',
    insufficient_core_evidence: 'Evidência central insuficiente',
  };
  return labels[value] || 'Confiança não classificada';
}

export default function ComparisonPanel({ entities, onRemove, onOpenEntity }) {
  if (!entities?.length) return null;

  return (
    <section className="rounded-2xl border border-sky-200 bg-sky-50/40 p-5">
      <div className="flex items-center gap-2">
        <GitCompareArrows className="h-4 w-4 text-sky-700" />
        <h2 className="text-base font-semibold text-slate-900">Comparação lado a lado</h2>
      </div>
      <p className="mt-1 text-xs text-slate-600">
        A comparação preserva a linguagem e as métricas já calculadas pelo backend. Não produz vencedor nem nota composta.
      </p>

      <div className="mt-4 overflow-x-auto">
        <table className="min-w-[720px] w-full border-collapse text-sm">
          <thead>
            <tr>
              <th className="w-40 border-b border-slate-200 p-2 text-left text-xs font-medium text-slate-500">Critério</th>
              {entities.map((entity) => (
                <th key={entity.entity_id} className="border-b border-slate-200 p-2 text-left align-top">
                  <div className="flex items-start justify-between gap-2">
                    <button type="button" onClick={() => onOpenEntity?.(entity.entity_id)} className="font-semibold text-sky-800 hover:underline">
                      {entity.display_name || entity.legal_name}
                    </button>
                    <button type="button" onClick={() => onRemove?.(entity.entity_id)} aria-label="Remover da comparação" className="rounded p-1 text-slate-400 hover:bg-white hover:text-slate-700">
                      <X className="h-3.5 w-3.5" />
                    </button>
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">Leitura conjunta</td>
              {entities.map((entity) => <td key={entity.entity_id} className="border-b border-slate-200 p-2 align-top">{entity.assessment?.title || '—'}</td>)}
            </tr>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">Financeiro</td>
              {entities.map((entity) => <td key={entity.entity_id} className="border-b border-slate-200 p-2 align-top">{entity.financial?.public_interpretation?.headline || 'Sem leitura financeira pública.'}</td>)}
            </tr>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">PLA/CMR</td>
              {entities.map((entity) => <td key={entity.entity_id} className="border-b border-slate-200 p-2">{formatRatio(entity.financial?.capital?.pla_cmr_ratio)}</td>)}
            </tr>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">ILT</td>
              {entities.map((entity) => <td key={entity.entity_id} className="border-b border-slate-200 p-2">{formatRatio(entity.financial?.liquidity?.value)}</td>)}
            </tr>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">Período financeiro</td>
              {entities.map((entity) => <td key={entity.entity_id} className="border-b border-slate-200 p-2">{formatPeriod(entity.financial?.reference_period)}</td>)}
            </tr>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">Confiança financeira</td>
              {entities.map((entity) => <td key={entity.entity_id} className="border-b border-slate-200 p-2">{confidenceLabel(entity.financial?.evidence_confidence)}</td>)}
            </tr>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">Conduta</td>
              {entities.map((entity) => <td key={entity.entity_id} className="border-b border-slate-200 p-2 align-top">{entity.conduct?.summary || 'Sem leitura pública de Conduta.'}</td>)}
            </tr>
            <tr>
              <td className="border-b border-slate-200 p-2 font-medium text-slate-600">Janela de Conduta</td>
              {entities.map((entity) => (
                <td key={entity.entity_id} className="border-b border-slate-200 p-2">
                  {entity.conduct?.reference_window
                    ? `${entity.conduct.reference_window.start_month} a ${entity.conduct.reference_window.end_month}`
                    : '—'}
                </td>
              ))}
            </tr>
            <tr>
              <td className="p-2 font-medium text-slate-600">Razão reclamações</td>
              {entities.map((entity) => <td key={entity.entity_id} className="p-2">{formatRatio(entity.conduct?.pressure_ratio)}<div className="text-xs text-slate-500">{entity.conduct?.comparable_months ?? '—'} meses comparáveis</div></td>)}
            </tr>
          </tbody>
        </table>
      </div>
    </section>
  );
}
