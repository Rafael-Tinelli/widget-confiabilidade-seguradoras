import { useMemo, useState } from 'react';
import { ListFilter } from 'lucide-react';
import { loadCollection, loadLeaderboard } from './v2Data';

function formatMetric(entry, metric, unit) {
  const fieldByMetric = {
    insurance_premium_direct_12m: 'premium_direct_12m',
    pla_cmr_ratio: 'pla_cmr_ratio',
    ilt: 'ilt',
    conduct_observed_expected_ratio: 'conduct_pressure_ratio',
  };
  const value = entry?.[fieldByMetric[metric] || metric];
  if (typeof value !== 'number' || !Number.isFinite(value)) return '—';
  if (unit === 'BRL') {
    return value.toLocaleString('pt-BR', {
      style: 'currency',
      currency: 'BRL',
      maximumFractionDigits: 0,
    });
  }
  return value.toLocaleString('pt-BR', { maximumFractionDigits: 3 });
}

function publicClassLabel(value) {
  const labels = {
    favorable_reading: 'Leitura favorável no escopo avaliado',
    attention: 'Atenção',
    prudential_warning: 'Alerta prudencial',
    evidence_incomplete: 'Evidência incompleta',
  };
  return labels[value] || 'Ver perfil';
}

export default function ExplorePanel({ exploreIndex, onOpenEntity }) {
  const options = useMemo(() => [
    ...(exploreIndex?.leaderboards || []).map((item) => ({ ...item, kind: 'leaderboard' })),
    ...(exploreIndex?.collections || []).map((item) => ({ ...item, kind: 'collection' })),
  ], [exploreIndex]);

  const [selectedKey, setSelectedKey] = useState('');
  const [payload, setPayload] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const selected = options.find((item) => `${item.kind}:${item.id}` === selectedKey) || null;

  const loadSelected = async (event) => {
    const value = event.target.value;
    setSelectedKey(value);
    setPayload(null);
    setError('');
    if (!value) return;
    const [kind, id] = value.split(':', 2);
    setLoading(true);
    try {
      const next = kind === 'leaderboard' ? await loadLeaderboard(id) : await loadCollection(id);
      setPayload(next);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="flex items-center gap-2">
        <ListFilter className="h-4 w-4 text-slate-600" />
        <h2 className="text-base font-semibold text-slate-900">Explorar por critério</h2>
      </div>
      <p className="mt-1 text-sm text-slate-600">
        As listas abaixo respondem perguntas específicas. Nenhuma delas é um ranking geral de seguradoras.
      </p>

      <select
        value={selectedKey}
        onChange={loadSelected}
        className="mt-4 w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-sky-300"
      >
        <option value="">Escolha uma exploração</option>
        {exploreIndex?.leaderboards?.length ? <optgroup label="Listas por métrica">
          {exploreIndex.leaderboards.map((item) => (
            <option key={`leaderboard:${item.id}`} value={`leaderboard:${item.id}`}>{item.title}</option>
          ))}
        </optgroup> : null}
        {exploreIndex?.collections?.length ? <optgroup label="Coleções sem ordem">
          {exploreIndex.collections.map((item) => (
            <option key={`collection:${item.id}`} value={`collection:${item.id}`}>{item.title}</option>
          ))}
        </optgroup> : null}
      </select>

      {loading ? <p className="mt-4 text-sm text-slate-500">Carregando…</p> : null}
      {error ? <p className="mt-4 text-sm text-red-700">{error}</p> : null}

      {payload ? (
        <div className="mt-5">
          <h3 className="text-sm font-semibold text-slate-900">{payload.title}</h3>
          {payload.question ? <p className="mt-1 text-sm text-slate-600">{payload.question}</p> : null}
          {payload.ordered === false ? (
            <p className="mt-2 text-xs font-medium text-slate-500">Coleção não ordenada.</p>
          ) : null}

          <div className="mt-3 divide-y divide-slate-100 rounded-xl border border-slate-200">
            {(payload.entries || []).map((entry, index) => (
              <button
                type="button"
                key={`${entry.entity_id}-${index}`}
                onClick={() => onOpenEntity?.(entry.entity_id)}
                className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left hover:bg-slate-50"
              >
                <div className="min-w-0">
                  <div className="text-sm font-medium text-slate-900">
                    {entry.leaderboard_rank ? `${entry.leaderboard_rank}º · ` : ''}
                    {entry.display_name || entry.legal_name || entry.entity_id}
                  </div>
                  <div className="mt-0.5 text-xs text-slate-500">
                    {publicClassLabel(entry.public_class)}
                  </div>
                </div>
                {payload.type === 'public_numeric_leaderboard' ? (
                  <div className="shrink-0 text-sm font-semibold text-slate-700">
                    {formatMetric(entry, payload.metric, payload.unit)}
                  </div>
                ) : null}
              </button>
            ))}
          </div>

          {payload.caveats?.length ? (
            <ul className="mt-3 list-disc space-y-1 pl-5 text-xs text-slate-500">
              {payload.caveats.map((item) => <li key={item}>{item}</li>)}
            </ul>
          ) : null}
        </div>
      ) : selected ? (
        <p className="mt-4 text-xs text-slate-500">Selecione novamente se a exploração não carregar.</p>
      ) : null}
    </section>
  );
}
