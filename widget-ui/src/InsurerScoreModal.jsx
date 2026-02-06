import { useEffect, useMemo } from 'react';
import { X, Info, Calculator, Database, ShieldCheck } from 'lucide-react';

function safeNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function round2(n) {
  const x = safeNumber(n, 0);
  return Math.round(x * 100) / 100;
}

function fmtPct(n) {
  const x = safeNumber(n, null);
  if (x === null) return '—';
  return `${round2(x)}%`;
}

function fmtNum(n) {
  const x = safeNumber(n, null);
  if (x === null) return '—';
  return round2(x).toLocaleString('pt-BR');
}

function pick(obj, ...keys) {
  if (!obj || typeof obj !== 'object' || Array.isArray(obj)) return null;
  for (const k of keys) {
    const v = obj[k];
    if (v !== undefined && v !== null) return v;
  }
  return null;
}

function hasReputationData(rep) {
  if (!rep || typeof rep !== 'object' || Array.isArray(rep)) return false;
  const knownKeys = [
    'complaintsCount',
    'respondedCount',
    'resolvedCount',
    'finalizedCount',
    'scoreSum',
    'satisfactionCount',
    'averageScore',
    'total_claims',
    'responded_claims',
    'resolved_claims',
    'finalized_claims',
    'complaintsPerPremium',
    'complaints_per_premium',
    'satScore',
    'resolutionRate',
    'responseTimeDays',
  ];
  const hasKnownKey = knownKeys.some((k) => Object.prototype.hasOwnProperty.call(rep, k));
  if (!hasKnownKey) return false;
  return Object.values(rep).some((v) => v !== undefined && v !== null);
}

function isPlainObject(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

export default function InsurerScoreModal({ insurer, sources, onClose }) {
  const isOpen = Boolean(insurer);

  useEffect(() => {
    if (!isOpen) return undefined;
    const onKeyDown = (e) => {
      if (e.key === 'Escape') onClose?.();
    };
    // Scroll Lock: impede rolagem do site enquanto modal está aberto
    document.body.style.overflow = 'hidden';
    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('keydown', onKeyDown);
      document.body.style.overflow = '';
    };
  }, [isOpen, onClose]);

  const view = useMemo(() => {
    if (!insurer) return null;

    const name = insurer.name || '—';
    const id = insurer.id || '—';
    const cnpj = insurer.cnpj || insurer.cnpjKey || null;

    const d = insurer.data || {};
    const weights = d.weights || { solvency: 0.4, reputation: 0.45, innovation: 0.15 };

    const solvencyScore = safeNumber(d.solvencyScore ?? d.financialScore ?? d.financial_score, 0) || 0;
    const reputationScore = safeNumber(d.reputationScore ?? d.reputation_score, 0) || 0;
    const innovationScore = safeNumber(d.innovationScore, 0) || 0;

    // Reputação:
    // - `d.components.reputation` hoje é **numérico** (score) e NÃO contém as estatísticas do Consumidor.gov.
    // - As estatísticas/índices vêm em `insurer.reputation.statistics` (ou `insurer.components.reputation.statistics`).
    // - `d.componentsDetail.reputation` pode ter score/índices calculados pelo pipeline.
    const repStats =
      insurer?.reputation?.statistics ??
      insurer?.components?.reputation?.statistics ??
      null;

    const repIndexes =
      insurer?.reputation?.indexes ??
      insurer?.components?.reputation?.indexes ??
      null;

    const repDetail = d?.componentsDetail?.reputation ?? null;

    const repRaw = {
      ...(isPlainObject(repStats) ? repStats : {}),
      ...(isPlainObject(repIndexes) ? repIndexes : {}),
      ...(isPlainObject(repDetail) ? repDetail : {}),
    };

    const hasReputation = hasReputationData(repRaw);
    // Transparência: qual registro do Consumidor.gov foi associado.
    const repEntityName =
      insurer?.reputation?.display_name ??
      insurer?.reputation?.name ??
      insurer?.components?.reputation?.display_name ??
      insurer?.components?.reputation?.name ??
      null;

    const complaintsCount = pick(repRaw, 'complaintsCount', 'total_claims', 'complaints_count');
    const complaintsCountNum = Number(complaintsCount || 0) || 0;
    const isSmallSample = hasReputation && complaintsCountNum > 0 && complaintsCountNum < 15;
    const respondedCount = pick(repRaw, 'respondedCount', 'responded_claims', 'responded_count');
    const resolvedCount = pick(repRaw, 'resolvedCount', 'resolved_claims', 'resolved_count');
    const finalizedCount = pick(repRaw, 'finalizedCount', 'finalized_claims', 'finalized_count');

    const scoreSum = pick(repRaw, 'scoreSum', 'score_sum');
    const satisfactionCount = pick(repRaw, 'satisfactionCount', 'satisfaction_count');
    const averageScore =
      pick(repRaw, 'averageScore', 'avgScore', 'average_score') ??
      (scoreSum !== null && satisfactionCount ? scoreSum / satisfactionCount : null);

    const complaintsPerPremium = pick(repRaw, 'complaintsPerPremium', 'complaints_per_premium');
    const repHasComplaintsPerPremium = complaintsPerPremium !== null;

    const resolutionRate =
      pick(repRaw, 'resolutionRate', 'resolution_rate') ??
      (resolvedCount !== null && complaintsCount ? resolvedCount / complaintsCount : null);

    const responseTimeDays = pick(
      repRaw,
      'responseTimeDays',
      'response_time_days',
      'avgResponseTimeDays',
      'avg_response_time_days'
    );
    const repHasResponseTimeDays = responseTimeDays !== null;

    const repView = {
      ...(isPlainObject(repRaw) ? repRaw : {}),
      complaintsPerPremium: repHasComplaintsPerPremium ? complaintsPerPremium : complaintsCount,
      satScore: pick(repRaw, 'satScore', 'overallSatisfaction') ?? averageScore,
      resolutionRate,
      responseTimeDays: repHasResponseTimeDays ? responseTimeDays : respondedCount,
      reputationStatus: pick(repRaw, 'reputationStatus', 'reputation_status', 'status'),
      _hasComplaintsPerPremium: repHasComplaintsPerPremium,
      _hasResponseTimeDays: repHasResponseTimeDays,
      complaintsCount,
      respondedCount,
      resolvedCount,
      finalizedCount,
      averageScore,
    };

    // Nota final = soma ponderada.
    // Se não existe reputação (sem match no Consumidor.gov), a contribuição do pilar vira 0 (peso não é redistribuído).
    const contribSolvency = (weights.solvency || 0) * solvencyScore;
    const contribReputation = hasReputation ? (weights.reputation || 0) * reputationScore : 0;
    const contribInnovation = (weights.innovation || 0) * innovationScore;

    const computed = round2(contribSolvency + contribReputation + contribInnovation);
    const score = safeNumber(d.score, computed) ?? computed;

    const solv = d.componentsDetail?.solvency || {};
    const rep = repView || {};
    const inn = d.componentsDetail?.innovation || {};

    const srcSes = sources?.ses || null;
    const srcCg = sources?.consumidorGov || null;
    const srcOi = sources?.openInsurance || null;

    return {
      name,
      id,
      cnpj,
      score: round2(score),
      weights,
      hasReputation,
      repEntityName,
      isSmallSample,
      complaintsCountNum,
      solvencyScore: round2(solvencyScore),
      reputationScore: round2(reputationScore),
      innovationScore: round2(innovationScore),
      contribSolvency: round2(contribSolvency),
      contribReputation: round2(contribReputation),
      contribInnovation: round2(contribInnovation),
      computed,
      solv,
      rep,
      inn,
      srcSes,
      srcCg,
      srcOi,
      raw: insurer,
    };
  }, [insurer, sources]);

  if (!isOpen) return null;

  return (
    <div
      className="fixed inset-0 z-[99999] flex items-center justify-center bg-black/60 p-4 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-label="Detalhes da nota"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose?.();
      }}
    >
      {/* Ajuste Mobile: max-h-[85dvh] usa a altura dinâmica da viewport (desconta barra do navegador) */}
      <div className="w-full max-w-3xl rounded-2xl bg-white shadow-xl ring-1 ring-black/10 flex flex-col max-h-[85dvh]">
        {/* Header */}
        <div className="flex items-start justify-between gap-3 border-b border-slate-100 p-5">
          <div>
            <div className="flex items-center gap-2">
              <ShieldCheck className="h-5 w-5 text-slate-700" />
              <h2 className="text-base font-semibold text-slate-900">Como a nota é calculada</h2>
            </div>

            <p className="mt-2 text-sm text-slate-700">
              <span className="font-semibold">{view?.name}</span>
              <span className="text-slate-500"> • SUSEP: </span>
              <span className="font-mono text-slate-700">{view?.id}</span>
              {view?.cnpj ? (
                <>
                  <span className="text-slate-500"> • CNPJ: </span>
                  <span className="font-mono text-slate-700">{view?.cnpj}</span>
                </>
              ) : null}
            </p>
          </div>

          <button
            type="button"
            className="rounded-xl p-2 text-slate-600 hover:bg-slate-100"
            aria-label="Fechar"
            onClick={() => onClose?.()}
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        {/* Body */}
        <div className="max-h-[70vh] overflow-y-auto p-5">
          {/* Resumo matemático */}
          <div className="rounded-2xl bg-slate-50 p-4 ring-1 ring-slate-200">
            <div className="flex items-center gap-2">
              <Calculator className="h-4 w-4 text-slate-700" />
              <h3 className="text-sm font-semibold text-slate-900">Fórmula da nota final</h3>
            </div>

            <p className="mt-2 text-sm text-slate-700">
              A nota final é a soma dos 3 pilares ponderados por peso.
              Quando <strong>não existe dado de reputação</strong> (sem match no Consumidor.gov), a contribuição desse pilar vira <strong>0</strong> — o peso não é redistribuído.
            </p>

            <div className="mt-3 grid gap-3 sm:grid-cols-4">
              <div className="rounded-xl bg-white p-3 ring-1 ring-slate-200">
                <div className="text-xs text-slate-500">Nota final</div>
                <div className="mt-1 text-2xl font-semibold text-slate-900">{view?.score?.toFixed(0)}</div>
                <div className="mt-1 text-[11px] text-slate-500">Calculado: {view?.computed}</div>
              </div>

              <div className="rounded-xl bg-white p-3 ring-1 ring-slate-200">
                <div className="text-xs text-slate-500">Solvência</div>
                <div className="mt-1 text-sm font-semibold text-slate-900">
                  {view?.solvencyScore} × {fmtPct((view?.weights?.solvency || 0) * 100)}
                </div>
                <div className="mt-1 text-xs text-slate-600">Contribuição: {view?.contribSolvency}</div>
              </div>

              <div className="rounded-xl bg-white p-3 ring-1 ring-slate-200">
                <div className="text-xs text-slate-500">Reputação</div>
                <div className="mt-1 text-sm font-semibold text-slate-900">
                  {view?.hasReputation ? view?.reputationScore : '—'} × {fmtPct((view?.weights?.reputation || 0) * 100)}
                </div>
                <div className="mt-1 text-xs text-slate-600">
                  Contribuição: {view?.hasReputation ? view?.contribReputation : 0}
                </div>
                {!view?.hasReputation ? (
                  <div className="mt-1 text-[11px] text-amber-700">Sem dados Consumidor.gov: contribuição zerada.</div>
                ) : null}
              </div>

              <div className="rounded-xl bg-white p-3 ring-1 ring-slate-200">
                <div className="text-xs text-slate-500">Open Insurance</div>
                <div className="mt-1 text-sm font-semibold text-slate-900">
                  {view?.innovationScore} × {fmtPct((view?.weights?.innovation || 0) * 100)}
                </div>
                <div className="mt-1 text-xs text-slate-600">Contribuição: {view?.contribInnovation}</div>
              </div>
            </div>
          </div>

          {/* Fontes */}
          <div className="mt-5 grid gap-3 sm:grid-cols-3">
            <div className="rounded-2xl bg-white p-4 ring-1 ring-slate-200">
              <div className="flex items-center gap-2">
                <Database className="h-4 w-4 text-slate-700" />
                <h4 className="text-sm font-semibold text-slate-900">SUSEP (SES)</h4>
              </div>
              <p className="mt-2 text-xs text-slate-600">
                Fonte: BaseCompleta.zip (prêmios, sinistros, indicadores contábeis)
              </p>
              <p className="mt-2 text-xs text-slate-500">
                Atualização do snapshot: {view?.srcSes?.generatedAt ? new Date(view.srcSes.generatedAt).toLocaleString('pt-BR') : '—'}
              </p>
            </div>

            <div className="rounded-2xl bg-white p-4 ring-1 ring-slate-200">
              <div className="flex items-center gap-2">
                <Database className="h-4 w-4 text-slate-700" />
                <h4 className="text-sm font-semibold text-slate-900">Consumidor.gov</h4>
              </div>
              <p className="mt-2 text-xs text-slate-600">
                Fonte: Dados Abertos (reclamações e indicadores de atendimento)
              </p>
              <p className="mt-2 text-xs text-slate-500">
                Atualização do snapshot: {view?.srcCg?.generatedAt ? new Date(view.srcCg.generatedAt).toLocaleString('pt-BR') : '—'}
              </p>
            </div>

            <div className="rounded-2xl bg-white p-4 ring-1 ring-slate-200">
              <div className="flex items-center gap-2">
                <Database className="h-4 w-4 text-slate-700" />
                <h4 className="text-sm font-semibold text-slate-900">Open Insurance</h4>
              </div>
              <p className="mt-2 text-xs text-slate-600">
                Fonte: participantes / dados públicos do ecossistema
              </p>
              <p className="mt-2 text-xs text-slate-500">
                Atualização do snapshot: {view?.srcOi?.generatedAt ? new Date(view.srcOi.generatedAt).toLocaleString('pt-BR') : '—'}
              </p>
            </div>
          </div>

          {/* Detalhes por pilar */}
          <div className="mt-5 grid gap-4">
            <section className="rounded-2xl bg-white p-4 ring-1 ring-slate-200">
              <div className="flex items-center gap-2">
                <Info className="h-4 w-4 text-slate-700" />
                <h3 className="text-sm font-semibold text-slate-900">Pilar 1 — Solvência (SES/SUSEP)</h3>
              </div>

              <div className="mt-3 grid gap-3 sm:grid-cols-3">
                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Prêmios (média 5 anos)</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">
                    {fmtNum(view?.solv?.avgPrem5y ?? view?.solv?.premiums ?? view?.raw?.components?.financials?.premiums)}
                  </div>
                </div>
                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Sinistros (média 5 anos)</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">
                    {fmtNum(view?.solv?.avgClaims5y ?? view?.solv?.claims ?? view?.raw?.components?.financials?.claims)}
                  </div>
                </div>
                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Loss ratio</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">
                    {view?.solv?.lossRatio === null || view?.solv?.lossRatio === undefined ? '—' : fmtPct(view?.solv?.lossRatio * 100)}
                  </div>
                  <div className="mt-1 text-[11px] text-slate-500">Fórmula: sinistros ÷ prêmios (mesma janela)</div>
                </div>
              </div>

              <div className="mt-3 grid gap-3 sm:grid-cols-2">
                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Net worth ratio</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">
                    {view?.solv?.netWorthRatio === null || view?.solv?.netWorthRatio === undefined ? '—' : fmtPct(view?.solv?.netWorthRatio * 100)}
                  </div>
                  <div className="mt-1 text-[11px] text-slate-500">Indicador do pipeline (razão patrimonial).</div>
                </div>

                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Status</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">{view?.solv?.lossRatioStatus || '—'}</div>
                  <div className="mt-1 text-[11px] text-slate-500">Ajuda a entender quando há dados insuficientes/inválidos.</div>
                </div>
              </div>
            </section>

            <section className="rounded-2xl bg-white p-4 ring-1 ring-slate-200">
              <div className="flex items-center gap-2">
                <Info className="h-4 w-4 text-slate-700" />
                <h3 className="text-sm font-semibold text-slate-900">Pilar 2 — Reputação (Consumidor.gov)</h3>
              </div>
              
              {view?.repEntityName ? (
                <div className="mt-1 text-xs text-slate-500">
                  Match no Consumidor.gov:{' '}
                  <span className="font-medium text-slate-700">{view.repEntityName}</span>
                </div>
              ) : null}

              {view?.hasReputation && view?.isSmallSample ? (
                <div className="mt-3 rounded-xl bg-amber-50 p-3 text-sm text-amber-900 ring-1 ring-amber-200">
                  Amostra pequena ({view.complaintsCountNum} reclamações). O score deste pilar é suavizado para evitar distorções.
                </div>
              ) : null}

              {!view?.hasReputation ? (
                <div className="mt-3 rounded-xl bg-amber-50 p-3 text-sm text-amber-900 ring-1 ring-amber-200">
                  Sem dados associados a esta empresa no Consumidor.gov (matching por nome/CNPJ não encontrado). Por isso, a contribuição deste pilar na nota final é <strong>0</strong>.
                </div>
              ) : (
                <div className="mt-3 grid gap-3 sm:grid-cols-4">
                  <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                    <div className="text-xs text-slate-500">{view?.rep?._hasComplaintsPerPremium ? 'Índice' : 'Reclamações'}</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">{fmtNum(view?.rep?.complaintsPerPremium)}</div>
                    <div className="mt-1 text-[11px] text-slate-500">{view?.rep?._hasComplaintsPerPremium ? 'Reclamações por prêmio' : 'Reclamações (total)'}</div>
                  </div>
                  <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                    <div className="text-xs text-slate-500">Satisfação</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">{fmtNum(view?.rep?.satScore)}</div>
                  </div>
                  <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                    <div className="text-xs text-slate-500">Resolvida</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">
                      {view?.rep?.resolutionRate === null || view?.rep?.resolutionRate === undefined ? '—' : fmtPct(view?.rep?.resolutionRate * 100)}
                    </div>
                  </div>
                  <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                    <div className="text-xs text-slate-500">{view?.rep?._hasResponseTimeDays ? 'Tempo (dias)' : 'Respondidas'}</div>
                    <div className="mt-1 text-sm font-semibold text-slate-900">{fmtNum(view?.rep?.responseTimeDays)}</div>
                  </div>
                </div>
              )}

              <div className="mt-3 text-xs text-slate-500">
                Status: <span className="font-mono text-slate-700">{view?.rep?.reputationStatus || '—'}</span>
              </div>
            </section>

            <section className="rounded-2xl bg-white p-4 ring-1 ring-slate-200">
              <div className="flex items-center gap-2">
                <Info className="h-4 w-4 text-slate-700" />
                <h3 className="text-sm font-semibold text-slate-900">Pilar 3 — Open Insurance</h3>
              </div>

              <div className="mt-3 grid gap-3 sm:grid-cols-3">
                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Participação</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">
                    {view?.inn?.openInsurance === true ? 'Participa' : 'Sem indicação'}
                  </div>
                </div>
                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Score do pilar</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">{view?.innovationScore}</div>
                </div>
                <div className="rounded-xl bg-slate-50 p-3 ring-1 ring-slate-200">
                  <div className="text-xs text-slate-500">Status</div>
                  <div className="mt-1 text-sm font-semibold text-slate-900">{view?.inn?.participantsStatus || '—'}</div>
                </div>
              </div>

              <p className="mt-3 text-xs text-slate-500">
                Regra atual do widget: empresas participantes recebem score superior (ex.: 80) neste pilar.
              </p>
            </section>

            {/* Debug opcional */}
            <details className="rounded-2xl bg-white p-4 ring-1 ring-slate-200">
              <summary className="cursor-pointer text-sm font-semibold text-slate-900">Dados brutos (debug)</summary>
              <pre className="mt-3 max-h-72 overflow-auto rounded-xl bg-slate-50 p-3 text-xs text-slate-800 ring-1 ring-slate-200">
{JSON.stringify(view?.raw, null, 2)}
              </pre>
            </details>
          </div>
        </div>

        {/* Footer */}
        <div className="flex items-center justify-center md:justify-end gap-2 border-t border-slate-100 p-4 bg-slate-50 rounded-b-2xl">
          <button
            type="button"
            className="w-full md:w-auto rounded-xl bg-slate-900 px-6 py-3 text-sm font-bold text-white hover:bg-slate-800 shadow-lg"
            onClick={() => onClose?.()}
          >
            Fechar
          </button>
        </div>
      </div>
    </div>
  );
}
