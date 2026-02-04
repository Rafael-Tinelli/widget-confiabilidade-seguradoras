import { ShieldCheck, Award, BadgeCheck } from 'lucide-react';

function clampPct(n) {
  const x = Number(n);
  if (!Number.isFinite(x)) return 0;
  return Math.max(0, Math.min(100, x));
}

function safeNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export default function InsurerCard({ insurer, onOpenScoreModal }) {
  const name = insurer?.name || '—';
  const id = insurer?.id || '';
  const cnpj = insurer?.cnpj || insurer?.cnpjKey || null;

  const data = insurer?.data || {};
  const flags = insurer?.flags || {};

  // Nota final (0–100)
  const score = safeNumber(data.score ?? data.final_score ?? data.financial_score, 0);

  // 3 pilares (compatível com snapshots antigos e novos)
  const solvencyScore = safeNumber(
    data.solvencyScore ?? data.financialScore ?? data.financial_score,
    0
  );
  const reputationScore = safeNumber(data.reputationScore ?? data.reputation_score, 0);
  const innovationScore = safeNumber(data.innovationScore, 0);

  // Disponibilidade de reputação (Consumidor.gov) — compatível com snapshots antigos e novos
  const rep =
    data?.components?.reputation ??
    insurer?.components?.reputation ??
    data?.componentsDetail?.reputation ??
    null;

  const hasReputation = (() => {
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

    const hasAnyValue = Object.values(rep).some((v) => v !== undefined && v !== null);
    return hasAnyValue;
  })();

  const openInsurance = Boolean(
    data.openInsuranceParticipant === true ||
      data.open_insurance === true ||
      flags.openInsuranceParticipant === true ||
      flags.open_insurance_participant === true ||
      flags.opinParticipant === true ||
      flags.opin === true ||
      innovationScore >= 80
  );

  const handleOpen = () => onOpenScoreModal?.(insurer);
  const handleKeyDown = (e) => {
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault();
      handleOpen();
    }
  };

  return (
    <div
      role="button"
      tabIndex={0}
      onClick={handleOpen}
      onKeyDown={handleKeyDown}
      aria-label={`Ver detalhes da nota de ${name}`}
      className="group relative cursor-pointer rounded-2xl bg-white p-6 shadow-sm ring-1 ring-black/5 transition hover:-translate-y-0.5 hover:shadow-md focus:outline-none focus:ring-2 focus:ring-slate-300"
    >
      {/* Header */}
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-base font-semibold leading-tight text-slate-900">
            {name}
          </h3>
          <p className="mt-1 text-xs text-slate-500">
            SUSEP: <span className="font-mono">{id || '—'}</span>
            {cnpj ? (
              <>
                {' '}• CNPJ: <span className="font-mono">{cnpj}</span>
              </>
            ) : null}
          </p>
        </div>

        {/* Score */}
        <div className="flex items-center gap-2 rounded-xl bg-slate-50 px-3 py-2 ring-1 ring-black/5">
          <ShieldCheck className="h-5 w-5 text-slate-700" />
          <div className="text-right">
            <div className="text-xs font-medium text-slate-500">Nota</div>
            <div className="text-lg font-semibold text-slate-900">
              {score.toFixed(0)}
            </div>
          </div>
        </div>
      </div>

      {/* Pilares */}
      <div className="mt-4 grid grid-cols-3 gap-3">
        <div>
          <div className="flex items-center justify-between text-xs text-slate-600">
            <span>Solvência</span>
            <span className="font-semibold text-slate-900">{solvencyScore.toFixed(0)}</span>
          </div>
          <div className="mt-1 h-2 w-full rounded-full bg-slate-100">
            <div
              className="h-2 rounded-full bg-slate-900/70"
              style={{ width: `${clampPct(solvencyScore)}%` }}
            />
          </div>
        </div>

        <div>
          <div className="flex items-center justify-between text-xs text-slate-600">
            <span>Reputação</span>
            <span className="font-semibold text-slate-900">
              {hasReputation ? reputationScore.toFixed(0) : '—'}
            </span>
          </div>
          <div className="mt-1 h-2 w-full rounded-full bg-slate-100">
            <div
              className="h-2 rounded-full bg-slate-900/50"
              style={{ width: `${clampPct(hasReputation ? reputationScore : 0)}%` }}
            />
          </div>
          {!hasReputation ? (
            <div className="mt-1 text-[10px] text-slate-500">Sem dados do Consumidor.gov</div>
          ) : null}
        </div>

        <div>
          <div className="flex items-center justify-between text-xs text-slate-600">
            <span>Open Insurance</span>
            <span className="font-semibold text-slate-900">{innovationScore.toFixed(0)}</span>
          </div>
          <div className="mt-1 h-2 w-full rounded-full bg-slate-100">
            <div
              className="h-2 rounded-full bg-slate-900/40"
              style={{ width: `${clampPct(innovationScore)}%` }}
            />
          </div>
        </div>
      </div>

      {/* Badges e dica */}
      <div className="mt-4 flex flex-wrap items-center gap-2">
        {openInsurance ? (
          <span className="inline-flex items-center gap-1 rounded-full bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-800 ring-1 ring-emerald-200">
            <BadgeCheck className="h-4 w-4" />
            Participante OPIN
          </span>
        ) : (
          <span className="inline-flex items-center gap-1 rounded-full bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700 ring-1 ring-slate-200">
            <Award className="h-4 w-4" />
            Sem sinal OPIN
          </span>
        )}

        <span className="text-xs text-slate-500">Clique para ver a metodologia</span>
      </div>
    </div>
  );
}
