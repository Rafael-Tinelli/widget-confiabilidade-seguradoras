import { useEffect } from 'react';
import { X, ExternalLink, ShieldCheck } from 'lucide-react';

function isFiniteNumber(value) {
  return typeof value === 'number' && Number.isFinite(value);
}

function formatNumber(value, options = {}) {
  if (!isFiniteNumber(value)) return '—';
  return value.toLocaleString('pt-BR', options);
}

function formatRatio(value) {
  return isFiniteNumber(value)
    ? value.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : '—';
}

function formatPercent(value) {
  if (!isFiniteNumber(value)) return '—';
  return `${(value * 100).toLocaleString('pt-BR', {
    minimumFractionDigits: 1,
    maximumFractionDigits: 1,
  })}%`;
}

function formatMoney(value) {
  if (!isFiniteNumber(value)) return '—';
  return value.toLocaleString('pt-BR', {
    style: 'currency',
    currency: 'BRL',
    maximumFractionDigits: 0,
  });
}

function formatPeriod(value) {
  const text = String(value || '').replace(/\D/g, '');
  if (text.length !== 6) return value || '—';
  return `${text.slice(4, 6)}/${text.slice(0, 4)}`;
}

function humanize(value) {
  return String(value || '—').replaceAll('_', ' ');
}

function Metric({ label, metric, formatter = formatNumber }) {
  if (!metric || typeof metric !== 'object') return null;
  const available = metric.availability === 'available' && metric.value !== null;
  return (
    <div className="rounded-lg border border-slate-200 bg-slate-50 p-3">
      <div className="text-xs font-medium text-slate-500">{label}</div>
      <div className="mt-1 text-base font-semibold text-slate-900">
        {available ? formatter(metric.value) : '—'}
      </div>
      {metric.meaning ? <p className="mt-1 text-xs text-slate-600">{metric.meaning}</p> : null}
      {metric.public_use && metric.public_use !== 'displayable' ? (
        <p className="mt-1 text-[11px] font-medium text-amber-700">
          Uso público: {humanize(metric.public_use)}
        </p>
      ) : null}
    </div>
  );
}

function Evidence({ evidence }) {
  if (!evidence || typeof evidence !== 'object') return null;
  return (
    <div className="mt-2 text-xs text-slate-500">
      {evidence.authority ? <span>Fonte: {evidence.authority}</span> : null}
      {evidence.reference ? <span> · {evidence.reference}</span> : null}
      {evidence.url ? (
        <a
          href={evidence.url}
          target="_blank"
          rel="noreferrer"
          className="ml-2 inline-flex items-center gap-1 text-sky-700 hover:underline"
        >
          abrir fonte <ExternalLink className="h-3 w-3" />
        </a>
      ) : null}
    </div>
  );
}

function ProfileLink({ profileId, children, onNavigateProfile }) {
  if (!profileId || !onNavigateProfile) return <span>{children}</span>;
  return (
    <button
      type="button"
      onClick={() => onNavigateProfile(profileId)}
      className="text-left font-medium text-sky-700 hover:underline"
    >
      {children}
    </button>
  );
}

function Section({ title, children }) {
  return (
    <section className="border-t border-slate-200 pt-5 first:border-t-0 first:pt-0">
      <h3 className="text-sm font-semibold text-slate-900">{title}</h3>
      <div className="mt-3">{children}</div>
    </section>
  );
}

function Assessment({ assessment }) {
  if (!assessment) return null;
  if (assessment.availability === 'not_applicable') {
    return (
      <Section title="Avaliação">
        <div className="rounded-lg bg-slate-50 p-4 text-sm text-slate-700">
          Esta identidade não recebe avaliação conjunta no universo ordinário atual.
          {assessment.reason ? (
            <div className="mt-2 text-xs text-slate-500">Motivo: {humanize(assessment.reason)}</div>
          ) : null}
        </div>
      </Section>
    );
  }

  const financial = assessment.financial || {};
  const conduct = assessment.conduct || {};
  const capitalMetric = financial.capital?.technical?.ratio;
  const liquidityMetric = financial.liquidity?.technical?.ratio;
  const operation = assessment.operation_context?.insurance_premium_direct_12m;
  const conductTechnical = conduct.technical || {};

  return (
    <>
      <Section title="Leitura conjunta">
        <div className="rounded-xl border border-slate-200 p-4">
          <div className="text-base font-semibold text-slate-900">
            {assessment.headline || 'Leitura disponível'}
          </div>
          {assessment.summary ? <p className="mt-2 text-sm text-slate-700">{assessment.summary}</p> : null}
          {assessment.why_it_matters ? (
            <p className="mt-2 text-sm text-slate-600">{assessment.why_it_matters}</p>
          ) : null}
          {assessment.mandatory_limit ? (
            <p className="mt-3 rounded-lg bg-slate-50 p-3 text-xs text-slate-600">
              {assessment.mandatory_limit}
            </p>
          ) : null}
        </div>
      </Section>

      <Section title="Financeiro">
        <div className="mb-3 text-xs text-slate-500">
          Competência de referência: {formatPeriod(financial.reference_period)} · confiança:{' '}
          {humanize(financial.evidence_confidence)}
        </div>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <Metric label="PLA/CMR" metric={capitalMetric} formatter={formatRatio} />
          <Metric label="ILT" metric={liquidityMetric} formatter={formatRatio} />
        </div>
        {financial.capital?.plain_language ? (
          <p className="mt-3 text-sm text-slate-700">{financial.capital.plain_language}</p>
        ) : null}
        {financial.liquidity?.plain_language ? (
          <p className="mt-2 text-sm text-slate-700">{financial.liquidity.plain_language}</p>
        ) : null}
        {financial.operating_context?.signal ? (
          <p className="mt-2 text-xs text-slate-500">
            Contexto operacional: {humanize(financial.operating_context.signal)}. Esse contexto não
            substitui capital ou liquidez.
          </p>
        ) : null}
        {operation ? (
          <div className="mt-3">
            <Metric label="Prêmio direto em 12 meses" metric={operation} formatter={formatMoney} />
          </div>
        ) : null}
      </Section>

      <Section title="Conduta">
        <p className="text-sm text-slate-700">{conduct.plain_language || 'Sem leitura pública disponível.'}</p>
        <div className="mt-2 text-xs text-slate-500">
          Comparabilidade: {humanize(conduct.comparability_state)}
          {conduct.reason_code ? ` · ${humanize(conduct.reason_code)}` : ''}
        </div>
        {conduct.reference_window ? (
          <div className="mt-1 text-xs text-slate-500">
            Janela: {conduct.reference_window.start_month || '—'} a{' '}
            {conduct.reference_window.end_month || '—'}
          </div>
        ) : null}
        <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
          <Metric
            label="Reclamações observadas"
            metric={conductTechnical.observed_complaints_12m}
            formatter={(value) => formatNumber(value, { maximumFractionDigits: 0 })}
          />
          <Metric
            label="Reclamações esperadas"
            metric={conductTechnical.expected_complaints_12m}
            formatter={(value) => formatNumber(value, { maximumFractionDigits: 1 })}
          />
          <Metric
            label="Razão observadas/esperadas"
            metric={conductTechnical.observed_expected_ratio}
            formatter={formatRatio}
          />
          <Metric
            label="Meses comparáveis"
            metric={conductTechnical.comparable_months}
            formatter={(value) => formatNumber(value, { maximumFractionDigits: 0 })}
          />
        </div>
        {(conductTechnical.persistence || conductTechnical.trend) ? (
          <p className="mt-3 text-xs text-slate-500">
            Persistência: {humanize(conductTechnical.persistence)} · Tendência:{' '}
            {humanize(conductTechnical.trend)}
          </p>
        ) : null}
      </Section>
    </>
  );
}

function SandboxConduct({ value }) {
  if (!value || value.availability !== 'available') return null;
  const metrics = value.metrics || {};
  const window = value.reference_window;
  return (
    <Section title="Conduta no Sandbox">
      <p className="text-sm text-slate-700">{value.plain_language}</p>
      {window ? (
        <p className="mt-2 text-xs text-slate-500">
          Janela: {window.start_month || '—'} a {window.end_month || '—'}
        </p>
      ) : null}
      <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-2">
        <Metric label="Reclamações" metric={metrics.complaints} />
        <Metric label="Taxa de resposta" metric={metrics.response_rate} formatter={formatPercent} />
        <Metric label="Avaliações de satisfação" metric={metrics.satisfaction_count} />
        <Metric label="Satisfação média" metric={metrics.average_satisfaction} formatter={formatRatio} />
      </div>
    </Section>
  );
}

export default function InsurerProfileModal({ profile, onClose, onNavigateProfile }) {
  const isOpen = Boolean(profile);

  useEffect(() => {
    if (!isOpen) return undefined;
    const onKeyDown = (event) => {
      if (event.key === 'Escape') onClose?.();
    };
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = 'hidden';
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.body.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [isOpen, onClose]);

  if (!profile) return null;

  const identity = profile.identity || {};
  const isBrand = profile.profile_kind === 'brand';
  const name = isBrand
    ? identity.name || profile.profile_id
    : identity.display_name || identity.legal_name || profile.profile_id;
  const relationshipContext = profile.relationship_context || {};
  const brandRelationships = isBrand ? profile.relationships || [] : [];
  const sandbox = isBrand ? profile.sandbox_conduct_context : profile.sandbox_conduct;

  return (
    <div
      className="fixed inset-0 z-[99999] flex items-center justify-center bg-black/60 p-4 backdrop-blur-sm"
      role="dialog"
      aria-modal="true"
      aria-label={`Detalhes de ${name}`}
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose?.();
      }}
    >
      <div className="flex max-h-[88dvh] w-full max-w-4xl flex-col rounded-2xl bg-white shadow-xl">
        <div className="flex items-start justify-between gap-4 border-b border-slate-200 p-5">
          <div>
            <div className="flex items-center gap-2">
              <ShieldCheck className="h-5 w-5 text-slate-700" />
              <h2 className="text-lg font-semibold text-slate-900">{name}</h2>
            </div>
            {profile.public_summary?.headline ? (
              <div className="mt-1 text-sm font-medium text-slate-600">
                {profile.public_summary.headline}
              </div>
            ) : null}
          </div>
          <button
            type="button"
            onClick={onClose}
            aria-label="Fechar"
            className="rounded-lg p-2 text-slate-500 hover:bg-slate-100 hover:text-slate-900"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="space-y-5 overflow-y-auto p-5">
          <Section title="Resposta rápida">
            <p className="text-sm leading-relaxed text-slate-700">
              {profile.public_summary?.quick_answer || 'Sem resumo público disponível.'}
            </p>
          </Section>

          <Section title="Identidade e situação">
            <dl className="grid grid-cols-1 gap-2 text-sm md:grid-cols-2">
              <div><dt className="text-slate-500">Tipo</dt><dd className="font-medium text-slate-900">{humanize(identity.entity_type)}</dd></div>
              {!isBrand ? <div><dt className="text-slate-500">CNPJ</dt><dd className="font-mono text-slate-900">{identity.cnpj || '—'}</dd></div> : null}
              {!isBrand ? <div><dt className="text-slate-500">Código SUSEP/FIP</dt><dd className="font-mono text-slate-900">{identity.fip_code || '—'}</dd></div> : null}
              {!isBrand ? <div><dt className="text-slate-500">Situação regulatória</dt><dd className="text-slate-900">{profile.regulatory?.label || '—'}</dd></div> : null}
              {isBrand && identity.aliases?.length ? <div className="md:col-span-2"><dt className="text-slate-500">Também pesquisável como</dt><dd className="text-slate-900">{identity.aliases.join(', ')}</dd></div> : null}
            </dl>
          </Section>

          {!isBrand && profile.lifecycle ? (
            <Section title="Lifecycle">
              <div className="text-sm text-slate-700">
                Situação cadastral: {humanize(profile.lifecycle.legal_lifecycle?.cadastral_status)}
                {profile.lifecycle.legal_lifecycle?.status_date
                  ? ` · desde ${profile.lifecycle.legal_lifecycle.status_date}`
                  : ''}
              </div>
              {profile.lifecycle.successor_profile_id ? (
                <div className="mt-2 text-sm text-slate-700">
                  Sucessora:{' '}
                  <ProfileLink
                    profileId={profile.lifecycle.successor_profile_id}
                    onNavigateProfile={onNavigateProfile}
                  >
                    {profile.lifecycle.successor_name || profile.lifecycle.successor_profile_id}
                  </ProfileLink>
                </div>
              ) : null}
            </Section>
          ) : null}

          {!isBrand && relationshipContext.economic_group ? (
            <Section title="Grupo econômico observado">
              <div className="text-sm font-medium text-slate-900">
                {relationshipContext.economic_group.group_name || 'Grupo sem nome'}
              </div>
              <p className="mt-1 text-xs text-slate-600">{relationshipContext.economic_group.public_note}</p>
              {relationshipContext.economic_group.related_entities?.length ? (
                <ul className="mt-2 space-y-1 text-sm">
                  {relationshipContext.economic_group.related_entities.map((item) => (
                    <li key={item.profile_id}>
                      <ProfileLink profileId={item.profile_id} onNavigateProfile={onNavigateProfile}>
                        {item.name}
                      </ProfileLink>
                    </li>
                  ))}
                </ul>
              ) : null}
            </Section>
          ) : null}

          {(brandRelationships.length || relationshipContext.direct_relationships?.length || relationshipContext.brands?.length) ? (
            <Section title="Relações verificadas">
              <div className="space-y-3">
                {brandRelationships.map((relation, index) => (
                  <div key={`${relation.target_profile_id}-${index}`} className="rounded-lg border border-slate-200 p-3 text-sm">
                    <div>{humanize(relation.relationship_type)} →{' '}
                      <ProfileLink profileId={relation.target_profile_id} onNavigateProfile={onNavigateProfile}>
                        {relation.target_name}
                      </ProfileLink>
                    </div>
                    {relation.scope ? <p className="mt-1 text-xs text-slate-600">{relation.scope}</p> : null}
                    <Evidence evidence={relation.evidence} />
                  </div>
                ))}
                {(relationshipContext.direct_relationships || []).map((relation, index) => (
                  <div key={`${relation.target_profile_id}-${index}`} className="rounded-lg border border-slate-200 p-3 text-sm">
                    <div>{humanize(relation.relationship_type)} →{' '}
                      <ProfileLink profileId={relation.target_profile_id} onNavigateProfile={onNavigateProfile}>
                        {relation.target_name || relation.target_profile_id}
                      </ProfileLink>
                    </div>
                    <Evidence evidence={relation.evidence} />
                  </div>
                ))}
                {(relationshipContext.brands || []).map((brand) => (
                  <div key={brand.profile_id} className="rounded-lg border border-slate-200 p-3 text-sm">
                    Marca relacionada:{' '}
                    <ProfileLink profileId={brand.profile_id} onNavigateProfile={onNavigateProfile}>
                      {brand.name}
                    </ProfileLink>
                    {brand.aliases?.length ? (
                      <div className="mt-1 text-xs text-slate-500">Aliases: {brand.aliases.join(', ')}</div>
                    ) : null}
                    <Evidence evidence={brand.evidence} />
                  </div>
                ))}
              </div>
            </Section>
          ) : null}

          {!isBrand && relationshipContext.conduct_reconciliation?.length ? (
            <Section title="Reconciliação de Conduta">
              <div className="space-y-3">
                {relationshipContext.conduct_reconciliation.map((relation) => (
                  <div key={relation.relationship_id} className="rounded-lg border border-slate-200 p-3 text-sm text-slate-700">
                    <div>{humanize(relation.relationship_type)}</div>
                    <div className="mt-1 text-xs text-slate-500">
                      Política: {humanize(relation.pressure_policy)} · estado:{' '}
                      {humanize(relation.reconciliation_state)}
                    </div>
                  </div>
                ))}
              </div>
            </Section>
          ) : null}

          <Assessment assessment={profile.assessment} />
          <SandboxConduct value={sandbox} />

          {profile.limits?.length ? (
            <Section title="Limites da leitura">
              <ul className="list-disc space-y-1 pl-5 text-sm text-slate-600">
                {profile.limits.map((limit) => <li key={limit}>{limit}</li>)}
              </ul>
            </Section>
          ) : null}
        </div>
      </div>
    </div>
  );
}
