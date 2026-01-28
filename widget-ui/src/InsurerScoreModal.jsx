import { useEffect, useMemo } from "react";

function clamp(n, min, max) {
  return Math.max(min, Math.min(max, n));
}

function fmtBRL(v) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return new Intl.NumberFormat("pt-BR", {
    style: "currency",
    currency: "BRL",
    maximumFractionDigits: 0,
  }).format(Number(v));
}

function fmtNum(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return new Intl.NumberFormat("pt-BR", {
    maximumFractionDigits: digits,
    minimumFractionDigits: digits,
  }).format(Number(v));
}

function fmtPct(v, digits = 2) {
  if (v === null || v === undefined || Number.isNaN(Number(v))) return "—";
  return `${fmtNum(Number(v) * 100, digits)}%`;
}

/**
 * Repete (no frontend) a “mesma lógica” descrita no intelligence.py
 * para exibir a matemática e os passos intermediários.
 */
function calcLossScore(lossRatio) {
  if (lossRatio === null || lossRatio === undefined || Number.isNaN(Number(lossRatio))) return null;
  const lr = Number(lossRatio);
  if (lr <= 0.6) return 100;
  if (lr <= 0.8) return 80;
  if (lr <= 1.0) return 60;
  if (lr <= 1.2) return 40;
  if (lr <= 1.5) return 20;
  return 0;
}

function calcRatioScore(netWorthRatio) {
  if (netWorthRatio === null || netWorthRatio === undefined || Number.isNaN(Number(netWorthRatio))) return null;
  const r = Number(netWorthRatio);
  if (r <= 0) return 0;
  // ratio_score = clamp(50 + 20*log10(netWorthRatio), 0..100)
  const score = 50 + 20 * Math.log10(r);
  return clamp(score, 0, 100);
}

function calcPressureScore(pressureIdx) {
  if (pressureIdx === null || pressureIdx === undefined || Number.isNaN(Number(pressureIdx))) return null;
  const p = Number(pressureIdx);
  if (p <= 0.5) return 100;
  if (p <= 1.0) return 70;
  if (p <= 1.5) return 40;
  return 10;
}

function calcSatisfactionScore(satisfaction) {
  if (satisfaction === null || satisfaction === undefined || Number.isNaN(Number(satisfaction))) return null;
  const s = Number(satisfaction);
  if (s >= 4.0) return 100;
  if (s >= 3.0) return 70;
  if (s >= 2.0) return 40;
  return 10;
}

function fmtISODate(iso) {
  if (!iso) return null;
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return null;
  return d.toLocaleString("pt-BR");
}

export default function InsurerScoreModal({ insurer, meta, onClose }) {
  // ESC fecha
  useEffect(() => {
    function onKeyDown(e) {
      if (e.key === "Escape") onClose?.();
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [onClose]);

  const computed = useMemo(() => {
    const data = insurer?.data || {};
    const cd = data.componentsDetail || {};

    const weights = data.weights || { solvency: 0.35, reputation: 0.45, innovation: 0.20 };

    // Pilar 1: Solvência (SES)
    const solv = cd.solvency || {};
    const lossRatio = solv.lossRatio ?? data.lossRatio ?? null;
    const netWorthRatio = solv.netWorthRatio ?? data.netWorthRatio ?? null;
    const premiums = solv.premiums ?? data.premiums ?? null;
    const claims = solv.claims ?? data.claims ?? null;
    const netWorth = solv.netWorth ?? data.net_worth ?? null;

    const lossScore = calcLossScore(lossRatio);
    const ratioScore = calcRatioScore(netWorthRatio);
    const solvCalcScore =
      (ratioScore === null || lossScore === null)
        ? null
        : clamp(0.7 * ratioScore + 0.3 * lossScore, 0, 100);

    const solvScore = solv.score ?? data.components?.solvency ?? data.score?.components?.solvency ?? null;

    // Pilar 2: Reputação (Consumidor.gov)
    const rep = cd.reputation || {};
    const pressureIdx = rep.pressureIdx ?? null;
    const satisfaction = rep.satisfaction ?? null;
    const complaintsIndex = rep.complaintsIndex ?? null;
    const marketRatePerBRL = rep.marketRatePerBRL ?? null;
    const observedRatePerBRL = rep.observedRatePerBRL ?? null;

    const pressureScore = calcPressureScore(pressureIdx);
    const satisfactionScore = calcSatisfactionScore(satisfaction);
    const repCalcScore =
      pressureScore === null
        ? null
        : (satisfaction === 0 || satisfaction === null)
          ? pressureScore
          : clamp(0.8 * pressureScore + 0.2 * (satisfactionScore ?? 0), 0, 100);

    const repScore = rep.score ?? data.components?.reputation ?? null;

    // Pilar 3: Inovação (Open Insurance)
    const inn = cd.innovation || {};
    const isOpenInsurance = Boolean(inn.isOpenInsurance ?? data.isOpenInsurance);
    const productsCount = Number(inn.productsCount ?? data.openInsuranceProductsCount ?? 0);
    const productsScore = clamp(productsCount / 50, 0, 1);
    const innCalcScore = clamp(60 + 20 * (isOpenInsurance ? 1 : 0) + 20 * productsScore, 0, 100);

    const innScore = inn.score ?? data.components?.innovation ?? null;

    // Nota final
    const usedSolv = Number.isFinite(Number(solvScore)) ? Number(solvScore) : solvCalcScore ?? 0;
    const usedRep = Number.isFinite(Number(repScore)) ? Number(repScore) : repCalcScore ?? 0;
    const usedInn = Number.isFinite(Number(innScore)) ? Number(innScore) : innCalcScore ?? 0;

    const final =
      clamp(
        usedSolv * (weights.solvency ?? 0) +
          usedRep * (weights.reputation ?? 0) +
          usedInn * (weights.innovation ?? 0),
        0,
        100
      );

    return {
      weights,
      solv: { lossRatio, netWorthRatio, premiums, claims, netWorth, lossScore, ratioScore, solvCalcScore, solvScore, usedSolv },
      rep: { pressureIdx, satisfaction, complaintsIndex, marketRatePerBRL, observedRatePerBRL, pressureScore, satisfactionScore, repCalcScore, repScore, usedRep },
      inn: { isOpenInsurance, productsCount, productsScore, innCalcScore, innScore, usedInn },
      final,
    };
  }, [insurer]);

  if (!insurer) return null;

  const updatedAt = fmtISODate(meta?.generatedAt);

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4"
      onMouseDown={(e) => {
        // click fora fecha (mas não fecha ao clicar dentro)
        if (e.target === e.currentTarget) onClose?.();
      }}
      aria-modal="true"
      role="dialog"
    >
      <div className="w-full max-w-4xl rounded-2xl bg-white shadow-xl">
        <div className="flex items-start justify-between gap-4 border-b p-5">
          <div>
            <h2 className="text-xl font-bold leading-tight text-gray-900">{insurer.name}</h2>
            <div className="mt-1 text-sm text-gray-600">
              <span className="font-medium">CNPJ:</span> {insurer.cnpj || "não informado"}{" "}
              {insurer.coenti ? (
                <>
                  <span className="mx-2 text-gray-300">•</span>
                  <span className="font-medium">SUSEP (Coenti):</span> {insurer.coenti}
                </>
              ) : null}
              {updatedAt ? (
                <>
                  <span className="mx-2 text-gray-300">•</span>
                  <span className="font-medium">Dados gerados:</span> {updatedAt}
                </>
              ) : null}
            </div>
          </div>

          <button
            className="rounded-full border px-3 py-1 text-sm font-semibold text-gray-700 hover:bg-gray-50"
            onClick={onClose}
          >
            Fechar
          </button>
        </div>

        <div className="max-h-[80vh] overflow-y-auto p-5">
          {/* Nota final */}
          <div className="rounded-xl border bg-gray-50 p-4">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <div className="text-sm font-semibold text-gray-700">Nota final</div>
                <div className="text-3xl font-extrabold text-gray-900">{fmtNum(computed.final, 2)}</div>
              </div>
              <div className="text-sm text-gray-700">
                <div className="font-semibold">Fórmula</div>
                <code className="block rounded bg-white px-2 py-1 text-xs">
                  final = (solvência × {fmtNum(computed.weights.solvency, 2)}) + (reputação × {fmtNum(computed.weights.reputation, 2)}) + (inovação × {fmtNum(computed.weights.innovation, 2)})
                </code>
              </div>
            </div>
          </div>

          {/* 3 pilares */}
          <div className="mt-5 grid grid-cols-1 gap-4 md:grid-cols-3">
            {/* Solvência */}
            <div className="rounded-xl border p-4">
              <div className="flex items-baseline justify-between">
                <div className="text-sm font-bold text-gray-900">Pilar 1 — Solvência</div>
                <div className="text-xs font-semibold text-gray-500">peso {fmtNum(computed.weights.solvency, 2)}</div>
              </div>

              <div className="mt-2 text-2xl font-extrabold text-gray-900">
                {fmtNum(computed.solv.usedSolv, 2)}
              </div>

              <div className="mt-3 space-y-2 text-sm text-gray-700">
                <div className="flex justify-between gap-3"><span>Prêmios</span><span className="font-semibold">{fmtBRL(computed.solv.premiums)}</span></div>
                <div className="flex justify-between gap-3"><span>Sinistros</span><span className="font-semibold">{fmtBRL(computed.solv.claims)}</span></div>
                <div className="flex justify-between gap-3"><span>PL / Patrimônio Líquido</span><span className="font-semibold">{fmtBRL(computed.solv.netWorth)}</span></div>
                <div className="flex justify-between gap-3"><span>Loss ratio</span><span className="font-semibold">{fmtPct(computed.solv.lossRatio, 2)}</span></div>
                <div className="flex justify-between gap-3"><span>PL/Prêmios</span><span className="font-semibold">{fmtNum(computed.solv.netWorthRatio, 4)}</span></div>
              </div>

              <div className="mt-4 text-xs text-gray-600">
                <div className="font-semibold text-gray-800">Matemática (transparência)</div>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  ratioScore = clamp(50 + 20·log10(PL/Prêmios), 0..100) = {computed.solv.ratioScore === null ? "—" : fmtNum(computed.solv.ratioScore, 2)}
                </code>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  lossScore (faixas) = {computed.solv.lossScore === null ? "—" : fmtNum(computed.solv.lossScore, 0)}
                </code>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  solvência = 0.7·ratioScore + 0.3·lossScore = {computed.solv.solvCalcScore === null ? "—" : fmtNum(computed.solv.solvCalcScore, 2)}
                </code>
              </div>

              <div className="mt-3 text-xs text-gray-500">
                Fonte: SUSEP/SES (Base Completa) — prêmios, sinistros e PL.
              </div>
            </div>

            {/* Reputação */}
            <div className="rounded-xl border p-4">
              <div className="flex items-baseline justify-between">
                <div className="text-sm font-bold text-gray-900">Pilar 2 — Reputação</div>
                <div className="text-xs font-semibold text-gray-500">peso {fmtNum(computed.weights.reputation, 2)}</div>
              </div>

              <div className="mt-2 text-2xl font-extrabold text-gray-900">
                {fmtNum(computed.rep.usedRep, 2)}
              </div>

              <div className="mt-3 space-y-2 text-sm text-gray-700">
                <div className="flex justify-between gap-3"><span>Índice de reclamações</span><span className="font-semibold">{fmtNum(computed.rep.complaintsIndex, 2)}</span></div>
                <div className="flex justify-between gap-3"><span>Pressão (observado/mercado)</span><span className="font-semibold">{fmtNum(computed.rep.pressureIdx, 2)}</span></div>
                <div className="flex justify-between gap-3"><span>Satisfação (0–5)</span><span className="font-semibold">{computed.rep.satisfaction ?? "—"}</span></div>
              </div>

              <div className="mt-4 text-xs text-gray-600">
                <div className="font-semibold text-gray-800">Matemática (transparência)</div>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  pressãoScore (faixas por pressão) = {computed.rep.pressureScore === null ? "—" : fmtNum(computed.rep.pressureScore, 0)}
                </code>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  satisfaçãoScore (faixas) = {computed.rep.satisfactionScore === null ? "—" : fmtNum(computed.rep.satisfactionScore, 0)}
                </code>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  reputação = (satisfação=0 ? pressãoScore : 0.8·pressãoScore + 0.2·satisfaçãoScore) = {computed.rep.repCalcScore === null ? "—" : fmtNum(computed.rep.repCalcScore, 2)}
                </code>
              </div>

              <div className="mt-3 text-xs text-gray-500">
                Fonte: Consumidor.gov.br (dados abertos) — volume e qualidade de atendimento (normalizado).
              </div>
            </div>

            {/* Inovação */}
            <div className="rounded-xl border p-4">
              <div className="flex items-baseline justify-between">
                <div className="text-sm font-bold text-gray-900">Pilar 3 — Inovação</div>
                <div className="text-xs font-semibold text-gray-500">peso {fmtNum(computed.weights.innovation, 2)}</div>
              </div>

              <div className="mt-2 text-2xl font-extrabold text-gray-900">
                {fmtNum(computed.inn.usedInn, 2)}
              </div>

              <div className="mt-3 space-y-2 text-sm text-gray-700">
                <div className="flex justify-between gap-3"><span>Open Insurance</span><span className="font-semibold">{computed.inn.isOpenInsurance ? "Sim" : "Não"}</span></div>
                <div className="flex justify-between gap-3"><span>Produtos no Open Insurance</span><span className="font-semibold">{computed.inn.productsCount}</span></div>
              </div>

              <div className="mt-4 text-xs text-gray-600">
                <div className="font-semibold text-gray-800">Matemática (transparência)</div>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  productsScore = clamp(produtos/50, 0..1) = {fmtNum(computed.inn.productsScore, 2)}
                </code>
                <code className="mt-1 block rounded bg-gray-50 px-2 py-1">
                  inovação = 60 + 20·(OpenInsurance) + 20·productsScore = {fmtNum(computed.inn.innCalcScore, 2)}
                </code>
              </div>

              <div className="mt-3 text-xs text-gray-500">
                Fonte: Open Insurance Brasil — participação e quantidade de produtos publicados.
              </div>
            </div>
          </div>

          {/* Fontes e notas */}
          <div className="mt-5 rounded-xl border p-4">
            <div className="text-sm font-bold text-gray-900">De onde vêm os dados (resumo)</div>
            <ul className="mt-2 list-disc space-y-1 pl-5 text-sm text-gray-700">
              <li><b>SUSEP/SES (Base Completa):</b> prêmios, sinistros e patrimônio líquido (PL) para compor Solvência.</li>
              <li><b>Consumidor.gov.br (dados abertos):</b> reclamações e satisfação (normalizado por “pressão” vs mercado) para Reputação.</li>
              <li><b>Open Insurance Brasil:</b> participação e volume de produtos publicados para Inovação.</li>
            </ul>
            <div className="mt-3 text-xs text-gray-500">
              Observação: quando algum insumo não existe ou é zero (ex.: prêmios muito baixos), a conta pode perder significado estatístico — por isso a auditoria e o modal mostram os insumos.
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
