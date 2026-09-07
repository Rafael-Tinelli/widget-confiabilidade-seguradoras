(() => {
  "use strict";

  const root = document.querySelector("[data-rk2-root]");
  const input = document.querySelector("#rk2-search-input");
  const form = document.querySelector("#rk2-search-form");
  const suggestions = document.querySelector("#rk2-search-suggestions");
  if (!root || !input || !form || !suggestions) return;

  const NOISE_QUERIES = new Set([
    "seguro",
    "seguros",
    "seguradora",
    "seguradoras",
    "ranking",
    "ranking seguradoras",
    "melhor seguradora",
    "melhores seguradoras",
    "confiabilidade",
  ]);
  const EMAIL_LIKE = /[^\s@]+@[^\s@]+\.[^\s@]+/;
  const sent = new Set();
  let timer = null;

  function normalizeUnknownMarketQuery(value) {
    const text = String(value || "").trim();
    if (!text) return "";

    const digits = text.replace(/\D+/g, "");
    const nonCnpj = text.replace(/[\d./\-\s]+/g, "");
    if (digits.length === 14 && !nonCnpj) return digits;

    return text
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLocaleLowerCase("pt-BR")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  function isEligibleUnknownQuery(value) {
    const raw = String(value || "").trim();
    if (!raw || raw.includes("@") || EMAIL_LIKE.test(raw)) return false;
    const normalized = normalizeUnknownMarketQuery(raw);
    if (!normalized || NOISE_QUERIES.has(normalized)) return false;
    if (/^\d+$/.test(normalized)) return normalized.length === 14;
    return normalized.length >= 3;
  }

  function configuredSameOriginEndpoint() {
    const configured = String(root.dataset.unknownMarketQueryEndpoint || "").trim();
    if (!configured) return null;
    try {
      const endpoint = new URL(configured, window.location.origin);
      if (endpoint.origin !== window.location.origin) return null;
      return endpoint.toString();
    } catch {
      return null;
    }
  }

  const endpoint = configuredSameOriginEndpoint();
  if (!endpoint) return;

  function zeroResultsStillCurrent(rawQuery) {
    if (root.dataset.loadState !== "ready") return false;
    if (String(input.value || "") !== rawQuery) return false;
    if (suggestions.querySelector("[data-suggestion-index]")) return false;
    return Boolean(suggestions.querySelector(".rk2-suggestion-empty"));
  }

  function scheduleUnknownQueryObservation() {
    window.clearTimeout(timer);
    timer = null;

    const rawQuery = String(input.value || "");
    if (!isEligibleUnknownQuery(rawQuery)) return;

    const normalized = normalizeUnknownMarketQuery(rawQuery);
    if (sent.has(normalized)) return;

    timer = window.setTimeout(() => {
      timer = null;
      if (!zeroResultsStillCurrent(rawQuery)) return;
      sent.add(normalized);

      fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: normalized }),
        credentials: "omit",
        keepalive: true,
        referrerPolicy: "same-origin",
      }).catch(() => {
        // Telemetria de demanda é observacional e nunca pode quebrar a consulta.
      });
    }, 800);
  }

  input.addEventListener("input", scheduleUnknownQueryObservation);
  form.addEventListener("submit", () => window.setTimeout(scheduleUnknownQueryObservation, 0));
})();
