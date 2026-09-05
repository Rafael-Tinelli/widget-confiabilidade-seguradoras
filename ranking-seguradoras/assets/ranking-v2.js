(() => {
  "use strict";

  const root = document.querySelector("[data-rk2-root]");
  if (!root) return;

  const PUBLIC_BASE = String(root.dataset.publicBase || "/ranking-seguradoras/data/v2/public").replace(/\/+$/, "");
  const PAGE_URL = root.dataset.pageUrl || "/ranking-seguradoras/index2.php";
  const MAX_COMPARE = 4;
  const LIST_PAGE_SIZE = 10;

  const $ = (selector, context = document) => context.querySelector(selector);
  const $$ = (selector, context = document) => Array.from(context.querySelectorAll(selector));

  const els = {
    searchForm: $("#rk2-search-form"),
    searchInput: $("#rk2-search-input"),
    searchSubmit: $(".rk2-search__submit"),
    searchClear: $("#rk2-search-clear"),
    suggestions: $("#rk2-search-suggestions"),
    population: $("#rk2-population"),
    result: $("#rk2-result"),
    activeContext: $("#rk2-active-context"),
    activeName: $("#rk2-active-name"),
    activeMeta: $("#rk2-active-meta"),
    contextChange: $("#rk2-context-change"),
    contextClose: $("#rk2-context-close"),

    compareSearch: $("#rk2-compare-search"),
    compareResults: $("#rk2-compare-results"),
    compareCount: $("#rk2-compare-count"),
    compareChips: $("#rk2-compare-chips"),
    compareEmpty: $("#rk2-compare-empty"),
    compareGrid: $("#rk2-compare-grid"),
    compareClear: $("#rk2-compare-clear"),
    compareShare: $("#rk2-compare-share"),

    listSearch: $("#rk2-list-search"),
    list: $("#rk2-list"),
    listStatus: $("#rk2-list-status"),
    listPrev: $("#rk2-list-prev"),
    listNext: $("#rk2-list-next"),
    listPage: $("#rk2-list-page"),
    listRange: $("#rk2-list-range"),

    exploreGrid: $("#rk2-explore-grid"),
    boardPanel: $("#rk2-board-panel"),
    collectionsBody: $("#rk2-collections-body"),
    toast: $("#rk2-toast"),
  };

  const state = {
    searchIndex: null,
    distributionManifest: null,
    manifestFiles: new Map(),
    entries: [],
    entryByProfileId: new Map(),
    profileCache: new Map(),
    explorer: null,
    explorerByProfileId: new Map(),
    insurerEntries: [],
    exploreIndex: null,
    compareIds: [],
    listFilter: "all",
    listPageNumber: 1,
    suggestionIndex: -1,
    currentSuggestions: [],
    compareSuggestions: [],
    compareSuggestionIndex: -1,
    currentProfileId: null,
    activeBoard: null,
    toastTimer: null,
    profileRequestToken: 0,
    compareRequestToken: 0,
    boardRequestToken: 0,
    routeRequestToken: 0,
    restoringView: false,
    scrollPersistTimer: null,
  };

  const LABELS = {
    capital: {
      capital_meets_or_exceeds_cmr: "Requisito atendido",
      capital_below_cmr: "Abaixo do mínimo",
      capital_signal_unavailable: "Sem conclusão",
    },
    liquidity: {
      ilt_at_or_above_arithmetic_parity: "Sem pressão pela referência",
      ilt_below_arithmetic_parity: "Merece atenção",
      ilt_signal_unavailable: "Sem conclusão",
    },
    conduct: {
      above_expected_with_sufficient_evidence: "Acima do esperado",
      below_expected_with_sufficient_evidence: "Abaixo do esperado",
      not_distinguishable_from_expected: "Sem diferença clara",
      pressure_inconclusive_denominator_sensitivity: "Sensível ao denominador",
      pressure_unavailable_insufficient_temporal_coverage: "Histórico insuficiente",
      pressure_unavailable_not_comparable: "Não comparável",
    },
  };


  const PUBLIC_STATUS = {
    active_licensed: "Autorizada e ativa no cadastro consultado",
    historical: "Registro histórico",
    inactive: "Registro sem atividade atual",
    revoked: "Autorização encerrada",
  };

  const PUBLIC_REGIME = {
    sandbox: "Participante do Sandbox SUSEP",
    special: "Regime especial",
  };

  const EXPLORE_COPY = {
    largest_by_direct_premium: {
      title: "Maiores seguradoras por volume de seguros",
      question: "Quais seguradoras registraram maior volume econômico de seguros no período analisado?",
    },
    highest_pla_cmr_ratio: {
      title: "Maior relação entre patrimônio ajustado e capital mínimo",
      question: "Quais seguradoras apresentaram maior relação entre o patrimônio ajustado e o capital mínimo exigido?",
    },
    highest_ilt: {
      title: "Maiores indicadores de liquidez observados",
      question: "Quais seguradoras apresentaram maior indicador de liquidez na competência analisada?",
    },
    lowest_conduct_pressure_ratio: {
      title: "Menor pressão relativa de reclamações",
      question: "Entre as seguradoras com dados comparáveis, quais tiveram menos reclamações em relação ao tamanho da operação?",
    },
    highest_conduct_pressure_ratio: {
      title: "Maior pressão relativa de reclamações",
      question: "Entre as seguradoras com dados comparáveis, quais tiveram mais reclamações em relação ao tamanho da operação?",
    },
  };

  const COLLECTION_COPY = {
    financial_core_without_current_adverse_signal: "Sem alerta financeiro central na competência analisada",
    favorable_joint_assessment: "Sem alerta conjunto relevante nos sinais centrais",
    favorable_with_below_expected_conduct: "Sem alerta conjunto e com reclamações abaixo da referência",
    conduct_improving_but_still_adverse: "Reclamações ainda acima da referência, com melhora recente",
    conduct_persistent_above_expected: "Reclamações persistentemente acima da referência",
  };

  function publicText(value) {
    // A linguagem pública é responsabilidade do contrato gerado. O frontend apenas
    // normaliza o tipo para renderização e nunca corrige metodologia ou redação.
    return String(value ?? "");
  }

  function setCatalogReady(ready) {
    root.setAttribute("aria-busy", "false");
    root.dataset.loadState = ready ? "ready" : "error";
    [els.searchInput, els.searchSubmit, els.compareSearch, els.listSearch].forEach((control) => {
      if (!control) return;
      control.disabled = !ready;
      control.setAttribute("aria-disabled", String(!ready));
    });
  }

  function formatCNPJ(value) {
    const d = String(value ?? "").replace(/\D/g, "");
    if (d.length !== 14) return value || "";
    return `${d.slice(0,2)}.${d.slice(2,5)}.${d.slice(5,8)}/${d.slice(8,12)}-${d.slice(12)}`;
  }

  function marketIdentity(profile) {
    const value = profile?.identity?.market_identity;
    return value && typeof value === "object" ? value : null;
  }

  function isMarketIdentity(profile) {
    return Boolean(marketIdentity(profile));
  }

  function regulatoryPublicStatus(profile) {
    const market = marketIdentity(profile);
    if (market?.public_label) return market.public_label;
    if (profile?.profile_kind === "brand") return "Marca / identidade comercial";
    if (profile?.regulatory?.regime === "sandbox") return "Participante do Sandbox SUSEP";
    if (profile?.regulatory?.filter_bucket === "insurers") return "Seguradora autorizada pela SUSEP";
    if (profile?.lifecycle?.is_historical) return "Empresa em registro histórico";
    return profile?.regulatory?.label || "Empresa identificada";
  }

  function regulatoryTechnicalStatus(profile) {
    return PUBLIC_STATUS[profile?.regulatory?.status] || null;
  }

  function regulatoryTechnicalRegime(profile) {
    const regime = profile?.regulatory?.regime;
    if (!regime || regime === "ordinary") return null;
    return PUBLIC_REGIME[regime] || null;
  }

  function updateActiveContext(profile) {
    if (!els.activeContext) return;
    els.activeName.textContent = profileName(profile);
    const bits = [regulatoryPublicStatus(profile)];
    if (profile?.identity?.fip_code) bits.push(`SUSEP ${profile.identity.fip_code}`);
    els.activeMeta.textContent = bits.join(" · ");
    els.activeContext.hidden = false;
  }

  function esc(value) {
    return String(value ?? "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#039;");
  }

  function normalize(value) {
    return String(value ?? "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLocaleLowerCase("pt-BR")
      .replace(/[^a-z0-9]+/g, " ")
      .trim();
  }

  function digits(value) {
    return String(value ?? "").replace(/\D/g, "");
  }

  function finite(value) {
    if (value === null || value === undefined || value === "") return null;
    const number = Number(value);
    return Number.isFinite(number) ? number : null;
  }

  function formatNumber(value, decimals = 2) {
    const number = finite(value);
    if (number === null) return "Não disponível";
    return number.toLocaleString("pt-BR", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  }

  function formatInteger(value) {
    const number = finite(value);
    return number === null ? "Não disponível" : Math.round(number).toLocaleString("pt-BR");
  }

  function formatPercent(value) {
    const number = finite(value);
    if (number === null) return "Não disponível";
    return number.toLocaleString("pt-BR", { style: "percent", maximumFractionDigits: 1 });
  }

  function formatBRLCompact(value) {
    const number = finite(value);
    if (number === null) return "Não disponível";
    const abs = Math.abs(number);
    if (abs >= 1e9) return `R$�{()