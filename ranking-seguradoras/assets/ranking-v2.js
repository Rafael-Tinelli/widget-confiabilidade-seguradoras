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
    if (abs >= 1e9) return `R$ ${(number / 1e9).toLocaleString("pt-BR", { maximumFractionDigits: 1 })} bi`;
    if (abs >= 1e6) return `R$ ${(number / 1e6).toLocaleString("pt-BR", { maximumFractionDigits: 1 })} mi`;
    if (abs >= 1e3) return `R$ ${(number / 1e3).toLocaleString("pt-BR", { maximumFractionDigits: 1 })} mil`;
    return number.toLocaleString("pt-BR", { style: "currency", currency: "BRL", maximumFractionDigits: 0 });
  }

  function formatPeriod(value) {
    const raw = String(value ?? "");
    if (!/^\d{6}$/.test(raw)) return raw || "Não disponível";
    const y = raw.slice(0, 4);
    const m = Number(raw.slice(4, 6));
    const months = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];
    return m >= 1 && m <= 12 ? `${months[m - 1]}/${y}` : raw;
  }

  function metricValue(metric) {
    if (!metric || metric.availability !== "available") return null;
    return metric.value;
  }

  function canDisplay(metric) {
    return Boolean(metric && metric.availability === "available" && metric.public_use === "displayable");
  }

  function profileName(profile) {
    if (profile?.profile_kind === "brand") return profile?.identity?.name || "Identidade de mercado";
    return profile?.identity?.display_name || profile?.identity?.legal_name || "Entidade";
  }

  function entryTypeLabel(entry) {
    if (entry.market_role_label) return entry.market_role_label;
    if (entry.result_kind === "brand") return "Marca / identidade comercial";
    if (entry.entity_type === "sandbox_participant") return "Sandbox";
    if (entry.filter_bucket === "insurers") return "Seguradora";
    if (entry.filter_bucket === "historical") return "Histórica";
    return "Outra entidade";
  }

  function entryTypeClass(entry) {
    if (entry.market_role_label) return "market";
    if (entry.result_kind === "brand") return "brand";
    if (entry.entity_type === "sandbox_participant") return "sandbox";
    if (entry.filter_bucket === "insurers") return "insurer";
    return "other";
  }

  function assessmentTone(profile) {
    if (isMarketIdentity(profile)) return "market";
    if (profile?.profile_kind === "brand") return "brand";
    const publicClass = profile?.assessment?.public_class;
    if (publicClass === "favorable_reading") return "favorable";
    if (publicClass === "prudential_warning") return "adverse";
    if (publicClass === "attention") return "caution";
    if (profile?.regulatory?.regime === "sandbox") return "caution";
    return "unknown";
  }

  function signalTone(signal) {
    const tone = signal?.tone;
    if (tone === "favorable") return "favorable";
    if (tone === "adverse") return "adverse";
    if (tone === "caution") return "caution";
    if (tone === "neutral") return "neutral";
    return "unknown";
  }

  function safePublicPath(value) {
    const path = String(value || "");
    const parts = path.split("/");
    if (
      !path ||
      path.startsWith("/") ||
      path.includes("\\") ||
      !/^[A-Za-z0-9._/-]+$/.test(path) ||
      parts.some((part) => !part || part === "." || part === "..")
    ) {
      throw new Error("O pacote público contém um caminho de arquivo inválido.");
    }
    return path;
  }

  function bytesToHex(buffer) {
    return Array.from(new Uint8Array(buffer), (byte) => byte.toString(16).padStart(2, "0")).join("");
  }

  async function sha256Text(value) {
    if (!globalThis.crypto?.subtle || typeof TextEncoder !== "function") {
      throw new Error("Este navegador não oferece a verificação de integridade exigida.");
    }
    const bytes = new TextEncoder().encode(value);
    return bytesToHex(await globalThis.crypto.subtle.digest("SHA-256", bytes));
  }

  async function loadDistributionManifest() {
    const path = `${PUBLIC_BASE}/distribution_manifest.json`;
    const response = await fetch(path, { credentials: "same-origin", cache: "no-cache" });
    if (!response.ok) throw new Error(`Falha ao carregar o manifesto público (${response.status}).`);

    let manifest;
    try {
      manifest = JSON.parse(await response.text());
    } catch {
      throw new Error("O manifesto público não contém JSON válido.");
    }

    const build = manifest?.build || {};
    const publicPackage = manifest?.public_package || {};
    const files = Array.isArray(publicPackage.files) ? publicPackage.files : [];
    if (
      manifest?.artifact !== "v2_public_distribution_manifest" ||
      !/^v2-gate4-full-[0-9]+-a[0-9]+$/.test(String(build.build_id || "")) ||
      !/^[0-9a-f]{40}$/.test(String(build.source_head_sha || "")) ||
      !/^[0-9a-f]{64}$/.test(String(publicPackage.package_sha256 || "")) ||
      files.length !== Number(publicPackage.files_count)
    ) {
      throw new Error("O manifesto público é incompatível ou incompleto.");
    }

    const verifiedFiles = new Map();
    files.forEach((row) => {
      const relativePath = safePublicPath(row?.path);
      const digest = String(row?.sha256 || "");
      if (!/^[0-9a-f]{64}$/.test(digest) || verifiedFiles.has(relativePath)) {
        throw new Error("O manifesto público possui arquivos inválidos ou duplicados.");
      }
      verifiedFiles.set(relativePath, digest);
    });
    ["search_index.json", "insurer_explorer.json", "explore_index.json"].forEach((required) => {
      if (!verifiedFiles.has(required)) {
        throw new Error(`O manifesto público não contém ${required}.`);
      }
    });

    state.distributionManifest = manifest;
    state.manifestFiles = verifiedFiles;
    return manifest;
  }

  async function fetchJSON(relativePath, expectedArtifact) {
    const safePath = safePublicPath(relativePath);
    const expectedDigest = state.manifestFiles.get(safePath);
    if (!expectedDigest) throw new Error("O arquivo solicitado não pertence à geração pública ativa.");

    const response = await fetch(`${PUBLIC_BASE}/${safePath}`, {
      credentials: "same-origin",
      cache: "no-cache",
    });
    if (!response.ok) throw new Error(`Falha ao carregar dados públicos (${response.status}).`);

    const raw = await response.text();
    const actualDigest = await sha256Text(raw);
    if (actualDigest !== expectedDigest) {
      throw new Error("A verificação de integridade da geração pública falhou.");
    }

    let payload;
    try {
      payload = JSON.parse(raw);
    } catch {
      throw new Error("Um arquivo da geração pública não contém JSON válido.");
    }
    if (expectedArtifact && payload?.artifact !== expectedArtifact) {
      throw new Error("Um arquivo da geração pública possui contrato incompatível.");
    }
    return payload;
  }

  async function loadProfile(profileId) {
    if (state.profileCache.has(profileId)) return state.profileCache.get(profileId);
    const entry = state.entryByProfileId.get(profileId);
    if (!entry) throw new Error("Perfil não localizado no índice público.");
    const path = safePublicPath(entry.profile_path);
    if (!path.startsWith("profiles/") || !path.endsWith(".json")) {
      throw new Error("O índice público aponta para um perfil inválido.");
    }
    const profile = await fetchJSON(path, "v2_public_entity_or_brand_profile");
    if (profile.profile_id !== profileId) {
      throw new Error("O perfil carregado não corresponde ao índice público.");
    }
    state.profileCache.set(profileId, profile);
    return profile;
  }

  function levenshtein(a, b, max = 3) {
    if (Math.abs(a.length - b.length) > max) return max + 1;
    const prev = Array.from({ length: b.length + 1 }, (_, i) => i);
    for (let i = 1; i <= a.length; i += 1) {
      const curr = [i];
      let min = curr[0];
      for (let j = 1; j <= b.length; j += 1) {
        const cost = a[i - 1] === b[j - 1] ? 0 : 1;
        curr[j] = Math.min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost);
        min = Math.min(min, curr[j]);
      }
      if (min > max) return max + 1;
      prev.splice(0, prev.length, ...curr);
    }
    return prev[b.length];
  }

  function candidateScore(entry, query) {
    const q = normalize(query);
    if (!q) return 999;
    const name = normalize(entry.name);
    const aliases = (entry.aliases || []).map(normalize);
    const text = normalize(entry.search_text || "");
    const qDigits = digits(query);

    if (name === q || aliases.includes(q)) return 0;
    if (qDigits && (digits(entry.cnpj) === qDigits || String(entry.fip_code || "") === qDigits)) return 0;
    if (name.startsWith(q) || aliases.some((alias) => alias.startsWith(q))) return 1;
    if (` ${name}`.includes(` ${q}`) || text.includes(q)) return 2;

    const qTokens = q.split(" ").filter(Boolean);
    const allTokensPresent = qTokens.length > 0 && qTokens.every((token) => text.includes(token));
    if (allTokensPresent) return 3;

    if (q.length >= 4) {
      const firstWord = name.split(" ")[0] || "";
      const distance = levenshtein(q, firstWord, 2);
      if (distance <= 2) return 4 + distance / 10;
    }
    return 999;
  }

  function candidatePriority(entry) {
    if (entry.filter_bucket === "insurers" && entry.result_kind !== "brand") return 0;
    if (entry.market_role_label) return 1;
    if (entry.result_kind === "brand") return 2;
    if (entry.entity_type === "sandbox_participant") return 3;
    if (entry.filter_bucket === "historical") return 4;
    return 5;
  }

  function findCandidates(query, source = state.entries, limit = 8) {
    return source
      .map((entry) => ({ entry, score: candidateScore(entry, query) }))
      .filter((row) => row.score < 999)
      .sort((a, b) => (
        a.score - b.score
        || candidatePriority(a.entry) - candidatePriority(b.entry)
        || String(a.entry.name).localeCompare(String(b.entry.name), "pt-BR")
      ))
      .slice(0, limit);
  }

  function renderSuggestions(query) {
    const candidates = findCandidates(query);
    state.currentSuggestions = candidates;
    state.suggestionIndex = -1;

    if (!normalize(query)) {
      hideSuggestions();
      return;
    }

    if (!candidates.length) {
      els.suggestions.innerHTML = `<div class="rk2-suggestion-empty"><strong>Não encontramos esse nome na base atual.</strong><span>Confira a grafia ou tente outra forma do nome, CNPJ ou código SUSEP.</span></div>`;
      els.suggestions.hidden = false;
      els.searchInput.setAttribute("aria-expanded", "true");
      els.searchInput.removeAttribute("aria-activedescendant");
      return;
    }

    els.suggestions.innerHTML = candidates.map(({ entry }, index) => `
      <button
        class="rk2-suggestion"
        id="rk2-search-option-${index}"
        type="button"
        role="option"
        aria-selected="false"
        data-suggestion-index="${index}"
      >
        <span class="rk2-suggestion__main">
          <strong>${esc(entry.name)}</strong>
          <small>${esc(entry.disambiguation || "Identidade disponível no catálogo público")}</small>
        </span>
        <span class="rk2-suggestion__meta">
          <span class="rk2-type rk2-type--${entryTypeClass(entry)}">${esc(entryTypeLabel(entry))}</span>
          <span class="rk2-suggestion__go">Abrir perfil</span>
        </span>
      </button>
    `).join("");

    els.suggestions.hidden = false;
    els.searchInput.setAttribute("aria-expanded", "true");
    els.searchInput.removeAttribute("aria-activedescendant");
  }

  function hideSuggestions() {
    els.suggestions.hidden = true;
    els.searchInput.setAttribute("aria-expanded", "false");
    els.searchInput.removeAttribute("aria-activedescendant");
    state.currentSuggestions = [];
    state.suggestionIndex = -1;
  }

  function setActiveSuggestion(index) {
    const buttons = $$("[data-suggestion-index]", els.suggestions);
    if (!buttons.length) return;
    state.suggestionIndex = Math.max(0, Math.min(index, buttons.length - 1));
    buttons.forEach((button, i) => {
      const active = i === state.suggestionIndex;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    const active = buttons[state.suggestionIndex];
    if (active) {
      els.searchInput.setAttribute("aria-activedescendant", active.id);
      active.scrollIntoView({ block: "nearest" });
    }
  }

  function exactEntriesForQuery(query, source = state.entries) {
    const q = normalize(query);
    const qDigits = digits(query);
    if (!q && !qDigits) return [];
    return source.filter((entry) => {
      const names = [entry.name, ...(entry.aliases || [])].map(normalize);
      return names.includes(q)
        || (qDigits && digits(entry.cnpj) === qDigits)
        || (qDigits && String(entry.fip_code || "") === qDigits);
    });
  }

  function exactEntryForQuery(query, source = state.entries) {
    const exact = exactEntriesForQuery(query, source);
    return exact.length === 1 ? exact[0] : null;
  }

  function showToast(message) {
    if (!els.toast) return;
    window.clearTimeout(state.toastTimer);
    els.toast.textContent = message;
    els.toast.hidden = false;
    state.toastTimer = window.setTimeout(() => { els.toast.hidden = true; }, 3400);
  }

  function currentViewSnapshot() {
    return {
      scrollY: Math.max(0, Math.round(window.scrollY || 0)),
      compareIds: [...state.compareIds],
      listFilter: state.listFilter,
      listPageNumber: state.listPageNumber,
      listSearch: els.listSearch?.value || "",
      activeBoard: state.activeBoard ? { ...state.activeBoard } : null,
    };
  }

  function persistCurrentViewState({ force = false } = {}) {
    if (state.restoringView && !force) return;
    try {
      const previous = history.state && typeof history.state === "object" ? history.state : {};
      history.replaceState({ ...previous, rk2View: currentViewSnapshot() }, "", window.location.href);
    } catch (error) {
      console.warn("Não foi possível preservar o estado atual da ferramenta.", error);
    }
  }

  function scheduleViewPersistence() {
    if (state.restoringView) return;
    window.clearTimeout(state.scrollPersistTimer);
    state.scrollPersistTimer = window.setTimeout(() => persistCurrentViewState(), 180);
  }

  function pageRouteURL() {
    const base = window.location.origin && window.location.origin !== "null"
      ? window.location.origin
      : "https://sanida.invalid";
    return new URL(PAGE_URL, base);
  }

  function sameLocationTarget(url) {
    const current = new URL(window.location.href);
    return current.pathname === url.pathname
      && current.search === url.search
      && current.hash === url.hash;
  }

  function routeHash(kind, value) {
    const cleanKind = String(kind || "").trim();
    const cleanValue = String(value ?? "").trim();
    if (!cleanKind || !cleanValue || !["perfil", "comparar", "consulta"].includes(cleanKind)) return "";
    return `#${cleanKind}=${encodeURIComponent(cleanValue)}`;
  }

  function parseRouteHash(hash) {
    const raw = String(hash || "").replace(/^#/, "");
    for (const kind of ["perfil", "comparar", "consulta"]) {
      const prefix = `${kind}=`;
      if (!raw.startsWith(prefix)) continue;
      try {
        const value = decodeURIComponent(raw.slice(prefix.length)).trim();
        return value ? { kind, value } : null;
      } catch (_error) {
        return null;
      }
    }
    return null;
  }

  function legacyRouteFromURL(url) {
    const profileId = url.searchParams.get("perfil");
    if (profileId) return { kind: "perfil", value: profileId };

    const compareParam = url.searchParams.get("comparar");
    if (compareParam) return { kind: "comparar", value: compareParam };

    const query = url.searchParams.get("q");
    if (query) return { kind: "consulta", value: String(query).slice(0, 120) };
    return null;
  }

  function migrateLegacyRoute(url) {
    const route = legacyRouteFromURL(url);
    if (!route) return { url, route: null };

    try {
      const target = pageRouteURL();
      const remaining = new URLSearchParams(url.search);
      ["perfil", "comparar", "q"].forEach((key) => remaining.delete(key));
      const queryString = remaining.toString();
      target.search = queryString ? `?${queryString}` : "";
      target.hash = routeHash(route.kind, route.value);
      const previous = history.state && typeof history.state === "object" ? history.state : {};
      history.replaceState(previous, "", `${target.pathname}${target.search}${target.hash}`);
      return { url: target, route };
    } catch (error) {
      console.warn("Não foi possível migrar uma URL antiga da ferramenta.", error);
      return { url, route };
    }
  }

  function normalizeOriginHistoryEntry(origin) {
    if (!origin?.type) return;

    const url = pageRouteURL();
    let rk2 = null;

    if (origin.type === "comparison") {
      const ids = validComparisonIds((origin.compareIds || state.compareIds).join(","));
      if (ids.length < 2) return;
      url.hash = routeHash("comparar", ids.join(","));
      rk2 = { mode: "comparison" };
    } else if (origin.type === "list") {
      url.hash = "#lista";
      rk2 = { mode: "section", section: "lista" };
    } else if (origin.type === "board") {
      url.hash = "#explorar";
      rk2 = { mode: "section", section: "explorar" };
    } else if (origin.type === "profile" && origin.profileId) {
      url.hash = routeHash("perfil", origin.profileId);
      rk2 = { mode: "profile", profileId: origin.profileId };
    } else {
      return;
    }

    const previous = history.state && typeof history.state === "object" ? history.state : {};
    history.replaceState(
      { ...previous, rk2, rk2View: currentViewSnapshot() },
      "",
      `${url.pathname}${url.search}${url.hash}`,
    );
  }

  function updateURL(profileId, origin = null) {
    try {
      persistCurrentViewState({ force: true });
      if (origin) normalizeOriginHistoryEntry(origin);
      const url = pageRouteURL();
      url.hash = routeHash("perfil", profileId);
      if (sameLocationTarget(url)) return;
      history.pushState(
        { rk2: { mode: "profile", profileId, origin } },
        "",
        `${url.pathname}${url.search}${url.hash}`,
      );
    } catch (error) {
      console.warn("Não foi possível atualizar a URL do perfil.", error);
    }
  }

  function clearURL() {
    try {
      persistCurrentViewState({ force: true });
      const url = pageRouteURL();
      url.search = "";
      url.hash = "";
      if (sameLocationTarget(url)) {
        const previous = history.state && typeof history.state === "object" ? history.state : {};
        history.replaceState({ ...previous, rk2: { mode: "search" }, rk2View: currentViewSnapshot() }, "", url.pathname);
        return;
      }
      history.pushState(
        { rk2: { mode: "search" }, rk2View: currentViewSnapshot() },
        "",
        url.pathname,
      );
    } catch (error) {
      console.warn("Não foi possível limpar a URL do perfil.", error);
    }
  }

  function shareURL(kind, value) {
    const url = pageRouteURL();
    url.search = "";
    url.hash = routeHash(kind, value);
    return url.toString();
  }

  function profileShareURL(profileId) {
    return shareURL("perfil", profileId);
  }

  function comparisonShareURL() {
    return shareURL("comparar", state.compareIds.join(","));
  }

  function queryShareURL(query) {
    return shareURL("consulta", String(query || "").slice(0, 120));
  }

  async function shareOrCopy({ title, text, url }) {
    if (navigator.share) {
      try {
        await navigator.share({ title, text, url });
        return;
      } catch (error) {
        if (error?.name === "AbortError") return;
      }
    }
    try {
      await navigator.clipboard.writeText(url);
      showToast("Link copiado para compartilhar.");
    } catch (error) {
      console.warn("Não foi possível copiar o link.", error);
      showToast("Não foi possível copiar o link neste navegador.");
    }
  }

  function identityLine(profile) {
    const market = marketIdentity(profile);
    if (market) {
      const bits = [];
      if (market.legal_name) bits.push(market.legal_name);
      if (market.cnpj) bits.push(`CNPJ ${formatCNPJ(market.cnpj)}`);
      return bits.join(" · ") || "Identidade de mercado documentada.";
    }
    if (profile.profile_kind === "brand") {
      const aliases = profile.identity?.aliases || [];
      return aliases.length ? `Também encontrada como: ${aliases.join(", ")}` : "Identidade comercial relacionada a uma entidade regulada.";
    }
    const bits = [];
    if (profile.identity?.cnpj) bits.push(`CNPJ ${formatCNPJ(profile.identity.cnpj)}`);
    if (profile.identity?.fip_code) bits.push(`SUSEP ${profile.identity.fip_code}`);
    return bits.join(" · ") || "Identidade localizada no cadastro público usado pela ferramenta.";
  }

  function profileStatus(profile) {
    const market = marketIdentity(profile);
    if (market?.public_label) return market.public_label;
    if (profile.profile_kind === "brand") return "Identidade comercial relacionada";
    return regulatoryPublicStatus(profile);
  }

  function isHistoricalProfile(profile) {
    return Boolean(profile?.lifecycle?.is_historical);
  }

  function isSandboxProfile(profile) {
    return profile?.regulatory?.regime === "sandbox";
  }

  function renderQuickAnswer(profile, label = "Resposta rápida") {
    const quick = profile.public_summary?.quick_answer;
    if (!quick) return "";
    return `
      <section class="rk2-quick">
        <div class="rk2-quick__label">${esc(label)}</div>
        <p>${esc(publicText(quick))}</p>
      </section>
    `;
  }

  function renderIdentityDetails(profile) {
    if (!isOrdinaryInsurer(profile)) return "";
    const identity = profile.identity || {};
    const status = regulatoryPublicStatus(profile);
    const regime = regulatoryTechnicalRegime(profile);
    const facts = [];
    if (status) facts.push(["Na SUSEP", status]);
    if (identity.cnpj) facts.push(["CNPJ", formatCNPJ(identity.cnpj)]);
    if (identity.fip_code) facts.push(["Código SUSEP", identity.fip_code]);
    if (regime) facts.push(["Regime", regime]);
    if (!facts.length) return "";
    return `
      <section class="rk2-regulatory-identity" aria-label="Identificação e situação na SUSEP">
        <div class="rk2-regulatory-identity__head">
          <span>Quem estamos avaliando</span>
          <strong>Identificação e situação na SUSEP</strong>
        </div>
        <p>Estes dados confirmam qual pessoa jurídica está sendo analisada e em que situação ela aparece nas fontes regulatórias usadas pela ferramenta.</p>
        <div class="rk2-regulatory-identity__grid">
          ${facts.map(([label, value]) => `<div><small>${esc(label)}</small><strong>${esc(publicText(value))}</strong></div>`).join("")}
        </div>
      </section>
    `;
  }

  function renderAssessment(profile) {
    const a = profile.assessment || {};
    if (a.availability === "available" || a.availability === "incomplete") {
      const label = a.availability === "incomplete" ? "Leitura com limites" : "O que os sinais permitem dizer";
      return `
        <section class="rk2-assessment">
          <div class="rk2-assessment__eyebrow">${esc(label)}</div>
          <h3>${esc(publicText(a.headline || "Avaliação conjunta"))}</h3>
          <p class="rk2-assessment__summary">${esc(publicText(a.summary || ""))}</p>
          ${a.why_it_matters ? `<p class="rk2-assessment__why">${esc(publicText(a.why_it_matters))}</p>` : ""}
          ${a.mandatory_limit ? `<p class="rk2-assessment__limit">${esc(publicText(a.mandatory_limit))}</p>` : ""}
        </section>
      `;
    }

    const reason = a.reason === "brand_never_inherits_entity_assessment" || isMarketIdentity(profile)
      ? "Esta identidade não herda a avaliação da seguradora relacionada. Abra a entidade regulada para consultar os sinais próprios dela."
      : "Esta empresa não participa da mesma análise comparativa das seguradoras autorizadas. O perfil continua útil para identificar quem ela é, suas relações e os dados disponíveis.";

    return `
      <section class="rk2-assessment rk2-assessment--secondary">
        <div class="rk2-assessment__eyebrow">Escopo da avaliação</div>
        <h3>Sem avaliação conjunta própria neste perfil</h3>
        <p class="rk2-assessment__summary">${esc(reason)}</p>
      </section>
    `;
  }

  function signalMetricHTML(label, value) {
    return value ? `<div class="rk2-signal__metric"><span>${esc(label)}: </span><strong>${esc(value)}</strong></div>` : "";
  }

  function renderSignals(profile) {
    const financial = profile.assessment?.financial;
    const conduct = profile.assessment?.conduct;
    if (!financial && !conduct) return "";

    const capital = financial?.capital;
    const liquidity = financial?.liquidity;
    const observed = conduct?.technical?.observed_complaints_12m;
    const capitalRatio = metricValue(capital?.technical?.ratio);
    const iltRatio = metricValue(liquidity?.technical?.ratio);
    const conductText = conduct?.relationship_context
      ? "Há uma relação documentada que exige cuidado para comparar as reclamações desta empresa. Veja o contexto de identidade logo abaixo."
      : (conduct?.plain_language || "Não há informação suficiente para esta leitura.");

    return `
      <section class="rk2-signal-grid" aria-label="Sinais principais">
        <article class="rk2-signal" data-tone="${signalTone(capital)}">
          <div class="rk2-signal__head">
            <strong>Capital</strong>
            <span class="rk2-signal__state">${esc(LABELS.capital[capital?.state] || "Sem conclusão")}</span>
          </div>
          <p>${esc(publicText(capital?.plain_language || "Não há informação suficiente para esta leitura."))}</p>
          ${capitalRatio !== null ? signalMetricHTML("PLA/CMR", formatNumber(capitalRatio, 2)) : ""}
        </article>
        <article class="rk2-signal" data-tone="${signalTone(liquidity)}">
          <div class="rk2-signal__head">
            <strong>Liquidez</strong>
            <span class="rk2-signal__state">${esc(LABELS.liquidity[liquidity?.state] || "Sem conclusão")}</span>
          </div>
          <p>${esc(publicText(liquidity?.plain_language || "Não há informação suficiente para esta leitura."))}</p>
          ${iltRatio !== null ? signalMetricHTML("ILT", formatNumber(iltRatio, 2)) : ""}
        </article>
        <article class="rk2-signal" data-tone="${signalTone(conduct)}">
          <div class="rk2-signal__head">
            <strong>Reclamações</strong>
            <span class="rk2-signal__state">${esc(LABELS.conduct[conduct?.state] || "Sem conclusão")}</span>
          </div>
          <p>${esc(publicText(conductText))}</p>
          ${canDisplay(observed) ? signalMetricHTML("Reclamações observadas", formatInteger(observed.value)) : ""}
        </article>
      </section>
    `;
  }

  function contextCard(label, title, text, profileIds = [], options = {}) {
    const links = profileIds
      .filter((id) => state.entryByProfileId.has(id))
      .map((id) => `<button type="button" data-open-profile="${esc(id)}">Abrir perfil relacionado</button>`)
      .join("");
    const meta = options.meta ? `<small class="rk2-context-card__meta">${esc(options.meta)}</small>` : "";
    const body = text
      ? (options.collapsible
        ? `<details class="rk2-context-card__details"><summary>Entenda esta relação</summary><p>${esc(publicText(text))}</p></details>`
        : `<p>${esc(publicText(text))}</p>`)
      : "";
    return `
      <article class="rk2-context-card">
        <span class="rk2-context-card__label">${esc(label)}</span>
        <strong>${esc(publicText(title))}</strong>
        ${meta}
        ${body}
        ${links ? `<div class="rk2-context-card__links">${links}</div>` : ""}
      </article>
    `;
  }

  function renderRelationships(profile) {
    const cards = [];

    if (profile.profile_kind === "brand") {
      (profile.relationships || []).forEach((rel) => {
        cards.push(contextCard(
          rel.relationship_type === "risk_carrier" ? "Seguradora relacionada ao risco" : "Relação documentada",
          rel.target_name || "Entidade relacionada",
          rel.scope || rel.evidence?.fact || "",
          rel.target_profile_id ? [rel.target_profile_id] : [],
          { collapsible: Boolean(rel.scope && rel.evidence?.fact) }
        ));
      });
    } else {
      const rc = profile.relationship_context || {};
      const group = rc.economic_group;
      if (group?.group_name) {
        cards.push(contextCard(
          "Grupo econômico observado",
          group.group_name,
          group.public_note || "Grupo é contexto; não prova sucessão ou transferência de carteira.",
          (group.related_entities || []).slice(0, 4).map((row) => row.profile_id)
        ));
      }

      (rc.brands || []).forEach((brand) => {
        cards.push(contextCard(
          "Marca relacionada",
          brand.name || "Marca",
          brand.scope || "Relação verificada no cadastro.",
          brand.profile_id ? [brand.profile_id] : []
        ));
      });

      (rc.direct_relationships || []).forEach((rel) => {
        cards.push(contextCard(
          "Histórico societário ou sucessão",
          rel.target_name || rel.relationship_type || "Relação documentada",
          rel.scope || rel.evidence?.fact || "",
          rel.target_profile_id ? [rel.target_profile_id] : []
        ));
      });

      (rc.conduct_reconciliation || []).forEach((rel) => {
        const targets = (rel.targets || []).map((row) => row.profile_id).filter(Boolean);
        const names = (rel.targets || []).map((row) => row.name).filter(Boolean).join(", ");
        const contractContext = rel.public_context || profile.assessment?.conduct?.relationship_context || "";
        const fallbackContext = rel.pressure_policy === "brand_specific_exposure_required"
          ? "A relação é documentada, mas ainda não existe uma medida de tamanho específica e comparável que permita dizer se o volume de reclamações é alto ou baixo sem distorção."
          : "Relação preservada para explicar contra quem a reclamação é registrada e quem assume o risco do seguro.";
        const meta = [
          rel.scope ? `Escopo: ${rel.scope}` : "",
          rel.verified_as_of ? `Verificado em ${rel.verified_as_of}` : "",
        ].filter(Boolean).join(" · ");
        cards.push(contextCard(
          "Contexto importante para interpretar reclamações",
          names || "Relação documentada com outra seguradora",
          contractContext || fallbackContext,
          targets,
          { collapsible: true, meta }
        ));
      });
    }

    if (profile.lifecycle?.is_historical && profile.lifecycle?.successor_profile_id) {
      cards.unshift(contextCard(
        "Sucessão",
        profile.lifecycle.successor_name || "Entidade sucessora",
        "A avaliação atual deve ser consultada na sucessora e não transferida retroativamente.",
        [profile.lifecycle.successor_profile_id]
      ));
    }

    if (!cards.length) return "";
    return `
      <section class="rk2-context">
        <h3>Identidade e relações relevantes</h3>
        <div class="rk2-context-grid">${cards.join("")}</div>
      </section>
    `;
  }

  function sandboxMetricsBlock(context) {
    if (!context || context.availability !== "available") return "";
    const metrics = context.metrics || {};
    const blocks = [];
    if (canDisplay(metrics.complaints)) blocks.push(["Reclamações", formatInteger(metrics.complaints.value)]);
    if (canDisplay(metrics.response_rate)) blocks.push(["Taxa de resposta", formatPercent(metrics.response_rate.value)]);
    if (canDisplay(metrics.average_satisfaction)) blocks.push(["Satisfação média", formatNumber(metrics.average_satisfaction.value, 2)]);
    const months = context.trajectory_context?.months_observed;
    if (canDisplay(months)) blocks.push(["Meses observados", formatInteger(months.value)]);

    return `
      <section class="rk2-sandbox">
        <h3>Reclamações e atendimento no Sandbox</h3>
        <p>${esc(context.plain_language || "Há informações de reclamações e atendimento disponíveis para este participante do Sandbox.")}</p>
        ${blocks.length ? `<div class="rk2-sandbox-metrics">${blocks.map(([label, value]) => `
          <div class="rk2-sandbox-metric"><small>${esc(label)}</small><strong>${esc(value)}</strong></div>
        `).join("")}</div>` : ""}
        ${context.trajectory_context?.public_limit ? `<p>${esc(context.trajectory_context.public_limit)}</p>` : ""}
      </section>
    `;
  }

  function renderSandbox(profile) {
    return sandboxMetricsBlock(profile.sandbox_conduct_context || profile.sandbox_conduct);
  }

  function technicalHelpFor(profile, key, displayValue) {
    const a = profile.assessment || {};
    const conduct = a.conduct || {};
    const technical = conduct.technical || {};
    const expected = technical.expected_complaints_12m;
    const ratio = technical.observed_expected_ratio;
    const comparable = technical.comparable_months;

    const common = {
      financial_period: {
        title: "Foto financeira de referência",
        what: "É o mês ao qual pertencem os números de capital e liquidez mostrados neste perfil. Eles representam uma fotografia daquele momento, e não uma média dos últimos 12 meses.",
        importance: "Usar a mesma data para capital e liquidez evita comparar números de períodos diferentes e ajuda a enxergar a situação financeira naquele ponto do tempo.",
        interpret: "Leia PLA/CMR e ILT como uma foto. Uma foto merece atenção quando mostra pressão, mas não prova sozinha uma tendência; o histórico, quando disponível, é usado para observar o filme da empresa.",
      },
      pla_cmr: {
        title: "Capital disponível ÷ mínimo exigido (PLA/CMR)",
        what: "Compara o patrimônio considerado para fins prudenciais com o Capital Mínimo Requerido da seguradora.",
        importance: "O capital funciona como uma margem de proteção para absorver perdas inesperadas e sustentar a capacidade financeira da operação. O mínimo é definido pelas regras prudenciais aplicáveis às seguradoras.",
        interpret: "1,0 significa que o patrimônio considerado alcança exatamente o mínimo exigido. Abaixo de 1,0 há insuficiência frente ao requisito e isso merece atenção. Acima de 1,0 o requisito está atendido, mas valores cada vez maiores não significam, sem limite, uma seguradora cada vez melhor. Ficar abaixo do mínimo também não prova, sozinho, insolvência ou falta de pagamento de sinistros.",
      },
      ilt: {
        title: "Liquidez total: recursos ÷ compromissos (ILT)",
        what: "Compara recursos que podem ser realizados no curto e no longo prazo com compromissos também de curto e longo prazo.",
        importance: "A liquidez ajuda a perceber se a base de recursos acompanha as obrigações da seguradora. Quando essa relação fica pressionada, cumprir compromissos ao longo do tempo pode exigir mais atenção financeira.",
        interpret: "1,0 representa equilíbrio aritmético entre os recursos e os compromissos considerados pela fórmula. Abaixo de 1,0 há pressão nessa relação; acima de 1,0, a referência aritmética está atendida. Esse ponto de 1,0 não é um limite prudencial oficial da SUSEP e não prevê, sozinho, atraso ou falta de pagamento de uma obrigação específica.",
      },
      complaints_observed: {
        title: "Reclamações observadas",
        what: "É a quantidade de reclamações registrada na parte da janela que pôde ser usada na comparação.",
        importance: "A contagem é o ponto de partida, mas não basta para comparar empresas: uma seguradora com operação muito maior pode acumular mais reclamações simplesmente porque atende um volume maior de negócios.",
        interpret: "Não leia esse número como alto ou baixo isoladamente. Ele ganha sentido quando é confrontado com o tamanho da operação e com a referência esperada para os mesmos meses.",
      },
      premium_direct_12m: {
        title: "Tamanho da operação",
        what: "Usamos o volume de prêmios diretos de seguros como uma medida econômica do tamanho da operação. Em seguros, “prêmio” é o valor contabilizado da atividade de seguros; não significa recompensa ou bônus.",
        importance: "Essa régua permite colocar a quantidade de reclamações em perspectiva. Sem ela, uma empresa pequena e uma empresa muito maior seriam comparadas como se tivessem a mesma exposição.",
        interpret: "O número serve para dimensionar a operação, não para medir qualidade. Também não equivale ao número de clientes, apólices ou sinistros.",
      },
      complaints_context: {
        title: "Referência para interpretar as reclamações",
        what: (() => {
          const parts = [];
          if (canDisplay(expected)) parts.push(`a referência proporcional ao tamanho é ${formatNumber(expected.value, 1)} reclamações`);
          if (canDisplay(comparable)) parts.push(`a comparação usa ${formatInteger(comparable.value)} meses`);
          return parts.length
            ? `A ferramenta estima quantas reclamações seriam esperadas para uma operação desse tamanho nos meses em que os dados podem ser comparados; neste perfil, ${parts.join(" e ")}.`
            : "A ferramenta estima uma referência de reclamações proporcional ao tamanho da operação e usa apenas os meses em que reclamações e tamanho podem ser comparados.";
        })(),
        importance: "A pergunta deixa de ser apenas “quantas reclamações existem?” e passa a ser “esse volume é maior ou menor do que esperaríamos para uma operação desse tamanho?”.",
        interpret: (() => {
          if (canDisplay(ratio)) {
            const r = Number(ratio.value);
            const direction = r > 1 ? "acima" : r < 1 ? "abaixo" : "igual";
            return `A razão observadas/esperadas resume essa comparação: 1,0 significa coincidência com a referência. Neste perfil, ${formatNumber(r, 3)}× coloca o volume observado ${direction} da referência. A direção, sozinha, ainda não determina a conclusão: quantidade de meses e incerteza da amostra também entram na leitura.`;
          }
          return "A razão observadas/esperadas resume a comparação: 1,0 significa coincidência com a referência; acima de 1 indica mais reclamações do que a referência proporcional ao tamanho e abaixo de 1 indica menos. A direção, sozinha, não determina a conclusão.";
        })(),
      },
    };

    return common[key] || null;
  }

  function technicalRows(profile) {
    const rows = [];
    const a = profile.assessment || {};
    if (a.financial?.reference_period) rows.push({ group: "financial", key: "financial_period", label: "Foto financeira de referência", value: formatPeriod(a.financial.reference_period) });

    const cap = a.financial?.capital?.technical?.ratio;
    if (canDisplay(cap)) rows.push({ group: "financial", key: "pla_cmr", label: "Capital disponível ÷ mínimo exigido (PLA/CMR)", value: formatNumber(cap.value, 4) });
    const ilt = a.financial?.liquidity?.technical?.ratio;
    if (canDisplay(ilt)) rows.push({ group: "financial", key: "ilt", label: "Liquidez total: recursos ÷ compromissos (ILT)", value: formatNumber(ilt.value, 4) });

    const conduct = a.conduct?.technical || {};
    if (canDisplay(conduct.observed_complaints_12m)) rows.push({ group: "conduct", key: "complaints_observed", label: "Reclamações observadas", value: formatInteger(conduct.observed_complaints_12m.value) });
    const premium = a.operation_context?.insurance_premium_direct_12m;
    if (canDisplay(premium)) rows.push({ group: "conduct", key: "premium_direct_12m", label: "Tamanho da operação · volume de seguros em 12 meses", value: formatBRLCompact(premium.value) });

    const contextParts = [];
    if (canDisplay(conduct.expected_complaints_12m)) contextParts.push(`referência ${formatNumber(conduct.expected_complaints_12m.value, 1)}`);
    if (canDisplay(conduct.comparable_months)) contextParts.push(`${formatInteger(conduct.comparable_months.value)} meses`);
    if (contextParts.length || canDisplay(conduct.observed_expected_ratio)) {
      rows.push({
        group: "conduct",
        key: "complaints_context",
        label: "Referência para interpretar as reclamações",
        value: contextParts.join(" · ") || `${formatNumber(conduct.observed_expected_ratio.value, 3)}×`,
      });
    }

    return rows;
  }

  function renderTechnicalHelp(profile, row, index) {
    const help = technicalHelpFor(profile, row.key, row.value.replace?.("×", "") || row.value);
    if (!help) return { trigger: "", panel: "" };
    const id = `rk2-tech-help-${index}`;
    return {
      trigger: `<button class="rk2-tech-help-trigger" type="button" data-tech-help-toggle aria-expanded="false" aria-controls="${esc(id)}" aria-label="Entender ${esc(row.label)}">?</button>`,
      panel: `
        <div class="rk2-tech-help" id="${esc(id)}" data-tech-help hidden>
          <dl class="rk2-tech-help__summary">
            <div><dt>O que é</dt><dd>${esc(help.what)}</dd></div>
            <div><dt>Por que importa</dt><dd>${esc(help.importance)}</dd></div>
            <div><dt>Como interpretar</dt><dd>${esc(help.interpret)}</dd></div>
          </dl>
          <button type="button" class="rk2-tech-help__method" data-section-target="#metodologia">Metodologia e fontes</button>
        </div>
      `,
    };
  }

  function renderTechnical(profile) {
    const rows = technicalRows(profile);
    if (!rows.length) return "";
    const groups = [
      ["financial", "Saúde financeira", "Capital e liquidez respondem perguntas diferentes e usam a mesma foto financeira de referência."],
      ["conduct", "Reclamações e tamanho da operação", "A quantidade de reclamações é lida em conjunto com o tamanho da operação e com o período que realmente pode ser comparado."],
    ];
    return `
      <details class="rk2-technical">
        <summary>Ver números e entender como eles foram lidos</summary>
        <div class="rk2-technical__body">
          ${groups.map(([group, title, intro]) => {
            const groupRows = rows.filter((row) => row.group === group);
            if (!groupRows.length) return "";
            return `
              <section class="rk2-data-group rk2-data-group--${esc(group)}" aria-label="${esc(title)}">
                <div class="rk2-data-group__head"><strong>${esc(title)}</strong><p>${esc(intro)}</p></div>
                ${groupRows.map((row) => {
                  const globalIndex = rows.indexOf(row);
                  const help = renderTechnicalHelp(profile, row, globalIndex);
                  return `
                    <div class="rk2-data-item">
                      <div class="rk2-data-row">
                        <span class="rk2-data-row__label"><span class="rk2-data-row__label-text">${esc(row.label)}</span>${help.trigger}</span>
                        <strong class="rk2-data-row__value">${esc(row.value)}</strong>
                      </div>
                      ${help.panel}
                    </div>
                  `;
                }).join("")}
              </section>
            `;
          }).join("")}
        </div>
      </details>
    `;
  }

  function closeTechnicalHelpers(except = null) {
    document.querySelectorAll("[data-tech-help-toggle]").forEach((button) => {
      if (button === except) return;
      const panelId = button.getAttribute("aria-controls");
      const panel = panelId ? document.getElementById(panelId) : null;
      button.setAttribute("aria-expanded", "false");
      if (panel) panel.hidden = true;
    });
  }

  function toggleTechnicalHelp(button) {
    const panelId = button.getAttribute("aria-controls");
    const panel = panelId ? document.getElementById(panelId) : null;
    if (!panel) return;
    const willOpen = button.getAttribute("aria-expanded") !== "true";
    closeTechnicalHelpers(willOpen ? button : null);
    button.setAttribute("aria-expanded", willOpen ? "true" : "false");
    panel.hidden = !willOpen;
  }

  function isOrdinaryInsurer(profile) {
    return profile.profile_kind === "entity" && profile.regulatory?.filter_bucket === "insurers";
  }

  function profileOriginAction() {
    const origin = history.state?.rk2?.origin;
    if (!origin?.label) return "";
    return `<button class="rk2-return" type="button" data-history-back>← ${esc(origin.label)}</button>`;
  }

  function whatsAppShareHref(profile) {
    const text = `Veja esta consulta sobre ${profileName(profile)} na ferramenta da Sanida: ${profileShareURL(profile.profile_id)}`;
    return `https://wa.me/?text=${encodeURIComponent(text)}`;
  }

  function renderProfile(profile) {
    const name = profileName(profile);
    const limits = profile.limits || [];
    const tone = assessmentTone(profile);
    const ordinary = isOrdinaryInsurer(profile);
    const complexIdentity = isMarketIdentity(profile) || profile.profile_kind === "brand" || isHistoricalProfile(profile) || isSandboxProfile(profile);

    const primaryContent = ordinary
      ? `${renderAssessment(profile)}${renderIdentityDetails(profile)}${renderSignals(profile)}${renderRelationships(profile)}${renderSandbox(profile)}`
      : `${renderQuickAnswer(profile, isMarketIdentity(profile) ? "Quem é esta empresa" : "Resposta rápida")}${renderRelationships(profile)}${renderSandbox(profile)}${renderAssessment(profile)}${renderSignals(profile)}`;

    return `
      <div class="rk2-result-shell">
        ${profileOriginAction()}
        <div class="rk2-profile ${complexIdentity ? "rk2-profile--identity-first" : ""}" data-tone="${tone}">
          <div class="rk2-profile__head">
            <div>
              <span class="rk2-profile__type">${esc(publicText(profile.public_summary?.headline || profileStatus(profile)))}</span>
              <h2 tabindex="-1" data-profile-heading>${esc(name)}</h2>
              <div class="rk2-profile__legal">${esc(identityLine(profile))}</div>
            </div>
            <div class="rk2-profile__status">${esc(profileStatus(profile))}</div>
          </div>

          ${primaryContent}
          ${renderTechnical(profile)}

          <div class="rk2-profile__foot">
            <ul class="rk2-profile__limits">
              ${limits.map((item) => `<li>• ${esc(publicText(item))}</li>`).join("")}
            </ul>
            <div class="rk2-profile__actions">
              ${ordinary ? `<button class="rk2-btn rk2-btn--primary" type="button" data-compare-profile="${esc(profile.profile_id)}">Comparar esta seguradora</button>` : ""}
              <button class="rk2-btn rk2-btn--ghost" type="button" data-share-profile="${esc(profile.profile_id)}">Compartilhar consulta</button>
              <a class="rk2-btn rk2-btn--ghost" href="${esc(whatsAppShareHref(profile))}" target="_blank" rel="noopener noreferrer">WhatsApp</a>
              <button class="rk2-btn rk2-btn--ghost" type="button" data-new-search>Nova consulta</button>
            </div>
            <div class="rk2-profile__explore" aria-label="Explorar outras áreas da ferramenta">
              <span>Explorar também:</span>
              <button type="button" data-section-target="#explorar">Rankings por critério</button>
              <button type="button" data-section-target="#metodologia">Metodologia</button>
            </div>
          </div>
        </div>
      </div>
    `;
  }

  async function openProfile(profileId, { updateHistory = true, scroll = true, origin = null } = {}) {
    const entry = state.entryByProfileId.get(profileId);
    if (!entry) {
      showToast("Esse perfil não foi localizado no catálogo público atual.");
      return false;
    }

    const requestToken = ++state.profileRequestToken;
    root.classList.add("rk2--result-mode");
    els.result.hidden = false;
    els.result.innerHTML = `<div class="rk2-result-shell"><div class="rk2-loading-card">Carregando perfil…</div></div>`;
    hideSuggestions();
    els.searchInput.value = entry.name || "";
    els.searchClear.hidden = false;

    try {
      const profile = await loadProfile(profileId);
      if (requestToken !== state.profileRequestToken) return false;
      root.classList.remove("rk2--search-editing");
      state.currentProfileId = profileId;
      updateActiveContext(profile);
      if (updateHistory) updateURL(profileId, origin);
      if (requestToken !== state.profileRequestToken) return false;
      els.result.innerHTML = renderProfile(profile);
      requestAnimationFrame(() => {
        $("[data-profile-heading]", els.result)?.focus({ preventScroll: true });
        if (scroll) window.scrollTo({ top: 0, behavior: "smooth" });
      });
      return true;
    } catch (error) {
      if (requestToken !== state.profileRequestToken) return false;
      console.error(error);
      els.result.innerHTML = `<div class="rk2-result-shell"><div class="rk2-error"><strong>Não foi possível abrir o perfil.</strong><p>${esc(error.message)}</p><button class="rk2-btn rk2-btn--ghost" type="button" data-retry-profile="${esc(profileId)}">Tentar novamente</button></div></div>`;
      return false;
    }
  }

  function resetSearchView({ updateHistory = true, scroll = true, focus = true } = {}) {
    state.profileRequestToken += 1;
    root.classList.remove("rk2--result-mode", "rk2--search-editing");
    state.currentProfileId = null;
    els.result.hidden = true;
    els.result.innerHTML = "";
    els.searchInput.value = "";
    els.searchClear.hidden = true;
    if (els.activeContext) els.activeContext.hidden = true;
    hideSuggestions();
    if (updateHistory) clearURL();
    if (scroll) requestAnimationFrame(() => window.scrollTo({ top: 0, behavior: "smooth" }));
    if (focus) window.setTimeout(() => els.searchInput.focus({ preventScroll: true }), 250);
  }

  function explorerFor(profileId) {
    return state.explorerByProfileId.get(profileId) || null;
  }

  function compareCandidateEntries(query) {
    const source = state.insurerEntries.filter((entry) => !state.compareIds.includes(entry.profile_id));
    return findCandidates(query, source, 7).map((row) => row.entry);
  }

  function renderCompareResults(query) {
    const entries = compareCandidateEntries(query);
    state.compareSuggestions = entries;
    state.compareSuggestionIndex = -1;
    els.compareSearch.removeAttribute("aria-activedescendant");

    if (!normalize(query) || state.compareIds.length >= MAX_COMPARE) {
      els.compareResults.hidden = true;
      els.compareResults.innerHTML = "";
      els.compareSearch.setAttribute("aria-expanded", "false");
      return;
    }

    if (!entries.length) {
      els.compareResults.innerHTML = `<div class="rk2-mini-empty">Nenhuma seguradora comparável encontrada com esse nome.</div>`;
      els.compareResults.hidden = false;
      els.compareSearch.setAttribute("aria-expanded", "true");
      return;
    }

    els.compareResults.innerHTML = entries.map((entry, index) => `
      <button
        class="rk2-mini-result"
        id="rk2-compare-option-${index}"
        type="button"
        role="option"
        aria-selected="false"
        data-compare-index="${index}"
        data-compare-add="${esc(entry.profile_id)}"
      >
        <span>${esc(entry.name)}</span><small>${esc(entry.fip_code ? `SUSEP ${entry.fip_code}` : "Seguradora")}</small>
      </button>
    `).join("");
    els.compareResults.hidden = false;
    els.compareSearch.setAttribute("aria-expanded", "true");
  }

  function setActiveCompareSuggestion(index) {
    const buttons = $$('[data-compare-index]', els.compareResults);
    if (!buttons.length) return;
    state.compareSuggestionIndex = Math.max(0, Math.min(index, buttons.length - 1));
    buttons.forEach((button, i) => {
      const active = i === state.compareSuggestionIndex;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-selected", active ? "true" : "false");
    });
    const active = buttons[state.compareSuggestionIndex];
    if (active) {
      els.compareSearch.setAttribute("aria-activedescendant", active.id);
      active.scrollIntoView({ block: "nearest" });
    }
  }

  function hideCompareResults() {
    els.compareResults.hidden = true;
    els.compareSearch.setAttribute("aria-expanded", "false");
    els.compareSearch.removeAttribute("aria-activedescendant");
    state.compareSuggestionIndex = -1;
  }

  function renderCompareChips() {
    const isFull = state.compareIds.length >= MAX_COMPARE;
    els.compareCount.textContent = `${state.compareIds.length} de ${MAX_COMPARE} selecionadas`;
    els.compareSearch.disabled = isFull;
    els.compareSearch.setAttribute("aria-disabled", String(isFull));
    els.compareSearch.placeholder = isFull ? "Limite de 4 seguradoras atingido" : "Digite um nome…";
    if (els.compareClear) els.compareClear.hidden = state.compareIds.length === 0;
    if (els.compareShare) {
      const canShare = state.compareIds.length >= 2;
      els.compareShare.disabled = !canShare;
      els.compareShare.setAttribute("aria-disabled", String(!canShare));
      els.compareShare.title = canShare
        ? "Compartilhar esta comparação"
        : "Selecione ao menos duas seguradoras para compartilhar a comparação";
    }
    els.compareChips.innerHTML = state.compareIds.map((id) => {
      const entry = state.entryByProfileId.get(id);
      return `<span class="rk2-chip">${esc(entry?.name || id)}<button type="button" data-compare-remove="${esc(id)}" aria-label="Remover ${esc(entry?.name || "seguradora")}">×</button></span>`;
    }).join("");
  }

  function compareSignal(profile, key) {
    if (key === "capital") {
      const s = profile.assessment?.financial?.capital;
      const ratio = metricValue(s?.technical?.ratio);
      return {
        key,
        state: s?.state || null,
        tone: s?.tone || "missing",
        label: LABELS.capital[s?.state] || "Sem conclusão",
        text: publicText(s?.plain_language || "Dado não disponível."),
        metric: ratio !== null ? `PLA/CMR ${formatNumber(ratio, 2)}` : "",
      };
    }

    if (key === "liquidity") {
      const s = profile.assessment?.financial?.liquidity;
      const ratio = metricValue(s?.technical?.ratio);
      return {
        key,
        state: s?.state || null,
        tone: s?.tone || "missing",
        label: LABELS.liquidity[s?.state] || "Sem conclusão",
        text: publicText(s?.plain_language || "Dado não disponível."),
        metric: ratio !== null ? `ILT ${formatNumber(ratio, 2)}` : "",
      };
    }

    const s = profile.assessment?.conduct;
    const observed = s?.technical?.observed_complaints_12m;

    return {
      key,
      state: s?.state || null,
      tone: s?.tone || "missing",
      label: LABELS.conduct[s?.state] || "Sem conclusão",
      text: publicText(s?.plain_language || "Dado não disponível."),
      metric: canDisplay(observed)
        ? `${formatInteger(observed.value)} reclamações observadas`
        : "",
    };
  }

  function compareAssessment(profile) {
    const a = profile.assessment || {};
    const available = a.availability === "available";

    let tone = "missing";
    if (available) {
      if (a.public_class === "favorable_reading") tone = "favorable";
      else if (a.public_class === "prudential_warning") tone = "adverse";
      else if (a.public_class === "attention") tone = "caution";
      else tone = "neutral";
    }

    return {
      tone,
      label: publicText(
        a.headline ||
          (available
            ? "Leitura conjunta disponível"
            : "Análise conjunta incompleta")
      ),
      text: publicText(
        a.summary ||
          "Ainda não há dados suficientes para formar uma conclusão conjunta segura."
      ),
    };
  }

  function compareVisual(signal) {
    const tone = signal?.tone || "missing";
    const stateName = signal?.state || "";

    // Conduta sem diferença clara é informativa, não favorável/adversa.
    if (stateName === "not_distinguishable_from_expected") {
      return { symbol: "≈", tone: "neutral", label: "Sem diferença clara" };
    }

    if (tone === "favorable") {
      return { symbol: "✓", tone: "favorable", label: "Sem alerta neste critério" };
    }

    if (tone === "adverse") {
      return { symbol: "✕", tone: "adverse", label: "Há alerta neste critério" };
    }

    if (tone === "caution" || tone === "attention") {
      return { symbol: "!", tone: "caution", label: "Merece atenção" };
    }

    return { symbol: "—", tone: "missing", label: "Sem conclusão segura" };
  }

  function compareCompanyMeta(profile) {
    const bits = [];
    if (profile.identity?.fip_code) bits.push(`SUSEP ${profile.identity.fip_code}`);
    if (profile.identity?.cnpj) bits.push(formatCNPJ(profile.identity.cnpj));
    return bits.join(" · ");
  }

  function compareCell(profile, key) {
    if (key === "assessment") {
      const signal = compareAssessment(profile);
      return { ...signal, visual: compareVisual(signal) };
    }

    const signal = compareSignal(profile, key);
    return { ...signal, visual: compareVisual(signal) };
  }

  function renderMatrixCell(profile, key) {
    const cell = compareCell(profile, key);

    return `
      <div class="rk2-cmp-cell" data-tone="${esc(cell.visual.tone)}">
        <span
          class="rk2-cmp-mark"
          data-tone="${esc(cell.visual.tone)}"
          aria-label="${esc(cell.visual.label)}"
          title="${esc(cell.visual.label)}"
        >${esc(cell.visual.symbol)}</span>
        <div class="rk2-cmp-cell__body">
          <strong>${esc(cell.label)}</strong>
          ${cell.metric ? `<small>${esc(cell.metric)}</small>` : ""}
        </div>
      </div>
    `;
  }

  function renderCompareMatrix(profiles) {
    const rows = [
      {
        key: "assessment",
        title: "Síntese dos sinais",
        help: "Síntese conjunta dos sinais disponíveis, sem transformar os critérios em uma nota.",
      },
      {
        key: "capital",
        title: "Capital mínimo exigido",
        help: "Verifica se o patrimônio ajustado alcança o capital mínimo exigido na competência analisada.",
      },
      {
        key: "liquidity",
        title: "Liquidez",
        help: "Mostra se o indicador de liquidez ficou abaixo da referência aritmética usada pela metodologia.",
      },
      {
        key: "conduct",
        title: "Reclamações",
        help: "Compara reclamações ao tamanho da operação somente quando os dados são realmente comparáveis.",
      },
    ];

    const companyColumns = profiles.map((profile) => `
      <div class="rk2-cmp-company">
        <strong>${esc(profileName(profile))}</strong>
        <small>${esc(compareCompanyMeta(profile))}</small>
        <button
          type="button"
          class="rk2-cmp-profile-link"
          data-open-profile="${esc(profile.profile_id)}"
        >Ver perfil</button>
      </div>
    `).join("");

    const matrixRows = rows.map((row) => `
      <div class="rk2-cmp-criterion">
        <strong>${esc(row.title)}</strong>
        <small>${esc(row.help)}</small>
      </div>
      ${profiles.map((profile) => renderMatrixCell(profile, row.key)).join("")}
    `).join("");

    const mobileCards = profiles.map((profile) => `
      <section class="rk2-cmp-mobile-card">
        <div class="rk2-cmp-mobile-card__title">
          <strong>${esc(profileName(profile))}</strong>
          <small>${esc(compareCompanyMeta(profile))}</small>
          <button
            type="button"
            class="rk2-cmp-profile-link"
            data-open-profile="${esc(profile.profile_id)}"
          >Ver perfil completo</button>
        </div>

        ${rows.map((row) => {
          const cell = compareCell(profile, row.key);
          return `
            <div class="rk2-cmp-mobile-row">
              <div class="rk2-cmp-mobile-row__criterion">
                <strong>${esc(row.title)}</strong>
              </div>
              <div class="rk2-cmp-mobile-row__value" data-tone="${esc(cell.visual.tone)}">
                <span
                  class="rk2-cmp-mark"
                  data-tone="${esc(cell.visual.tone)}"
                  aria-label="${esc(cell.visual.label)}"
                >${esc(cell.visual.symbol)}</span>
                <div>
                  <strong>${esc(cell.label)}</strong>
                  ${cell.metric ? `<small>${esc(cell.metric)}</small>` : ""}
                </div>
              </div>
            </div>
          `;
        }).join("")}
      </section>
    `).join("");

    return `
      <div class="rk2-cmp-legend" aria-label="Legenda da comparação">
        <span><b class="rk2-cmp-mark" data-tone="favorable">✓</b> sem alerta</span>
        <span><b class="rk2-cmp-mark" data-tone="adverse">✕</b> alerta</span>
        <span><b class="rk2-cmp-mark" data-tone="caution">!</b> atenção</span>
        <span><b class="rk2-cmp-mark" data-tone="neutral">≈</b> sem diferença clara</span>
        <span><b class="rk2-cmp-mark" data-tone="missing">—</b> sem conclusão</span>
      </div>

      <div
        class="rk2-cmp-matrix"
        style="--rk2-cmp-count:${profiles.length}"
        aria-label="Comparação lado a lado"
      >
        <div class="rk2-cmp-corner">
          <strong>Critério</strong>
          <small>O que está sendo comparado</small>
        </div>
        ${companyColumns}
        ${matrixRows}
      </div>

      <div class="rk2-cmp-mobile">
        ${mobileCards}
      </div>
    `;
  }

  async function renderComparison() {
    const requestToken = ++state.compareRequestToken;
    const requestedIds = [...state.compareIds];
    renderCompareChips();
    hideCompareResults();

    if (requestedIds.length < 2) {
      els.compareEmpty.hidden = false;
      els.compareGrid.hidden = true;
      els.compareGrid.innerHTML = "";
      return;
    }

    els.compareEmpty.hidden = true;
    els.compareGrid.hidden = false;
    els.compareGrid.innerHTML = `<div class="rk2-loading-card">Carregando comparação…</div>`;

    try {
      const profiles = await Promise.all(requestedIds.map((id) => loadProfile(id)));
      if (requestToken !== state.compareRequestToken) return;
      if (requestedIds.join("|") !== state.compareIds.join("|")) return;
      els.compareGrid.innerHTML = renderCompareMatrix(profiles);
    } catch (error) {
      if (requestToken !== state.compareRequestToken) return;
      console.error(error);
      els.compareGrid.innerHTML = `
        <div class="rk2-error">
          <strong>Não foi possível montar a comparação.</strong>
          <p>${esc(error.message)}</p>
          <button class="rk2-btn rk2-btn--ghost" type="button" data-retry-comparison>Tentar novamente</button>
        </div>
      `;
    }
  }

  function addCompare(profileId) {
    if (!state.insurerEntries.some((entry) => entry.profile_id === profileId)) {
      showToast("Somente seguradoras da lista regulatória atual podem ser adicionadas à comparação.");
      return false;
    }
    if (state.compareIds.includes(profileId)) {
      showToast("Essa seguradora já está na comparação.");
      return false;
    }
    if (state.compareIds.length >= MAX_COMPARE) {
      showToast("A comparação aceita até quatro seguradoras.");
      return false;
    }
    state.compareIds.push(profileId);
    els.compareSearch.value = "";
    persistCurrentViewState();
    renderComparison();
    return true;
  }

  function removeCompare(profileId) {
    const previousLength = state.compareIds.length;
    state.compareIds = state.compareIds.filter((id) => id !== profileId);
    if (state.compareIds.length === previousLength) return false;
    persistCurrentViewState();
    renderComparison();
    return true;
  }

  function listFilteredEntries() {
    const q = normalize(els.listSearch.value);
    return state.insurerEntries.filter((entry) => {
      const explorer = explorerFor(entry.profile_id);
      const matchesText = !q || normalize(`${entry.name} ${entry.search_text || ""}`).includes(q);
      if (!matchesText) return false;
      if (state.listFilter === "eligible") return Boolean(state.explorer) && explorer?.assessment?.eligible === true;
      if (state.listFilter === "incomplete") return Boolean(state.explorer) && explorer?.assessment?.eligible !== true;
      return true;
    });
  }

  function renderList() {
    const entries = listFilteredEntries();
    const pages = Math.max(1, Math.ceil(entries.length / LIST_PAGE_SIZE));
    state.listPageNumber = Math.min(state.listPageNumber, pages);
    const start = (state.listPageNumber - 1) * LIST_PAGE_SIZE;
    const pageRows = entries.slice(start, start + LIST_PAGE_SIZE);

    els.listStatus.textContent = `${entries.length.toLocaleString("pt-BR")} seguradoras neste recorte.`;
    els.list.innerHTML = pageRows.length ? pageRows.map((entry) => {
      const explorer = explorerFor(entry.profile_id);
      const ready = explorer?.assessment?.eligible === true;
      const label = !state.explorer
        ? "Status da avaliação indisponível neste carregamento"
        : (ready ? "Avaliação comparável" : (explorer ? "Avaliação limitada pelos dados disponíveis" : "Avaliação ainda não disponível"));
      return `
        <article class="rk2-list-row">
          <div class="rk2-list-row__name"><strong>${esc(entry.name)}</strong><small>${esc(entry.disambiguation || "Seguradora")}</small></div>
          <div class="rk2-list-row__meta"><span class="rk2-list-badge ${ready ? "rk2-list-badge--ready" : ""}">${esc(label)}</span><button type="button" data-open-profile="${esc(entry.profile_id)}">Consultar</button></div>
        </article>
      `;
    }).join("") : `<div class="rk2-suggestion-empty">Nenhuma seguradora corresponde a esse filtro.</div>`;

    const end = Math.min(start + LIST_PAGE_SIZE, entries.length);
    els.listPage.textContent = `Página ${state.listPageNumber} de ${pages}`;
    els.listRange.textContent = entries.length ? `${start + 1}–${end} de ${entries.length}` : "0 seguradoras";
    els.listPrev.disabled = state.listPageNumber <= 1;
    els.listNext.disabled = state.listPageNumber >= pages;
  }

  function boardValue(board, row) {
    if (board.metric === "insurance_premium_direct_12m") return formatBRLCompact(row.premium_direct_12m);
    if (board.metric === "pla_cmr_ratio") return formatNumber(row.pla_cmr_ratio, 2);
    if (board.metric === "ilt") return formatNumber(row.ilt, 2);
    if (board.metric === "conduct_observed_expected_ratio") return formatNumber(row.conduct_pressure_ratio, 2);
    return "—";
  }

  async function renderBoard(kind, id, { scroll = true, persist = false } = {}) {
    const requestToken = ++state.boardRequestToken;
    state.activeBoard = { kind, id };
    if (persist) persistCurrentViewState();
    els.boardPanel.hidden = false;
    els.boardPanel.innerHTML = `<div class="rk2-loading-card">Carregando recorte…</div>`;
    try {
      const isCollection = kind === "collection";
      const folder = isCollection ? "collections" : "leaderboards";
      if (!/^[A-Za-z0-9_-]+$/.test(String(id || ""))) {
        throw new Error("O identificador do recorte é inválido.");
      }
      const board = await fetchJSON(
        `${folder}/${id}.json`,
        isCollection ? "v2_public_semantic_collection" : "v2_public_metric_leaderboard",
      );
      if (requestToken !== state.boardRequestToken) return;
      if (state.activeBoard?.kind !== kind || state.activeBoard?.id !== id) return;
      els.boardPanel.innerHTML = `
        <div class="rk2-board-head">
          <div class="rk2-board-head__top"><div><h3>${esc((EXPLORE_COPY[board.id]?.title) || publicText(board.title))}</h3><p>${esc(board.question || (isCollection ? "Coleção sem ordem interna de melhor ou pior." : ""))}</p></div><button class="rk2-board-close" type="button" data-board-close aria-label="Fechar recorte">×</button></div>
        </div>
        <div class="rk2-board-list">
          ${(board.entries || []).map((row, index) => {
            const profileId = `entity:${row.entity_id}`;
            return `<div class="rk2-board-row"><span class="rk2-board-rank">${isCollection ? "•" : `${row.leaderboard_rank || index + 1}º`}</span><div class="rk2-board-name"><strong>${esc(row.display_name || row.legal_name || row.entity_id)}</strong>${state.entryByProfileId.has(profileId) ? `<button type="button" data-open-profile="${esc(profileId)}">Abrir perfil</button>` : ""}</div>${isCollection ? "" : `<div class="rk2-board-value">${esc(boardValue(board,row))}</div>`}</div>`;
          }).join("")}
        </div>
        ${(board.caveats || []).length ? `<ul class="rk2-board-caveats">${board.caveats.map((item) => `<li>${esc(item)}</li>`).join("")}</ul>` : ""}
      `;
      if (scroll) requestAnimationFrame(() => els.boardPanel.scrollIntoView({ behavior: "smooth", block: "nearest" }));
    } catch (error) {
      if (requestToken !== state.boardRequestToken) return;
      console.error(error);
      els.boardPanel.innerHTML = `<div class="rk2-error"><strong>Não foi possível carregar esse recorte.</strong><p>${esc(error.message)}</p><button class="rk2-btn rk2-btn--ghost" type="button" data-retry-board data-board-kind="${esc(kind)}" data-board-id="${esc(id)}">Tentar novamente</button></div>`;
    }
  }

  function renderExploreIndex() {
    const boards = state.exploreIndex?.leaderboards || [];
    const collections = state.exploreIndex?.collections || [];

    els.exploreGrid.innerHTML = boards.map((board) => `
      <article class="rk2-explore-card">
        <h3>${esc((EXPLORE_COPY[board.id]?.title) || publicText(board.title))}</h3>
        <p>${esc((EXPLORE_COPY[board.id]?.question) || publicText(board.question || "Lista ordenada exclusivamente pelo critério indicado."))}</p>
        <div class="rk2-explore-card__foot"><span>Top ${board.top_positions || 10} · sem ranking geral</span><button class="rk2-btn rk2-btn--ghost" type="button" data-board-open="${esc(board.id)}">Ver lista</button></div>
      </article>
    `).join("") || `<div class="rk2-suggestion-empty">Nenhum leaderboard público disponível.</div>`;

    els.collectionsBody.innerHTML = collections.map((collection) => `
      <article class="rk2-collection-card">
        <strong>${esc(COLLECTION_COPY[collection.id] || publicText(collection.title))}</strong>
        <p>${esc((collection.caveats || ["Coleção sem ordem interna."])[0])}</p>
        <button type="button" data-collection-open="${esc(collection.id)}">Ver ${Number(collection.entity_count || 0).toLocaleString("pt-BR")} integrantes</button>
      </article>
    `).join("") || "Nenhuma coleção semântica disponível.";
  }


  function scrollToSection(selector) {
    const target = typeof selector === "string" ? $(selector) : selector;
    if (!target) return;
    const header = document.getElementById("menu");
    const headerHeight = header ? Math.min(header.getBoundingClientRect().height, 88) : 0;
    const top = window.scrollY + target.getBoundingClientRect().top - headerHeight - 14;
    window.scrollTo({ top: Math.max(0, top), behavior: "smooth" });
  }

  function openSearchEditor() {
    root.classList.add("rk2--search-editing");
    window.setTimeout(() => {
      els.searchInput.focus({ preventScroll: true });
      els.searchInput.select();
    }, 30);
  }

  async function restoreViewState(view) {
    state.restoringView = true;
    try {
      resetSearchView({ updateHistory: false, scroll: false, focus: false });
      if (!view || typeof view !== "object") return;

      state.compareIds = Array.isArray(view.compareIds)
        ? view.compareIds.filter((id) => state.insurerEntries.some((entry) => entry.profile_id === id)).slice(0, MAX_COMPARE)
        : [];
      await renderComparison();

      if (els.listSearch) els.listSearch.value = String(view.listSearch || "").slice(0, 120);
      if (["all", "eligible", "incomplete"].includes(view.listFilter)) state.listFilter = view.listFilter;
      if (!state.explorer && state.listFilter !== "all") state.listFilter = "all";
      const requestedPage = Number(view.listPageNumber || 1);
      state.listPageNumber = Number.isFinite(requestedPage) ? Math.max(1, Math.floor(requestedPage)) : 1;
      $$('[data-list-filter]').forEach((button) => button.classList.toggle("is-active", button.dataset.listFilter === state.listFilter));
      renderList();

      if (view.activeBoard?.kind && view.activeBoard?.id && ["leaderboard", "collection"].includes(view.activeBoard.kind)) {
        await renderBoard(view.activeBoard.kind, String(view.activeBoard.id), { scroll: false, persist: false });
      } else {
        state.boardRequestToken += 1;
        state.activeBoard = null;
        els.boardPanel.hidden = true;
        els.boardPanel.innerHTML = "";
      }

      await new Promise((resolve) => {
        requestAnimationFrame(() => {
          window.scrollTo({ top: Math.max(0, Number(view.scrollY || 0)), behavior: "auto" });
          requestAnimationFrame(resolve);
        });
      });
    } finally {
      state.restoringView = false;
    }
  }

  function originForElement(element) {
    if (!element) return null;
    if (element.closest("#rk2-board-panel") && state.activeBoard) {
      const title = $(".rk2-board-head h3", els.boardPanel)?.textContent?.trim();
      return { type: "board", label: title ? `Voltar para ${title}` : "Voltar para o ranking" };
    }
    if (element.closest("#rk2-compare-grid")) {
      return { type: "comparison", label: "Voltar à comparação", compareIds: [...state.compareIds] };
    }
    if (element.closest("#rk2-list")) return { type: "list", label: "Voltar à lista de seguradoras" };
    if (element.closest("#rk2-result") && state.currentProfileId) {
      return { type: "profile", label: "Voltar ao perfil anterior", profileId: state.currentProfileId };
    }
    return null;
  }

  function navigateFromProfileToSection(selector) {
    if (!state.currentProfileId) {
      scrollToSection(selector);
      return;
    }
    try {
      persistCurrentViewState({ force: true });
      const section = String(selector || "").replace(/^#/, "");
      const url = pageRouteURL();
      url.hash = encodeURIComponent(section);
      history.pushState(
        { rk2: { mode: "section", section }, rk2View: currentViewSnapshot() },
        "",
        `${url.pathname}${url.search}${url.hash}`,
      );
    } catch (error) {
      console.warn("Não foi possível registrar a navegação entre seções.", error);
    }
    resetSearchView({ updateHistory: false, scroll: false, focus: false });
    requestAnimationFrame(() => scrollToSection(selector));
  }

  function validComparisonIds(raw) {
    const seen = new Set();
    return String(raw || "")
      .split(",")
      .map((id) => id.trim())
      .filter((id) => {
        if (!id || seen.has(id)) return false;
        if (!state.insurerEntries.some((entry) => entry.profile_id === id)) return false;
        seen.add(id);
        return true;
      })
      .slice(0, MAX_COMPARE);
  }

  function safeHashSelector(hash) {
    if (!hash || hash === "#") return "";
    try {
      const decoded = decodeURIComponent(hash);
      return /^#[A-Za-z][A-Za-z0-9_-]*$/.test(decoded) ? decoded : "";
    } catch (_error) {
      return "";
    }
  }

  async function applyURLState(event = null) {
    window.clearTimeout(state.scrollPersistTimer);
    const routeToken = ++state.routeRequestToken;
    let url = new URL(window.location.href);
    const migrated = migrateLegacyRoute(url);
    url = migrated.url;
    const route = migrated.route || parseRouteHash(url.hash);

    // Estados de aplicação são compartilháveis, mas não formam uma superfície SEO própria.
    // O hash serve apenas para restaurar a experiência do usuário; a página indexável continua sendo o hub limpo.
    if (route?.kind === "perfil") {
      const profileId = route.value;
      if (state.entryByProfileId.has(profileId)) {
        await openProfile(profileId, { updateHistory: false, scroll: false });
        return;
      }
      resetSearchView({ updateHistory: false, scroll: false, focus: false });
      if (routeToken === state.routeRequestToken) showToast("O perfil deste link não está disponível no catálogo público atual.");
      return;
    }

    if (route?.kind === "comparar") {
      resetSearchView({ updateHistory: false, scroll: false, focus: false });
      const ids = validComparisonIds(route.value);
      state.compareIds = ids;
      await renderComparison();
      if (routeToken !== state.routeRequestToken) return;
      if (ids.length < 2) showToast("O link de comparação não contém duas seguradoras válidas no catálogo atual.");
      requestAnimationFrame(() => scrollToSection("#comparar"));
      return;
    }

    if (route?.kind === "consulta") {
      resetSearchView({ updateHistory: false, scroll: false, focus: false });
      const query = String(route.value || "").slice(0, 120);
      els.searchInput.value = query;
      els.searchClear.hidden = !query;
      const exact = exactEntryForQuery(query);
      if (exact) {
        await openProfile(exact.profile_id, { updateHistory: false, scroll: false });
        return;
      }
      renderSuggestions(query);
      if (routeToken !== state.routeRequestToken) return;
      requestAnimationFrame(() => scrollToSection("#consultar"));
      return;
    }

    const view = event?.state?.rk2View || history.state?.rk2View || null;
    const sectionHash = safeHashSelector(url.hash);
    if (sectionHash && $(sectionHash)) {
      resetSearchView({ updateHistory: false, scroll: false, focus: false });
      if (view) await restoreViewState(view);
      if (routeToken !== state.routeRequestToken) return;
      requestAnimationFrame(() => scrollToSection(sectionHash));
      return;
    }

    if (view) {
      await restoreViewState(view);
      if (routeToken !== state.routeRequestToken) return;
      return;
    }

    resetSearchView({ updateHistory: false, scroll: false, focus: false });
  }

  function bindEvents() {
    els.searchInput.addEventListener("input", () => {
      els.searchClear.hidden = !els.searchInput.value;
      renderSuggestions(els.searchInput.value);
    });

    els.searchInput.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (els.suggestions.hidden) renderSuggestions(els.searchInput.value);
        setActiveSuggestion(state.suggestionIndex + 1);
        return;
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (els.suggestions.hidden) renderSuggestions(els.searchInput.value);
        setActiveSuggestion(state.suggestionIndex <= 0 ? state.currentSuggestions.length - 1 : state.suggestionIndex - 1);
        return;
      }
      if (event.key === "Escape") {
        hideSuggestions();
        return;
      }
      if (event.key === "Enter" && state.suggestionIndex >= 0) {
        event.preventDefault();
        const row = state.currentSuggestions[state.suggestionIndex];
        if (row?.entry?.profile_id) openProfile(row.entry.profile_id);
      }
    });

    els.searchForm.addEventListener("submit", (event) => {
      event.preventDefault();
      const exact = exactEntryForQuery(els.searchInput.value);
      if (exact) {
        openProfile(exact.profile_id);
        return;
      }
      renderSuggestions(els.searchInput.value);
      if (state.currentSuggestions.length) {
        setActiveSuggestion(0);
        showToast("Escolha uma das correspondências para confirmar a identidade.");
      } else {
        showToast("Não encontramos esse nome na base atual.");
      }
    });

    els.searchClear.addEventListener("click", () => {
      if (root.classList.contains("rk2--result-mode")) {
        els.searchInput.value = "";
        els.searchClear.hidden = true;
        hideSuggestions();
        els.searchInput.focus();
      } else {
        resetSearchView();
      }
    });

    els.contextChange?.addEventListener("click", openSearchEditor);
    els.contextClose?.addEventListener("click", () => resetSearchView());

    els.compareClear?.addEventListener("click", () => {
      state.compareIds = [];
      persistCurrentViewState();
      renderComparison();
      els.compareSearch.focus({ preventScroll: true });
    });

    els.compareShare?.addEventListener("click", () => {
      if (state.compareIds.length < 2) return;
      const names = state.compareIds.map((id) => state.entryByProfileId.get(id)?.name).filter(Boolean).join(" × ");
      shareOrCopy({
        title: "Comparação de seguradoras — Sanida",
        text: names ? `Comparação: ${names}` : "Comparação de seguradoras",
        url: comparisonShareURL(),
      });
    });

    els.suggestions.addEventListener("click", (event) => {
      const button = event.target.closest("[data-suggestion-index]");
      if (!button) return;
      const row = state.currentSuggestions[Number(button.dataset.suggestionIndex)];
      if (row?.entry?.profile_id) openProfile(row.entry.profile_id);
    });

    document.addEventListener("click", (event) => {
      if (!event.target.closest("#rk2-search-form")) hideSuggestions();
      if (!event.target.closest(".rk2-mini-search")) hideCompareResults();

      if (event.target.closest("[data-reload-app]")) {
        window.location.reload();
        return;
      }

      const technicalHelp = event.target.closest("[data-tech-help-toggle]");
      if (technicalHelp) {
        toggleTechnicalHelp(technicalHelp);
        return;
      }

      const back = event.target.closest("[data-history-back]");
      if (back) {
        history.back();
        return;
      }

      const retryProfile = event.target.closest("[data-retry-profile]");
      if (retryProfile) {
        state.profileCache.delete(retryProfile.dataset.retryProfile);
        openProfile(retryProfile.dataset.retryProfile, { updateHistory: false });
      }

      const retryComparison = event.target.closest("[data-retry-comparison]");
      if (retryComparison) {
        state.compareIds.forEach((id) => state.profileCache.delete(id));
        renderComparison();
      }

      const retryBoard = event.target.closest("[data-retry-board]");
      if (retryBoard) {
        renderBoard(retryBoard.dataset.boardKind, retryBoard.dataset.boardId, { persist: false });
      }

      const open = event.target.closest("[data-open-profile]");
      if (open) openProfile(open.dataset.openProfile, { origin: originForElement(open) });

      const fresh = event.target.closest("[data-new-search]");
      if (fresh) resetSearchView();

      const sectionTarget = event.target.closest("[data-section-target]");
      if (sectionTarget) {
        navigateFromProfileToSection(sectionTarget.dataset.sectionTarget);
      }

      const shareProfile = event.target.closest("[data-share-profile]");
      if (shareProfile) {
        const id = shareProfile.dataset.shareProfile;
        const entry = state.entryByProfileId.get(id);
        shareOrCopy({
          title: `${entry?.name || "Consulta de empresa"} — Sanida`,
          text: `Consulta de identidade e sinais públicos de ${entry?.name || "empresa"}.`,
          url: profileShareURL(id),
        });
      }

      const addFromProfile = event.target.closest("[data-compare-profile]");
      if (addFromProfile) {
        addCompare(addFromProfile.dataset.compareProfile);
        navigateFromProfileToSection("#comparar");
      }

      const add = event.target.closest("[data-compare-add]");
      if (add) addCompare(add.dataset.compareAdd);

      const remove = event.target.closest("[data-compare-remove]");
      if (remove) removeCompare(remove.dataset.compareRemove);

      const board = event.target.closest("[data-board-open]");
      if (board) renderBoard("leaderboard", board.dataset.boardOpen, { persist: true });

      const collection = event.target.closest("[data-collection-open]");
      if (collection) renderBoard("collection", collection.dataset.collectionOpen, { persist: true });

      const close = event.target.closest("[data-board-close]");
      if (close) {
        state.boardRequestToken += 1;
        state.activeBoard = null;
        els.boardPanel.hidden = true;
        els.boardPanel.innerHTML = "";
        persistCurrentViewState();
      }
    });

    els.compareSearch.addEventListener("input", () => renderCompareResults(els.compareSearch.value));
    els.compareSearch.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (els.compareResults.hidden) renderCompareResults(els.compareSearch.value);
        setActiveCompareSuggestion(state.compareSuggestionIndex + 1);
        return;
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (els.compareResults.hidden) renderCompareResults(els.compareSearch.value);
        setActiveCompareSuggestion(state.compareSuggestionIndex <= 0 ? state.compareSuggestions.length - 1 : state.compareSuggestionIndex - 1);
        return;
      }
      if (event.key === "Escape") {
        hideCompareResults();
        return;
      }
      if (event.key === "Enter") {
        if (state.compareSuggestionIndex >= 0) {
          event.preventDefault();
          const entry = state.compareSuggestions[state.compareSuggestionIndex];
          if (entry?.profile_id) addCompare(entry.profile_id);
          return;
        }
        const source = state.insurerEntries.filter((entry) => !state.compareIds.includes(entry.profile_id));
        const exact = exactEntryForQuery(els.compareSearch.value, source);
        if (exact) {
          event.preventDefault();
          addCompare(exact.profile_id);
        }
      }
    });

    els.listSearch.addEventListener("input", () => {
      state.listPageNumber = 1;
      renderList();
      persistCurrentViewState();
    });

    $$('[data-list-filter]').forEach((button) => {
      button.addEventListener("click", () => {
        $$('[data-list-filter]').forEach((item) => item.classList.remove("is-active"));
        button.classList.add("is-active");
        state.listFilter = button.dataset.listFilter || "all";
        state.listPageNumber = 1;
        renderList();
        persistCurrentViewState();
      });
    });

    els.listPrev.addEventListener("click", () => {
      if (state.listPageNumber > 1) {
        state.listPageNumber -= 1;
        renderList();
        persistCurrentViewState();
        els.listStatus.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    });

    els.listNext.addEventListener("click", () => {
      const pages = Math.max(1, Math.ceil(listFilteredEntries().length / LIST_PAGE_SIZE));
      if (state.listPageNumber < pages) {
        state.listPageNumber += 1;
        renderList();
        persistCurrentViewState();
        els.listStatus.scrollIntoView({ behavior: "smooth", block: "nearest" });
      }
    });

    $$(".rk2-local-nav a").forEach((link) => {
      link.addEventListener("click", (event) => {
        const href = link.getAttribute("href");
        if (!href?.startsWith("#")) return;
        if (!state.currentProfileId) {
          persistCurrentViewState();
          return;
        }
        event.preventDefault();
        navigateFromProfileToSection(href);
      });
    });

    window.addEventListener("scroll", scheduleViewPersistence, { passive: true });
    window.addEventListener("pagehide", () => persistCurrentViewState({ force: true }));
    window.addEventListener("popstate", applyURLState);
  }

  async function init() {
    bindEvents();

    try {
      await loadDistributionManifest();
      const [searchIndex, explorer, exploreIndex] = await Promise.all([
        fetchJSON("search_index.json", "v2_public_search_index"),
        fetchJSON("insurer_explorer.json", "v2_public_insurer_explorer"),
        fetchJSON("explore_index.json", "v2_public_explore_index"),
      ]);

      state.searchIndex = searchIndex;
      state.entries = Array.isArray(searchIndex.entries) ? searchIndex.entries : [];
      state.entries.forEach((entry) => state.entryByProfileId.set(entry.profile_id, entry));
      state.insurerEntries = state.entries.filter((entry) => entry.filter_bucket === "insurers");

      state.explorer = explorer;
      (explorer?.entities || []).forEach((entity) => state.explorerByProfileId.set(`entity:${entity.entity_id}`, entity));
      $$('[data-list-filter]').forEach((button) => {
        if (button.dataset.listFilter === "all") return;
        const unavailable = !state.explorer;
        button.disabled = unavailable;
        button.title = unavailable ? "O índice de cobertura da avaliação não foi carregado." : "";
      });

      state.exploreIndex = exploreIndex;
      setCatalogReady(true);

      const pop = searchIndex.population || {};
      const profiles = Number(pop.profiles || state.entries.length);
      const insurers = Number(pop.ordinary_current_insurer_profiles || state.insurerEntries.length);
      els.population.textContent = `${profiles.toLocaleString("pt-BR")} empresas, marcas e registros pesquisáveis · ${insurers.toLocaleString("pt-BR")} seguradoras no cadastro atual`;

      renderList();
      if (state.exploreIndex) renderExploreIndex();
      else {
        els.exploreGrid.innerHTML = `<div class="rk2-suggestion-empty">O índice de exploração não está disponível neste pacote.</div>`;
        els.collectionsBody.textContent = "As coleções não estão disponíveis neste pacote.";
      }

      if ("scrollRestoration" in history) history.scrollRestoration = "manual";
      const previousState = history.state && typeof history.state === "object" ? history.state : {};
      history.replaceState(
        {
          ...previousState,
          rk2: previousState.rk2 || { mode: "initial" },
          rk2View: previousState.rk2View || currentViewSnapshot(),
        },
        "",
        window.location.pathname + window.location.search + window.location.hash,
      );
      await applyURLState();
    } catch (error) {
      console.error(error);
      setCatalogReady(false);
      els.population.textContent = "Não foi possível carregar o catálogo público.";
      els.list.innerHTML = `<div class="rk2-error"><strong>Falha ao carregar os dados públicos.</strong><p>${esc(error.message)}</p><button class="rk2-btn rk2-btn--ghost" type="button" data-reload-app>Tentar novamente</button></div>`;
      els.exploreGrid.innerHTML = `<div class="rk2-error"><strong>Falha ao carregar a exploração.</strong><p>Verifique se os JSONs públicos foram publicados em ${esc(PUBLIC_BASE)}.</p></div>`;
    }
  }

  init();
})();
