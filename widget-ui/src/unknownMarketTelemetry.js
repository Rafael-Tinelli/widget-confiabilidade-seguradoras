import { useEffect, useRef } from 'react';

const NOISE_QUERIES = new Set([
  'seguro',
  'seguros',
  'seguradora',
  'seguradoras',
  'ranking',
  'ranking seguradoras',
  'melhor seguradora',
  'melhores seguradoras',
  'confiabilidade',
]);

export function normalizeUnknownMarketQuery(value) {
  const text = String(value || '').trim();
  if (!text) return '';

  const digits = text.replace(/\D+/g, '');
  const nonCnpj = text.replace(/[\d./\-\s]+/g, '');
  if (digits.length === 14 && !nonCnpj) return digits;

  return text
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toLocaleLowerCase('pt-BR')
    .replace(/[^a-z0-9]+/g, ' ')
    .trim();
}

function isEligibleUnknownQuery(value) {
  const normalized = normalizeUnknownMarketQuery(value);
  if (!normalized || NOISE_QUERIES.has(normalized)) return false;
  if (/^\d+$/.test(normalized)) return normalized.length === 14;
  return normalized.length >= 3;
}

function configuredSameOriginEndpoint() {
  const configured = String(import.meta.env.VITE_UNKNOWN_MARKET_QUERY_ENDPOINT || '').trim();
  if (!configured || typeof window === 'undefined') return null;
  try {
    const endpoint = new URL(configured, window.location.origin);
    if (endpoint.origin !== window.location.origin) return null;
    return endpoint.toString();
  } catch {
    return null;
  }
}

export function useUnknownMarketQueryTelemetry(query, resultCount, ready) {
  const sent = useRef(new Set());

  useEffect(() => {
    if (!ready || resultCount !== 0) return undefined;
    const normalized = normalizeUnknownMarketQuery(query);
    if (!isEligibleUnknownQuery(normalized) || sent.current.has(normalized)) {
      return undefined;
    }
    const endpoint = configuredSameOriginEndpoint();
    if (!endpoint) return undefined;

    const timer = window.setTimeout(() => {
      sent.current.add(normalized);
      fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: normalized }),
        credentials: 'omit',
        keepalive: true,
        referrerPolicy: 'same-origin',
      }).catch(() => {
        // Demand telemetry is observational and must never break the user experience.
      });
    }, 800);

    return () => window.clearTimeout(timer);
  }, [query, resultCount, ready]);
}
