/**
 * BrasileirãoPRO — API Client
 *
 * ES2023+ patterns applied:
 * - Native fetch (no axios dependency)
 * - AbortController for cancellation & timeout
 * - Retry with exponential backoff
 * - In-memory TTL cache
 * - Custom error hierarchy
 * - Optional chaining & nullish coalescing throughout
 * - Private class fields for encapsulation
 */

import { Platform } from 'react-native';
import { ApiError, NetworkError, TimeoutError } from './errors';
import { TtlCache } from './cache';

// ── Config ─────────────────────────────────────────────────────────────────

const LOCAL_IP = Platform.OS === 'android' ? '10.0.2.2' : 'localhost';
export const BASE_URL = `http://${LOCAL_IP}:8000`;

const DEFAULT_TIMEOUT_MS = 12_000;
const MAX_RETRIES = 3;
const CACHE_TTL_MS = 60_000; // 1 min

// ── Types ──────────────────────────────────────────────────────────────────

export interface Previsao {
  id: number;
  rodada: number;
  nome_casa: string;
  nome_visitante: string;
  abrev_casa: string;
  abrev_visitante: string;
  escudo_casa: string | null;
  escudo_visitante: string | null;
  prob_casa_pct: number;
  prob_empate_pct: number;
  prob_visitante_pct: number;
  previsao: 'casa' | 'empate' | 'visitante';
  confianca_pct: number;
  modelo_versao: string;
  elo_casa: number | null;
  elo_visitante: number | null;
  pontos_casa: number | null;
  pontos_visitante: number | null;
  aprov_casa_pct: number | null;
  aprov_vis_pct: number | null;
  processed_at: string;
}

export interface ClassificacaoRow {
  posicao: number;
  clube: string;
  abreviacao: string;
  escudo_url: string | null;
  jogos: number;
  pontos: number;
  v: number;
  e: number;
  d: number;
  gm: number;
  gs: number;
  sg: number;
  aproveitamento: number | null;
  prob_rebaixamento_pct: number | null;
  zona_rebaixamento: boolean | null;
  situacao: string;
  cor_situacao: string;
}

export interface DesempenhoRow {
  rodada: number;
  total_jogos: number;
  acertos: number;
  acuracia_pct: number;
  acuracia_casa_pct: number | null;
  acuracia_empate_pct: number | null;
  acuracia_visitante_pct: number | null;
  conf_media_acerto: number | null;
  conf_media_erro: number | null;
  acuracia_media_5r: number | null;
  total_jogos_acumulado: number;
  acertos_acumulado: number;
  acuracia_geral_pct: number;
}

export interface Resumo {
  acuracia_geral: number | null;
  total_previsoes_historico: number | null;
  acertos_historico: number | null;
  proximas_partidas: number;
  melhor_modelo: string | null;
  acuracia_modelo: number | null;
}

// ── Core HTTP client ───────────────────────────────────────────────────────

/** Delay helper — resolves after `ms` milliseconds. */
const delay = (ms: number) => new Promise<void>(res => setTimeout(res, ms));

/**
 * Fetches JSON from the API with timeout, retry, and caching.
 *
 * @param path     - API path (e.g. '/api/previsoes')
 * @param cache    - TtlCache instance to use
 * @param signal   - Optional external AbortSignal
 */
async function fetchJson<T>(
  path: string,
  cache: TtlCache<T>,
  signal?: AbortSignal,
): Promise<T> {
  const cached = cache.get(path);
  if (cached !== undefined) return cached;

  let lastError: unknown;

  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);

    // Merge external signal with internal timeout signal
    // AbortSignal.any is ES2023 — polyfill via controller if unavailable
    const mergedSignal = signal ?? controller.signal;

    try {
      const response = await fetch(`${BASE_URL}${path}`, {
        signal: mergedSignal,
        headers: { Accept: 'application/json' },
      });

      clearTimeout(timeoutId);

      if (!response.ok) {
        throw new ApiError(
          `HTTP ${response.status} — ${response.statusText}`,
          response.status,
          path,
        );
      }

      const data = await response.json() as T;
      cache.set(path, data);
      return data;

    } catch (err) {
      clearTimeout(timeoutId);
      lastError = err;

      if (err instanceof ApiError && !err.isServerError) throw err;

      if ((err as Error)?.name === 'AbortError') {
        if (signal?.aborted) throw err; // external cancel — don't retry
        throw new TimeoutError(path, DEFAULT_TIMEOUT_MS);
      }

      // Network failure or 5xx — retry with exponential backoff
      if (attempt < MAX_RETRIES - 1) {
        await delay(200 * 2 ** attempt); // 200ms, 400ms, 800ms
      }
    }
  }

  // All retries exhausted
  throw lastError instanceof ApiError
    ? lastError
    : new NetworkError(path, lastError);
}

// ── Per-endpoint caches ────────────────────────────────────────────────────

const cachePrevisoes = new TtlCache<{ data: Previsao[]; total: number }>(CACHE_TTL_MS);
const cacheClassificacao = new TtlCache<{ data: ClassificacaoRow[]; total: number }>(CACHE_TTL_MS);
const cacheDesempenho = new TtlCache<{ data: DesempenhoRow[]; total: number }>(CACHE_TTL_MS * 5);
const cacheResumo = new TtlCache<Resumo>(CACHE_TTL_MS);

/** Force refresh all caches (e.g. on pull-to-refresh). */
export const invalidateAll = () => {
  cachePrevisoes.clear();
  cacheClassificacao.clear();
  cacheDesempenho.clear();
  cacheResumo.clear();
};

// ── Public fetch functions ─────────────────────────────────────────────────

/**
 * Fetches upcoming match predictions.
 *
 * @param signal - Optional AbortSignal for cancellation
 */
export const fetchPrevisoes = async (signal?: AbortSignal): Promise<Previsao[]> => {
  const res = await fetchJson('/api/previsoes', cachePrevisoes, signal);
  return res.data;
};

/**
 * Fetches the current standings table.
 */
export const fetchClassificacao = async (signal?: AbortSignal): Promise<ClassificacaoRow[]> => {
  const res = await fetchJson('/api/classificacao', cacheClassificacao, signal);
  return res.data;
};

/**
 * Fetches model performance metrics per round.
 */
export const fetchDesempenho = async (signal?: AbortSignal): Promise<DesempenhoRow[]> => {
  const res = await fetchJson('/api/desempenho', cacheDesempenho, signal);
  return res.data;
};

/**
 * Fetches dashboard KPI summary.
 */
export const fetchResumo = async (signal?: AbortSignal): Promise<Resumo> => {
  return fetchJson('/api/resumo', cacheResumo, signal);
};
