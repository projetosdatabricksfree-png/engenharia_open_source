/**
 * React hooks for fetching BrasileirãoPRO data.
 *
 * ES2023+ patterns:
 * - AbortController cleanup on unmount
 * - Nullish coalescing for defaults
 * - Optional chaining for safe access
 * - Logical assignment (??=) for state defaults
 */

import { useCallback, useEffect, useRef, useState } from 'react';
import {
  fetchPrevisoes,
  fetchClassificacao,
  fetchDesempenho,
  fetchResumo,
  invalidateAll,
  type Previsao,
  type ClassificacaoRow,
  type DesempenhoRow,
  type Resumo,
} from '../api/client';
import { NetworkError, TimeoutError } from '../api/errors';

// ── Shared types ──────────────────────────────────────────────────────────

export interface UseDataResult<T> {
  data: T | null;
  loading: boolean;
  refreshing: boolean;
  error: string | null;
  refresh: () => void;
}

// ── Generic hook ──────────────────────────────────────────────────────────

/**
 * Generic data-fetching hook with:
 * - Automatic abort on unmount
 * - Pull-to-refresh support
 * - Human-readable error messages
 *
 * @param fetcher - Async function that accepts an AbortSignal
 */
function useAsyncData<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
): UseDataResult<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(async (isRefresh = false) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    isRefresh ? setRefreshing(true) : setLoading(true);
    setError(null);

    try {
      const result = await fetcher(controller.signal);
      if (!controller.signal.aborted) {
        setData(result);
      }
    } catch (err) {
      if ((err as Error)?.name === 'AbortError') return;

      const message =
        err instanceof TimeoutError ? 'Conexão lenta — tente novamente' :
        err instanceof NetworkError ? 'API indisponível — verifique se está rodando em :8000' :
        (err as Error)?.message ?? 'Erro desconhecido';

      setError(message);
    } finally {
      if (!controller.signal.aborted) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, [fetcher]);

  useEffect(() => {
    load();
    return () => abortRef.current?.abort();
  }, [load]);

  const refresh = useCallback(() => load(true), [load]);

  return { data, loading, refreshing, error, refresh };
}

// ── Public hooks ──────────────────────────────────────────────────────────

export const usePrevisoes = () =>
  useAsyncData<Previsao[]>(signal => fetchPrevisoes(signal));

export const useClassificacao = () =>
  useAsyncData<ClassificacaoRow[]>(signal => fetchClassificacao(signal));

export const useDesempenho = () =>
  useAsyncData<DesempenhoRow[]>(signal => fetchDesempenho(signal));

export const useResumo = () =>
  useAsyncData<Resumo>(signal => fetchResumo(signal));

/**
 * Combined hook that fetches predictions + summary in parallel.
 * Uses Promise.allSettled so one failure doesn't cancel the other.
 */
export const useHomeData = () => {
  const [previsoes, setPrevisoes] = useState<Previsao[] | null>(null);
  const [resumo, setResumo] = useState<Resumo | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const load = useCallback(async (isRefresh = false) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    isRefresh ? setRefreshing(true) : setLoading(true);
    setError(null);

    const [prevResult, resumoResult] = await Promise.allSettled([
      fetchPrevisoes(controller.signal),
      fetchResumo(controller.signal),
    ]);

    if (controller.signal.aborted) return;

    if (prevResult.status === 'fulfilled') setPrevisoes(prevResult.value);
    if (resumoResult.status === 'fulfilled') setResumo(resumoResult.value);

    const firstError = [prevResult, resumoResult].find(r => r.status === 'rejected');
    if (firstError?.status === 'rejected') {
      const err = firstError.reason as Error;
      setError(err instanceof NetworkError
        ? 'API indisponível — inicie com `make api-up`'
        : err.message ?? 'Erro ao carregar dados');
    }

    setLoading(false);
    setRefreshing(false);
  }, []);

  useEffect(() => {
    load();
    return () => abortRef.current?.abort();
  }, [load]);

  const refresh = useCallback(() => {
    invalidateAll();
    load(true);
  }, [load]);

  return { previsoes, resumo, loading, refreshing, error, refresh };
};
