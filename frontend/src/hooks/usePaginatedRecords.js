import { useState, useEffect, useCallback, useRef } from "react";
import { API_URL } from "../config";

const PAGE_SIZE = 50;
const DEBOUNCE_MS = 500;

export const BASE_URL = `${API_URL}/invalid-summary`;

/**
 * usePaginatedRecords
 *
 * Common fetch logic shared by all 3 list pages.
 * Handles: debounced search, load-more pagination, abort on stale requests,
 * abort on page unmount, and forwards extra query params to the API.
 *
 * Page-specific concerns (client-side filtering, color counts, etc.)
 * are intentionally left to the individual pages.
 *
 * @param {Object} [options]
 * @param {Object} [options.extraParams] - Static query params merged into every
 *                                         request, e.g. { status: "APPROVED" }.
 *                                         Must be stable (defined outside render
 *                                         or memoised) to avoid infinite loops.
 */
export function usePaginatedRecords({ extraParams = {} } = {}) {
  const [page, setPage] = useState(1);
  const [records, setRecords] = useState([]);
  const [totalRecords, setTotalRecords] = useState(0);
  const [totalPages, setTotalPages] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  // Any extra top-level API response fields (e.g. red / green / yellow)
  const [meta, setMeta] = useState({});

  // searchInput → bound to <TextField>
  // search      → debounced value that actually triggers fetches
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");

  const abortRef = useRef(null);
  // Each fetch gets a unique incrementing ID. Only the fetch whose ID matches
  // the latest value is allowed to update state or set loading=false.
  // This is the correct fix for "stuck loading" — no special-casing of AbortError.
  const fetchIdRef = useRef(0);

  // Serialise extraParams so the fetch callback has a stable primitive dependency
  // (avoids re-creating fetchRecords when the caller passes an object literal)
  const extraParamsKey = JSON.stringify(extraParams);

  // ── Abort in-flight request on unmount (page navigation) ─────────────────
  useEffect(() => {
    return () => {
      if (abortRef.current) abortRef.current.abort();
    };
  }, []);

  // ── Debounce search input ─────────────────────────────────────────────────
  useEffect(() => {
    const t = setTimeout(() => setSearch(searchInput), DEBOUNCE_MS);
    return () => clearTimeout(t);
  }, [searchInput]);

  // ── Helper: build a URL and fetch one page ────────────────────────────────
  const fetchOnePage = (params, signal) => {
    const qs = new URLSearchParams();
    Object.entries(params).forEach(([k, v]) => qs.set(k, v));
    return fetch(`${BASE_URL}?${qs}`, { signal }).then((r) => {
      if (!r.ok) throw new Error(`Failed to fetch data: ${r.status}`);
      return r.json();
    });
  };

  // ── Core fetch ────────────────────────────────────────────────────────────
  const fetchRecords = useCallback(
    async (pageNum, isNew = false) => {
      // Cancel the previous in-flight request
      if (abortRef.current) abortRef.current.abort();
      const controller = new AbortController();
      abortRef.current = controller;

      const fetchId = ++fetchIdRef.current;

      if (isNew) {
        setRecords([]);
        setTotalRecords(0);
        setTotalPages(0);
        setMeta({});
      }

      setLoading(true);
      setError(null);

      try {
        const parsed = JSON.parse(extraParamsKey);
        const baseParams = { page: pageNum, size: PAGE_SIZE, search: search || "", ...parsed };


        const qs = new URLSearchParams();
        Object.entries(baseParams).forEach(([k, v]) => qs.set(k, v));
        const url = `${BASE_URL}?${qs}`;
        const res = await fetch(url, { signal: controller.signal });
        if (!res.ok) throw new Error(`Failed to fetch data: ${res.status}`);
        const data = await res.json();

        if (fetchId !== fetchIdRef.current) return;

        const incoming = Array.isArray(data?.data) ? data.data : [];
        const total = data?.total_invalid_records ?? incoming.length;

        setTotalPages(Math.ceil(total / PAGE_SIZE));
        setRecords((prev) => (isNew ? incoming : [...prev, ...incoming]));
        setTotalRecords(total);

        const { data: _d, total_invalid_records: _t, ...rest } = data;
        setMeta(rest);

      } catch (err) {
        if (fetchId !== fetchIdRef.current) return;
        if (err.name !== "AbortError") {
          console.error("usePaginatedRecords:", err);
          setError(err);
        }
      } finally {
        if (fetchId === fetchIdRef.current) setLoading(false);
      }
    },
    [search, extraParamsKey]
  );

  // ── Reset + refetch when search or extraParams change ────────────────────
  useEffect(() => {
    setPage(1);
    fetchRecords(1, true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [search, extraParamsKey]);

  // ── Load next page ────────────────────────────────────────────────────────
  useEffect(() => {
    if (page > 1) fetchRecords(page, false);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [page]);

  // ── Public helpers ────────────────────────────────────────────────────────
  const loadMore = useCallback(() => setPage((p) => p + 1), []);

  const retry = useCallback(
    () => fetchRecords(page, page === 1),
    [fetchRecords, page]
  );

  return {
    records,          // raw records from API
    totalRecords,
    totalPages,
    page,
    loading,
    error,
    searchInput,      // bind to <TextField value>
    setSearchInput,   // bind to <TextField onChange>
    search,           // debounced search term for custom secondary fetches
    loadMore,         // call on "Load More" button
    retry,            // call from <ErrorPage onRetry>
    meta,             // extra API top-level fields (red, green, yellow, ...)
  };
}