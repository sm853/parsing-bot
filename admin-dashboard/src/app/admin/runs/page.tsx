'use client';

import { useCallback, useEffect, useState } from 'react';
import { DashboardFilters } from '@/components/filters/DashboardFilters';
import { RunsTable } from '@/components/tables/RunsTable';
import type { RunFilters, RunRow } from '@/lib/types';

const DEFAULT_LIMIT = 20;

export default function RunsPage() {
  const [filters, setFilters] = useState<RunFilters>({ page: 1, limit: DEFAULT_LIMIT });
  const [rows, setRows] = useState<RunRow[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchRuns = useCallback(async (f: RunFilters) => {
    setLoading(true);
    setError(null);
    try {
      const params = new URLSearchParams();
      if (f.from)     params.set('from', f.from);
      if (f.to)       params.set('to', f.to);
      if (f.status)   params.set('status', f.status);
      if (f.channel)  params.set('channel', f.channel);
      if (f.username) params.set('username', f.username);
      params.set('page', String(f.page ?? 1));
      params.set('limit', String(f.limit ?? DEFAULT_LIMIT));

      const res = await fetch(`/api/admin/runs?${params.toString()}`);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      setRows(data.rows);
      setTotal(data.total);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch runs');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchRuns(filters);
  }, [filters, fetchRuns]);

  const page = filters.page ?? 1;
  const limit = filters.limit ?? DEFAULT_LIMIT;
  const totalPages = Math.max(1, Math.ceil(total / limit));

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">All Runs</h1>
          <p className="text-sm text-gray-500 mt-0.5">{total.toLocaleString()} total sessions</p>
        </div>
      </div>

      <DashboardFilters
        filters={filters}
        onFiltersChange={(f) => setFilters({ ...f, page: 1 })}
      />

      {error && (
        <div className="bg-red-50 border border-red-200 text-red-700 text-sm rounded-lg px-4 py-3">
          {error}
        </div>
      )}

      <RunsTable rows={rows} loading={loading} />

      {/* Pagination */}
      <div className="flex items-center justify-between text-sm text-gray-600">
        <span>
          Page {page} of {totalPages} &mdash; {total.toLocaleString()} runs
        </span>
        <div className="flex gap-2">
          <button
            disabled={page <= 1 || loading}
            onClick={() => setFilters((f) => ({ ...f, page: (f.page ?? 1) - 1 }))}
            className="px-3 py-1.5 rounded-lg border border-gray-300 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            ← Prev
          </button>
          <button
            disabled={page >= totalPages || loading}
            onClick={() => setFilters((f) => ({ ...f, page: (f.page ?? 1) + 1 }))}
            className="px-3 py-1.5 rounded-lg border border-gray-300 hover:bg-gray-50 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
          >
            Next →
          </button>
        </div>
      </div>
    </div>
  );
}
