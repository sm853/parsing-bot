'use client';

import { useRouter } from 'next/navigation';
import type { RunRow } from '@/lib/types';
import { StatusBadge } from '@/components/ui/StatusBadge';
import { format, parseISO } from 'date-fns';

interface RunsTableProps {
  rows: RunRow[];
  loading?: boolean;
}

function formatDuration(ms: number | null): string {
  if (ms === null) return '—';
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function formatDate(iso: string | null): string {
  if (!iso) return '—';
  return format(parseISO(iso), 'MMM d, HH:mm');
}

const SKELETON_ROWS = 5;

export function RunsTable({ rows, loading = false }: RunsTableProps) {
  const router = useRouter();

  if (!loading && rows.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        <div className="flex items-center justify-center py-16 text-gray-400 text-sm">
          No runs found matching the current filters.
        </div>
      </div>
    );
  }

  return (
    <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-100 text-sm">
          <thead className="bg-gray-50">
            <tr>
              {['ID', 'User', 'Channel', 'Status', 'Duration', 'Rows', 'Started', ''].map((h) => (
                <th
                  key={h}
                  className="px-4 py-3 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide"
                >
                  {h}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100">
            {loading
              ? Array.from({ length: SKELETON_ROWS }).map((_, i) => (
                  <tr key={i} className="animate-pulse">
                    {Array.from({ length: 8 }).map((__, j) => (
                      <td key={j} className="px-4 py-3">
                        <div className="h-4 bg-gray-200 rounded w-full" />
                      </td>
                    ))}
                  </tr>
                ))
              : rows.map((row) => (
                  <tr
                    key={row.id}
                    className="hover:bg-gray-50 cursor-pointer transition-colors"
                    onClick={() => router.push(`/admin/runs/${row.id}`)}
                  >
                    <td className="px-4 py-3 font-mono text-xs text-gray-500">#{row.id}</td>
                    <td className="px-4 py-3 text-gray-800">
                      {row.username ? (
                        <span>@{row.username}</span>
                      ) : (
                        <span className="text-gray-400">{row.telegram_user_id}</span>
                      )}
                    </td>
                    <td className="px-4 py-3 text-gray-700">
                      {row.selected_channel ?? <span className="text-gray-400">—</span>}
                    </td>
                    <td className="px-4 py-3">
                      <StatusBadge status={row.status} />
                    </td>
                    <td className="px-4 py-3 text-gray-600">
                      {formatDuration(row.parsing_duration_ms)}
                    </td>
                    <td className="px-4 py-3 text-gray-600">
                      {row.result_rows ?? '—'}
                    </td>
                    <td className="px-4 py-3 text-gray-500">
                      {formatDate(row.started_at)}
                    </td>
                    <td className="px-4 py-3">
                      <span className="text-indigo-600 hover:text-indigo-800 font-medium text-xs">
                        View →
                      </span>
                    </td>
                  </tr>
                ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
