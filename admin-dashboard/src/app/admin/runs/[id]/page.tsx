export const dynamic = 'force-dynamic';

import { notFound } from 'next/navigation';
import Link from 'next/link';
import { getSessionById } from '@/lib/queries';
import { StatusBadge } from '@/components/ui/StatusBadge';
import { KpiCard } from '@/components/ui/KpiCard';
import { format, parseISO } from 'date-fns';

interface PageProps {
  params: { id: string };
}

function formatDate(iso: string | null): string {
  if (!iso) return '—';
  return format(parseISO(iso), 'MMM d yyyy, HH:mm:ss');
}

function formatDuration(ms: number | null): string {
  if (ms === null) return '—';
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

export default async function RunDetailPage({ params }: PageProps) {
  const session = await getSessionById(params.id);
  if (!session) notFound();

  const options = session.selected_options
    ? Object.entries(session.selected_options)
    : [];

  return (
    <div className="space-y-6 max-w-5xl">
      {/* Back */}
      <Link
        href="/admin/runs"
        className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-800 transition-colors"
      >
        ← Back to Runs
      </Link>

      {/* Session header */}
      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <div className="flex items-start justify-between gap-4">
          <div className="space-y-1">
            <div className="flex items-center gap-3">
              <h1 className="text-xl font-bold text-gray-900 font-mono">
                Session #{session.id}
              </h1>
              <StatusBadge status={session.status} />
            </div>
            <p className="text-sm text-gray-500">
              Channel:{' '}
              <span className="font-mono font-medium text-gray-700">
                {session.selected_channel ?? '—'}
              </span>
            </p>
            <p className="text-sm text-gray-500">
              User:{' '}
              <span className="font-medium text-gray-700">
                {session.username ? `@${session.username}` : session.telegram_user_id}
              </span>
            </p>
          </div>
          <div className="text-right text-xs text-gray-400 space-y-1">
            <p>Started: {formatDate(session.started_at)}</p>
            <p>Finished: {formatDate(session.finished_at)}</p>
          </div>
        </div>
      </div>

      {/* KPI mini row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <KpiCard title="Duration"   value={formatDuration(session.parsing_duration_ms)} />
        <KpiCard title="Attempts"   value={session.attempts_count} />
        <KpiCard title="Result Rows" value={session.result_rows ?? '—'} />
        <KpiCard title="Error Code" value={session.error_code ?? '—'} subtitle={session.error_message ?? undefined} />
      </div>

      {/* Run details */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Session metadata */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-sm font-semibold text-gray-700 mb-4">Session Details</h2>
          <dl className="space-y-2 text-sm">
            {[
              ['Period',      session.selected_period ?? '—'],
              ['Format',      session.selected_format ?? '—'],
              ['File type',   session.result_file_type ?? '—'],
              ['File URL',    session.result_file_url ?? '—'],
              ['Created',     formatDate(session.created_at)],
              ['Updated',     formatDate(session.updated_at)],
            ].map(([label, value]) => (
              <div key={label} className="flex items-start gap-2">
                <dt className="w-28 text-gray-500 shrink-0">{label}</dt>
                <dd className="text-gray-800 break-all">{value}</dd>
              </div>
            ))}
          </dl>
        </div>

        {/* Selected options */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h2 className="text-sm font-semibold text-gray-700 mb-4">Selected Options</h2>
          {options.length === 0 ? (
            <p className="text-sm text-gray-400">No options recorded.</p>
          ) : (
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="pb-2 text-left text-xs font-semibold text-gray-500 uppercase">Key</th>
                  <th className="pb-2 text-left text-xs font-semibold text-gray-500 uppercase">Value</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {options.map(([key, val]) => (
                  <tr key={key}>
                    <td className="py-2 pr-4 font-mono text-gray-600">{key}</td>
                    <td className="py-2 text-gray-800">{JSON.stringify(val)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* Attempts table */}
      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <h2 className="text-sm font-semibold text-gray-700 mb-4">Attempts</h2>
        {session.attempts.length === 0 ? (
          <p className="text-sm text-gray-400">No attempts recorded.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm divide-y divide-gray-100">
              <thead className="bg-gray-50">
                <tr>
                  {['#', 'Status', 'Started', 'Finished', 'Duration', 'Error Code', 'Error Message'].map((h) => (
                    <th
                      key={h}
                      className="px-4 py-2.5 text-left text-xs font-semibold text-gray-500 uppercase tracking-wide"
                    >
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {session.attempts.map((attempt) => (
                  <tr key={attempt.id}>
                    <td className="px-4 py-2.5 text-gray-600">{attempt.attempt_number}</td>
                    <td className="px-4 py-2.5">
                      <StatusBadge status={attempt.status} />
                    </td>
                    <td className="px-4 py-2.5 text-gray-500 text-xs">{formatDate(attempt.started_at)}</td>
                    <td className="px-4 py-2.5 text-gray-500 text-xs">{formatDate(attempt.finished_at)}</td>
                    <td className="px-4 py-2.5 text-gray-600">{formatDuration(attempt.duration_ms)}</td>
                    <td className="px-4 py-2.5 font-mono text-xs text-red-700">{attempt.error_code ?? '—'}</td>
                    <td className="px-4 py-2.5 text-gray-500 text-xs max-w-xs truncate">
                      {attempt.error_message ?? '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
