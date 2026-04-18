import { getOverviewMetrics, getTimeseries, getChannelStats, getErrorStats, getRuns } from '@/lib/queries';
import { KpiCard } from '@/components/ui/KpiCard';
import { StatusBadge } from '@/components/ui/StatusBadge';
import { RefreshButton } from '@/components/ui/RefreshButton';
import { RunsPerDayChart } from '@/components/charts/RunsPerDayChart';
import { SuccessRateChart } from '@/components/charts/SuccessRateChart';
import { RunsTable } from '@/components/tables/RunsTable';

function formatDuration(ms: number | null): string {
  if (ms === null) return '—';
  if (ms < 1000) return `${ms}ms`;
  return `${(ms / 1000).toFixed(1)}s`;
}

export const revalidate = 60;

export default async function AdminPage() {
  const [metrics, timeseries, channels, errors, latestRuns] = await Promise.all([
    getOverviewMetrics(),
    getTimeseries(30),
    getChannelStats(),
    getErrorStats(),
    getRuns({ limit: 10, page: 1 }),
  ]);

  const successCount = timeseries.reduce((s, d) => s + d.success, 0);
  const failedCount = timeseries.reduce((s, d) => s + d.failed, 0);
  const otherCount = metrics.totalSessions - successCount - failedCount;

  return (
    <div className="space-y-8">
      {/* Top bar */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Parsing Bot Admin</h1>
          <p className="text-sm text-gray-500 mt-0.5">Monitor and analyze Telegram parsing sessions</p>
        </div>
        <RefreshButton />
      </div>

      {/* KPI cards */}
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
        <KpiCard
          title="Total Sessions"
          value={metrics.totalSessions.toLocaleString()}
          subtitle="all time"
        />
        <KpiCard
          title="Success Rate"
          value={`${metrics.successRate}%`}
          subtitle="all time"
        />
        <KpiCard
          title="Avg Duration"
          value={formatDuration(metrics.avgDurationMs)}
          subtitle="successful runs"
        />
        <KpiCard
          title="Active Now"
          value={metrics.activeNow}
          subtitle="running sessions"
        />
        <KpiCard
          title="Total Users"
          value={metrics.totalUsers.toLocaleString()}
          subtitle="unique users"
        />
      </div>

      {/* Today row */}
      <div className="grid grid-cols-3 gap-4">
        <KpiCard title="Today — Runs"    value={metrics.runsToday}    />
        <KpiCard title="Today — Success" value={metrics.successToday} />
        <KpiCard title="Today — Failed"  value={metrics.failedToday}  />
      </div>

      {/* Runs per day chart — full width */}
      <RunsPerDayChart data={timeseries} />

      {/* Success rate + top channels — side by side */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <SuccessRateChart
          success={successCount}
          failed={failedCount}
          other={Math.max(otherCount, 0)}
        />

        {/* Top channels */}
        <div className="bg-white rounded-xl border border-gray-200 p-6">
          <h3 className="text-sm font-semibold text-gray-700 mb-4">Top Channels</h3>
          {channels.length === 0 ? (
            <p className="text-sm text-gray-400">No channel data.</p>
          ) : (
            <div className="space-y-3">
              {channels.slice(0, 8).map((ch) => (
                <div key={ch.channel} className="flex items-center justify-between">
                  <span className="text-sm text-gray-800 font-mono">{ch.channel}</span>
                  <div className="flex items-center gap-3">
                    <span className="text-xs text-gray-500">{ch.count} runs</span>
                    <span className="text-xs font-medium text-green-700">{ch.successRate}%</span>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Top errors */}
      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <h3 className="text-sm font-semibold text-gray-700 mb-4">Top Errors</h3>
        {errors.length === 0 ? (
          <p className="text-sm text-gray-400">No errors recorded.</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  <th className="pb-2 text-left text-xs font-semibold text-gray-500 uppercase">Code</th>
                  <th className="pb-2 text-left text-xs font-semibold text-gray-500 uppercase">Message</th>
                  <th className="pb-2 text-right text-xs font-semibold text-gray-500 uppercase">Count</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50">
                {errors.map((e) => (
                  <tr key={e.error_code}>
                    <td className="py-2 pr-4 font-mono text-xs text-red-700 whitespace-nowrap">{e.error_code}</td>
                    <td className="py-2 pr-4 text-gray-600 text-xs">{e.error_message}</td>
                    <td className="py-2 text-right font-semibold text-gray-800">{e.count}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Latest 10 runs */}
      <div>
        <h3 className="text-sm font-semibold text-gray-700 mb-3">Latest Runs</h3>
        <RunsTable rows={latestRuns.rows} />
      </div>
    </div>
  );
}
