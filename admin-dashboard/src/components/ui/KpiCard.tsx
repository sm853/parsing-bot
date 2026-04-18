'use client';

import clsx from 'clsx';

interface KpiCardProps {
  title: string;
  value: string | number;
  subtitle?: string;
  trend?: number;
  loading?: boolean;
}

export function KpiCard({ title, value, subtitle, trend, loading = false }: KpiCardProps) {
  if (loading) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 p-6 animate-pulse">
        <div className="h-4 bg-gray-200 rounded w-2/3 mb-4" />
        <div className="h-8 bg-gray-200 rounded w-1/2 mb-2" />
        <div className="h-3 bg-gray-200 rounded w-3/4" />
      </div>
    );
  }

  const trendPositive = trend !== undefined && trend > 0;
  const trendNegative = trend !== undefined && trend < 0;

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6 flex flex-col gap-1">
      <p className="text-sm font-medium text-gray-500 uppercase tracking-wide">{title}</p>
      <p className="text-3xl font-bold text-gray-900 mt-1">{value}</p>
      <div className="flex items-center gap-2 mt-1">
        {subtitle && <p className="text-sm text-gray-500">{subtitle}</p>}
        {trend !== undefined && (
          <span
            className={clsx(
              'text-xs font-semibold px-1.5 py-0.5 rounded',
              trendPositive && 'text-green-700 bg-green-100',
              trendNegative && 'text-red-700 bg-red-100',
              !trendPositive && !trendNegative && 'text-gray-600 bg-gray-100'
            )}
          >
            {trendPositive && '▲'}
            {trendNegative && '▼'}
            {!trendPositive && !trendNegative && '–'}
            {trend !== 0 ? ` ${Math.abs(trend)}%` : ' 0%'}
          </span>
        )}
      </div>
    </div>
  );
}
