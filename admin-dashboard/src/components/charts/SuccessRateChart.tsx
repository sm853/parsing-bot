'use client';

import { PieChart, Pie, Cell, Tooltip, ResponsiveContainer } from 'recharts';

interface SuccessRateChartProps {
  success: number;
  failed: number;
  other: number;
}

const COLORS = ['#22c55e', '#ef4444', '#d1d5db'];

export function SuccessRateChart({ success, failed, other }: SuccessRateChartProps) {
  const total = success + failed + other;

  if (total === 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 p-6">
        <h3 className="text-sm font-semibold text-gray-700 mb-4">Success Rate</h3>
        <div className="flex items-center justify-center h-48 text-gray-400 text-sm">
          No data available
        </div>
      </div>
    );
  }

  const successPct = Math.round((success / total) * 100);
  const data = [
    { name: 'Success', value: success },
    { name: 'Failed', value: failed },
    { name: 'Other', value: other },
  ];

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-6">
      <h3 className="text-sm font-semibold text-gray-700 mb-4">Success Rate</h3>
      <div className="relative">
        <ResponsiveContainer width="100%" height={200}>
          <PieChart>
            <Pie
              data={data}
              cx="50%"
              cy="50%"
              innerRadius={60}
              outerRadius={90}
              dataKey="value"
              startAngle={90}
              endAngle={-270}
            >
              {data.map((_, index) => (
                <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{ fontSize: 12, borderRadius: 8 }}
              formatter={(value: number) => [value, '']}
            />
          </PieChart>
        </ResponsiveContainer>
        <div className="absolute inset-0 flex items-center justify-center pointer-events-none">
          <div className="text-center">
            <p className="text-2xl font-bold text-gray-900">{successPct}%</p>
            <p className="text-xs text-gray-500">success</p>
          </div>
        </div>
      </div>
      <div className="flex justify-center gap-4 mt-2">
        {data.map((entry, index) => (
          <div key={entry.name} className="flex items-center gap-1.5">
            <span className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: COLORS[index] }} />
            <span className="text-xs text-gray-600">{entry.name} ({entry.value})</span>
          </div>
        ))}
      </div>
    </div>
  );
}
