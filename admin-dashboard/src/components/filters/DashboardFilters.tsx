'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import type { RunFilters, SessionStatus } from '@/lib/types';

const STATUS_OPTIONS: { value: SessionStatus | ''; label: string }[] = [
  { value: '',                label: 'All Statuses'    },
  { value: 'success',         label: 'Success'         },
  { value: 'failed',          label: 'Failed'          },
  { value: 'running',         label: 'Running'         },
  { value: 'queued',          label: 'Queued'          },
  { value: 'partial_success', label: 'Partial Success' },
  { value: 'cancelled',       label: 'Cancelled'       },
];

interface DashboardFiltersProps {
  filters: RunFilters;
  onFiltersChange: (filters: RunFilters) => void;
}

export function DashboardFilters({ filters, onFiltersChange }: DashboardFiltersProps) {
  const [channelInput, setChannelInput] = useState(filters.channel ?? '');
  const [usernameInput, setUsernameInput] = useState(filters.username ?? '');
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const debounce = useCallback(
    (field: 'channel' | 'username', value: string) => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
      debounceRef.current = setTimeout(() => {
        onFiltersChange({ ...filters, [field]: value || undefined, page: 1 });
      }, 400);
    },
    [filters, onFiltersChange]
  );

  useEffect(() => {
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, []);

  const handleChannelChange = (v: string) => {
    setChannelInput(v);
    debounce('channel', v);
  };

  const handleUsernameChange = (v: string) => {
    setUsernameInput(v);
    debounce('username', v);
  };

  const handleReset = () => {
    setChannelInput('');
    setUsernameInput('');
    onFiltersChange({ page: 1, limit: filters.limit });
  };

  const inputClass =
    'block w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 placeholder-gray-400 focus:border-indigo-500 focus:outline-none focus:ring-1 focus:ring-indigo-500';

  return (
    <div className="bg-white rounded-xl border border-gray-200 p-4">
      <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3 items-end">
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">From</label>
          <input
            type="date"
            className={inputClass}
            value={filters.from ?? ''}
            onChange={(e) =>
              onFiltersChange({ ...filters, from: e.target.value || undefined, page: 1 })
            }
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">To</label>
          <input
            type="date"
            className={inputClass}
            value={filters.to ?? ''}
            onChange={(e) =>
              onFiltersChange({ ...filters, to: e.target.value || undefined, page: 1 })
            }
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Status</label>
          <select
            className={inputClass}
            value={filters.status ?? ''}
            onChange={(e) =>
              onFiltersChange({
                ...filters,
                status: (e.target.value as SessionStatus) || undefined,
                page: 1,
              })
            }
          >
            {STATUS_OPTIONS.map((o) => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Channel</label>
          <input
            type="text"
            placeholder="@channel"
            className={inputClass}
            value={channelInput}
            onChange={(e) => handleChannelChange(e.target.value)}
          />
        </div>
        <div>
          <label className="block text-xs font-medium text-gray-600 mb-1">Username</label>
          <input
            type="text"
            placeholder="username"
            className={inputClass}
            value={usernameInput}
            onChange={(e) => handleUsernameChange(e.target.value)}
          />
        </div>
      </div>
      <div className="mt-3 flex justify-end">
        <button
          onClick={handleReset}
          className="text-sm text-gray-500 hover:text-gray-700 underline"
        >
          Reset filters
        </button>
      </div>
    </div>
  );
}
