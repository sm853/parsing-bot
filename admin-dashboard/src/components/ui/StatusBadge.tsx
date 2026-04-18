import clsx from 'clsx';
import type { SessionStatus, AttemptStatus } from '@/lib/types';

const statusConfig: Record<SessionStatus | AttemptStatus, { label: string; classes: string }> = {
  success:         { label: 'Success',         classes: 'bg-green-100 text-green-800' },
  failed:          { label: 'Failed',           classes: 'bg-red-100 text-red-800' },
  running:         { label: 'Running',          classes: 'bg-blue-100 text-blue-800' },
  queued:          { label: 'Queued',           classes: 'bg-gray-100 text-gray-700' },
  partial_success: { label: 'Partial Success',  classes: 'bg-yellow-100 text-yellow-800' },
  cancelled:       { label: 'Cancelled',        classes: 'bg-gray-100 text-gray-500' },
};

interface StatusBadgeProps {
  status: SessionStatus | AttemptStatus;
}

export function StatusBadge({ status }: StatusBadgeProps) {
  const config = statusConfig[status] ?? { label: status, classes: 'bg-gray-100 text-gray-600' };
  return (
    <span
      className={clsx(
        'inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium',
        config.classes
      )}
    >
      {config.label}
    </span>
  );
}
