export const dynamic = 'force-dynamic';

import { NextRequest, NextResponse } from 'next/server';
import { recomputeDailyStats } from '@/lib/queries';

export async function POST(_request: NextRequest) {
  try {
    await recomputeDailyStats();
    return NextResponse.json({ ok: true, recomputedAt: new Date().toISOString() });
  } catch (err) {
    console.error('[refresh] failed:', err);
    return NextResponse.json({ error: 'Failed to recompute stats' }, { status: 500 });
  }
}
