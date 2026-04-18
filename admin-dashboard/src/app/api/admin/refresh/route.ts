export const dynamic = 'force-dynamic';

import { NextRequest, NextResponse } from 'next/server';
import { recomputeDailyStats } from '@/lib/queries';

export async function POST(request: NextRequest) {
  try {
    const adminSecret = process.env.ADMIN_SECRET;
    if (adminSecret) {
      const authHeader = request.headers.get('authorization');
      const token = authHeader?.replace('Bearer ', '');
      if (token !== adminSecret) {
        return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
      }
    }

    await recomputeDailyStats();
    return NextResponse.json({ ok: true, recomputedAt: new Date().toISOString() });
  } catch (err) {
    console.error('[refresh] failed:', err);
    return NextResponse.json({ error: 'Failed to recompute stats' }, { status: 500 });
  }
}
