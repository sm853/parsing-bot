import { NextResponse } from 'next/server';
import { getOverviewMetrics } from '@/lib/queries';

export async function GET() {
  try {
    const data = await getOverviewMetrics();
    return NextResponse.json(data);
  } catch (err) {
    console.error('[overview] failed:', err);
    return NextResponse.json({ error: 'Failed to fetch overview metrics' }, { status: 500 });
  }
}
