export const dynamic = 'force-dynamic';

import { NextRequest, NextResponse } from 'next/server';
import { getTimeseries } from '@/lib/queries';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = request.nextUrl;
    const days = searchParams.get('days') ? parseInt(searchParams.get('days')!, 10) : 30;
    const data = await getTimeseries(days);
    return NextResponse.json(data);
  } catch (err) {
    console.error('[timeseries] failed:', err);
    return NextResponse.json({ error: 'Failed to fetch timeseries' }, { status: 500 });
  }
}
