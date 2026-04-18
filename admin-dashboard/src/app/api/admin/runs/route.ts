import { NextRequest, NextResponse } from 'next/server';
import { getRuns } from '@/lib/queries';
import type { RunFilters, SessionStatus } from '@/lib/types';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = request.nextUrl;

    const filters: RunFilters = {
      from: searchParams.get('from') ?? undefined,
      to: searchParams.get('to') ?? undefined,
      status: (searchParams.get('status') as SessionStatus) || undefined,
      channel: searchParams.get('channel') ?? undefined,
      username: searchParams.get('username') ?? undefined,
      page: searchParams.get('page') ? parseInt(searchParams.get('page')!, 10) : 1,
      limit: searchParams.get('limit') ? parseInt(searchParams.get('limit')!, 10) : 20,
    };

    const { rows, total } = await getRuns(filters);
    return NextResponse.json({
      rows,
      total,
      page: filters.page,
      limit: filters.limit,
    });
  } catch (err) {
    console.error('[runs] failed:', err);
    return NextResponse.json({ error: 'Failed to fetch runs' }, { status: 500 });
  }
}
