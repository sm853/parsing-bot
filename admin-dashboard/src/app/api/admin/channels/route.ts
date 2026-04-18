export const dynamic = 'force-dynamic';

import { NextResponse } from 'next/server';
import { getChannelStats } from '@/lib/queries';

export async function GET() {
  try {
    const data = await getChannelStats();
    return NextResponse.json(data);
  } catch (err) {
    console.error('[channels] failed:', err);
    return NextResponse.json({ error: 'Failed to fetch channel stats' }, { status: 500 });
  }
}
