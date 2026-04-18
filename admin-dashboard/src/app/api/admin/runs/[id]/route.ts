export const dynamic = 'force-dynamic';

import { NextRequest, NextResponse } from 'next/server';
import { getSessionById } from '@/lib/queries';

export async function GET(
  _request: NextRequest,
  { params }: { params: { id: string } }
) {
  try {
    const session = await getSessionById(params.id);
    if (!session) {
      return NextResponse.json({ error: 'Session not found' }, { status: 404 });
    }
    return NextResponse.json(session);
  } catch (err) {
    console.error('[runs/id] failed:', err);
    return NextResponse.json({ error: 'Failed to fetch session' }, { status: 500 });
  }
}
