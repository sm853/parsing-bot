import { NextResponse } from 'next/server';
import { getErrorStats } from '@/lib/queries';

export async function GET() {
  try {
    const data = await getErrorStats();
    return NextResponse.json(data);
  } catch (err) {
    console.error('[errors] failed:', err);
    return NextResponse.json({ error: 'Failed to fetch error stats' }, { status: 500 });
  }
}
