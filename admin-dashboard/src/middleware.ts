import { NextResponse } from 'next/server';
import type { NextRequest } from 'next/server';

// Protect all /admin and /api/admin routes.
// Allow /login and /api/auth/* without a token.
export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  if (pathname.startsWith('/login') || pathname.startsWith('/api/auth')) {
    return NextResponse.next();
  }

  const token = request.cookies.get('admin_token')?.value;
  const expected = process.env.ADMIN_SECRET;

  if (!expected || token !== expected) {
    const loginUrl = new URL('/login', request.url);
    loginUrl.searchParams.set('from', pathname);
    return NextResponse.redirect(loginUrl);
  }

  return NextResponse.next();
}

export const config = {
  matcher: ['/admin/:path*', '/api/admin/:path*'],
};
