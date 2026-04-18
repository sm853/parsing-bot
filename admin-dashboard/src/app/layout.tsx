import type { Metadata } from 'next';
import Link from 'next/link';
import './globals.css';

export const metadata: Metadata = {
  title: 'Parsing Bot Admin',
  description: 'Admin dashboard for Telegram parsing bot',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>
        <header className="bg-white border-b border-gray-200 sticky top-0 z-10">
          <div className="max-w-screen-xl mx-auto px-6 h-14 flex items-center gap-6">
            <span className="font-semibold text-gray-900 text-sm">Parsing Bot</span>
            <nav className="flex items-center gap-1">
              <Link
                href="/admin"
                className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-md transition-colors"
              >
                Overview
              </Link>
              <Link
                href="/admin/runs"
                className="px-3 py-1.5 text-sm text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-md transition-colors"
              >
                Runs
              </Link>
            </nav>
          </div>
        </header>
        <main className="max-w-screen-xl mx-auto px-6 py-8">{children}</main>
      </body>
    </html>
  );
}
