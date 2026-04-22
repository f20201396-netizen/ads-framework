import Link from 'next/link'
import type { ReactNode } from 'react'

const links = [
  { href: '/', label: 'Overview' },
  { href: '/campaigns', label: 'Campaigns' },
  { href: '/creatives', label: 'Creatives' },
  { href: '/audiences', label: 'Audiences' },
  { href: '/pixels', label: 'Pixels' },
  { href: '/catalogs', label: 'Catalogs' },
  { href: '/diagnostics', label: 'Diagnostics' },
]

export function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="min-h-screen">
      <header className="border-b border-border bg-card/80 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-3">
          <h1 className="text-sm font-semibold uppercase tracking-wide">Meta Ads Dashboard</h1>
          <nav className="flex flex-wrap gap-2 text-sm text-muted-foreground">
            {links.map((link) => (
              <Link key={link.href} className="rounded px-2 py-1 hover:bg-white/10" href={link.href}>
                {link.label}
              </Link>
            ))}
          </nav>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-6 py-6">{children}</main>
    </div>
  )
}
