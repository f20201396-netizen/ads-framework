import './globals.css'
import type { Metadata } from 'next'
import type { ReactNode } from 'react'
import { AppShell } from '@/components/app-shell'
import { ThemeProvider } from '@/components/theme-provider'

export const metadata: Metadata = {
  title: 'Meta Ads Dashboard',
  description: 'Frontend for Meta Ads warehouse analytics',
}

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <ThemeProvider attribute="class" defaultTheme="dark" enableSystem={false}>
          <AppShell>{children}</AppShell>
        </ThemeProvider>
      </body>
    </html>
  )
}
