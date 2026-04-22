'use client'

import { addDays, format } from 'date-fns'
import { usePathname, useRouter, useSearchParams } from 'next/navigation'

const presets = [
  { label: 'Today', days: 0 },
  { label: 'Last 7d', days: 7 },
  { label: 'Last 30d', days: 30 },
  { label: 'Last 90d', days: 90 },
]

export function DateRangePicker() {
  const router = useRouter()
  const pathname = usePathname()
  const searchParams = useSearchParams()

  const applyDays = (days: number) => {
    const params = new URLSearchParams(searchParams.toString())
    const until = new Date()
    const since = days === 0 ? until : addDays(until, -days + 1)
    params.set('since', format(since, 'yyyy-MM-dd'))
    params.set('until', format(until, 'yyyy-MM-dd'))
    router.replace(`${pathname}?${params.toString()}`)
  }

  return (
    <div className="flex flex-wrap gap-2">
      {presets.map((preset) => (
        <button
          className="rounded border border-border px-2 py-1 text-xs hover:bg-white/10"
          key={preset.label}
          onClick={() => applyDays(preset.days)}
          type="button"
        >
          {preset.label}
        </button>
      ))}
    </div>
  )
}
