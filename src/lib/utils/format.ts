import { format } from 'date-fns'

const inrFormatter = new Intl.NumberFormat('en-IN', {
  style: 'currency',
  currency: 'INR',
  maximumFractionDigits: 2,
})

export function formatINR(n: number): string {
  if (!Number.isFinite(n)) return '₹0'
  const abs = Math.abs(n)
  if (abs >= 1e7) return `₹${(n / 1e7).toFixed(2).replace(/\.00$/, '')}Cr`
  if (abs >= 1e5) return `₹${(n / 1e5).toFixed(2).replace(/\.00$/, '')}L`
  return inrFormatter.format(n)
}

export function formatCompact(n: number): string {
  if (!Number.isFinite(n)) return '0'
  return new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(n)
}

export function formatPct(n: number, decimals = 2): string {
  if (!Number.isFinite(n)) return '0%'
  return `${(n * 100).toFixed(decimals)}%`
}

export function formatDate(d: Date | string, fmt = 'dd MMM yyyy'): string {
  return format(typeof d === 'string' ? new Date(d) : d, fmt)
}

export function deltaColor(value: number, inverse = false): string {
  if (value === 0) return 'text-slate-400'
  const positiveClass = inverse ? 'text-red-400' : 'text-emerald-400'
  const negativeClass = inverse ? 'text-emerald-400' : 'text-red-400'
  return value > 0 ? positiveClass : negativeClass
}
