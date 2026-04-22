import { Card } from '@/components/ui/card'
import { deltaColor } from '@/lib/utils/format'

export function MetricCard({ label, value, delta, inverse = false }: { label: string; value: string; delta: number; inverse?: boolean }) {
  return (
    <Card className="space-y-2">
      <p className="text-xs uppercase tracking-wide text-slate-400">{label}</p>
      <p className="text-2xl font-semibold">{value}</p>
      <p className={`text-xs ${deltaColor(delta, inverse)}`}>{delta >= 0 ? '+' : ''}{delta.toFixed(2)}%</p>
    </Card>
  )
}
