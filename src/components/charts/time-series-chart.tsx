'use client'

import { Line, LineChart, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import { formatINR } from '@/lib/utils/format'

type Datum = Record<string, number | string>

type Metric = {
  key: string
  color: string
  yAxisId?: 'left' | 'right'
}

export function TimeSeriesChart({ data, metrics }: { data: Datum[]; metrics: Metric[] }) {
  return (
    <div className="h-72 w-full rounded-lg border border-border p-2">
      <ResponsiveContainer>
        <LineChart data={data}>
          <XAxis dataKey="date" />
          <YAxis yAxisId="left" />
          <YAxis orientation="right" yAxisId="right" />
          <Tooltip formatter={(value) => (typeof value === 'number' ? formatINR(value) : value)} />
          {metrics.map((metric) => (
            <Line key={metric.key} dataKey={metric.key} dot={false} stroke={metric.color} yAxisId={metric.yAxisId ?? 'left'} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
