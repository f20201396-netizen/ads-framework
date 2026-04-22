'use client'

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

type BreakdownDatum = {
  label: string
  value: number
}

export function BreakdownChart({ data }: { data: BreakdownDatum[] }) {
  return (
    <div className="h-72 w-full rounded-lg border border-border p-2">
      <ResponsiveContainer>
        <BarChart data={data}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="label" />
          <YAxis />
          <Tooltip />
          <Bar dataKey="value" fill="#22c55e" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}
