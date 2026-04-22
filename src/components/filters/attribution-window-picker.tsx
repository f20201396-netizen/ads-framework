'use client'

import { useEffect, useState } from 'react'

const options = ['1d_click', '7d_click', '7d_view'] as const

export function AttributionWindowPicker() {
  const [value, setValue] = useState<(typeof options)[number]>('7d_click')

  useEffect(() => {
    const saved = window.localStorage.getItem('attribution_window')
    if (saved && options.includes(saved as (typeof options)[number])) {
      setValue(saved as (typeof options)[number])
    }
  }, [])

  const onChange = (nextValue: (typeof options)[number]) => {
    setValue(nextValue)
    window.localStorage.setItem('attribution_window', nextValue)
  }

  return (
    <select
      className="rounded border border-border bg-transparent px-2 py-1 text-xs"
      onChange={(event) => onChange(event.target.value as (typeof options)[number])}
      value={value}
    >
      {options.map((option) => (
        <option key={option} value={option}>
          {option}
        </option>
      ))}
    </select>
  )
}
