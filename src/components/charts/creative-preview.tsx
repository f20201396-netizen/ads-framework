'use client'

import { useState } from 'react'

const formats = ['MOBILE_FEED_STANDARD', 'INSTAGRAM_STORY', 'INSTAGRAM_REELS', 'FACEBOOK_REELS_MOBILE']

export function CreativePreview({ src }: { src: string }) {
  const [format, setFormat] = useState(formats[0])

  return (
    <div className="space-y-2 rounded-lg border border-border p-3">
      <select
        className="rounded border border-border bg-transparent px-2 py-1 text-xs"
        onChange={(event) => setFormat(event.target.value)}
        value={format}
      >
        {formats.map((item) => (
          <option key={item} value={item}>
            {item}
          </option>
        ))}
      </select>
      <iframe className="h-96 w-full rounded" sandbox="allow-same-origin allow-scripts" src={`${src}?format=${format}`} title="Creative preview" />
    </div>
  )
}
