import type { HTMLAttributes } from 'react'
import { cn } from '@/lib/utils/cn'

export function Badge({ className, ...props }: HTMLAttributes<HTMLSpanElement>) {
  return (
    <span
      className={cn('inline-flex rounded-full bg-white/10 px-2 py-0.5 text-xs font-medium uppercase tracking-wide', className)}
      {...props}
    />
  )
}
