import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'

type Issue = {
  severity: 'low' | 'medium' | 'high'
  message: string
}

export function IssuesPanel({ issues, recommendations }: { issues: Issue[]; recommendations: string[] }) {
  return (
    <Card className="space-y-3">
      <h3 className="text-sm font-semibold uppercase tracking-wide">Issues & Recommendations</h3>
      <div className="space-y-2">
        {issues.map((issue) => (
          <div className="rounded border border-border p-2" key={issue.message}>
            <Badge className={issue.severity === 'high' ? 'bg-red-500/20' : issue.severity === 'medium' ? 'bg-yellow-500/20' : 'bg-blue-500/20'}>
              {issue.severity}
            </Badge>
            <p className="mt-1 text-sm">{issue.message}</p>
          </div>
        ))}
      </div>
      <ul className="list-disc space-y-1 pl-5 text-sm text-slate-300">
        {recommendations.map((item) => (
          <li key={item}>{item}</li>
        ))}
      </ul>
    </Card>
  )
}
