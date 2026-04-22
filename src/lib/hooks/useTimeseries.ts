import useSWR from 'swr'
import { apiClient } from '@/lib/api/client'

export type TimeseriesParams = {
  account_id: string
  since: string
  until: string
}

export function useTimeseries(params: TimeseriesParams) {
  const key = ['timeseries', params] as const
  return useSWR(key, () => apiClient.getInsightsTimeseries(new URLSearchParams(params).toString()), {
    dedupingInterval: 60_000,
    revalidateOnFocus: false,
  })
}
