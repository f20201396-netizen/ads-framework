import type { components } from '@/types/api'

export type ApiErrorPayload = {
  error: {
    code: string
    message: string
    details?: unknown
  }
}

export class ApiError extends Error {
  constructor(public payload: ApiErrorPayload) {
    super(payload.error.message)
    this.name = 'ApiError'
  }
}

const baseUrl = process.env.NEXT_PUBLIC_API_BASE_URL ?? 'http://localhost:8000'

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${baseUrl}${path}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
    cache: 'no-store',
  })
  const json = (await response.json()) as T | ApiErrorPayload

  if (!response.ok && 'error' in (json as ApiErrorPayload)) {
    throw new ApiError(json as ApiErrorPayload)
  }

  return json as T
}

export type TimeseriesResponse = components['schemas']['TimeseriesResponse']

export const apiClient = {
  getInsightsTimeseries: (query: string) => request<TimeseriesResponse>(`/insights/timeseries?${query}`),
}
