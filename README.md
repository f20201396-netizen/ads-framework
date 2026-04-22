# Meta Ads Dashboard (Frontend)

Next.js 14 App Router frontend for visualizing the Meta Ads data warehouse.

## Stack

- Next.js 14 + React 18
- TypeScript (strict)
- Tailwind CSS (dark mode default)
- SWR (data fetching)
- Recharts (charts)
- @tanstack/react-table (tables)
- date-fns (date formatting)
- openapi-typescript (typed API generation)

## Environment

Copy `.env.example` to `.env.local` and configure:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

## Setup

```bash
npm install
npm run gen:api
npm run dev
```

Open `http://localhost:3000`.

## OpenAPI type generation

Types are generated from the backend OpenAPI spec:

```bash
bash scripts/gen-api-client.sh ../meta-ads-backend/openapi.json
```

or via npm script:

```bash
npm run gen:api
```

Generated output: `src/types/api.ts`.

> Note: in this container, `../meta-ads-backend/openapi.json` is not present, so generation fails until the backend repo/spec is available.

## Current project structure

```text
.
├── .env.example
├── scripts/
│   └── gen-api-client.sh
├── src/
│   ├── app/
│   ├── components/
│   │   ├── charts/
│   │   ├── filters/
│   │   ├── tables/
│   │   └── ui/
│   ├── lib/
│   │   ├── api/
│   │   ├── hooks/
│   │   └── utils/
│   └── types/
└── ...tooling config files
```

## What is implemented so far

- App shell layout + dark theme provider
- Top-level dashboard navigation scaffold
- Shared component foundations:
  - `DateRangePicker`
  - `AttributionWindowPicker`
  - `MetricCard`
  - `TimeSeriesChart`
  - `BreakdownChart`
  - `EntityTable`
  - `CreativePreview`
  - `IssuesPanel`
- Formatting helpers in `src/lib/utils/format.ts`
- Typed API client scaffold + SWR `useTimeseries` hook

## Development notes

- Keep filters URL-driven where applicable.
- Do not hardcode API contracts; regenerate from OpenAPI whenever backend changes.
- Prefer extending shared components over duplicating page-specific UI logic.
