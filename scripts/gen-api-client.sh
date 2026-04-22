#!/usr/bin/env bash
set -euo pipefail

SPEC_PATH="${1:-../meta-ads-backend/openapi.json}"
OUT_PATH="src/types/api.ts"

if [[ ! -f "$SPEC_PATH" ]]; then
  echo "OpenAPI spec not found at $SPEC_PATH" >&2
  exit 1
fi

npx openapi-typescript "$SPEC_PATH" --output "$OUT_PATH"
echo "Generated $OUT_PATH from $SPEC_PATH"
