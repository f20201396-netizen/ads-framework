.PHONY: help up down migrate seed openapi lint test

help:
	@echo "Usage: make <target>"
	@echo ""
	@echo "  up        Start postgres + api + worker via docker-compose"
	@echo "  down      Stop and remove containers"
	@echo "  migrate   Run alembic migrations (requires DB running)"
	@echo "  seed      Historical backfill from 2024-01-01 to today"
	@echo "  openapi   Export openapi.json to repo root"
	@echo "  lint      Run ruff linter"
	@echo "  test      Run pytest"

up:
	docker compose up --build -d

down:
	docker compose down

migrate:
	alembic upgrade head

seed:
	@python - <<'EOF'
import asyncio
import datetime
from services.worker.jobs.backfill import historical_backfill

since = "2024-01-01"
until = datetime.date.today().isoformat()
print(f"Starting backfill {since} → {until} …")
result = asyncio.run(historical_backfill(since=since, until=until))
print("Done:", result)
EOF

openapi:
	python scripts/export_openapi.py

lint:
	ruff check .

test:
	pytest -x -q
