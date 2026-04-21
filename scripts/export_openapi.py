#!/usr/bin/env python3
"""
Export the FastAPI OpenAPI schema to openapi.json in the repo root.

Usage:
    python scripts/export_openapi.py
"""

import json
import sys
from pathlib import Path

# Ensure repo root is on sys.path
repo_root = Path(__file__).parent.parent
sys.path.insert(0, str(repo_root))

from services.api.main import app  # noqa: E402

schema = app.openapi()
out_path = repo_root / "openapi.json"
out_path.write_text(json.dumps(schema, indent=2))
print(f"Wrote {out_path} ({len(schema['paths'])} paths)")
