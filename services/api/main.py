"""
FastAPI application factory.

Error contract
--------------
All 4xx/5xx responses use the shape:
    {"error": {"code": "...", "message": "...", "details": {...}}}

CORS
----
Allowed origins: settings.frontend_origin + http://localhost:3000
"""

import logging
from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from services.api.routers import admin, audiences, catalogs, insights, structure
from services.shared.config import settings

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="Meta Ads Data Warehouse API",
        version="1.0.0",
        description=(
            "Read-only FastAPI service over the Meta Ads warehouse. "
            "Exposes structure, insights, audiences, catalogs, and admin endpoints."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # -----------------------------------------------------------------------
    # CORS
    # -----------------------------------------------------------------------
    allowed_origins = list(
        {settings.frontend_origin, "http://localhost:3000"} - {""}
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )

    # -----------------------------------------------------------------------
    # Error handlers — uniform {error: {code, message, details?}} contract
    # -----------------------------------------------------------------------

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:
        log.exception("Unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": {"code": "internal_error", "message": "An unexpected error occurred."}},
        )

    @app.exception_handler(ValidationError)
    async def _pydantic_validation(request: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "Request validation failed.",
                    "details": exc.errors(),
                }
            },
        )

    # FastAPI's own RequestValidationError (query/path params)
    from fastapi.exceptions import RequestValidationError

    @app.exception_handler(RequestValidationError)
    async def _request_validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={
                "error": {
                    "code": "validation_error",
                    "message": "Request validation failed.",
                    "details": exc.errors(),
                }
            },
        )

    from fastapi import HTTPException as FastAPIHTTPException

    @app.exception_handler(FastAPIHTTPException)
    async def _http_exception(request: Request, exc: FastAPIHTTPException) -> JSONResponse:
        code = _status_to_code(exc.status_code)
        return JSONResponse(
            status_code=exc.status_code,
            content={"error": {"code": code, "message": exc.detail}},
        )

    # -----------------------------------------------------------------------
    # Routers
    # -----------------------------------------------------------------------
    app.include_router(structure.router)
    app.include_router(insights.router)
    app.include_router(audiences.router)
    app.include_router(catalogs.router)
    app.include_router(admin.router)

    # -----------------------------------------------------------------------
    # Health check
    # -----------------------------------------------------------------------
    @app.get("/health", tags=["health"], include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


def _status_to_code(status_code: int) -> str:
    mapping: dict[int, str] = {
        400: "bad_request",
        401: "unauthorized",
        403: "forbidden",
        404: "not_found",
        409: "conflict",
        422: "validation_error",
        429: "rate_limited",
        500: "internal_error",
        503: "service_unavailable",
    }
    return mapping.get(status_code, f"http_{status_code}")


app = create_app()
