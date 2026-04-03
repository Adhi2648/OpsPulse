from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from opspulse.api.routes import router
from opspulse.core.settings import get_settings
from opspulse.utils.logging import configure_logging


def create_app() -> FastAPI:
    """Create and configure the OpsPulse FastAPI application."""
    settings = get_settings()
    configure_logging(settings.log_level)

    app = FastAPI(
        title="OpsPulse Analytics API",
        version="0.1.0",
        summary="Warehouse-backed analytics API for KPI, backlog, workflow, exception, and team performance queries.",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router)
    return app


app = create_app()
