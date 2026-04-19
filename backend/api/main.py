import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import router
from api.async_store import get_async_client, ensure_indexes

logger = logging.getLogger("api.main")


def _parse_cors_origins() -> list[str]:
    raw = os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://localhost:3000",
    )
    return [o.strip() for o in raw.split(",") if o.strip()]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: ensure Mongo indexes exist. Safe to call repeatedly.
    try:
        await ensure_indexes()
    except Exception as e:  # don't crash the API if Mongo is slow on boot
        logger.warning("[STARTUP] ensure_indexes failed: %s", e)
    yield
    # Shutdown: close async MongoDB client
    client = get_async_client()
    client.close()


app = FastAPI(
    title="Data Processor API",
    description="API server for dataset processor.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=_parse_cors_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")
