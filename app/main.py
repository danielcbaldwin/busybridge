"""Main FastAPI application entry point."""

import logging
import logging.handlers
import os
import sys
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware

from app.config import get_settings
from app.database import close_database, get_database

# Configure logging.  stdout always; a rotating file handler when
# the log directory is writable.
_log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

logging.basicConfig(
    level=logging.INFO,
    format=_log_format,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _install_file_logging(log_dir: str) -> bool:
    """Best-effort rotating file logging.

    Production mounts ``/data`` as a writable volume, but dev / CI /
    sandbox environments often do not — and this runs at import
    time, so a failure here would make the whole app un-importable.
    On an unwritable directory it logs a warning and returns
    ``False``; stdout logging still applies.
    """
    try:
        os.makedirs(log_dir, exist_ok=True)
        handler = logging.handlers.TimedRotatingFileHandler(
            os.path.join(log_dir, "busybridge.log"),
            when="midnight",
            backupCount=14,
        )
        handler.setFormatter(logging.Formatter(_log_format))
        logging.getLogger().addHandler(handler)
        return True
    except OSError as e:
        logger.warning(
            "file logging disabled (%s not writable: %s); "
            "logging to stdout only",
            log_dir, e,
        )
        return False


_install_file_logging(get_settings().log_dir)


# Rate limiter (shared instance lives in app.rate_limit to avoid circular imports)
from app.rate_limit import limiter


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    settings = get_settings()
    logger.info(f"Starting Calendar Sync Engine...")
    logger.info(f"Public URL: {settings.public_url}")
    logger.info(f"Database: {settings.database_path}")

    # -----------------------------------------------------------------------
    # Catastrophic-recovery path: if a restore-pending.zip exists next to the
    # database file, restore it NOW — before aiosqlite opens the DB and before
    # the scheduler fires a single sync job.
    #
    # To trigger: drop a BusyBridge backup ZIP at
    #   <data dir>/restore-pending.zip
    # (i.e. ./data/restore-pending.zip on the host)
    # then start (or restart) the container.  The file is archived as
    # restore-pending-done-<timestamp>.zip after a successful restore so it
    # will not re-trigger on the next restart.
    # -----------------------------------------------------------------------
    _startup_restored = False
    _restore_pending = os.path.join(
        os.path.dirname(settings.database_path), "restore-pending.zip"
    )
    if os.path.exists(_restore_pending):
        logger.warning("=" * 60)
        logger.warning("STARTUP RESTORE: restore-pending.zip detected.")
        logger.warning("Restoring database before opening connections.")
        logger.warning("Sync will NOT start until restore is complete.")
        logger.warning("=" * 60)
        try:
            from app.sync.backup import apply_startup_restore
            _meta = await apply_startup_restore(_restore_pending)
            # Archive so it doesn't re-trigger on the next restart
            _done = _restore_pending.replace(
                ".zip",
                f"-done-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip",
            )
            os.rename(_restore_pending, _done)
            _startup_restored = True
            logger.warning(
                f"STARTUP RESTORE COMPLETE: restored backup "
                f"'{_meta.get('backup_id', 'unknown')}'. "
                f"Archived restore file to {os.path.basename(_done)}."
            )
            logger.warning(
                "Calendar events will be reconciled automatically on the "
                "first consistency check. You may also trigger it manually "
                "via POST /api/admin/consistency/check."
            )
        except Exception as exc:
            logger.error("=" * 60)
            logger.error(f"STARTUP RESTORE FAILED: {exc}")
            logger.error(
                "Refusing to start — sync must not run on an unknown state. "
                "Fix the restore-pending.zip and restart."
            )
            logger.error("=" * 60)
            raise SystemExit(1)

    # Initialize database (opens aiosqlite — restored file if we just swapped it)
    try:
        await get_database()
    except Exception as exc:
        logger.error("=" * 60)
        logger.error(f"DATABASE INIT FAILED: {exc}")
        logger.error(
            "Schema creation or migration did not complete — refusing to "
            "start on a half-migrated database."
        )
        logger.error("=" * 60)
        raise SystemExit(1)
    logger.info("Database initialized")

    # Initialize encryption manager if key exists
    if os.path.exists(settings.encryption_key_file):
        try:
            from app.encryption import init_encryption_manager
            from app.config import get_encryption_key
            key = get_encryption_key()
            init_encryption_manager(key)
        except Exception as exc:
            logger.error("=" * 60)
            logger.error(f"ENCRYPTION INIT FAILED: {exc}")
            logger.error(
                "The encryption key file exists but could not be loaded. "
                "OAuth tokens and the session secret cannot be derived "
                "without it — refusing to start."
            )
            logger.error("=" * 60)
            raise SystemExit(1)
        logger.info("Encryption manager initialized")

    # After a startup restore, clear all sync tokens so every calendar does a
    # clean full re-fetch on the first sync rather than using stale tokens.
    if _startup_restored:
        try:
            from app.sync.backup import _clear_sync_tokens
            await _clear_sync_tokens()
            logger.info("Startup restore: sync tokens cleared — full re-sync on first run")
        except Exception as e:
            logger.warning(f"Could not clear sync tokens after restore: {e}")

    # Start background scheduler
    try:
        from app.jobs.scheduler import setup_scheduler
        scheduler = setup_scheduler()
        logger.info("Background scheduler started")
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")

    # Optional: BB_FAKE_GOOGLE=1 boots the app with the in-memory
    # FakeGoogleCalendar wired into the ledger runtime.  Useful for
    # end-to-end smoke tests against a running uvicorn (no real
    # Google credentials required).  Strictly opt-in.
    if os.environ.get("BB_FAKE_GOOGLE") == "1":
        try:
            from app.ledger.runtime import set_google_client_factory
            from tests.fakes.google_calendar import FakeGoogleCalendar

            _fake = FakeGoogleCalendar()
            # Register every calendar already in the DB so it's
            # immediately addressable.
            db_conn = await get_database()
            rows = await (await db_conn.execute(
                "SELECT DISTINCT main_calendar_id FROM users "
                "WHERE main_calendar_id IS NOT NULL"
            )).fetchall()
            for row in rows:
                try:
                    _fake.add_calendar(row[0], row[0])
                except Exception:
                    pass
            rows = await (await db_conn.execute(
                "SELECT DISTINCT google_calendar_id FROM client_calendars"
            )).fetchall()
            for row in rows:
                try:
                    _fake.add_calendar(row[0], row[0])
                except Exception:
                    pass

            async def _factory(user_id, email):
                return _fake

            set_google_client_factory(_factory)
            # Expose on app.state so external test scripts can
            # poke events into it via a debug endpoint.
            app.state.fake_google = _fake
            logger.warning(
                "BB_FAKE_GOOGLE=1: ledger runtime wired to FakeGoogleCalendar "
                "(NOT for production use)",
            )
        except Exception as e:
            logger.error(f"Failed to install fake Google client: {e}")

    yield

    # Shutdown
    logger.info("Shutting down...")

    # Stop scheduler
    try:
        from app.jobs.scheduler import shutdown_scheduler
        shutdown_scheduler()
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")

    # Close database
    await close_database()
    logger.info("Shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Calendar Sync Engine",
    description="A self-hosted, multi-user calendar synchronization service",
    version="1.0.0",
    lifespan=lifespan,
)

# Add rate limiter — middleware applies default_limits to all endpoints;
# exception handler formats the 429 response.
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# Add CORS middleware
settings = get_settings()
allowed_origins = [settings.public_url]
# Also allow localhost variants for development
if settings.public_url.startswith("http://localhost") or settings.public_url.startswith("https://localhost"):
    allowed_origins.extend([
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ])

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    allow_headers=["*"],
)


# Security headers applied to every response.  All front-end assets are
# served from /static (vendored — no third-party CDN), so the CSP can
# pin every source to 'self'.  'unsafe-eval' is required by the
# Tailwind Play runtime and Alpine's expression evaluator; 'unsafe-
# inline' covers the small inline <script>/<style> blocks in the
# templates.  img-src allows https: so Google profile avatars render.
_CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
    "style-src 'self' 'unsafe-inline'; "
    "img-src 'self' data: https:; "
    "font-src 'self'; "
    "connect-src 'self'; "
    "frame-ancestors 'none'; "
    "base-uri 'self'; "
    "form-action 'self'"
)
_HSTS_ENABLED = settings.public_url.lower().startswith("https://")


@app.middleware("http")
async def security_headers(request: Request, call_next):
    """Attach defensive response headers to every response."""
    response = await call_next(request)
    response.headers["Content-Security-Policy"] = _CSP
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    if _HSTS_ENABLED:
        response.headers["Strict-Transport-Security"] = (
            "max-age=31536000; includeSubDomains"
        )
    return response


# Health check endpoint
@app.get("/health")
@limiter.exempt
async def health_check():
    """Readiness probe for monitoring.

    Deliberately checks more than connectivity: a half-migrated schema
    or an uninitialised encryption manager would let a bare ``SELECT 1``
    report healthy while sync and auth are silently broken.
    """
    try:
        db = await get_database()
        # Schema readiness — a core ledger table must be queryable.
        await db.execute("SELECT 1 FROM ledger_projections LIMIT 1")
        # Once OOBE is complete, OAuth tokens are unreadable without the
        # encryption manager — an uninitialised one means broken auth.
        from app.database import is_oobe_completed
        from app.encryption import is_encryption_initialized
        if await is_oobe_completed() and not is_encryption_initialized():
            raise RuntimeError("encryption manager not initialized")
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"status": "unhealthy", "error": str(e)},
        )


# ---------------------------------------------------------------------------
# Debug endpoints — only mounted when BB_FAKE_GOOGLE=1
# ---------------------------------------------------------------------------
if os.environ.get("BB_FAKE_GOOGLE") == "1":
    @app.post("/_fake/calendars/{calendar_id}/events")
    @limiter.exempt
    async def _fake_insert_event(calendar_id: str, body: dict):
        """Plant an event on the FakeGoogleCalendar.  Only available
        when ``BB_FAKE_GOOGLE=1``.  Returns the inserted event dict."""
        fake = getattr(app.state, "fake_google", None)
        if fake is None:
            return JSONResponse(
                status_code=503,
                content={"error": "fake Google not initialised"},
            )
        try:
            return fake.insert_event(calendar_id, body)
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": str(e)})

    @app.delete("/_fake/calendars/{calendar_id}/events/{event_id}")
    @limiter.exempt
    async def _fake_delete_event(calendar_id: str, event_id: str):
        fake = getattr(app.state, "fake_google", None)
        if fake is None:
            return JSONResponse(status_code=503, content={"error": "fake Google not initialised"})
        try:
            fake.delete_event(calendar_id, event_id)
            return {"status": "deleted"}
        except Exception as e:
            return JSONResponse(status_code=400, content={"error": str(e)})

    @app.get("/_fake/calendars/{calendar_id}/events")
    @limiter.exempt
    async def _fake_list_events(calendar_id: str, show_deleted: bool = False):
        fake = getattr(app.state, "fake_google", None)
        if fake is None:
            return JSONResponse(status_code=503, content={"error": "fake Google not initialised"})
        return fake.list_events(calendar_id, show_deleted=show_deleted)

    @app.get("/_fake/calendars/{calendar_id}/events/{event_id}/instances")
    @limiter.exempt
    async def _fake_list_instances(
        calendar_id: str, event_id: str, show_deleted: bool = False,
    ):
        fake = getattr(app.state, "fake_google", None)
        if fake is None:
            return JSONResponse(status_code=503, content={"error": "fake Google not initialised"})
        return fake.list_instances(calendar_id, event_id, show_deleted=show_deleted)


# Include routers
from app.auth.routes import router as auth_router
from app.api import api_router
from app.ui.routes import router as ui_router
from app.ui.setup import router as setup_router

app.include_router(auth_router)
app.include_router(api_router)
app.include_router(ui_router)
app.include_router(setup_router)

# Mount static files
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


# Global exception handler
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Handle uncaught exceptions."""
    logger.exception(f"Unhandled exception: {exc}")

    # For API requests, return JSON
    if request.url.path.startswith("/api/"):
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"},
        )

    # For other requests, redirect to error page or show generic error
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "An unexpected error occurred"},
    )


# Redirect /favicon.ico to prevent 404 errors
@app.get("/favicon.ico")
async def favicon():
    """Return empty response for favicon."""
    from fastapi.responses import Response
    return Response(status_code=204)


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    log_level = settings.log_level.lower()

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=3000,
        log_level=log_level,
        reload=False,
    )
