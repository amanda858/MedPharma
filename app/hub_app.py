"""Client Hub app — runs on HUB_PORT (default 5240)."""

import os
import time
import shutil
import logging
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response, RedirectResponse, JSONResponse

from app.client_db import init_client_hub_db, normalize_claim_statuses, validate_session
from app.client_routes import router as client_hub_router
from app.notifications import start_daily_scheduler, get_notification_status
from app.database import init_db
from app.leads_app import app as leads_subapp
from app.config import DATABASE_PATH
from app.build_info import BUILD_MARKER

IS_PROD = bool(os.getenv("PORT"))  # Render sets PORT; local dev does not
log = logging.getLogger(__name__)

app = FastAPI(
    title="MedPharma Client Hub",
    description="MedPharma Revenue Cycle Management — Client Portal",
    version="2.0.0",
)


def _backup_db():
    """Create a timestamped backup of the SQLite database before any startup modifications."""
    try:
        if not os.path.exists(DATABASE_PATH):
            log.info("No database file found - skipping backup")
            return
        size = os.path.getsize(DATABASE_PATH)
        if size < 4096:  # empty/fresh DB, nothing worth backing up
            log.info(f"Database too small ({size} bytes) - skipping backup")
            return
        backup_dir = os.path.join(os.path.dirname(DATABASE_PATH), "backups")
        os.makedirs(backup_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(backup_dir, f"leads_{ts}.db")
        log.info(f"Creating DB backup: {size:,} bytes")
        shutil.copy2(DATABASE_PATH, dest)
        log.info(f"DB backup created: {dest}")
        
        # Keep only the 5 most recent backups
        backups = sorted(
            [os.path.join(backup_dir, f) for f in os.listdir(backup_dir) if f.endswith(".db")],
            key=os.path.getmtime,
            reverse=True
        )
        for old in backups[5:]:
            os.remove(old)
            log.info(f"Removed old backup: {old}")
    except Exception as e:
        log.error(f"DB backup failed: {e}")
        # Don't fail startup for backup issues
        pass


@app.on_event("startup")
async def startup():
    logging.basicConfig(level=logging.INFO)

    # ── Safety: check persistent disk on production ──
    if IS_PROD:
        try:
            if not os.path.ismount("/data"):
                log.error("🚨 PERSISTENT DISK NOT MOUNTED at /data — data may be ephemeral!")
            else:
                log.info("✅ Persistent disk mounted at /data")
        except Exception as e:
            log.error(f"Error checking disk mount: {e}")

    # ── Backup existing DB before any schema migrations ──
    try:
        _backup_db()
    except Exception as e:
        log.error(f"Startup error: DB backup failed: {e}")

    # Keep service available even if secondary startup tasks fail.
    try:
        init_db()
        log.info("✅ Leads database initialized")
        # Seed demo leads if database is empty
        from app.database import seed_demo_leads
        seed_demo_leads()
        log.info("✅ Demo leads seeded")
    except Exception as e:
        log.error(f"Startup error: leads DB init failed: {e}")

    try:
        init_client_hub_db()
        log.info("✅ Client hub database initialized")
    except Exception as e:
        log.error(f"Startup error: client DB init failed: {e}")

    try:
        normalize_claim_statuses()
        log.info("✅ Claim statuses normalized")
    except Exception as e:
        log.error(f"Startup error: normalize_claim_statuses failed: {e}")

    try:
        start_daily_scheduler()
        log.info("✅ Daily scheduler started")
    except Exception as e:
        log.error(f"Startup error: notification scheduler failed: {e}")

    log.info("🚀 Hub service startup complete")


app.include_router(client_hub_router)
app.mount("/admin/leads", leads_subapp)


@app.middleware("http")
async def admin_only_leads_guard(request: Request, call_next):
    path = request.url.path or ""
    if path.startswith("/admin/leads"):
        # Allow selected endpoints without authentication for operational checks.
        if (
            path.startswith("/admin/leads/api/admin/")
            or path.startswith("/admin/leads/api/export/")
            or path.startswith("/admin/leads/api/leads/poll-daily")
            or path.startswith("/admin/leads/api/leads/poll-status")
        ):
            return await call_next(request)

        token = request.cookies.get("hub_session")
        user = validate_session(token) if token else None
        if not user:
            if "/api/" in path:
                return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
            return RedirectResponse(url="/hub?next=/admin/leads/", status_code=307)
    return await call_next(request)


def _serve_hub():
    template_path = os.path.join(os.path.dirname(__file__), "templates", "client_hub.html")
    with open(template_path, "r", encoding="utf-8") as f:
        content = f.read()
    build_ts = str(int(time.time()))
    content = content.replace("</head>", f'<meta name="build" content="{build_ts}">\n</head>', 1)
    return Response(
        content=content,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "Surrogate-Control": "no-store",
            "CDN-Cache-Control": "no-store",
        }
    )


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    # Canonical single-link entry point: always route to leads.
    return RedirectResponse(url="/admin/leads/", status_code=307)


@app.get("/hub", response_class=HTMLResponse)
async def hub(request: Request):
    return _serve_hub()


@app.get("/portal", response_class=HTMLResponse)
async def portal(request: Request):
    return _serve_hub()


@app.get("/medpharma", response_class=HTMLResponse)
async def medpharma_redirect(request: Request):
    """Legacy redirect - consolidate to /hub."""
    return RedirectResponse(url="/hub", status_code=301)


@app.get("/mphub2026", response_class=HTMLResponse)
async def mphub2026_redirect(request: Request):
    """Legacy redirect - consolidate to /hub."""
    return RedirectResponse(url="/hub", status_code=301)


@app.get("/admin/leads", include_in_schema=False)
async def admin_leads_root():
    return RedirectResponse(url="/admin/leads/", status_code=307)


@app.get("/leads", include_in_schema=False)
async def leads_shortcut():
    # Human-friendly single link users can bookmark/share.
    return RedirectResponse(url="/admin/leads/", status_code=307)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "hub"}


@app.get("/buildz")
async def buildz():
    return {
        "ok": True,
        "service": "hub",
        "build_marker": BUILD_MARKER,
    }


@app.get("/api/admin/integrations/readiness")
async def integrations_readiness():
    """Compatibility endpoint for admin integrations readiness checks."""
    status = get_notification_status()
    return {
        "ok": True,
        "integrations": {
            "twilio": {
                "configured": status.get("twilio_configured", False),
                "sms_target": status.get("sms_target", ""),
                "missing_fields": status.get("missing_twilio_fields", []),
            },
            "email": {
                "configured": status.get("email_configured", False),
                "sendgrid_configured": status.get("sendgrid_configured", False),
                "smtp_configured": status.get("smtp_configured", False),
                "missing_fields": status.get("missing_email_fields", []),
                "recipients": status.get("email_recipients", []),
            },
            "mode": status.get("delivery_mode", "external"),
            "in_app_only": status.get("in_app_only_mode", False),
        },
    }
