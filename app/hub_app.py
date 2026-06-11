"""Client Hub app — runs on HUB_PORT (default 5240)."""

import os
import time
import shutil
import logging
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response, RedirectResponse, JSONResponse

from app.client_db import get_db, init_client_hub_db, normalize_claim_statuses, validate_session
from app.client_routes import router as client_hub_router
from app.notifications import start_daily_scheduler, get_notification_status
from app.config import DATABASE_PATH
from app.build_info import BUILD_MARKER

IS_PROD = bool(os.getenv("PORT"))  # Render sets PORT; local dev does not
log = logging.getLogger(__name__)

app = FastAPI(
    title="MedPharma Client Hub",
    description="MedPharma Revenue Cycle Management — Client Portal",
    version="2.0.0",
)
app.state.startup_ready = False
app.state.startup_status = {"db": False, "scheduler": False}


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
    app.state.startup_ready = False
    app.state.startup_status = {"db": False, "scheduler": False}

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

    try:
        init_client_hub_db()
        log.info("✅ Client hub database initialized")
        app.state.startup_status["db"] = True
    except Exception as e:
        log.error(f"Startup error: client DB init failed: {e}")
        raise RuntimeError("Client hub database initialization failed") from e

    try:
        normalize_claim_statuses()
        log.info("✅ Claim statuses normalized")
    except Exception as e:
        log.error(f"Startup error: normalize_claim_statuses failed: {e}")

    try:
        start_daily_scheduler()
        log.info("✅ Daily scheduler started")
        app.state.startup_status["scheduler"] = True
    except Exception as e:
        log.error(f"Startup error: notification scheduler failed: {e}")

    app.state.startup_ready = True
    log.info("🚀 Hub service startup complete")


app.include_router(client_hub_router)


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
    # Canonical single-link entry point: unified hub dashboard.
    return RedirectResponse(url="/hub", status_code=307)


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


@app.api_route("/admin/leads", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], include_in_schema=False)
@app.api_route("/admin/leads/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"], include_in_schema=False)
async def removed_leads_surface(request: Request, path: str = ""):
    if request.url.path.startswith("/admin/leads/api/"):
        return Response(
            content='{"detail":"Leads module has been removed"}',
            media_type="application/json",
            status_code=410,
        )
    return RedirectResponse(url="/hub", status_code=307)


@app.get("/leads", include_in_schema=False)
async def leads_shortcut():
    return RedirectResponse(url="/hub", status_code=307)


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "hub"}


@app.get("/readyz")
async def readyz():
    if not app.state.startup_ready or not app.state.startup_status.get("db"):
        return JSONResponse(
            status_code=503,
            content={"ok": False, "service": "hub", "ready": False, "status": app.state.startup_status},
        )

    try:
        conn = get_db()
        try:
            conn.execute("SELECT 1")
        finally:
            conn.close()
    except Exception as e:
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "service": "hub",
                "ready": False,
                "status": app.state.startup_status,
                "detail": str(e),
            },
        )

    # HIPAA visibility: surface chat encryption readiness so ops can spot a
    # missing key without admin login.
    try:
        from app.security import encryption_status
        chat_enc = encryption_status()
    except Exception:
        chat_enc = {"encryption": "unknown", "ready": False}

    return {
        "ok": True,
        "service": "hub",
        "ready": True,
        "status": app.state.startup_status,
        "chat_encryption": chat_enc,
    }


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
