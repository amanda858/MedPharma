"""Client Hub app — runs on HUB_PORT (default 5240)."""

import os
import time
import shutil
import logging
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response

from app.client_db import init_client_hub_db, normalize_claim_statuses
from app.client_routes import router as client_hub_router
from app.notifications import start_daily_scheduler, get_notification_status
from app.config import DATABASE_PATH

IS_PROD = bool(os.getenv("PORT"))  # Render sets PORT; local dev does not
log = logging.getLogger(__name__)

app = FastAPI(
    title="MedPharma Client Hub",
    description="MedPharma Revenue Cycle Management — Client Portal",
    version="2.0.0",
)


def _backup_db():
    """Create a timestamped backup of the SQLite database before any startup modifications."""
    if not os.path.exists(DATABASE_PATH):
        return
    size = os.path.getsize(DATABASE_PATH)
    if size < 4096:  # empty/fresh DB, nothing worth backing up
        return
    backup_dir = os.path.join(os.path.dirname(DATABASE_PATH), "backups")
    os.makedirs(backup_dir, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = os.path.join(backup_dir, f"leads_{ts}.db")
    shutil.copy2(DATABASE_PATH, dest)
    log.info(f"DB backup created: {dest} ({size:,} bytes)")
    # Keep only the 5 most recent backups
    backups = sorted(
        [os.path.join(backup_dir, f) for f in os.listdir(backup_dir) if f.endswith(".db")],
        key=os.path.getmtime,
        reverse=True
    )
    for old in backups[5:]:
        os.remove(old)
        log.info(f"Removed old backup: {old}")


@app.on_event("startup")
async def startup():
    logging.basicConfig(level=logging.INFO)

    # ── Safety: check persistent disk on production ──
    if IS_PROD:
        if not os.path.ismount("/data"):
            log.error("🚨 PERSISTENT DISK NOT MOUNTED at /data — data may be ephemeral!")
        else:
            log.info("✅ Persistent disk mounted at /data")

    # ── Backup existing DB before any schema migrations ──
    try:
        _backup_db()
    except Exception:
        log.exception("Startup warning: DB backup failed")

    # Keep service available even if secondary startup tasks fail.
    try:
        init_client_hub_db()
    except Exception:
        log.exception("Startup failed during DB init")

    try:
        normalize_claim_statuses()
    except Exception:
        log.exception("Startup warning: normalize_claim_statuses failed")

    try:
        start_daily_scheduler()
    except Exception:
        log.exception("Startup warning: notification scheduler failed")


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
    return _serve_hub()


@app.get("/hub", response_class=HTMLResponse)
async def hub(request: Request):
    return _serve_hub()


@app.get("/portal", response_class=HTMLResponse)
async def portal(request: Request):
    return _serve_hub()


@app.get("/medpharma", response_class=HTMLResponse)
async def medpharma(request: Request):
    return _serve_hub()


@app.get("/mphub2026", response_class=HTMLResponse)
async def mphub2026(request: Request):
    return _serve_hub()


@app.get("/healthz")
async def healthz():
    return {"ok": True, "service": "hub"}


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
