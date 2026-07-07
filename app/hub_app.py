"""Client Hub app — runs on HUB_PORT (default 5240)."""

import os
import time
import shutil
import logging
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.client_db import get_db, init_client_hub_db, normalize_claim_statuses, validate_session, backfill_missing_bill_dates, backfill_dos_from_claim_key, dedupe_resubmitted_claims
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

# Serve PWA icons and other static assets.
_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
if os.path.isdir(_STATIC_DIR):
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


def _backup_db():
    """Create a timestamped backup of the SQLite database before any startup modifications.

    Safety: if the live DB currently holds ZERO claims (e.g. it was wiped, or an
    account holding all the data was deleted), do NOT create a backup and do NOT
    rotate. A wiped DB always reports 0 claims, so this stops an empty snapshot
    from pushing a still-good pre-loss backup out of the retention window.
    Backups named KEEP_*.db are manually-preserved good copies and are never
    rotated out."""
    try:
        if not os.path.exists(DATABASE_PATH):
            log.info("No database file found - skipping backup")
            return
        size = os.path.getsize(DATABASE_PATH)
        if size < 4096:  # empty/fresh DB, nothing worth backing up
            log.info(f"Database too small ({size} bytes) - skipping backup")
            return
        # Never let an empty-of-claims DB create a snapshot or rotate out good
        # backups. This is the guard that protects a pre-loss backup from being
        # deleted by routine restarts after a data-loss event.
        try:
            import sqlite3 as _sqlite
            _bc = _sqlite.connect(f"file:{DATABASE_PATH}?mode=ro", uri=True)
            _claim_n = _bc.execute("SELECT COUNT(*) FROM claims_master").fetchone()[0]
            _bc.close()
        except Exception:
            _claim_n = -1  # unknown -> fall through and back up (safe default)
        if _claim_n == 0:
            log.warning("Live DB has 0 claims - skipping backup + rotation to preserve existing backups")
            return
        backup_dir = os.path.join(os.path.dirname(DATABASE_PATH), "backups")
        os.makedirs(backup_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(backup_dir, f"leads_{ts}.db")
        log.info(f"Creating DB backup: {size:,} bytes")
        shutil.copy2(DATABASE_PATH, dest)
        log.info(f"DB backup created: {dest}")

        # Keep the 20 most recent auto-backups; never rotate out protected
        # KEEP_*.db snapshots (manually preserved good copies).
        autos = sorted(
            [os.path.join(backup_dir, f) for f in os.listdir(backup_dir)
             if f.endswith(".db") and not f.startswith("KEEP_")],
            key=os.path.getmtime,
            reverse=True
        )
        for old in autos[20:]:
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
        fixed = backfill_missing_bill_dates()
        if fixed:
            log.info(f"✅ Backfilled Bill Date on {fixed} billed claim(s)")
        else:
            log.info("✅ Bill Dates already complete (nothing to backfill)")
    except Exception as e:
        log.error(f"Startup error: backfill_missing_bill_dates failed: {e}")

    try:
        dos_fixed = backfill_dos_from_claim_key()
        if dos_fixed:
            log.info(f"✅ Recovered DOS from accession on {dos_fixed} claim(s)")
        else:
            log.info("✅ Dates of service already complete (nothing to recover)")
    except Exception as e:
        log.error(f"Startup error: backfill_dos_from_claim_key failed: {e}")

    try:
        from app.client_routes import auto_import_pending_claim_files
        swept = auto_import_pending_claim_files()
        if swept.get("files"):
            log.info("✅ Auto-imported %s claim row(s) from %s pending file(s)",
                     swept["rows"], swept["files"])
        else:
            log.info("✅ No pending claim files to auto-import")
    except Exception as e:
        log.error(f"Startup error: auto_import_pending_claim_files failed: {e}")

    try:
        from app.client_routes import auto_import_pending_payment_files
        pswept = auto_import_pending_payment_files()
        if pswept.get("rows"):
            log.info("✅ Posted %s deposit payment row(s) totaling %.2f from ERA/deposit registers",
                     pswept["rows"], pswept["amount"])
        else:
            log.info("✅ No deposit/ERA registers to post")
    except Exception as e:
        log.error(f"Startup error: auto_import_pending_payment_files failed: {e}")

    try:
        collapsed = dedupe_resubmitted_claims()
        if collapsed:
            log.info(f"✅ Collapsed {collapsed} duplicate resubmitted claim line(s)")
        else:
            log.info("✅ No duplicate resubmitted claims to reconcile")
    except Exception as e:
        log.error(f"Startup error: dedupe_resubmitted_claims failed: {e}")

    try:
        start_daily_scheduler()
        log.info("✅ Daily scheduler started")
        app.state.startup_status["scheduler"] = True
    except Exception as e:
        log.error(f"Startup error: notification scheduler failed: {e}")

    app.state.startup_ready = True
    log.info("🚀 Hub service startup complete")


@app.middleware("http")
async def _no_store_api_responses(request: Request, call_next):
    """Prevent browsers/CDNs from caching dynamic API responses.

    Data endpoints under /hub/api (report totals, dashboard, production report,
    profiles, etc.) must always reflect the latest database state. Without an
    explicit no-store header an intermediary cache or the browser can serve a
    stale copy, so users' updates made today don't show up in "refreshed"
    totals. Mark every dynamic API response as non-cacheable.
    """
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/hub/api") or path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        response.headers["CDN-Cache-Control"] = "no-store"
    return response


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


@app.get("/diskz")
async def diskz():
    """Definitive persistence check: is /data a real mounted disk?

    mounted=True  -> persistent disk is attached (paid plan); data survives restarts.
    mounted=False -> /data is ephemeral; data is wiped on every redeploy/spin-down.
    """
    info = {"ok": True, "data_path": DATABASE_PATH}
    try:
        info["data_dir_is_mount"] = os.path.ismount("/data")
    except Exception as e:
        info["data_dir_is_mount"] = None
        info["mount_error"] = str(e)
    try:
        info["db_exists"] = os.path.exists(DATABASE_PATH)
        info["db_size_bytes"] = os.path.getsize(DATABASE_PATH) if os.path.exists(DATABASE_PATH) else 0
    except Exception as e:
        info["db_error"] = str(e)
    try:
        usage = shutil.disk_usage("/data" if os.path.isdir("/data") else ".")
        info["disk_total_mb"] = round(usage.total / 1048576, 1)
        info["disk_free_mb"] = round(usage.free / 1048576, 1)
    except Exception as e:
        info["disk_usage_error"] = str(e)
    info["persistent"] = bool(info.get("data_dir_is_mount"))
    return info


@app.get("/manifest.webmanifest", include_in_schema=False)
async def web_manifest():
    """PWA manifest so the hub can be installed to the iPhone/Android home screen."""
    manifest = {
        "name": "MedPharma Hub",
        "short_name": "MedPharma",
        "description": "MedPharma Revenue Cycle Management — Client Hub",
        "start_url": "/hub",
        "scope": "/",
        "display": "standalone",
        "orientation": "portrait-primary",
        "background_color": "#0d47a1",
        "theme_color": "#1565c0",
        "icons": [
            {"src": "/static/icon-192.png", "sizes": "192x192", "type": "image/png", "purpose": "any maskable"},
            {"src": "/static/icon-512.png", "sizes": "512x512", "type": "image/png", "purpose": "any maskable"},
        ],
    }
    return JSONResponse(
        manifest,
        media_type="application/manifest+json",
        headers={"Cache-Control": "public, max-age=3600"},
    )


# Minimal, HIPAA-safe service worker: it enables "Add to Home Screen" /
# installability but deliberately does NOT cache any responses, so no PHI is
# ever stored on the device by the worker. All requests pass straight through.
_SERVICE_WORKER_JS = """
self.addEventListener('install', (e) => { self.skipWaiting(); });
self.addEventListener('activate', (e) => { e.waitUntil(self.clients.claim()); });
self.addEventListener('fetch', (e) => { /* network passthrough, no caching */ });
""".lstrip()


@app.get("/service-worker.js", include_in_schema=False)
async def service_worker():
    return Response(
        content=_SERVICE_WORKER_JS,
        media_type="application/javascript",
        headers={
            "Service-Worker-Allowed": "/",
            "Cache-Control": "no-cache",
        },
    )


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
