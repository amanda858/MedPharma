"""Client Hub app — runs on HUB_PORT (default 5240)."""

import os
import time
import logging
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response

from app.client_db import init_client_hub_db, normalize_claim_statuses
from app.client_routes import router as client_hub_router
from app.notifications import start_daily_scheduler

IS_PROD = bool(os.getenv("PORT"))  # Render sets PORT; local dev does not
log = logging.getLogger(__name__)

app = FastAPI(
    title="MedPharma Client Hub",
    description="MedPharma Revenue Cycle Management — Client Portal",
    version="2.0.0",
)


@app.on_event("startup")
async def startup():
    logging.basicConfig(level=logging.INFO)
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
