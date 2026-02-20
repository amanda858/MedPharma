"""Client Hub app — runs on HUB_PORT (default 5240)."""

import hashlib
import os
import time
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from app.client_db import init_client_hub_db
from app.client_routes import router as client_hub_router

RENDER_URL = "https://medpharma-hub.onrender.com"
IS_PROD = bool(os.getenv("PORT"))  # Render sets PORT; local dev does not

app = FastAPI(
    title="MedPharma Client Hub",
    description="MedPharma Revenue Cycle Management — Client Portal",
    version="2.0.0",
)


@app.on_event("startup")
async def startup():
    init_client_hub_db()


app.include_router(client_hub_router)


def _serve_hub():
    with open("app/templates/client_hub.html", "r") as f:
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
    if not IS_PROD:
        return RedirectResponse(url=RENDER_URL, status_code=302)
    return _serve_hub()


@app.get("/hub", response_class=HTMLResponse)
async def hub(request: Request):
    if not IS_PROD:
        return RedirectResponse(url=RENDER_URL, status_code=302)
    return _serve_hub()


@app.get("/portal", response_class=HTMLResponse)
async def portal(request: Request):
    if not IS_PROD:
        return RedirectResponse(url=RENDER_URL, status_code=302)
    return _serve_hub()


@app.get("/medpharma", response_class=HTMLResponse)
async def medpharma(request: Request):
    if not IS_PROD:
        return RedirectResponse(url=RENDER_URL, status_code=302)
    return _serve_hub()


@app.get("/mphub2026", response_class=HTMLResponse)
async def mphub2026(request: Request):
    if not IS_PROD:
        return RedirectResponse(url=RENDER_URL, status_code=302)
    return _serve_hub()
