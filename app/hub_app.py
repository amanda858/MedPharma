"""Client Hub app — runs on HUB_PORT (default 5240)."""

import hashlib
import time
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse, Response

from app.client_db import init_client_hub_db
from app.client_routes import router as client_hub_router

app = FastAPI(
    title="MedPharma Client Hub",
    description="MedPharma Revenue Cycle Management — Client Portal",
    version="2.0.0",
)


@app.on_event("startup")
async def startup():
    init_client_hub_db()


app.include_router(client_hub_router)


@app.get("/")
async def root():
    return RedirectResponse(url="/portal", status_code=302)


@app.get("/hub")
async def hub_redirect():
    """Redirect old /hub URL so CDN cache is bypassed."""
    return RedirectResponse(url="/portal", status_code=302)


@app.get("/portal", response_class=HTMLResponse)
async def serve_client_hub():
    with open("app/templates/client_hub.html", "r") as f:
        content = f.read()
    # Inject build timestamp so every deploy busts any CDN/proxy cache
    build_ts = str(int(time.time()))
    content = content.replace("</head>", f'<meta name="build" content="{build_ts}">\n</head>', 1)
    etag = hashlib.md5(content.encode()).hexdigest()
    return Response(
        content=content,
        media_type="text/html",
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
            "ETag": etag,
            "Surrogate-Control": "no-store",
            "CDN-Cache-Control": "no-store",
        }
    )
