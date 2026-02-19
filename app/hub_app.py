"""Client Hub app — runs on HUB_PORT (default 5240)."""

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, RedirectResponse

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
    return RedirectResponse(url="/hub")


@app.get("/hub", response_class=HTMLResponse)
async def serve_client_hub():
    with open("app/templates/client_hub.html", "r") as f:
        return f.read()
