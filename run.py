"""Entry point — starts both services:
  Lab Leads  → http://localhost:8000   (set LAB_PORT to override)
  Client Hub → http://localhost:5240   (set HUB_PORT to override)
"""

import os
import multiprocessing
import uvicorn
from app.config import APP_HOST, LAB_PORT, HUB_PORT


def run_leads():
    uvicorn.run(
        "app.leads_app:app",
        host=APP_HOST,
        port=LAB_PORT,
        reload=False,
    )


def run_hub():
    uvicorn.run(
        "app.hub_app:app",
        host=APP_HOST,
        port=HUB_PORT,
        reload=False,
    )


if __name__ == "__main__":
    is_prod = bool(os.getenv("PORT"))

    if is_prod:
        # On Render: one process, pick which app based on SERVICE env var
        service = os.getenv("SERVICE", "hub")
        if service == "leads":
            uvicorn.run("app.leads_app:app", host=APP_HOST, port=LAB_PORT)
        else:
            uvicorn.run("app.hub_app:app", host=APP_HOST, port=HUB_PORT)
    else:
        print(f"Starting Lab Leads  → http://localhost:{LAB_PORT}")
        print(f"Starting Client Hub → http://localhost:{HUB_PORT}")
        leads_proc = multiprocessing.Process(target=run_leads, daemon=True)
        hub_proc   = multiprocessing.Process(target=run_hub,   daemon=True)
        leads_proc.start()
        hub_proc.start()
        leads_proc.join()
        hub_proc.join()
