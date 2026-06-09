"""Entry point — starts the client hub service."""

import os
import uvicorn
from app.config import APP_HOST, HUB_PORT


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
        # On Render: always serve the hub.
        render_port = int(os.getenv("PORT", "8000"))
        uvicorn.run("app.hub_app:app", host="0.0.0.0", port=render_port)
    else:
        print(f"Starting Client Hub → http://localhost:{HUB_PORT}")
        run_hub()
