"""Entry point — run with: python run.py"""

import os
import uvicorn
from app.config import APP_HOST, APP_PORT

if __name__ == "__main__":
    is_prod = bool(os.getenv("PORT"))  # Render sets PORT; local dev does not
    uvicorn.run(
        "app.main:app",
        host=APP_HOST,
        port=APP_PORT,
        reload=not is_prod,
    )
