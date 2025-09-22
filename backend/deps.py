import os
from fastapi import Header, HTTPException

ADMIN = os.getenv("ADMIN_API_KEY", "")

def require_api_key(x_api_key: str | None = Header(default=None, alias="X-API-Key")):
    if not ADMIN:
        # In dev if key not set, allow but warn
        return
    if not x_api_key or x_api_key != ADMIN:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
