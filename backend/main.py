from fastapi import FastAPI, UploadFile, File, HTTPException, Body, Query, Depends
from starlette.middleware.cors import CORSMiddleware
import os
import pandas as pd
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import Optional

from deps import require_api_key
from services.rag import answer_question
from services.metrics import get_overview
from services.analytics import kpis, daily_series, top_products
from services.ingest import index_table

app = FastAPI()

# ---- CORS ----
_CORS_ORIGINS = os.getenv("CORS_ALLOW_ORIGINS", "*")
_ALLOWLIST = [o.strip() for o in _CORS_ORIGINS.split(",")] if _CORS_ORIGINS != "*" else ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWLIST,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TENANT = os.getenv("TENANT_ID", "demo")

def db_url() -> str:
    user = os.getenv("POSTGRES_USER", "caffinate")
    pwd  = os.getenv("POSTGRES_PASSWORD", "caffinate123")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "caffinate")
    return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"

def get_engine():
    return create_engine(db_url())

def tenant_table(raw: str) -> str:
    # safe prefixing: tenant__tablename
    safe = "".join(c if (c.isalnum() or c == "_") else "_" for c in raw)
    return f"{TENANT}__{safe}"

@app.get("/health")
def health():
    try:
        with get_engine().connect() as conn:
            conn.execute(text("SELECT 1;"))
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/ingest_dataset")
async def ingest_dataset(table: str, file: UploadFile = File(...), _=Depends(require_api_key)):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name.")
    physical = tenant_table(table)
    try:
        content = await file.read()
        df = pd.read_csv(pd.io.common.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"CSV read failed: {e}")
    if df.empty:
        raise HTTPException(status_code=400, detail="CSV is empty.")
    try:
        with get_engine().begin() as conn:
            df.to_sql(physical, con=conn, if_exists="replace", index=False)
        return {
            "table": table,
            "physical_table": physical,
            "rows": int(len(df)),
            "columns": list(df.columns),
            "tenant": TENANT,
            "message": "ingested"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB write failed: {e}")

class AskRequest(BaseModel):
    question: str
    table: Optional[str] = None

@app.post("/ask")
def ask(payload: AskRequest = Body(...)):
    # map logical -> physical for RAG
    table_physical = tenant_table(payload.table) if payload.table else None
    return answer_question(payload.question, table_physical)

@app.get("/metrics/overview")
def metrics_overview(table: str = Query(...)):
    physical = tenant_table(table)
    try:
        with get_engine().connect() as conn:
            return get_overview(conn, physical)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/metrics/kpis")
def metrics_kpis(table: str = Query(...)):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name.")
    physical = tenant_table(table)
    with get_engine().connect() as conn:
        return kpis(conn, physical)

@app.get("/metrics/daily")
def metrics_daily_endpoint(table: str = Query(...)):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name.")
    physical = tenant_table(table)
    with get_engine().connect() as conn:
        return daily_series(conn, physical)

@app.get("/metrics/top_products")
def metrics_top_products_endpoint(table: str = Query(...), limit: int = Query(10, ge=1, le=50)):
    if not table.isidentifier():
        raise HTTPException(status_code=400, detail="Invalid table name.")
    physical = tenant_table(table)
    with get_engine().connect() as conn:
        return top_products(conn, physical, limit)

@app.post("/rag/index")
def rag_index(table: str = Query(...), limit: Optional[int] = Query(None), _=Depends(require_api_key)):
    physical = tenant_table(table)
    try:
        return index_table(table=physical, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
