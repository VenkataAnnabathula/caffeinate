from __future__ import annotations
import os
from typing import Dict, List
import pandas as pd
import numpy as np
from decimal import Decimal
from datetime import date, datetime
from sqlalchemy import create_engine, text

DIM = 768  # text-embedding-004

def _db_url() -> str:
    user = os.getenv("POSTGRES_USER", "caffinate")
    pwd  = os.getenv("POSTGRES_PASSWORD", "caffinate123")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB", "caffinate")
    return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{db}"

def load_df(table: str, limit: int | None = None) -> pd.DataFrame:
    eng = create_engine(_db_url())
    q = f'SELECT * FROM "{table}"'
    if limit and limit > 0:
        q += f" LIMIT {int(limit)}"
    with eng.connect() as conn:
        return pd.read_sql(text(q), conn)

def row_to_text(row: pd.Series, table: str) -> str:
    parts = [f"table={table}"]
    for k, v in row.items():
        parts.append(f"{k}={v}")
    return "; ".join(parts)

def dataframe_to_texts(df: pd.DataFrame, table: str) -> List[str]:
    return [row_to_text(df.iloc[i], table) for i in range(len(df))]

def _to_python_scalar(v):
    # Convert NumPy/Decimal/Timestamp → Python JSON-safe types
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.bool_,)):
        return bool(v)
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    return v

def _safe_meta_value(v):
    # Pinecone metadata must be str/num/bool or list[str]; no None/null
    if pd.isna(v):
        return None
    v = _to_python_scalar(v)
    if isinstance(v, (str, bool, int, float)):
        return v
    if isinstance(v, (list, tuple, set)):
        # Only list of strings is allowed
        return [str(x) for x in v if x is not None]
    # Fallback to string
    return str(v)

def dataframe_to_metadata(df: pd.DataFrame, table: str, texts: List[str], max_cols: int = 8) -> List[Dict]:
    cols = list(df.columns)[:max_cols]
    metas: List[Dict] = []
    for i in range(len(df)):
        md: Dict = {"table": table, "text": str(texts[i])[:800]}
        for c in cols:
            val = _safe_meta_value(df.iloc[i][c])
            if val is None:
                continue  # skip nulls entirely
            if isinstance(val, str) and len(val) > 256:
                val = val[:256]
            if isinstance(val, list):
                # ensure list-of-strings not too big
                val = [str(x)[:128] for x in val[:20]]
            md[c] = val
        metas.append(md)
    return metas

def index_table(table: str, limit: int | None = None) -> Dict:
    # Lazy import to avoid pulling SDKs unless needed
    from services.embeddings import get_embedder
    from services.vectorstore import ensure_index, upsert_vectors

    df = load_df(table, limit)
    if df.empty:
        return {"table": table, "rows_indexed": 0, "message": "table is empty"}

    texts = dataframe_to_texts(df, table)
    metas = dataframe_to_metadata(df, table, texts)

    ensure_index(dim=DIM, metric="cosine")
    embedder = get_embedder()
    vectors: List[List[float]] = embedder.embed_documents(texts)

    items = []
    for i, vec in enumerate(vectors):
        items.append({
            "id": f"{table}:{i}",
            "values": vec,
            "metadata": metas[i],  # sanitized types
        })

    # Chunk the upsert
    B = 200
    for s in range(0, len(items), B):
        upsert_vectors(items[s:s+B])

    return {"table": table, "rows_indexed": len(items), "dim": len(vectors[0]) if vectors else DIM}
