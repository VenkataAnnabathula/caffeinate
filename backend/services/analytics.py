from typing import Dict, List, Optional
from sqlalchemy import text

CANDIDATE_PRODUCT = ["product","item","sku","name"]
CANDIDATE_DATE = ["date","order_date","sale_date","day","timestamp","created_at"]

def _cols(conn, table: str) -> List[str]:
    rows = conn.execute(
        text("SELECT column_name FROM information_schema.columns WHERE table_name=:t ORDER BY ordinal_position"),
        {"t": table}
    ).fetchall()
    return [r[0] for r in rows]

def _pick(colnames: List[str], candidates: List[str]) -> Optional[str]:
    low = [c.lower() for c in colnames]
    for c in candidates:
        if c in low:
            return colnames[low.index(c)]
    return None

def _has(conn, table: str, col: str) -> bool:
    r = conn.execute(
        text("SELECT 1 FROM information_schema.columns WHERE table_name=:t AND column_name=:c"),
        {"t": table, "c": col}
    ).fetchone()
    return bool(r)

def kpis(conn, table: str) -> Dict:
    cols = _cols(conn, table)
    if not cols:
        return {"table": table, "exists": False}

    rows = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
    has_qty = "qty" in [c.lower() for c in cols]
    has_price = "price" in [c.lower() for c in cols]

    total_qty = None
    total_revenue = None
    if has_qty:
        total_qty = conn.execute(text(f'SELECT SUM(qty::numeric) FROM "{table}"')).scalar()
    if has_qty and has_price:
        total_revenue = conn.execute(text(f'SELECT SUM((qty::numeric)*(price::numeric)) FROM "{table}"')).scalar()

    return {
        "table": table, "exists": True, "row_count": int(rows),
        "has_qty": has_qty, "has_price": has_price,
        "total_qty": float(total_qty) if total_qty is not None else None,
        "total_revenue": float(total_revenue) if total_revenue is not None else None,
        "columns": cols
    }

def daily_series(conn, table: str) -> Dict:
    cols = _cols(conn, table)
    dcol = _pick(cols, CANDIDATE_DATE)
    has_qty = "qty" in [c.lower() for c in cols]
    has_price = "price" in [c.lower() for c in cols]

    if not dcol:
        return {"table": table, "has_date": False, "points": []}

    # revenue if possible, else just daily counts
    if has_qty and has_price:
        q = text(f'SELECT CAST("{dcol}" AS date) d, SUM((qty::numeric)*(price::numeric)) revenue '
                 f'FROM "{table}" GROUP BY d ORDER BY d')
    else:
        q = text(f'SELECT CAST("{dcol}" AS date) d, COUNT(*) ct FROM "{table}" GROUP BY d ORDER BY d')

    rows = conn.execute(q).fetchall()
    key = "revenue" if has_qty and has_price else "ct"
    return {"table": table, "has_date": True, "metric": key,
            "points": [{"date": str(r[0]), key: float(r[1]) if r[1] is not None else 0.0} for r in rows]}

def top_products(conn, table: str, limit: int = 10) -> Dict:
    cols = _cols(conn, table)
    pcol = _pick(cols, CANDIDATE_PRODUCT)
    has_qty = "qty" in [c.lower() for c in cols]

    if not pcol:
        return {"table": table, "has_product": False, "items": []}

    if has_qty:
        q = text(f'SELECT "{pcol}" as product, SUM(qty::numeric) qty '
                 f'FROM "{table}" GROUP BY "{pcol}" ORDER BY qty DESC LIMIT :lim')
    else:
        q = text(f'SELECT "{pcol}" as product, COUNT(*) qty '
                 f'FROM "{table}" GROUP BY "{pcol}" ORDER BY qty DESC LIMIT :lim')

    rows = conn.execute(q, {"lim": limit}).fetchall()
    return {"table": table, "has_product": True,
            "items": [{"product": r[0], "qty": float(r[1]) if r[1] is not None else 0.0} for r in rows]}
