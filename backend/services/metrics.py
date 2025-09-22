from sqlalchemy import text
from typing import Dict, List

def get_overview(conn, table: str) -> Dict:
    # table exists?
    exists = conn.execute(
        text("SELECT to_regclass(:t) IS NOT NULL"), {"t": table}
    ).scalar()
    if not exists:
        return {"table": table, "exists": False, "rows": 0, "columns": []}

    # row count
    rows = conn.execute(text(f'SELECT COUNT(*) FROM "{table}";')).scalar()

    # columns
    cols: List[str] = [
        r[0] for r in conn.execute(
            text("SELECT column_name FROM information_schema.columns "
                 "WHERE table_name=:t ORDER BY ordinal_position"),
            {"t": table}
        ).fetchall()
    ]

    return {"table": table, "exists": True, "rows": int(rows), "columns": cols}
