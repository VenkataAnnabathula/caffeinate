import os
import json
import requests
import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="caffeinate", layout="wide")
st.title("caffeinate — Analytics + RAG Assistant (MVP)")

# Sidebar settings (runs INSIDE Docker)
DEFAULT_BACKEND = os.getenv("BACKEND_URL", "http://backend:8000")
DEFAULT_API_KEY = os.getenv("ADMIN_API_KEY", "")
if "backend_url" not in st.session_state:
    st.session_state.backend_url = DEFAULT_BACKEND
if "api_key" not in st.session_state:
    st.session_state.api_key = DEFAULT_API_KEY

with st.sidebar:
    st.subheader("Settings")
    st.session_state.backend_url = st.text_input(
        "Backend URL",
        st.session_state.backend_url,
        help="Docker: http://backend:8000  |  Host: http://localhost:8000",
    )
    st.session_state.api_key = st.text_input(
        "API Key (X-API-Key)",
        value=st.session_state.api_key,
        type="password",
        help="Needed for Upload & Index (ADMIN_API_KEY in .env)",
    )
    st.caption("Tenant: " + os.getenv("TENANT_ID","demo"))

backend = st.session_state.backend_url
api_key = st.session_state.api_key or ""

def fetch_json(path: str, params: dict | None = None, method: str = "GET", files=None, body=None, headers: dict | None = None):
    url = f"{backend}{path}"
    try:
        if method == "GET":
            r = requests.get(url, params=params, headers=headers, timeout=120)
        elif method == "POST" and files is not None:
            r = requests.post(url, params=params, files=files, headers=headers, timeout=300)
        elif method == "POST" and body is not None:
            r = requests.post(url, headers={"Content-Type":"application/json", **(headers or {})}, data=json.dumps(body), timeout=300)
        else:
            r = requests.post(url, params=params, headers=headers, timeout=120)
        if not r.ok:
            return {"error": f"{r.status_code}: {r.text}"}
        return r.json()
    except Exception as e:
        return {"error": str(e)}

tab_upload, tab_overview, tab_ask = st.tabs(["📤 Upload dataset", "📈 Overview", "💬 Ask assistant"])

# ------------------ Upload ------------------
with tab_upload:
    st.markdown("Upload a CSV and choose the destination **logical** table name (we’ll prefix with tenant).")
    table = st.text_input("Table name", value="coffee_sales")
    file = st.file_uploader("CSV file", type=["csv"])
    if st.button("Upload"):
        if not file or not table:
            st.error("Please provide both a CSV file and a table name.")
        else:
            files = {"file": (file.name, file.getvalue(), "text/csv")}
            headers = {"X-API-Key": api_key} if api_key else {}
            resp = fetch_json("/ingest_dataset", params={"table": table}, method="POST", files=files, headers=headers)
            if "error" in resp:
                st.error(resp["error"])
            else:
                st.success("Upload successful.")
                st.json(resp)
                st.session_state["last_table"] = table

# ------------------ Overview ------------------
with tab_overview:
    st.markdown("See KPIs, daily trend, and top products.")
    table2 = st.text_input("Table to analyze", value=st.session_state.get("last_table","coffee_sales"))
    if st.button("Fetch overview"):
        # KPIs
        k = fetch_json("/metrics/kpis", params={"table": table2})
        if "error" in k:
            st.error(k["error"])
        else:
            c1, c2, c3 = st.columns([1,1,1])
            with c1: st.metric("Rows", k.get("row_count") or 0)
            with c2: st.metric("Total Qty", (k.get("total_qty") or 0))
            with c3: st.metric("Total Revenue", (k.get("total_revenue") or 0.0))
            st.caption(f"Columns: {', '.join(k.get('columns', []))}")
        # Daily series
        d = fetch_json("/metrics/daily", params={"table": table2})
        if "error" in d:
            st.error(d["error"])
        else:
            pts = d.get("points", [])
            if pts:
                df = pd.DataFrame(pts)
                metric_key = d.get("metric","value")
                st.plotly_chart(px.line(df, x="date", y=metric_key, title=f"Daily {metric_key}"), use_container_width=True)
            else:
                st.info("No daily series available (missing date column?).")
        # Top products
        top_lim = st.number_input("Top N products", min_value=1, max_value=50, value=10)
        t = fetch_json("/metrics/top_products", params={"table": table2, "limit": int(top_lim)})
        if "error" in t:
            st.error(t["error"])
        else:
            items = t.get("items", [])
            if items:
                df2 = pd.DataFrame(items)
                st.plotly_chart(px.bar(df2, x="product", y="qty", title="Top products", text_auto=True), use_container_width=True)
            else:
                st.info("No product breakdown available (missing product column?).")
    rag_limit = st.number_input("Index row limit (optional)", min_value=0, max_value=1000000, value=2000, step=100)
    if st.button("Build RAG index (optional)"):
        headers = {"X-API-Key": api_key} if api_key else {}
        st.json(fetch_json("/rag/index", params={"table": table2, "limit": int(rag_limit)}, method="POST", headers=headers))

# ------------------ Ask assistant ------------------
with tab_ask:
    st.markdown("RAG will activate once Gemini & Pinecone keys are set in `.env` and the table is indexed.")
    q_table = st.text_input("Context table (optional)", value=st.session_state.get("last_table",""))
    question = st.text_area("Your question", placeholder="e.g., What were total latte sales last week?")
    if st.button("Ask"):
        payload = {"question": question, "table": (q_table or None)}
        resp = fetch_json("/ask", method="POST", body=payload)
        if isinstance(resp, dict) and resp.get("status") == "ok" and resp.get("answer"):
            st.subheader(resp["answer"])
            with st.expander("Details"):
                st.json(resp)
        else:
            st.json(resp)

