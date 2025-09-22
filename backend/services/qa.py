import os, re
from typing import Dict, List, Any
from services.embeddings import get_embedder
from services.vectorstore import query_vectors
from langchain_google_genai import ChatGoogleGenerativeAI

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "models/gemini-1.5-flash")
TOP_K = int(os.getenv("RAG_TOP_K", "6"))
MAX_CONTEXT_CHARS = int(os.getenv("RAG_MAX_CONTEXT_CHARS", "4000"))

def _build_context(matches: List[Dict[str, Any]]) -> str:
    buf, total = [], 0
    for m in matches:
        md = m.get("metadata", {}) if isinstance(m, dict) else getattr(m, "metadata", {}) or {}
        line = md.get("text") or str(md)
        if not line: 
            continue
        if total + len(line) > MAX_CONTEXT_CHARS:
            break
        buf.append(line)
        total += len(line)
    return "\n".join(buf) if buf else "(no context found)"

def answer_with_rag(question: str, table: str | None = None) -> Dict:
    # 1) embed the query
    embedder = get_embedder()
    qvec = embedder.embed_query(question)

    # 2) retrieve from Pinecone
    flt = {"table": {"$eq": table}} if table else None
    res = query_vectors(qvec, top_k=TOP_K, filter=flt)
    matches = getattr(res, "matches", None) or res.get("matches", [])

    # 3) build context
    context = _build_context(matches)

    # 4) ask Gemini for a natural sentence
    llm = ChatGoogleGenerativeAI(
        model=GEMINI_MODEL,
        temperature=0.2,
        google_api_key=os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
    )
    prompt = (
        "You are a precise data analyst. Use only the provided context (facts extracted from a business dataset). "
        "Respond with ONE short, natural sentence. Include units/currency if relevant. "
        "If the context is insufficient, say so clearly.\n\n"
        f"Context:\n{context}\n\nQuestion: {question}\nAnswer (one short sentence):"
    )
    resp = llm.invoke(prompt)
    answer_text = getattr(resp, "content", None) or str(resp)
    ans = str(answer_text).strip()

    # If the model returned just a bare number, wrap it in a sentence
    if re.fullmatch(r"[-+]?\d+(\.\d+)?", ans):
        ans = f"The total is {ans}."

    return {
        "status": "ok",
        "model": GEMINI_MODEL,
        "top_k": TOP_K,
        "table": table,
        "answer": ans,
        "used_context_chars": min(len(context), MAX_CONTEXT_CHARS),
        "matches": [
            {
                "id": getattr(m, "id", None) or (m.get("id") if isinstance(m, dict) else None),
                "score": getattr(m, "score", None) or (m.get("score") if isinstance(m, dict) else None),
                "metadata": getattr(m, "metadata", None) or (m.get("metadata") if isinstance(m, dict) else {})
            } for m in matches[:3]
        ]
    }
