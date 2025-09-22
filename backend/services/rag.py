import os
from typing import Dict

def rag_config_ok() -> bool:
    need = ["GEMINI_API_KEY", "PINECONE_API_KEY", "PINECONE_INDEX"]
    return all(os.getenv(k) for k in need)

def answer_question(question: str, table: str | None = None) -> Dict:
    # If keys missing, return a friendly TODO
    if not rag_config_ok():
        return {
            "status": "todo",
            "message": "RAG not configured yet",
            "missing": {
                "GEMINI_API_KEY": bool(os.getenv("GEMINI_API_KEY")),
                "PINECONE_API_KEY": bool(os.getenv("PINECONE_API_KEY")),
                "PINECONE_INDEX":  bool(os.getenv("PINECONE_INDEX")),
            },
            "echo": {"question": question, "table": table}
        }
    # Lazy import the heavy bits to avoid startup errors
    from services.qa import answer_with_rag
    return answer_with_rag(question, table)
