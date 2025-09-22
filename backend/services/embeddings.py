import os
from typing import Optional
from langchain_google_genai import GoogleGenerativeAIEmbeddings

_MODEL = os.getenv("EMBEDDING_MODEL", "models/text-embedding-004")  # 768-d by default

def get_embedder(output_dimensionality: Optional[int] = None) -> GoogleGenerativeAIEmbeddings:
    """
    Returns a LangChain Embeddings instance for Gemini.
    Set GEMINI_API_KEY in environment.
    """
    kwargs = {"model": _MODEL}
    # You can optionally reduce dims via output_dimensionality (e.g., 768->512) later.
    if output_dimensionality:
        kwargs["output_dimensionality"] = int(output_dimensionality)
    return GoogleGenerativeAIEmbeddings(**kwargs)
