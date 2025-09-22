import os
from typing import List, Dict, Iterable
from pinecone import Pinecone, ServerlessSpec

_INDEX = os.getenv("PINECONE_INDEX", "caffinate-rag-768")
_CLOUD = os.getenv("PINECONE_CLOUD", "aws")
_REGION = os.getenv("PINECONE_REGION", "us-east-1")

def get_pc() -> Pinecone:
    return Pinecone(api_key=os.environ["PINECONE_API_KEY"])

def ensure_index(dim: int = 768, metric: str = "cosine"):
    pc = get_pc()
    if _INDEX not in [i.name for i in pc.list_indexes()]:
        pc.create_index(
            name=_INDEX,
            dimension=dim,
            metric=metric,
            spec=ServerlessSpec(cloud=_CLOUD, region=_REGION),
        )

def upsert_vectors(items: Iterable[Dict]):
    """
    items: iterable of {"id": str, "values": List[float], "metadata": {...}}
    """
    pc = get_pc()
    index = pc.Index(_INDEX)
    index.upsert(vectors=list(items))

def query_vectors(vector: List[float], top_k: int = 8, filter: Dict | None = None):
    pc = get_pc()
    index = pc.Index(_INDEX)
    return index.query(vector=vector, top_k=top_k, include_metadata=True, filter=filter)
