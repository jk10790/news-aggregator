"""Single shared embedder (ADR-8). Never rely on Chroma's default embedder —
parents and children must both be embedded explicitly through this module.
"""
from functools import lru_cache
from sentence_transformers import SentenceTransformer
from newsagg import config


@lru_cache(maxsize=1)
def get_model() -> SentenceTransformer:
    return SentenceTransformer(config.EMBEDDING_MODEL)   # "all-MiniLM-L6-v2", dim 384


def embed(texts: list[str]) -> list[list[float]]:
    return get_model().encode(texts, normalize_embeddings=True).tolist()


EXPECTED_DIM = 384
