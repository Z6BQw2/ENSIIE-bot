import json
import logging
import time

import faiss
import numpy as np

logger = logging.getLogger(__name__)


class FAISSIndexer:
    """Indexation FAISS (IVF_Flat, Flat, HNSW)"""

    def __init__(self, dimension: int = 1024, nlist: int = 10):
        self.dimension = dimension
        self.nlist = nlist
        self.index = None
        self.id_map: dict[int, str] = {}

    def build_index(self, embeddings: np.ndarray, doc_ids: list[str], method: str = "ivf_flat"):
        n = embeddings.shape[0]
        embeddings = embeddings.astype('float32')

        if method == "flat":
            self.index = faiss.IndexFlatIP(self.dimension)

        elif method == "ivf_flat":
            nlist = min(self.nlist, n)
            quantizer = faiss.IndexFlatIP(self.dimension)
            self.index = faiss.IndexIVFFlat(quantizer, self.dimension, nlist)
            faiss.normalize_L2(embeddings)
            self.index.train(embeddings)
            self.index.nprobe = min(3, nlist)

        elif method == "hnsw":
            self.index = faiss.IndexHNSWFlat(self.dimension, 32)

        else:
            raise ValueError(f"Méthode inconnue: {method}")

        if method != "ivf_flat":
            faiss.normalize_L2(embeddings)

        self.index.add(embeddings)
        self.id_map = {i: doc_id for i, doc_id in enumerate(doc_ids)}
        logger.info(f"Index FAISS ({method}): {self.index.ntotal} vecteurs")

    def search(self, query_embedding: np.ndarray, top_k: int = 5) -> list[tuple[str, float]]:
        query = query_embedding.reshape(1, -1).astype('float32')
        faiss.normalize_L2(query)
        distances, indices = self.index.search(query, top_k)

        return [
            (self.id_map.get(idx, "unknown"), float(dist))
            for dist, idx in zip(distances[0], indices[0])
            if idx != -1
        ]

    def save(self, index_path: str, mapping_path: str):
        faiss.write_index(self.index, index_path)
        with open(mapping_path, 'w') as f:
            json.dump(self.id_map, f)

    def load(self, index_path: str, mapping_path: str):
        self.index = faiss.read_index(index_path)
        with open(mapping_path, 'r') as f:
            self.id_map = {int(k): v for k, v in json.load(f).items()}


def compare_methods(embeddings: np.ndarray, doc_ids: list[str], query: np.ndarray) -> dict:
    """Benchmark des méthodes FAISS"""
    results = {}

    for method in ("flat", "ivf_flat", "hnsw"):
        indexer = FAISSIndexer(dimension=embeddings.shape[1])

        t0 = time.time()
        indexer.build_index(embeddings.copy(), doc_ids, method=method)
        build_time = time.time() - t0

        t0 = time.time()
        search_results = indexer.search(query, top_k=5)
        search_time = time.time() - t0

        results[method] = {
            'build_time': build_time,
            'search_time': search_time,
            'top_5': search_results,
        }

        print(f"\n--- {method.upper()} ---")
        print(f"  Build: {build_time:.4f}s | Search: {search_time:.6f}s")
        for doc_id, score in search_results:
            print(f"  → {doc_id[:16]}... score={score:.4f}")

    return results
