import hashlib


class Deduplicator:
    """Déduplication par hash SHA-256"""

    def __init__(self):
        self.seen_hashes: set[str] = set()

    def compute_hash(self, text: str) -> str:
        normalized = text.lower().strip()
        return hashlib.sha256(normalized.encode('utf-8')).hexdigest()

    def is_duplicate(self, text: str) -> bool:
        return self.compute_hash(text) in self.seen_hashes

    def add(self, text: str) -> str:
        h = self.compute_hash(text)
        self.seen_hashes.add(h)
        return h

    def reset(self):
        self.seen_hashes.clear()
