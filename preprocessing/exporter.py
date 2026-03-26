import json
import logging
from pathlib import Path
from typing import Optional

from .models import Language
from .store import DocumentStore

logger = logging.getLogger(__name__)


class VectorExporter:
    """Export JSONL pour la vectorisation (Personne 3)"""

    def __init__(self, store: DocumentStore):
        self.store = store

    def export_jsonl(self, output_path: Path, language: Optional[Language] = None):
        docs = self.store.get_by_language(language) if language else self.store.get_all()

        with open(output_path, 'w', encoding='utf-8') as f:
            for doc in docs:
                record = {
                    'id': doc.id,
                    'text': doc.content,
                    'metadata': {
                        'source_url': doc.source_url,
                        'source_type': doc.source_type.value,
                        'title': doc.title,
                        'language': doc.language.value,
                        'char_count': doc.char_count,
                        'word_count': doc.word_count,
                    }
                }
                f.write(json.dumps(record, ensure_ascii=False) + '\n')

        logger.info(f"Exporté {len(docs)} documents vers {output_path}")
