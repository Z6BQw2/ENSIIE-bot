import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

from .models import Language, SourceType, ProcessedBlock

logger = logging.getLogger(__name__)


class DocumentStore:
    """Stockage des documents traités dans SQLite"""

    def __init__(self, db_path: str = "./data/documents.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS documents (
                    id TEXT PRIMARY KEY,
                    content TEXT NOT NULL,
                    content_normalized TEXT,
                    language TEXT,
                    char_count INTEGER,
                    word_count INTEGER,
                    source_url TEXT,
                    source_type TEXT,
                    title TEXT,
                    crawl_date TEXT,
                    process_date TEXT,
                    metadata TEXT
                )
            ''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_language ON documents(language)')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_source_type ON documents(source_type)')
            conn.commit()

    def save(self, block: ProcessedBlock) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute('''
                    INSERT OR REPLACE INTO documents
                    (id, content, content_normalized, language, char_count, word_count,
                     source_url, source_type, title, crawl_date, process_date, metadata)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    block.id, block.content, block.content_normalized,
                    block.language.value, block.char_count, block.word_count,
                    block.source_url, block.source_type.value, block.title,
                    block.crawl_date.isoformat(), block.process_date.isoformat(),
                    json.dumps(block.metadata),
                ))
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")
            return False

    def get_all(self) -> list[ProcessedBlock]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute('SELECT * FROM documents').fetchall()
        return [self._to_block(r) for r in rows]

    def get_by_language(self, language: Language) -> list[ProcessedBlock]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                'SELECT * FROM documents WHERE language = ?',
                (language.value,)
            ).fetchall()
        return [self._to_block(r) for r in rows]

    def count(self) -> dict:
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute('SELECT COUNT(*) FROM documents').fetchone()[0]
            by_lang = dict(conn.execute(
                'SELECT language, COUNT(*) FROM documents GROUP BY language'
            ).fetchall())
            by_src = dict(conn.execute(
                'SELECT source_type, COUNT(*) FROM documents GROUP BY source_type'
            ).fetchall())
        return {'total': total, 'by_language': by_lang, 'by_source': by_src}

    def _to_block(self, row) -> ProcessedBlock:
        return ProcessedBlock(
            id=row['id'],
            content=row['content'],
            content_normalized=row['content_normalized'],
            language=Language(row['language']),
            char_count=row['char_count'],
            word_count=row['word_count'],
            source_url=row['source_url'],
            source_type=SourceType(row['source_type']),
            title=row['title'],
            crawl_date=datetime.fromisoformat(row['crawl_date']),
            process_date=datetime.fromisoformat(row['process_date']),
            metadata=json.loads(row['metadata']),
        )
