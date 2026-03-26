"""
Connecteurs d'ingestion pour les données de Personne 1 (Valentin)
- Site ENSIIE (Playwright scraper → ensiie_output/)
- LinkedIn posts (API → linkedin_posts.json)
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from .models import RawDocument, SourceType

logger = logging.getLogger(__name__)


class SiteIngestor:
    """Lit la sortie du scraper Playwright de Valentin (ensiie_output/)"""

    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.pages_dir = output_dir / "pages"
        self.files_dir = output_dir / "files"

    def iter_pages(self) -> list[RawDocument]:
        """Charge les pages HTML (déjà nettoyées en .txt par le scraper)"""
        if not self.pages_dir.exists():
            logger.warning(f"Dossier pages introuvable: {self.pages_dir}")
            return []

        docs = []
        json_files = list(self.pages_dir.glob("*.json"))
        logger.info(f"Ingestion de {len(json_files)} pages crawlées...")

        for json_path in json_files:
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)

                # Le scraper sépare contenu (.txt) et métadonnées (.json)
                content_file = meta.get("content_file")
                if content_file:
                    txt_path = self.output_dir / content_file
                else:
                    # Fallback: même nom mais .txt
                    txt_path = json_path.with_suffix(".txt")

                if not txt_path.exists():
                    logger.debug(f"Fichier texte introuvable: {txt_path}")
                    continue

                with open(txt_path, 'r', encoding='utf-8') as f:
                    content = f.read()

                if not content.strip():
                    continue

                # Convertir la date de scraping
                scraped_at = meta.get("scraped_at", "")
                try:
                    crawl_date = datetime.fromisoformat(scraped_at.replace("Z", "+00:00"))
                except (ValueError, AttributeError):
                    crawl_date = datetime.now()

                doc = RawDocument(
                    content=content,
                    source_url=meta.get("url", ""),
                    source_type=SourceType.WEBSITE,
                    title=meta.get("title", ""),
                    crawl_date=crawl_date,
                    metadata={
                        "description": meta.get("description", ""),
                        "keywords": meta.get("keywords", ""),
                        "language": meta.get("language", ""),
                        "author": meta.get("author", ""),
                        "depth": meta.get("depth", 0),
                        "parent_url": meta.get("parent_url"),
                        "headings": meta.get("headings", {}),
                        "word_count_original": meta.get("word_count", 0),
                    },
                )
                docs.append(doc)

            except Exception as e:
                logger.error(f"Erreur lecture {json_path.name}: {e}")

        logger.info(f"{len(docs)} pages chargées")
        return docs

    def iter_downloaded_pdfs(self) -> list[Path]:
        """Liste les PDFs téléchargés par le scraper"""
        if not self.files_dir.exists():
            return []
        pdfs = list(self.files_dir.glob("*.pdf"))
        logger.info(f"{len(pdfs)} PDFs trouvés dans {self.files_dir}")
        return pdfs

    def get_pdf_source_url(self, pdf_path: Path) -> str:
        """Récupère l'URL d'origine d'un PDF via son .json associé"""
        json_path = pdf_path.with_suffix(".json")
        if json_path.exists():
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                return meta.get("url", str(pdf_path))
            except Exception:
                pass
        return str(pdf_path)


class LinkedInIngestor:
    """Lit la sortie du LinkedIn fetcher de Valentin"""

    def __init__(self, json_path: Path):
        self.json_path = json_path

    def iter_posts(self) -> list[RawDocument]:
        """Charge les posts LinkedIn"""
        if not self.json_path.exists():
            logger.warning(f"Fichier LinkedIn introuvable: {self.json_path}")
            return []

        with open(self.json_path, 'r', encoding='utf-8') as f:
            posts = json.load(f)

        docs = []
        for post in posts:
            text = post.get("text", "").strip()
            if not text:
                continue

            # Convertir la date
            created = post.get("created_at", "")
            try:
                crawl_date = datetime.strptime(created, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                crawl_date = datetime.now()

            doc = RawDocument(
                content=text,
                source_url=post.get("url", ""),
                source_type=SourceType.SOCIAL_LINKEDIN,
                title=f"LinkedIn post {post.get('id', '')[:20]}",
                crawl_date=crawl_date,
                metadata={
                    "post_id": post.get("id", ""),
                    "visibility": post.get("visibility", ""),
                    "has_media": post.get("has_media", False),
                    "media_type": post.get("media_type", ""),
                },
            )
            docs.append(doc)

        logger.info(f"{len(docs)} posts LinkedIn chargés")
        return docs
