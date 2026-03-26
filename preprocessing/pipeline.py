import re
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from .models import SourceType, Language, RawDocument, ProcessedBlock
from .cleaner import TextCleaner
from .lang_detect import LanguageDetector
from .dedup import Deduplicator
from .pdf_extract import PDFExtractor
from .store import DocumentStore
from .ingest import LinkedInIngestor, SiteIngestor

logger = logging.getLogger(__name__)

HTML_TAG_RE = re.compile(r'<\s*[a-zA-Z][^>]*>')


class PreprocessingPipeline:
    """Orchestrateur du pipeline de pré-traitement"""

    def __init__(self, db_path: str = "./data/documents.db", keep_urls: bool = True):
        self.cleaner = TextCleaner(keep_urls=keep_urls)
        self.lang_detector = LanguageDetector()
        self.deduplicator = Deduplicator()
        self.pdf_extractor = PDFExtractor()
        self.store = DocumentStore(db_path)

        self.stats = {
            'processed': 0,
            'skipped': 0,
            'duplicates': 0,
            'errors': 0,
            'by_language': {'fr': 0, 'en': 0, 'unknown': 0},
        }

    # ----- Traitement unitaire -----

    def process_document(self, doc: RawDocument) -> Optional[ProcessedBlock]:
        try:
            # 1. Nettoyage
            if HTML_TAG_RE.search(doc.content):
                clean_text = self.cleaner.clean_html(doc.content)
            else:
                clean_text = self.cleaner.clean_text(doc.content)

            if len(clean_text) < 50:
                logger.warning(
                    f"Contenu trop court ({len(clean_text)} chars): {doc.source_url}"
                )
                self.stats['skipped'] += 1
                return None
            
            if self.cleaner.is_error_page(clean_text):
                logger.warning(f"Page d'erreur détectée: {doc.source_url}")
                self.stats['skipped'] += 1
                return None

            # 2. Déduplication
            if self.deduplicator.is_duplicate(clean_text):
                self.stats['duplicates'] += 1
                logger.info(f"Doublon ignoré: {doc.source_url}")
                return None

            doc_hash = self.deduplicator.add(clean_text)

            # 3. Langue
            language = self.lang_detector.detect(clean_text)

            # 4. Construction du bloc
            block = ProcessedBlock(
                id=doc_hash,
                content=clean_text,
                content_normalized=self.cleaner.normalize_for_search(clean_text),
                language=language,
                char_count=len(clean_text),
                word_count=len(clean_text.split()),
                source_url=doc.source_url,
                source_type=doc.source_type,
                title=doc.title,
                crawl_date=doc.crawl_date,
                process_date=datetime.now(),
                metadata=doc.metadata,
            )

            # 5. Stockage
            self.store.save(block)
            self.stats['processed'] += 1
            self.stats['by_language'][language.value] += 1
            return block

        except Exception as e:
            logger.error(f"Erreur traitement {doc.source_url}: {e}")
            self.stats['errors'] += 1
            return None

    # ----- Batch PDF -----

    def process_pdf(self, pdf_path: Path, source_url: str = "") -> Optional[ProcessedBlock]:
        text, metadata = self.pdf_extractor.extract(pdf_path)
        if not text:
            return None

        doc = RawDocument(
            content=text,
            source_url=source_url or str(pdf_path),
            source_type=SourceType.PDF,
            title=metadata.get('title') or pdf_path.stem,
            metadata=metadata,
        )
        return self.process_document(doc)

    def process_pdf_directory(self, pdf_dir: Path) -> list[ProcessedBlock]:
        results = []
        pdf_files = list(pdf_dir.glob('*.pdf'))
        logger.info(f"Traitement de {len(pdf_files)} PDFs...")

        for i, pdf_path in enumerate(pdf_files, 1):
            logger.info(f"[{i}/{len(pdf_files)}] {pdf_path.name}")
            block = self.process_pdf(pdf_path)
            if block:
                results.append(block)

        return results

    def process_scraped_site(self, scraper_output_dir: Path) -> list[ProcessedBlock]:
        """Ingère les pages + PDFs du scraper de Personne 1"""

        ingestor = SiteIngestor(scraper_output_dir)
        results = []

        # 1. Pages HTML (déjà nettoyées par le scraper)
        pages = ingestor.iter_pages()
        logger.info(f"Traitement de {len(pages)} pages web...")
        for i, doc in enumerate(pages, 1):
            logger.info(f"[page {i}/{len(pages)}] {doc.source_url[:80]}")
            block = self.process_document(doc)
            if block:
                results.append(block)

        # 2. PDFs téléchargés par le scraper
        pdfs = ingestor.iter_downloaded_pdfs()
        logger.info(f"Traitement de {len(pdfs)} PDFs téléchargés...")
        for i, pdf_path in enumerate(pdfs, 1):
            source_url = ingestor.get_pdf_source_url(pdf_path)
            logger.info(f"[pdf {i}/{len(pdfs)}] {pdf_path.name}")
            block = self.process_pdf(pdf_path, source_url=source_url)
            if block:
                results.append(block)

        return results

    def process_linkedin(self, json_path: Path) -> list[ProcessedBlock]:
        """Ingère les posts LinkedIn"""

        ingestor = LinkedInIngestor(json_path)
        results = []

        for doc in ingestor.iter_posts():
            block = self.process_document(doc)
            if block:
                results.append(block)

        return results

    # ----- Stats -----

    def print_stats(self):
        s = self.stats
        db = self.store.count()
        print("\n" + "=" * 60)
        print("STATISTIQUES DU PIPELINE")
        print("=" * 60)
        print(f"Documents traités:  {s['processed']}")
        print(f"Ignorés (courts):   {s['skipped']}")
        print(f"Doublons ignorés:   {s['duplicates']}")
        print(f"Erreurs:            {s['errors']}")
        print(f"\nPar langue:")
        print(f"  FR:      {s['by_language']['fr']}")
        print(f"  EN:      {s['by_language']['en']}")
        print(f"  Inconnu: {s['by_language']['unknown']}")
        print(f"\nBase de données: {db['total']} documents")
