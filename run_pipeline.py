#!/usr/bin/env python3

import logging
from pathlib import Path
import os
import dotenv

dotenv.load_dotenv()  # Charger les variables d'environnement depuis .env

from preprocessing import PreprocessingPipeline, VectorExporter, Language

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)

def main():
    pipeline = PreprocessingPipeline(
        db_path=Path(os.getenv("DB_PATH")),
        keep_urls=True,
    )

    scraper_dir = Path(os.getenv("SCRAPER_OUTPUT_DIR"))
    if scraper_dir.exists():
        pipeline.process_scraped_site(scraper_dir)
    else:
        print(f"⚠️  {scraper_dir} introuvable — lancer le scraper d'abord")

    pdf_dir = Path(os.getenv("PDF_DIR"))
    if pdf_dir.exists():
        pipeline.process_pdf_directory(pdf_dir)

    # 3. LinkedIn
    linkedin_path = Path(os.getenv("LINKEDIN_POSTS_PATH"))
    if linkedin_path.exists():
        pipeline.process_linkedin(linkedin_path)

    # Stats
    pipeline.print_stats()

    # Export JSONL
    exporter = VectorExporter(pipeline.store)
    # export_dir = Path(os.getenv("EXPORT_DIR"))
    # export_dir.mkdir(parents=True, exist_ok=True)

    raw_documents_path = Path(os.getenv("RAW_DOCUMENTS_PATH"))
    exporter.export_jsonl(raw_documents_path)

    # exporter.export_jsonl(export_dir / "all_documents.jsonl")
    # exporter.export_jsonl(export_dir / "documents_fr.jsonl", Language.FR)
    # exporter.export_jsonl(export_dir / "documents_en.jsonl", Language.EN)

    print(f"\nExport terminé dans {raw_documents_path}/")


if __name__ == "__main__":
    main()