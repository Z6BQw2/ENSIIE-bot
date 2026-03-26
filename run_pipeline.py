#!/usr/bin/env python3

import logging
from pathlib import Path

from preprocessing import PreprocessingPipeline, VectorExporter, Language

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)


def main():
    pipeline = PreprocessingPipeline(
        db_path="./data/documents.db",
        keep_urls=True,
    )

    scraper_dir = Path("./ensiie_output")
    if scraper_dir.exists():
        pipeline.process_scraped_site(scraper_dir)
    else:
        print(f"⚠️  {scraper_dir} introuvable — lancer le scraper d'abord")

    pdf_dir = Path("./pdfs_ensiie")
    if pdf_dir.exists():
        pipeline.process_pdf_directory(pdf_dir)

    # 3. LinkedIn
    linkedin_path = Path("./linkedin_posts.json")
    if linkedin_path.exists():
        pipeline.process_linkedin(linkedin_path)

    # Stats
    pipeline.print_stats()

    # Export JSONL
    exporter = VectorExporter(pipeline.store)
    export_dir = Path("./data/export")
    export_dir.mkdir(parents=True, exist_ok=True)

    exporter.export_jsonl(export_dir / "all_documents.jsonl")
    exporter.export_jsonl(export_dir / "documents_fr.jsonl", Language.FR)
    exporter.export_jsonl(export_dir / "documents_en.jsonl", Language.EN)

    print(f"\nExport terminé dans {export_dir}/")


if __name__ == "__main__":
    main()