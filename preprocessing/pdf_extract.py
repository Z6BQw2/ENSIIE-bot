import logging
from pathlib import Path

import pymupdf

logger = logging.getLogger(__name__)


class PDFExtractor:
    """Extraction de texte depuis les PDFs, avec fallback OCR"""

    MIN_CHARS_PER_PAGE = 30

    def extract(self, pdf_path: Path) -> tuple[str, dict]:
        try:
            doc = pymupdf.open(pdf_path)

            full_text = ""
            for page in doc:
                full_text += page.get_text() + "\n"

            metadata = {
                'page_count': len(doc),
                'author': doc.metadata.get('author', ''),
                'title': doc.metadata.get('title', ''),
                'subject': doc.metadata.get('subject', ''),
                'creation_date': doc.metadata.get('creationDate', ''),
            }

            avg_chars = len(full_text.strip()) / max(len(doc), 1)
            if avg_chars < self.MIN_CHARS_PER_PAGE:
                logger.warning(
                    f"Texte insuffisant ({avg_chars:.0f} chars/page), "
                    f"OCR en cours: {pdf_path.name}"
                )
                ocr_text = self._ocr_fallback(doc)
                if ocr_text:
                    full_text = ocr_text
                    metadata['ocr_applied'] = True

            doc.close()
            return full_text, metadata

        except Exception as e:
            logger.error(f"Erreur extraction PDF {pdf_path}: {e}")
            return "", {}

    def _ocr_fallback(self, doc) -> str:
        try:
            import pytesseract
            from PIL import Image

            full_text = ""
            for page in doc:
                pix = page.get_pixmap(dpi=300)
                img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                full_text += pytesseract.image_to_string(img, lang='fra+eng') + "\n"

            logger.info(f"OCR réussi: {len(full_text)} chars extraits")
            return full_text

        except ImportError:
            logger.error("pytesseract non installé: pip install pytesseract Pillow")
            return ""
        except Exception as e:
            logger.error(f"OCR échoué: {e}")
            return ""
