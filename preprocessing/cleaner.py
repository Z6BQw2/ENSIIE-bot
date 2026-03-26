import re
import unicodedata
import logging

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class TextCleaner:
    """Nettoyage et normalisation du texte"""

    PATTERNS_TO_REMOVE = [
        r'<script[^>]*>.*?</script>',
        r'<style[^>]*>.*?</style>',
        r'<!--.*?-->',
        r'<[^>]+>',
        r'&[a-zA-Z]+;',
        r'&#\d+;',
        r'https?://\S+',
        r'\s+',
    ]

    ERROR_PATTERNS = [
        r'erreur\s*40[0-9]',
        r'error\s*40[0-9]',
        r'page\s*not\s*found',
        r'accès\s*refusé',
        r'access\s*denied',
        r'forbidden',
    ]

    CMS_ARTIFACTS = re.compile(
        r'\b(Chapô|Corps de page|Soufflet|Afficher le bloc newsletter|'
        r'Aller au contenu principal)\b',
        re.IGNORECASE
    )

    CONTROL_CHARS = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]')

    def __init__(self, keep_urls: bool = False):
        self.keep_urls = keep_urls
        self._compile_patterns()
        self.error_patterns = [
            re.compile(p, re.IGNORECASE) for p in self.ERROR_PATTERNS
        ]

    def _compile_patterns(self):
        patterns = self.PATTERNS_TO_REMOVE.copy()
        if self.keep_urls:
            patterns = [p for p in patterns if 'http' not in p]
        self.compiled_patterns = [
            re.compile(p, re.DOTALL | re.IGNORECASE) for p in patterns
        ]

    def clean_html(self, html: str) -> str:
        """Nettoie le HTML et extrait le texte"""
        if not html:
            return ""

        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(['script', 'style', 'meta', 'link', 'noscript', 'header', 'footer', 'nav']):
            tag.decompose()

        return self.clean_text(soup.get_text(separator=' '))

    def clean_text(self, text: str) -> str:
        """Nettoie le texte brut"""
        if not text:
            return ""

        text = unicodedata.normalize('NFC', text)
        text = self.CONTROL_CHARS.sub('', text)
        text = self.CMS_ARTIFACTS.sub('', text)

        for pattern in self.compiled_patterns:
            text = pattern.sub(' ', text)

        return ' '.join(text.split()).strip()

    def normalize_for_search(self, text: str) -> str:
        """Lowercase, sans accents — pour recherche"""
        text = text.lower()
        text = unicodedata.normalize('NFD', text)
        return ''.join(c for c in text if unicodedata.category(c) != 'Mn')

    def is_error_page(self, text: str) -> bool:
        """Détecte les pages d'erreur HTTP"""
        # Court + contient un pattern d'erreur = page d'erreur
        if len(text) < 500:
            return any(p.search(text) for p in self.error_patterns)
        return False