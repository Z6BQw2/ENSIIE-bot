import logging

from langdetect import detect, DetectorFactory
from .models import Language

DetectorFactory.seed = 0
logger = logging.getLogger(__name__)


class LanguageDetector:
    """Détection de langue FR/EN avec fallback par mots-clés"""

    FR_KEYWORDS = {
        'le', 'la', 'les', 'de', 'du', 'des', 'un', 'une', 'et', 'est',
        'en', 'que', 'qui', 'dans', 'pour', 'sur', 'avec', 'ce', 'cette',
        'sont', 'nous', 'vous', 'ils', 'elle', 'ont', 'aux', 'par'
    }

    EN_KEYWORDS = {
        'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been',
        'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'shall', 'can',
        'for', 'with', 'this', 'that', 'these', 'those', 'from'
    }

    def __init__(self, min_text_length: int = 20):
        self.min_text_length = min_text_length

    def detect(self, text: str) -> Language:
        if not text or len(text) < self.min_text_length:
            return Language.UNKNOWN

        try:
            lang_code = detect(text)
            if lang_code == 'fr':
                return Language.FR
            elif lang_code == 'en':
                return Language.EN
            return self._fallback(text)
        except Exception:
            return self._fallback(text)

    def _fallback(self, text: str) -> Language:
        words = set(text.lower().split())
        fr = len(words & self.FR_KEYWORDS)
        en = len(words & self.EN_KEYWORDS)

        if fr > en:
            return Language.FR
        elif en > fr:
            return Language.EN
        return Language.UNKNOWN
