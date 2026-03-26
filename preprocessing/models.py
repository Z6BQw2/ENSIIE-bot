from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from enum import Enum


class SourceType(Enum):
    WEBSITE = "website"
    PDF = "pdf"
    SOCIAL_LINKEDIN = "linkedin"
    SOCIAL_FACEBOOK = "facebook"
    SOCIAL_TWITTER = "twitter"
    SOCIAL_INSTAGRAM = "instagram"


class Language(Enum):
    FR = "fr"
    EN = "en"
    UNKNOWN = "unknown"


@dataclass
class RawDocument:
    """Document brut en entrée du pipeline"""
    content: str
    source_url: str
    source_type: SourceType
    title: Optional[str] = None
    crawl_date: datetime = field(default_factory=datetime.now)
    metadata: dict = field(default_factory=dict)


@dataclass
class ProcessedBlock:
    """Bloc de texte traité, prêt pour vectorisation"""
    id: str
    content: str
    content_normalized: str
    language: Language
    char_count: int
    word_count: int
    source_url: str
    source_type: SourceType
    title: Optional[str]
    crawl_date: datetime
    process_date: datetime
    metadata: dict
