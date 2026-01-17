from .base import BaseExtractor
from .poipiku import PoipikuExtractor

AVAILABLE_EXTRACTORS: list[BaseExtractor] = [
    PoipikuExtractor()
]

def get_extractor_for_url(url: str) -> BaseExtractor:
    for extractor in AVAILABLE_EXTRACTORS:
        if extractor.can_handle(url):
            return extractor
    return None