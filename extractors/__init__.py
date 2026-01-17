from .base import BaseExtractor
from .dummy import DummyExtractor
from .poipiku import PoipikuExtractor

AVAILABLE_EXTRACTORS: list[BaseExtractor] = [
    DummyExtractor(),
    PoipikuExtractor()
]

for extractor in AVAILABLE_EXTRACTORS:
    # TODO: change this
    extractor.load_cookies("C:/Users/elfia/OneDrive/Desktop/DLFI.py/.archive/cookies/cookies.txt")

def get_extractor_for_url(url: str) -> BaseExtractor:
    for extractor in AVAILABLE_EXTRACTORS:
        if extractor.can_handle(url):
            return extractor
    return None