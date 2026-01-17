from .base import BaseExtractor

# Import your extractors here once created
# from .twitter import TwitterExtractor 

AVAILABLE_EXTRACTORS = [
    # TwitterExtractor()
]

def get_extractor_for_url(url: str) -> BaseExtractor:
    for extractor in AVAILABLE_EXTRACTORS:
        if extractor.can_handle(url):
            return extractor
    return None