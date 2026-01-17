# List of all active extractors
AVAILABLE_EXTRACTORS = [
    # TwitterExtractor()
]

def get_extractor_for_url(url: str):
    """Iterates through plugins to find one that matches the URL."""
    for extractor in AVAILABLE_EXTRACTORS:
        if extractor.can_handle(url):
            return extractor
    return None