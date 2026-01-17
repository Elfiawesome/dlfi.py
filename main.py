from dlfi import DLFI
from dlfi.ingestor import Ingestor
from extractors import get_extractor_for_url

ARCHIVE_DIR = ".archive/archive"
EXPORT_DIR = ".archive/export"

def scrape(url: str):
    # 1. Setup DB
    db = DLFI(ARCHIVE_DIR)
    ingestor = Ingestor(db)

    # 2. Find Plugin
    extractor = get_extractor_for_url(url)
    if not extractor:
        print(f"No extractor found for: {url}")
        return

    # 3. Execute
    # The extractor yields data, the ingestor saves it.
    print(f"Running {extractor.name} on {url}...")
    try:
        generator = extractor.extract(url)
        ingestor.run(generator)
    except Exception as e:
        print(f"Extraction failed: {e}")
        # Optional: Print traceback here for debugging
        import traceback
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    # Example usage
    target_url = "https://twitter.com/someuser"
    scrape(target_url)
    
    # db = DLFI(ARCHIVE_DIR)
    # db.export(EXPORT_DIR)