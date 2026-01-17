import os
import requests
import tempfile
from urllib.parse import urlparse
from pathlib import Path
from dlfi import DLFI

# Initialize your archive
db = DLFI("./my_manga_archive")

def process_manga_page(url: str, manga_slug: str, chapter: int, page_num: int):
    """
    Downloads a page and archives it into DL-FI structure:
    manga/{slug}/chapter_{num}/page_{num}.record
    """
    
    # 1. Construct the Target Path
    # Structure: manga/mask_danshi/chapter_1/page_01.record
    vault_path = f"manga/{manga_slug}/chapter_{chapter}"
    record_name = f"page_{page_num:02d}.record" # e.g., page_01.record
    full_record_path = f"{vault_path}/{record_name}"

    print(f"[*] Processing: {full_record_path}")

    # 2. Idempotency Check (Optimization)
    # If this record already exists, skip the download!
    if db._resolve_path(full_record_path):
        print("    -> Record already exists. Skipping download.")
        return

    # 3. Prepare Metadata
    # It is crucial to store the Source URL for archival history
    filename_from_url = os.path.basename(urlparse(url).path)
    metadata = {
        "source_url": url,
        "original_filename": filename_from_url,
        "manga_name": manga_slug.replace("_", " ").title(),
        "chapter": chapter,
        "page": page_num,
        "scraped_at": 2024 # You can use time.time()
    }

    # 4. Download to Temp File
    print(f"    -> Downloading from {url}...")
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        
        # Create a temp file to hold the image before archiving
        # We look at the extension from the URL (e.g., .jpeg)
        ext = Path(filename_from_url).suffix
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp_file:
            for chunk in response.iter_content(chunk_size=8192):
                tmp_file.write(chunk)
            temp_path = tmp_file.name

        # 5. Commit to DL-FI
        print("    -> Archiving...")
        
        # Ensure Vaults exist (recursive)
        db.create_vault(vault_path)
        
        # Create the Record container
        db.create_record(full_record_path, metadata)
        
        # Move file into archive (Hashes it, stores it, links it)
        db.append_file(full_record_path, temp_path)
        
        # 6. Cleanup Temp
        os.remove(temp_path)
        print("    -> Success ✅")

    except Exception as e:
        print(f"    -> ERROR ❌: {e}")
        # Clean up temp if it exists
        if 'temp_path' in locals() and os.path.exists(temp_path):
            os.remove(temp_path)

# --- Usage Example ---

target_url = "https://iweb11.mangapicgallery.com/r/newpiclink/mask_danshi_wa_koishitakunai_no_ni/1/fd67b6bda1ab9fa0d14037c3912c2c81.jpeg"

# You would usually loop through a list of URLs here
process_manga_page(
    url=target_url,
    manga_slug="mask_danshi_wa_koishitakunai_no_ni",
    chapter=1,
    page_num=1
)

db.export("./archive_export")