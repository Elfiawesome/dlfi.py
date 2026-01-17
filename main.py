from dlfi import DLFI
import os
import shutil

ARCHIVE_PATH = "./archive_test"
EXPORT_PATH = "./archive_export"

def main():
    # Reset for clean test
    if os.path.exists(ARCHIVE_PATH): shutil.rmtree(ARCHIVE_PATH)
    if os.path.exists(EXPORT_PATH): shutil.rmtree(EXPORT_PATH)

    print("--- Initializing ---")
    archive = DLFI(ARCHIVE_PATH)

    # 1. Setup Data
    print("--- Creating Data ---")
    # Author
    archive.create_record("people/hirohiko_araki.record", {"born": 1960, "job": "Manga Artist"})
    
    # Manga
    archive.create_vault("manga/jojo")
    
    # Page Record
    archive.create_record("manga/jojo/page_1.record", {"chapter": 1})

    # Dummy File
    dummy_file = "page.jpg"
    with open(dummy_file, "w") as f: f.write("IMAGE_DATA")
    archive.append_file("manga/jojo/page_1.record", dummy_file)

    # 2. Link & Tag
    print("--- Linking & Tagging ---")
    
    # Tagging the Vault
    archive.add_tag("manga/jojo", "Supernatural")
    archive.add_tag("manga/jojo", "Action")

    # Linking Vault -> Author
    archive.link("manga/jojo", "people/hirohiko_araki.record", "AUTHORED_BY")

    # Linking Page -> Author (Just to show granular linking)
    archive.link("manga/jojo/page_1.record", "people/hirohiko_araki.record", "DRAWN_BY")

    # 3. Export
    print("--- Exporting to Static Files ---")
    archive.export(EXPORT_PATH)

    # 4. Verify Export
    print("--- Verification ---")
    
    # Check Jojo Vault Metadata
    jojo_meta_path = f"{EXPORT_PATH}/manga/jojo/_meta.json"
    if os.path.exists(jojo_meta_path):
        with open(jojo_meta_path, 'r') as f:
            print(f"Jojo Meta: {f.read()}")
    else:
        print("ERROR: Jojo meta missing")

    # Check Page File
    page_file_path = f"{EXPORT_PATH}/manga/jojo/page_1.record/page.jpg"
    if os.path.exists(page_file_path):
        print("SUCCESS: Page file exported correctly.")
    else:
        print("ERROR: Page file missing in export.")

    archive.close()
    if os.path.exists(dummy_file): os.remove(dummy_file)

if __name__ == "__main__":
    main()