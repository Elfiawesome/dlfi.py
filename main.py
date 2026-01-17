from dlfi import DLFI
import os

ARCHIVE_PATH = "./archive_test"

def main():
    print("--- Testing Write Operations ---")
    archive = DLFI(ARCHIVE_PATH)

    # 1. Create Hierarchy
    print("1. Creating Vault 'manga/jojo'...")
    vault_id = archive.create_vault("manga/jojo")
    print(f"   -> Created Vault UUID: {vault_id}")

    # 2. Create Record with Metadata
    print("2. Creating Record 'manga/jojo/page_1.record'...")
    meta = {"chapter": 1, "artist": "Araki", "scan_group": "Anon"}
    record_id = archive.create_record("manga/jojo/page_1.record", metadata=meta)
    print(f"   -> Created Record UUID: {record_id}")

    # 3. Create a Dummy File to simulate a download
    dummy_file = "test_image.jpg"
    with open(dummy_file, "wb") as f:
        f.write(b"fake_image_content_data_12345")
    
    # 4. Store File
    print("3. Appending file to record...")
    archive.append_file("manga/jojo/page_1.record", dummy_file)
    print("   -> File hashed, stored, and linked.")

    # 5. Verify Database Content
    print("4. Verifying DB...")
    cursor = archive.conn.execute("""
        SELECT n.name, n.cached_path, b.storage_path 
        FROM nodes n
        JOIN node_files nf ON n.uuid = nf.node_uuid
        JOIN blobs b ON nf.file_hash = b.hash
        WHERE n.uuid = ?
    """, (record_id,))
    
    row = cursor.fetchone()
    if row:
        print(f"   SUCCESS! Found record: {row[0]}")
        print(f"   Path: {row[1]}")
        print(f"   Blob stored at: .dlfi/storage/{row[2]}")
    else:
        print("   ERROR: Could not verify data.")

    archive.close()
    
    # Cleanup dummy file
    if os.path.exists(dummy_file):
        os.remove(dummy_file)

if __name__ == "__main__":
    main()