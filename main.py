from dlfi import DLFI
import os

# Define where we want our archive
ARCHIVE_PATH = "./archive_test"

def main():
    print("--- Initializing DL-FI ---")
    
    # This should create the folders and database
    archive = DLFI(ARCHIVE_PATH)
    
    # Check if files exist
    if os.path.exists(f"{ARCHIVE_PATH}/.dlfi/db.sqlite"):
        print("✅ Database created successfully.")
    
    if os.path.exists(f"{ARCHIVE_PATH}/.dlfi/db.sqlite-wal"):
        print("✅ Write-Ahead Logging (WAL) is active (Speed Optimization).")
        
    if os.path.exists(f"{ARCHIVE_PATH}/.dlfi/storage"):
        print("✅ Storage directory created.")

    # Simple Query to ensure schema is ready
    cursor = archive.conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    print(f"✅ Tables initialized: {tables}")

    archive.close()

if __name__ == "__main__":
    main()