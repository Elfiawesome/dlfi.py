from dlfi import DLFI
import shutil
import os

ARCHIVE_PATH = "./archive_test_rels"

def main():
    if os.path.exists(ARCHIVE_PATH): shutil.rmtree(ARCHIVE_PATH)
    db = DLFI(ARCHIVE_PATH)

    print("--- Setup ---")
    
    # 1. Create the Artist
    db.create_record("people/araki.record", {"job": "mangaka"})
    
    # 2. Create the Vault and a Record inside it
    db.create_vault("manga/jojo")
    db.create_record("manga/jojo/chapter1.record", {"pages": 20})
    
    # 3. Create a totally unrelated vault
    db.create_vault("manga/naruto")
    db.create_record("manga/naruto/chapter1.record")

    # 4. Link the CHILD record to the Artist
    print("Linking 'manga/jojo/chapter1.record' -> 'people/araki.record' (DRAWN_BY)")
    db.link("manga/jojo/chapter1.record", "people/araki.record", "DRAWN_BY")

    print("\n--- TEST 1: Direct Relationship ---")
    # Find the specific chapter drawn by Araki
    results = db.query().related_to("people/araki.record", "DRAWN_BY").execute()
    for r in results:
        print(f"MATCH: {r['path']}")
    # Expected: manga/jojo/chapter1.record

    print("\n--- TEST 2: Recursive/Child Relationship (The Vault Query) ---")
    # Find any VAULT that contains something drawn by Araki
    # This simulates: "Show me all Manga series that Araki worked on"
    results = db.query()\
        .type("VAULT")\
        .contains_related("people/araki.record", "DRAWN_BY")\
        .execute()
        
    for r in results:
        print(f"MATCH: {r['path']}")
    # Expected: manga/jojo
    # Should NOT find: manga/naruto

    db.export("./archive_export")
    db.close()

if __name__ == "__main__":
    main()