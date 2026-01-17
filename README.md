# DL-FI: Digital Library & File Indexer

DL-FI is a local Digital Asset Management (DAM) system designed for archivists. It combines a hierarchical file system approach (Vaults & Records) with a Graph Database (Relationships), allowing for complex organization of files, metadata, and provenance.

It allows you to scrape content, store it with deduplication, tag it with relationships, and export the entire archive as a static, server-less website.

## 📂 Architecture

The system operates in two modes:
1.  **Active (Local):** An SQLite database + a content-addressable blob storage (`.dlfi/storage/`).
2.  **Static (Exported):** A generated JSON/Folder structure that requires no database engine to read.

### Terminology
*   **Vault:** A folder/category (e.g., `manga/jojo`). Can contain other Vaults or Records.
*   **Record:** An individual item entity (e.g., `chapter_1.record`). Holds Metadata and Files.
*   **Blob:** The physical file (image/pdf). Stored by Hash (SHA256) to prevent duplicates.
*   **Relationship:** A directional link between two nodes (e.g., `Chapter 1` --DRAWN_BY--> `Araki`).

---

## 🚀 Quick Start

### 1. Initialization
```python
from dlfi import DLFI

# Initialize the library pointing to your data folder
archive = DLFI("./my_archive")
```

### 2. Storing Data
DL-FI handles directory creation recursively. You don't need to create parent folders manually.

```python
# Create a Vault (Folder)
archive.create_vault("manga/jojo")

# Create a Record (Item) with Metadata
meta = {"chapter": 1, "release_date": "1987-01-01"}
archive.create_record("manga/jojo/ch1.record", metadata=meta)

# Attach a File to the Record
# 'filename_override' allows you to rename the file inside the archive
archive.append_file(
    record_path="manga/jojo/ch1.record", 
    file_path="/tmp/downloaded_image.jpg",
    filename_override="page_01.jpg"
)
```

### 3. Creating Relationships
Instead of simple tags, DL-FI uses a Graph system.

```python
# Create the nodes
archive.create_record("people/araki.record", {"job": "Mangaka"})

# Link them
archive.link(
    source="manga/jojo/ch1.record", 
    target="people/araki.record", 
    relation="AUTHORED_BY"
)

# Primitive Tags (Simple strings)
archive.add_tag("manga/jojo", "action")
```

---

## 🔍 Query System

The `query()` method uses a Builder Pattern.

### Basic Filtering
```python
# Find all records inside a specific vault
results = archive.query().inside("manga/jojo").execute()

# Find specific metadata
results = archive.query().meta_eq("chapter", 1).execute()
```

### Relationship Queries
**Direct Relationship:** Find records pointing to a specific target.
```python
# "Find items authored by Araki"
results = archive.query()\
    .related_to("people/araki.record", "AUTHORED_BY")\
    .execute()
```

**Recursive Relationship (Deep Search):** Find Vaults that contain *any* child related to a target.
```python
# "Find all Manga Series (Vaults) that contain chapters drawn by Araki"
results = archive.query()\
    .type("VAULT")\
    .contains_related("people/araki.record", "DRAWN_BY")\
    .execute()
```

---

## 📦 Exporting (Static Site)

You can convert the database into a standard file system structure with `_meta.json` sidecars. This is useful for hosting the archive on a web server or browsing via file explorer.

```python
archive.export("./export_folder")
```

**Output Structure:**
```text
/export_folder/
├── index.json                  # Global UUID map
└── manga/
    └── jojo/
        ├── _meta.json          # Contains Tags/Relations
        └── ch1.record/
            ├── _meta.json
            └── page_01.jpg     # The actual file
```

---

## 🛠 Internal Storage (`.dlfi`)

Do not touch the `.dlfi` folder manually.
*   `db.sqlite`: Stores the structure and metadata.
*   `storage/`: Stores files using `xx/yy/hash...` structure.

If you delete `db.sqlite`, you lose the structure, but the raw files remain in `storage`.
