# DL-FI

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

A local-first Digital Asset Management (DAM) system designed for archivists and data hoarders. DL-FI combines a hierarchical file system (Vaults & Records) with graph-based relationships, content-addressable storage with deduplication, and a modular extractor system for scraping content from the web.

---

## ✨ Features

- **Hierarchical Organization** — Nest Vaults (folders) and Records (items) infinitely deep
- **Graph Relationships** — Link any node to any other with named relationships (e.g., `AUTHORED_BY`, `PART_OF`)
- **Content Deduplication** — Files are stored by SHA-256 hash; duplicates cost zero extra space
- **Modular Extractors** — Plugin-based scrapers for websites (Poipiku included, easily extensible)
- **Cookie Support** — Use exported browser cookies for authenticated scraping
- **Static Export** — Generate a portable folder structure with JSON metadata (no database required to browse)
- **Streaming Ingestion** — Download large files without loading them entirely into RAM

---

## 📑 Table of Contents

- [Installation](#-installation)
- [Quick Start](#-quick-start)
- [Core Concepts](#-core-concepts)
- [Scraping with Extractors](#-scraping-with-extractors)
- [Query System](#-query-system)
- [Exporting](#-exporting-static-site)
- [Writing Custom Extractors](#-writing-custom-extractors)
- [Project Structure](#-project-structure)
- [Troubleshooting](#-troubleshooting)

---

## 📦 Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/dlfi.git
cd dlfi

# Install dependencies
pip install requests
```

> **Requirements:** Python 3.9+, `requests`

---

## 🚀 Quick Start

### Basic Usage (Manual Archiving)

```python
from dlfi import DLFI

# Initialize the archive
archive = DLFI("./my_archive")

# Create structure (parents are created automatically)
archive.create_vault("artwork/landscapes")
archive.create_record("artwork/landscapes/sunset", metadata={
    "title": "Sunset Over Mountains",
    "artist": "Jane Doe",
    "year": 2024
})

# Attach files
archive.append_file(
    record_path="artwork/landscapes/sunset",
    file_path="/path/to/sunset.jpg",
    filename_override="main.jpg"
)

# Add tags and relationships
archive.add_tag("artwork/landscapes/sunset", "nature")
archive.create_record("people/jane_doe", metadata={"role": "artist"})
archive.link("artwork/landscapes/sunset", "people/jane_doe", "CREATED_BY")

# Export to static files
archive.export("./public_archive")

# Always close when done
archive.close()
```

### Scraping from the Web

```python
from dlfi import DLFI
from dlfi.job import Job, JobConfig

archive = DLFI("./my_archive")

# Configure job with cookies for authenticated access
config = JobConfig(cookies="/path/to/cookies.txt")
job = Job(config)
job.db = archive

# Scrape a single post
job.run("https://poipiku.com/12345/67890.html")

# Scrape an entire profile with password-protected content
job.run("https://poipiku.com/12345/", {
    "password": "secret123",
    "password_list": ["pass1", "pass2", "pass3"]
})

archive.export("./export")
archive.close()
```

---

## 📚 Core Concepts

### Terminology

| Term | Description |
|------|-------------|
| **Vault** | A container/folder that holds other Vaults or Records |
| **Record** | An individual item with metadata and attached files |
| **Blob** | The physical file, stored by content hash (SHA-256) |
| **Relationship** | A directed, labeled edge between two nodes |
| **Tag** | A simple string label attached to a node |

### Storage Architecture

```
my_archive/
└── .dlfi/                    # System directory (don't modify manually)
    ├── db.sqlite             # Metadata, relationships, structure
    ├── storage/              # Content-addressable blob storage
    │   └── a1/b2/a1b2c3...   # Files stored by hash prefix sharding
    └── temp/                 # Temporary download staging area
```

**Key Benefits:**
- If you download the same image twice, it's only stored once
- Renaming/moving records doesn't duplicate file data
- The database can be rebuilt if you have the blobs (future feature)

---

## 🕷️ Scraping with Extractors

### Supported Sites

| Site | URL Patterns | Features |
|------|--------------|----------|
| **Poipiku** | `poipiku.com/{user_id}` (profile)<br>`poipiku.com/{user_id}/{post_id}.html` (post) | Password-protected posts, batch profile scraping |

### Using Cookies

Many sites require authentication. Export cookies from your browser using an extension like [Get cookies.txt LOCALLY](https://chromewebstore.google.com/detail/get-cookiestxt-locally/cclelndahbckbenkjhflpdbgdldlbecc) (Netscape format).

```python
config = JobConfig(cookies="/path/to/cookies.txt")
```

### Extractor Configuration

Each extractor accepts site-specific options:

```python
# Poipiku options
job.run("https://poipiku.com/12345/67890.html", {
    "password": "single_password",       # Try this password first
    "password_list": ["pw1", "pw2"]      # Then try these
})
```

---

## 🔍 Query System

DL-FI provides a fluent query builder for searching your archive.

### Basic Queries

```python
# All records inside a vault (recursive)
results = archive.query().inside("artwork/landscapes").execute()

# Filter by node type
results = archive.query().type("RECORD").execute()

# Filter by metadata
results = archive.query().meta_eq("year", 2024).execute()

# Filter by tag
results = archive.query().has_tag("nature").execute()
```

### Relationship Queries

```python
# Find all items created by a specific person
results = archive.query()\
    .related_to("people/jane_doe", "CREATED_BY")\
    .execute()

# Find vaults containing ANY item by this person (recursive search)
results = archive.query()\
    .type("VAULT")\
    .contains_related("people/jane_doe", "CREATED_BY")\
    .execute()
```

### Combining Filters

```python
results = archive.query()\
    .inside("artwork")\
    .type("RECORD")\
    .has_tag("nature")\
    .meta_eq("year", 2024)\
    .execute()

# Results format
for item in results:
    print(f"{item['path']} ({item['type']})")
    print(f"  UUID: {item['uuid']}")
    print(f"  Metadata: {item['metadata']}")
```

---

## 📤 Exporting (Static Site)

Convert your archive to a portable folder structure:

```python
archive.export("./public")
```

### Output Structure

```
public/
├── index.json                    # UUID → Path lookup map
├── artwork/
│   ├── _meta.json                # Vault metadata
│   └── landscapes/
│       ├── _meta.json
│       └── sunset/
│           ├── _meta.json        # Record metadata + relationships
│           └── main.jpg          # Actual file
└── people/
    └── jane_doe/
        └── _meta.json
```

### Metadata File Format

```json
{
  "uuid": "550e8400-e29b-41d4-a716-446655440000",
  "type": "RECORD",
  "title": "Sunset Over Mountains",
  "artist": "Jane Doe",
  "tags": ["nature", "landscape"],
  "relationships": [
    {"relation": "CREATED_BY", "target_path": "people/jane_doe"}
  ],
  "files": ["main.jpg"]
}
```

---

## 🔧 Writing Custom Extractors

Create a new file in `extractors/` and implement the `BaseExtractor` interface:

```python
# extractors/mysite.py
import re
from typing import Generator
from .base import BaseExtractor
from dlfi.models import DiscoveredNode, DiscoveredFile

class MySiteExtractor(BaseExtractor):
    
    @property
    def name(self) -> str:
        return "MySite"
    
    def can_handle(self, url: str) -> bool:
        return "mysite.com" in url
    
    def default_config(self) -> dict:
        return {"quality": "high"}
    
    def extract(self, url: str, config: dict) -> Generator[DiscoveredNode, None, None]:
        cfg = self.default_config() | config
        
        # Fetch page
        resp = self._request("GET", url)
        
        # Parse and yield nodes
        yield DiscoveredNode(
            suggested_path="mysite/item_123",
            node_type="RECORD",
            metadata={"source_url": url},
            files=[
                DiscoveredFile(
                    original_name="image.jpg",
                    source_url="https://mysite.com/image.jpg",
                    stream=self._request("GET", "https://...", stream=True).raw
                )
            ],
            tags=["scraped"],
            relationships=[]
        )
```

Register it in `extractors/__init__.py`:

```python
from .mysite import MySiteExtractor

AVAILABLE_EXTRACTORS = [
    PoipikuExtractor(),
    MySiteExtractor(),  # Add here
]
```

---

## 📁 Project Structure

```
dlfi/
├── dlfi/
│   ├── __init__.py       # Package exports
│   ├── core.py           # DLFI class, QueryBuilder, storage logic
│   ├── job.py            # Job runner for extractors
│   ├── logger.py         # Logging configuration
│   └── models.py         # DiscoveredNode, DiscoveredFile dataclasses
├── extractors/
│   ├── __init__.py       # Extractor registry
│   ├── base.py           # BaseExtractor ABC
│   └── poipiku.py        # Poipiku implementation
├── main.py               # Example entry point
└── README.md
```

---

## ❓ Troubleshooting

### "No extractor found for URL"

The URL pattern isn't recognized. Check `can_handle()` in the extractor or add a new one.

### "Permission denied" when accessing files

Ensure the archive directory is writable and no other process has locked `db.sqlite`.

### Cookies not working

1. Ensure the file is in **Netscape/Mozilla format** (not JSON)
2. Check the cookie file path is absolute or relative to the working directory
3. Cookies may have expired — re-export from browser

### Missing images after scrape

- Check logs for `"Failed to ingest file"` errors
- The site may require authentication (use cookies)
- Password-protected content needs the correct password in config

### Database locked errors

SQLite locks on concurrent writes. Ensure only one process accesses the archive at a time.

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.

---

## 🤝 Contributing

Contributions welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/new-extractor`)
3. Write tests for new functionality
4. Submit a pull request

**Priority areas:**
- New site extractors
- Better error recovery
- Web UI for browsing exports
- Database migration tools
