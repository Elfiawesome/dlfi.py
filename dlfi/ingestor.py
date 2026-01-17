import os
from typing import Generator
from dlfi.core import DLFI
from dlfi.models import DiscoveredNode

class Ingestor:
    def __init__(self, db: DLFI):
        self.db = db

    def run(self, generator: Generator[DiscoveredNode, None, None]):
        """
        Consumes the generator and commits data to the DB.
        """
        print("[Ingestor] Starting import...")
        count = 0

        for node in generator:
            count += 1
            print(f"[Ingestor] Processing: {node.suggested_path}")
            
            # 1. Create Node (Vault or Record)
            if node.node_type == "VAULT":
                self.db.create_vault(node.suggested_path, metadata=node.metadata)
            else:
                self.db.create_record(node.suggested_path, metadata=node.metadata)

            # 2. Add Tags
            for tag in node.tags:
                self.db.add_tag(node.suggested_path, tag)

            # 3. Process Files
            for file_obj in node.files:
                try:
                    self.db.append_file(
                        record_path=node.suggested_path,
                        file_path=file_obj.local_path,
                        filename_override=file_obj.original_name
                    )
                except Exception as e:
                    print(f"[Ingestor] Error appending file {file_obj.original_name}: {e}")
                finally:
                    # CLEANUP: Remove temp file
                    if file_obj.delete_after_import and os.path.exists(file_obj.local_path):
                        os.remove(file_obj.local_path)

            # 4. Process Relationships
            # Note: This expects the target to exist. 
            # If your extractor yields children before parents, this might fail.
            for rel_name, target_path in node.relationships:
                try:
                    self.db.link(node.suggested_path, target_path, rel_name)
                except ValueError as e:
                    print(f"[Ingestor] Warning - Could not link {rel_name} to {target_path}: {e}")

        print(f"[Ingestor] Finished. Processed {count} nodes.")