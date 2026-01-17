from typing import Generator
from .base import BaseExtractor
from dlfi.models import DiscoveredNode, DiscoveredFile

class DummyExtractor(BaseExtractor):
    """
    A test extractor that follows the standard structure:
    - people/{username}
    - download/{extractor_name}/{content}
    """
    name = "Dummy Test"
    slug = "dummy" # Used for folder naming

    def can_handle(self, url: str) -> bool:
        return "test" in url

    def extract(self, url: str) -> Generator[DiscoveredNode, None, None]:
        print(f"[{self.name}] Simulating extraction for {url}")

        # 1. Define Standard Paths
        username = "test_user"
        
        # A. The Person (Global Entity)
        # Stored in people/username
        author_path = f"people/{username}"
        
        # B. The Content (Source Specific)
        # Stored in download/extractor_slug/unique_id
        post_id = "12345"
        post_path = f"download/{self.slug}/post_{post_id}"

        # 2. Yield The Person (Vault)
        yield DiscoveredNode(
            suggested_path=author_path,
            node_type="VAULT",
            metadata={
                "bio": "I am a simulated user",
                "source": "Simulation"
            },
            tags=["person", "simulated"]
        )

        # 3. Yield The Post (Record)
        # Simulate downloading a file
        try:
            # Using google favicon as a stable test file
            temp_file = self.download_to_temp("https://www.google.com/favicon.ico", "icon.ico")
            files_list = [
                DiscoveredFile(
                    local_path=temp_file,
                    original_name="avatar.ico",
                    source_url="google.com"
                )
            ]
        except Exception as e:
            print(f"[{self.name}] Download failed ({e}), yielding record without file.")
            files_list = []

        yield DiscoveredNode(
            suggested_path=post_path,
            node_type="RECORD",
            metadata={"content": "Hello World", "likes": 42},
            tags=["simulation", "post"],
            files=files_list,
            # Link the content back to the person
            relationships=[
                ("AUTHORED_BY", author_path)
            ]
        )