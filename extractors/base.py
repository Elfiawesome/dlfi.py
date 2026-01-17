import os
import requests
import tempfile
import http.cookiejar
from abc import ABC, abstractmethod
from typing import Generator, Optional
from dlfi.models import DiscoveredNode

class BaseExtractor(ABC):
    def __init__(self):
        self.session = requests.Session()
        
        # User-Agent spoofing is almost always required
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        })
        
        self._load_cookies()

    def _load_cookies(self):
        """
        Looks for a 'cookies.txt' (Netscape format) in the current working directory
        and loads it into the session.
        """
        cookie_file = "cookies.txt"
        if os.path.exists(cookie_file):
            try:
                # Use standard library to parse Netscape cookie file
                jar = http.cookiejar.MozillaCookieJar(cookie_file)
                jar.load(ignore_discard=True, ignore_expires=True)
                self.session.cookies.update(jar)
                print(f"[{self.name}] Loaded cookies from {cookie_file}")
            except Exception as e:
                print(f"[{self.name}] Failed to load cookies: {e}")

    def download_to_temp(self, url: str, filename_hint: str = "file") -> str:
        """
        Downloads a file synchronously to a temporary location.
        Returns the path to the temp file.
        """
        print(f"[{self.name}] Downloading: {url}")
        
        with self.session.get(url, stream=True) as r:
            r.raise_for_status()
            
            # Determine extension
            ext = os.path.splitext(filename_hint)[1]
            if not ext:
                # Try to guess from content-type if needed, strictly simple for now
                ext = ".bin"

            # Create temp file
            # delete=False because we need to close it before the Ingestor can read/move it
            tf = tempfile.NamedTemporaryFile(delete=False, suffix=ext)
            
            for chunk in r.iter_content(chunk_size=8192):
                tf.write(chunk)
            
            tf.close()
            return tf.name

    @property
    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def can_handle(self, url: str) -> bool:
        pass

    @abstractmethod
    def extract(self, url: str) -> Generator[DiscoveredNode, None, None]:
        """
        The main logic. Yields DiscoveredNode objects.
        """
        pass