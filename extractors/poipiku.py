import re
from typing import Generator
from .base import BaseExtractor
from dlfi.models import DiscoveredNode, DiscoveredFile

class PoipikuExtractor(BaseExtractor):
    name = "Poipiku"
    slug = "poipiku"
    URL_PATTERN = re.compile(r'poipiku\.com\/(\d+)(?:\/(\d+)\.html)?')
    CDN_PATTERN = re.compile(r'\"(https:\/\/cdn\.poipiku\.com\/.*?)\"')

    def can_handle(self, url: str) -> bool:
        if self.URL_PATTERN.search(url):
            return True
        return False
    
    def extract(self, url: str) -> Generator[DiscoveredNode, None, None]:
        matches = self.URL_PATTERN.findall(url)
        for match in matches:
            if len(match) == 1:
                # TODO
                pass
            if len(match) == 2:
                yield from self.extract_post(match[0], match[1])
    
    def extract_post(self, user_id: str, post_id: str) -> Generator[DiscoveredNode, None, None]:
        print(f"[{self.name}] {user_id} - {post_id}")
        req = self.session.request("POST", "https://poipiku.com/f/ShowIllustDetailF.jsp", data={
            "ID": user_id,
            "TD": post_id,
            "AD": -1,
            "PAS": None
        }, headers={
            "Origin":"https://poipiku.com",
            "referer": f"https://poipiku.com/{user_id}/{post_id}.html"
        })
        
        
        data = req.json()
        if data:
            post_num = 0
            for img_link in self.CDN_PATTERN.findall(data['html']):
                temp_file = self.download_to_temp(str(img_link))
                ext = "." + img_link.split("?")[0].split(".")[-1]
                yield DiscoveredNode(
                    suggested_path=f"poipiku/users/{user_id}_{post_id}_{post_num}_{ext}",
                    node_type="RECORD",
                    metadata={},
                    files=[DiscoveredFile(
                        local_path=temp_file, 
                        original_name=f"{user_id}_{post_id}_{post_num}_{ext}",
                        source_url=img_link
                    )]
                )
                post_num += 1