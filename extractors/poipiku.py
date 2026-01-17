import re
import logging
from typing import Generator
from .base import BaseExtractor
from dlfi.models import DiscoveredNode, DiscoveredFile

logger = logging.getLogger(__name__)

class PoipikuExtractor(BaseExtractor):
    name = "Poipiku"
    slug = "poipiku"
    URL_PATTERN = re.compile(r'poipiku\.com\/(\d+)(?:\/(\d+)\.html)?')
    CDN_PATTERN = re.compile(r'\"(https:\/\/cdn\.poipiku\.com\/.*?)\"')
    CARD_PATTERN = re.compile(r'\<a class=\"IllustInfo\" href\=\"\/(\w+)\/(\w+)\.html\"\>')
    DESC_PATTERN = re.compile(r'\"IllustItemDesc\" \>(.*)\<\/h1\>')
    TAG_PATTERN = re.compile(r'\"TagName\"\>(.*)\<\/div\>')
    DETAIL_LINK = "https://poipiku.com/f/ShowIllustDetailF.jsp"

    def default_config(self) -> dict:
        return {
            "password": None,
            "password_list": None
        }

    def can_handle(self, url: str) -> bool:
        if self.URL_PATTERN.search(url):
            return True
        return False
    
    def extract(self, url: str, extr_config: dict = {}) -> Generator[DiscoveredNode, None, None]:
        matches = self.URL_PATTERN.findall(url)
        for match in matches:
            if len(match) == 2:
                if match[1] == "":
                    yield from self.extract_profile(match[0], extr_config)
                    continue
                yield from self.extract_post(match[0], match[1], extr_config)
    
    def extract_profile(self, user_id: str, extr_config: dict = {}) -> Generator[DiscoveredNode, None, None]:
        req = self.session.request("POST", f"https://poipiku.com/{user_id}")
        req.text
        for post in self.CARD_PATTERN.findall(req.text):
            yield from self.extract_post(user_id, post[1], extr_config)

    def extract_post(self, user_id: str, post_id: str, extr_config: dict = {}) -> Generator[DiscoveredNode, None, None]:
        logger.info(f"[{self.name}] Processing Post: {user_id} / {post_id}")
        
        req = self.session.request("GET", f"https://poipiku.com/{user_id}/{post_id}.html")
        desc = self.DESC_PATTERN.findall(req.text)[0]
        tags = self.TAG_PATTERN.findall(req.text)
            

        post_data = {
            "description": desc,
            "password": None,
            "poipiku_tags":[]
        }
        if tags:
            for tag in tags:
                post_data["poipiku_tags"].append(str(tag))

        if 'password_list' in extr_config and extr_config['password_list'] is not None:
            password_list = extr_config['password_list']
            success = False
            for _pass in password_list:
                try:
                    yield from self._extract_post(user_id, post_id, post_data, extr_config | {"password": _pass})
                    success = True
                    break
                except Exception as e:
                    logger.info("Failed to extract, trying a different password...")
            if success == False:
                raise Exception("Failed to extract post with all passwords")          
        else:
            yield from self._extract_post(user_id, post_id, post_data, extr_config)


    def _extract_post(self, user_id: str, post_id: str, post_data: dict, extr_config: dict) -> Generator[DiscoveredNode, None, None]:
        # Fetch the metadata/HTML to find image links
        req_data = {
            "ID": user_id,
            "TD": post_id,
            "AD": -1,
            "PAS": extr_config["password"]
        }
        req_headers = {
            "Origin":"https://poipiku.com",
            "referer": f"https://poipiku.com/{user_id}/{post_id}.html"
        }
        req = self.session.request("POST", self.DETAIL_LINK, data=req_data, headers=req_headers)

        data = None
        try:
            req.raise_for_status()
            data = req.json()
            if not data:
                raise Exception("No data returned")
        except Exception as e:
            logger.error(f"Failed to fetch metadata for {post_id}: {e}")
            raise
        
        if data['result'] != 1:
            raise Exception(f"Failed to correctly fetch metadata with data (wrong password?) {data}")

        post_num = 0
        for img_link in self.CDN_PATTERN.findall(data['html']):
            img_url = str(img_link)
            
            try:
                response = self.session.get(img_url, stream=True)
                response.raise_for_status()
                response.raw.decode_content = True

                ext = "." + img_url.split("?")[0].split(".")[-1]
                filename = f"{user_id}_{post_id}_{post_num}{ext}"

                yield DiscoveredNode(
                    suggested_path=f"poipiku/users/{user_id}/{filename}",
                    node_type="RECORD",
                    metadata=post_data | {"password": extr_config["password"]},
                    files=[DiscoveredFile(
                        stream=response.raw, 
                        original_name=filename,
                        source_url=img_url
                    )]
                )
            except Exception as e:
                raise e
            post_num += 1
