import re
import logging
from typing import Generator, List
from .base import BaseExtractor
from dlfi.models import DiscoveredNode, DiscoveredFile

logger = logging.getLogger(__name__)

class PoipikuExtractor(BaseExtractor):
	name = "Poipiku"
	slug = "poipiku"
	
	URL_PATTERN = re.compile(r'poipiku\.com\/(\d+)(?:\/(\d+)\.html)?')
	CDN_PATTERN = re.compile(r'[\"\'](https:\/\/cdn\.poipiku\.com\/.*?)[\"\']')
	CARD_PATTERN = re.compile(r'class=[\"\']IllustInfo[\"\']\s+href=[\"\']\/(\w+)\/(\w+)\.html[\"\']')
	DESC_PATTERN = re.compile(r'class=[\"\']IllustItemDesc[\"\'][^>]*>(.*?)<\/', re.IGNORECASE | re.DOTALL)
	TAG_PATTERN = re.compile(r'class=[\"\']TagName[\"\'][^>]*>(.*?)<\/', re.IGNORECASE)

	USERNAME_PATTERN = re.compile(r'class=[\"\']UserInfoUserName[\"\']\>?[\s\S]\<a href=".+">(.+)<\/a>')

	DETAIL_ENDPOINT = "https://poipiku.com/f/ShowIllustDetailF.jsp"

	def default_config(self) -> dict:
		return {
			"password": None,
			"password_list": []
		}

	def can_handle(self, url: str) -> bool:
		return bool(self.URL_PATTERN.search(url))

	def extract(self, url: str, extr_config: dict = {}) -> Generator[DiscoveredNode, None, None]:
		cfg = self.default_config() | extr_config
		match = self.URL_PATTERN.search(url)
		if not match:
			return

		user_id, post_id = match.groups()
		yield from self.process_profile_data(user_id, extr_config)
		if post_id:
			yield from self.process_post(user_id, post_id, cfg)
		else:
			yield from self.process_profile(user_id, cfg)
	
	def process_profile_data(self, user_id: str, config: dict) -> Generator[DiscoveredNode, None, None]:
		logger.info(f"[{self.name}] Processing profile Data: {user_id}")
		url = f"https://poipiku.com/{user_id}/"
		resp = self._request("GET", url)
		
		username = ""
		username_matches = self.USERNAME_PATTERN.findall(resp.text)
		if not username_matches:
			username = username_matches[0]

		yield DiscoveredNode(
			suggested_path=f"poipiku/users/{user_id}",
			node_type="VAULT",
			metadata={"username": username},
		)
	
	def process_profile(self, user_id: str, config: dict) -> Generator[DiscoveredNode, None, None]:
		logger.info(f"[{self.name}] Scanning profile: {user_id}")
		
		url = f"https://poipiku.com/{user_id}/"
		resp = self._request("GET", url)
		
		found = self.CARD_PATTERN.findall(resp.text)
		logger.info(f"[{self.name}] Found {len(found)} posts on profile page.")

		for p_user, p_id in found:
			# Ensure we only grab posts for this user (ignore recommendations)
			if str(p_user) == str(user_id):
				yield from self.process_post(p_user, p_id, config)

	def process_post(self, user_id: str, post_id: str, config: dict) -> Generator[DiscoveredNode, None, None]:
		logger.info(f"[{self.name}] Processing Post {post_id} (User: {user_id})")
		
		page_url = f"https://poipiku.com/{user_id}/{post_id}.html"
		
		# 1. Fetch Landing Page (Metadata)
		try:
			resp = self._request("GET", page_url)
		except Exception:
			# 404 or other error, skip this node
			return

		html = resp.text
		
		# Extract Metadata
		desc_match = self.DESC_PATTERN.search(html)
		description = desc_match.group(1).strip() if desc_match else ""
		tags = [t.strip() for t in self.TAG_PATTERN.findall(html)]

		metadata = {
			"description": description,
			"url": page_url,
			"author_id": user_id,
			"post_id": post_id
		}

		yield from self._resolve_images(user_id, post_id, config, metadata)

	def _resolve_images(self, user_id: str, post_id: str, config: dict, metadata: dict = {}) -> Generator[DiscoveredFile, None, None]:
		"""
		Tries passwords against the AJAX endpoint to get image URLs.
		"""
		passwords = []
		if config.get("password"):
			passwords.append(config["password"])
		if config.get("password_list"):
			passwords.extend(config["password_list"])
		
		# Always try empty password (for public posts) if not already included
		if "" not in passwords:
			passwords.append("")
		
		headers = {
			"Origin": "https://poipiku.com",
			"Referer": f"https://poipiku.com/{user_id}/{post_id}.html"
		}

		for pwd in passwords:
			data = {
				"ID": user_id,
				"TD": post_id,
				"AD": -1,
				"PAS": pwd
			}
			
			try:
				r = self._request("POST", self.DETAIL_ENDPOINT, data=data, headers=headers)
				json_data = r.json()
				
				# API Result: 1 = Success
				if json_data.get("result") == 1:
					html_content = json_data.get("html", "")
					images: list[str] = self.CDN_PATTERN.findall(html_content)
					if images:
						if pwd:
							logger.info(f"[{self.name}] Unlocked {post_id} with password.")
						
						for idx, img_url in enumerate(images):
							try:
								# Deduce extension from URL
								path_part = img_url.split("?")[0]
								ext = "." + path_part.split(".")[-1]
								filename = f"{user_id}_{post_id}_{idx}{ext}"
								
								img_resp = self._request("GET", img_url, stream=True)
								img_resp.raw.decode_content = True
								
								yield DiscoveredNode(
									suggested_path=f"poipiku/users/{user_id}/{post_id}_{idx}",
									node_type="RECORD",
									metadata=(metadata | {"image_num": idx} | ({"password_used": pwd} if pwd else metadata)),
									files=[DiscoveredFile(
										original_name=filename,
										source_url=img_url,
										stream=img_resp.raw
									)]
								)
							except Exception as e:
								logger.error(f"[{self.name}] Error streaming image {img_url}: {e}")
								raise e
					else:
						logger.error(f"[{self.name}] Failed to resolve images for {post_id}. Result: {json_data} . Exhausted password list.")
			except Exception as e:
				logger.debug(f"[{self.name}] Password check failed for {post_id}: {e}")