import re
import logging
import time
from typing import Generator, List, Optional
from .base import BaseExtractor
from dlfi.models import DiscoveredNode, DiscoveredFile

logger = logging.getLogger(__name__)

class PoipikuExtractor(BaseExtractor):
	name = "Poipiku"
	slug = "poipiku"
	
	# Matching Logic
	URL_PATTERN = re.compile(r'https?://(?:www\.)?poipiku\.com/(\d+)(?:/(\d+)(?:\.html)?)?')
	
	# Metadata Regex
	TITLE_PATTERN = re.compile(r'<title>(.*?)<', re.IGNORECASE)
	DESC_PATTERN = re.compile(r'class="IllustItemDesc"[^>]*>(.*?)</h1>', re.IGNORECASE | re.DOTALL)
	USER_NAME_PATTERN = re.compile(r'class="UserInfoUserName"[^>]*>.*?<a[^>]*>(.*?)</a>', re.IGNORECASE | re.DOTALL)
	
	# Content Regex
	IMG_SRC_PATTERN = re.compile(r'src="([^"]+)"')
	ILLUST_INFO_PATTERN = re.compile(r'class="IllustInfo"\s+href="([^"]+)"') # Extracts relative paths like /123/456.html
	
	# API Endpoints
	ROOT_URL = "https://poipiku.com"
	DETAIL_ENDPOINT = "https://poipiku.com/f/ShowIllustDetailF.jsp"
	APPEND_ENDPOINT = "https://poipiku.com/f/ShowAppendFileF.jsp"
	LIST_ENDPOINT = "https://poipiku.com/IllustListPcV.jsp"

	def default_config(self) -> dict:
		"""
		Exposes configuration for pagination, authentication, and throttling.
		"""
		return {
			# Authentication
			"password": "",             # Single password to try
			"password_list": [],        # List of passwords to iterate
			
			# Pagination Control
			"max_pages": 0,             # 0 = scrape all available pages for a user
			"page_start": 0,            # Start scraping from specific page index
			
			# Throttling
			"sleep_interval": 0.0,      # Seconds to wait between requests to avoid bans
			
			# Cookie / View Preferences
			"contents_view_mode": "1",  # '1' usually shows all contents
			"lang": "en",
		}

	def can_handle(self, url: str) -> bool:
		return bool(self.URL_PATTERN.match(url))

	def _init_headers(self, config: dict):
		"""
		Sets up session headers and cookies required by Poipiku to function correctly.
		"""
		# Specific cookies required for the site to render content correctly
		self.session.cookies.set("POIPIKU_CONTENTS_VIEW_MODE", config["contents_view_mode"], domain="poipiku.com")
		self.session.cookies.set("LANG", config["lang"], domain="poipiku.com")

		# Headers mimicking a browser interaction, crucial for the AJAX endpoints
		self.session.headers.update({
			"Accept": "application/json, text/javascript, */*; q=0.01",
			"X-Requested-With": "XMLHttpRequest",
			"Origin": self.ROOT_URL,
			"Sec-Fetch-Dest": "empty",
			"Sec-Fetch-Mode": "cors",
			"Sec-Fetch-Site": "same-origin",
		})

	def extract(self, url: str, extr_config: dict = {}) -> Generator[DiscoveredNode, None, None]:
		cfg = self.default_config() | extr_config
		self._init_headers(cfg)

		match = self.URL_PATTERN.match(url)
		if not match:
			return

		user_id, post_id = match.groups()

		if post_id:
			# Single Post Mode
			yield from self.process_post(user_id, post_id, cfg)
		else:
			# User Profile Mode
			yield from self.process_profile(user_id, cfg)

	def process_profile(self, user_id: str, config: dict) -> Generator[DiscoveredNode, None, None]:
		logger.info(f"[{self.name}] Scanning profile for user: {user_id}")
		
		page = config["page_start"]
		max_pages = config["max_pages"]
		
		# Create a Vault node for the user
		yield DiscoveredNode(
			suggested_path=f"poipiku/users/{user_id}",
			node_type="VAULT",
			metadata={"user_id": user_id}
		)

		while True:
			if max_pages > 0 and page >= max_pages:
				logger.info(f"[{self.name}] Reached max pages limit ({max_pages}).")
				break

			logger.debug(f"[{self.name}] Fetching page {page} for user {user_id}")
			
			params = {
				"PG": page,
				"ID": user_id,
				"KWD": ""
			}
			
			try:
				# We use the JSP endpoint used by the actual site for pagination
				resp = self._request("GET", self.LIST_ENDPOINT, params=params)
				html = resp.text
			except Exception as e:
				logger.error(f"[{self.name}] Failed to fetch page {page}: {e}")
				break

			# Find all post links on this page
			posts = self.ILLUST_INFO_PATTERN.findall(html)
			
			if not posts:
				logger.info(f"[{self.name}] No more posts found at page {page}.")
				break
				
			logger.info(f"[{self.name}] Found {len(posts)} posts on page {page}.")

			for post_rel_url in posts:
				# post_rel_url looks like "/1234/5678.html"
				try:
					p_parts = post_rel_url.strip("/").split("/")
					if len(p_parts) >= 2:
						p_user_id = p_parts[0]
						p_post_id = p_parts[1].replace(".html", "")
						
						yield from self.process_post(p_user_id, p_post_id, config)
				except Exception as e:
					logger.warning(f"[{self.name}] Failed parsing post URL {post_rel_url}: {e}")

			# Poipiku typically shows 48 items per page. If less, we are at the end.
			if len(posts) < 48:
				break
			
			page += 1
			if config.get("sleep_interval"):
				time.sleep(config["sleep_interval"])

	def process_post(self, user_id: str, post_id: str, config: dict) -> Generator[DiscoveredNode, None, None]:
		post_url = f"{self.ROOT_URL}/{user_id}/{post_id}.html"
		logger.info(f"[{self.name}] Processing Post {post_id}")

		# Update Referer for this specific post context
		self.session.headers.update({"Referer": post_url})

		try:
			resp = self._request("GET", post_url)
			html = resp.text
		except Exception as e:
			logger.error(f"[{self.name}] Failed to load post {post_id}: {e}")
			return

		# Metadata Extraction
		user_name_match = self.USER_NAME_PATTERN.search(html)
		desc_match = self.DESC_PATTERN.search(html)
		
		metadata = {
			"post_id": post_id,
			"user_id": user_id,
			"url": post_url,
			"user_name": user_name_match.group(1).strip() if user_name_match else user_id,
			"description": desc_match.group(1).strip() if desc_match else "",
		}

		# Resolve Image URLs
		# We need to determine if we need a password or just standard extraction
		image_urls = self._resolve_images(user_id, post_id, html, config)
		
		if not image_urls:
			logger.warning(f"[{self.name}] No images found for post {post_id}. It might be strictly locked.")
			return

		for idx, img_url in enumerate(image_urls):
			# Filename logic: postID_num.ext
			fname_base = f"{post_id}_{idx}"
			ext = "." + img_url.split(".")[-1].split("?")[0]
			filename = fname_base + ext

			# Attempt to stream the file
			try:
				img_resp = self._request("GET", img_url, stream=True)
				
				yield DiscoveredNode(
					suggested_path=f"poipiku/users/{user_id}/{fname_base}",
					node_type="RECORD",
					metadata=metadata | {"image_index": idx},
					files=[DiscoveredFile(
						original_name=filename,
						source_url=img_url,
						stream=img_resp.raw
					)]
				)
			except Exception as e:
				logger.error(f"[{self.name}] Failed to stream image {img_url}: {e}")

	def _resolve_images(self, user_id: str, post_id: str, html: str, config: dict) -> List[str]:
		"""
		Determines the correct strategy (Public vs Password) to get image links.
		Mimics gallery-dl's _extract_files_auth and _extract_files_noauth logic.
		"""
		
		# 1. Collect potential passwords
		passwords = []
		if config.get("password"):
			passwords.append(config["password"])
		if config.get("password_list"):
			passwords.extend(config["password_list"])
		passwords.append("") # Attempt empty password (public access via API)

		# 2. Check for restricted content markers in the HTML (e.g. "publish_pass")
		# Gallery-DL logic checks the thumbnail URL to see if it indicates a lock
		requires_password = False
		thumb_match = re.search(r'class="IllustItemThumbImg" src="([^"]+)"', html)
		if thumb_match:
			thumb_url = thumb_match.group(1)
			if "publish_pass" in thumb_url:
				requires_password = True
		
		found_images = []

		# Strategy A: It's just a public post or simple list
		# Try ShowAppendFileF.jsp first if we don't think it's locked. 
		# This endpoint is often more reliable for non-password multi-image posts.
		if not requires_password:
			res = self._fetch_append_files(user_id, post_id, "")
			if res:
				return res

		# Strategy B: Use ShowIllustDetailF.jsp (supports passwords)
		for pwd in passwords:
			# Skip empty password if we already know it requires one
			if requires_password and not pwd:
				continue
				
			data = {
				"ID": user_id,
				"TD": post_id,
				"AD": "-1",
				"PAS": pwd
			}
			
			try:
				r = self._request("POST", self.DETAIL_ENDPOINT, data=data)
				json_data = r.json()
				
				# Check for Poipiku specific error codes embedded in HTML or JSON result
				if json_data.get("result", 0) == 1: # 1 usually means success
					resp_html = json_data.get("html", "")
					# Extract URLs from the HTML returned by JSON
					urls = self.IMG_SRC_PATTERN.findall(resp_html)
					if urls:
						if pwd:
							logger.info(f"[{self.name}] Unlocked {post_id} with password.")
						return urls
				
				# If DetailF fails, sometimes it returns a redirect or error indicating
				# we should try the AppendFile endpoint, but usually only if unlocked.
			except Exception as e:
				logger.debug(f"[{self.name}] Password attempt '{pwd}' failed: {e}")

		# Strategy C: Last ditch attempt at AppendFile if we haven't tried it yet
		# (e.g. if we thought it was locked but regex was wrong)
		if requires_password: 
			res = self._fetch_append_files(user_id, post_id, config.get("password", ""))
			if res: return res

		return []

	def _fetch_append_files(self, user_id, post_id, password) -> List[str]:
		"""
		Hits the /f/ShowAppendFileF.jsp endpoint. 
		Used for pagination within a post or non-password protected lists.
		"""
		data = {
			"UID": user_id,
			"IID": post_id,
			"PAS": password,
			"MD": "0",
			"TWF": "-1",
		}
		try:
			r = self._request("POST", self.APPEND_ENDPOINT, data=data)
			json_data = r.json()
			html_content = json_data.get("html", "")
			return self.IMG_SRC_PATTERN.findall(html_content)
		except Exception:
			return []