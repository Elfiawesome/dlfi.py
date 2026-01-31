import json
import os
from pathlib import Path
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


class StaticSiteGenerator:
	"""Generates static HTML site for viewing the archive."""
	
	def __init__(self, dlfi_instance):
		self.dlfi = dlfi_instance
	
	def generate(self):
		"""Generate all static site files."""
		logger.info("Generating static site...")
		
		# Generate manifest
		manifest = self._build_manifest()
		self._write_manifest(manifest)
		
		# Generate index.html
		self._write_index_html()
		
		logger.info("Static site generation complete")
	
	def _build_manifest(self) -> dict:
		"""Build the complete manifest with all nodes and relationships."""
		manifest = {
			"version": 2,
			"encrypted": self.dlfi.config.encrypted,
			"nodes": {},
			"blobs": {}
		}
		
		if self.dlfi.config.encrypted:
			manifest["crypto"] = self.dlfi.crypto.get_config_for_static()
		
		# Build UUID -> Path map
		uuid_to_path = {}
		cursor = self.dlfi.conn.execute("SELECT uuid, cached_path FROM nodes")
		for row in cursor:
			uuid_to_path[row[0]] = row[1]
		
		# Fetch all nodes with their data
		nodes_cursor = self.dlfi.conn.execute(
			"SELECT uuid, type, name, cached_path, metadata, parent_uuid FROM nodes"
		)
		
		for n_uuid, n_type, n_name, n_path, n_meta, n_parent in nodes_cursor:
			node_data = {
				"uuid": n_uuid,
				"type": n_type,
				"name": n_name,
				"path": n_path,
				"parent": n_parent,
				"metadata": json.loads(n_meta) if n_meta else {}
			}
			
			# Fetch tags
			tags_cur = self.dlfi.conn.execute(
				"SELECT tag FROM tags WHERE node_uuid = ?", (n_uuid,)
			)
			node_data["tags"] = [r[0] for r in tags_cur]
			
			# Fetch relationships
			rels = []
			edges_cur = self.dlfi.conn.execute(
				"SELECT target_uuid, relation FROM edges WHERE source_uuid = ?", (n_uuid,)
			)
			for tgt_uuid, rel_name in edges_cur:
				tgt_path = uuid_to_path.get(tgt_uuid, "UNKNOWN")
				rels.append({"relation": rel_name, "target": tgt_path})
			node_data["relationships"] = rels
			
			# Fetch files
			files = []
			files_cur = self.dlfi.conn.execute("""
				SELECT nf.original_name, nf.file_hash, b.size_bytes, b.ext
				FROM node_files nf
				JOIN blobs b ON nf.file_hash = b.hash
				WHERE nf.node_uuid = ?
				ORDER BY nf.display_order
			""", (n_uuid,))
			
			for orig_name, file_hash, size_bytes, ext in files_cur:
				files.append({
					"name": orig_name,
					"hash": file_hash,
					"size": size_bytes,
					"ext": ext
				})
			node_data["files"] = files
			
			manifest["nodes"][n_uuid] = node_data
		
		# Blob partition info
		blobs_cursor = self.dlfi.conn.execute(
			"SELECT hash, size_bytes, ext FROM blobs"
		)
		for b_hash, b_size, b_ext in blobs_cursor:
			# Check if partitioned
			from .partition import FilePartitioner
			parts = FilePartitioner.get_part_files(self.dlfi.storage_dir, b_hash)
			
			manifest["blobs"][b_hash] = {
				"size": b_size,
				"ext": b_ext,
				"parts": len(parts) if len(parts) > 1 else 0
			}
		
		return manifest
	
	def _write_manifest(self, manifest: dict):
		"""Write manifest to file (encrypted if vault is encrypted)."""
		manifest_json = json.dumps(manifest, indent=2, ensure_ascii=False)
		manifest_path = self.dlfi.root / "manifest.json"
		
		if self.dlfi.crypto.enabled:
			encrypted = self.dlfi.crypto.encrypt(manifest_json.encode('utf-8'))
			with open(manifest_path, 'wb') as f:
				f.write(encrypted)
			logger.debug(f"Wrote encrypted manifest to {manifest_path}")
		else:
			with open(manifest_path, 'w', encoding='utf-8') as f:
				f.write(manifest_json)
			logger.debug(f"Wrote manifest to {manifest_path}")
	
	def _write_index_html(self):
		"""Generate the static HTML viewer."""
		html_path = self.dlfi.root / "index.html"
		
		# Check encryption config for embedding in HTML
		crypto_config = ""
		if self.dlfi.config.encrypted:
			crypto_config = json.dumps(self.dlfi.crypto.get_config_for_static())
		
		html_content = self._get_index_html_template(
			encrypted=self.dlfi.config.encrypted,
			crypto_config=crypto_config
		)
		
		with open(html_path, 'w', encoding='utf-8') as f:
			f.write(html_content)
		
		logger.info(f"Generated index.html at {html_path}")
	
	def _get_index_html_template(self, encrypted: bool, crypto_config: str) -> str:
		"""Return the complete HTML template for the static viewer."""
		return '''<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="UTF-8">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
	<title>DLFI Archive Viewer</title>
	<style>
		* {
			margin: 0;
			padding: 0;
			box-sizing: border-box;
		}
		
		:root {
			--bg-primary: #0a0a0a;
			--bg-secondary: #141414;
			--bg-tertiary: #1a1a1a;
			--text-primary: #ffffff;
			--text-secondary: #a0a0a0;
			--accent: #3b82f6;
			--accent-hover: #2563eb;
			--border: #2a2a2a;
			--success: #22c55e;
			--error: #ef4444;
		}
		
		body {
			font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, sans-serif;
			background: var(--bg-primary);
			color: var(--text-primary);
			min-height: 100vh;
			line-height: 1.6;
		}
		
		.container {
			max-width: 1400px;
			margin: 0 auto;
			padding: 20px;
		}
		
		header {
			background: var(--bg-secondary);
			border-bottom: 1px solid var(--border);
			padding: 16px 0;
			margin-bottom: 24px;
		}
		
		header h1 {
			font-size: 1.5rem;
			font-weight: 600;
			letter-spacing: -0.02em;
		}
		
		.header-content {
			display: flex;
			justify-content: space-between;
			align-items: center;
		}
		
		.status {
			display: flex;
			align-items: center;
			gap: 8px;
			font-size: 0.875rem;
			color: var(--text-secondary);
		}
		
		.status-dot {
			width: 8px;
			height: 8px;
			background: var(--success);
		}
		
		.status-dot.encrypted {
			background: var(--accent);
		}
		
		.status-dot.error {
			background: var(--error);
		}
		
		/* Password Modal */
		.modal-overlay {
			position: fixed;
			inset: 0;
			background: rgba(0, 0, 0, 0.8);
			display: flex;
			align-items: center;
			justify-content: center;
			z-index: 1000;
		}
		
		.modal {
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			padding: 32px;
			width: 100%;
			max-width: 400px;
		}
		
		.modal h2 {
			font-size: 1.25rem;
			margin-bottom: 8px;
		}
		
		.modal p {
			color: var(--text-secondary);
			font-size: 0.875rem;
			margin-bottom: 24px;
		}
		
		.form-group {
			margin-bottom: 16px;
		}
		
		.form-group label {
			display: block;
			font-size: 0.875rem;
			color: var(--text-secondary);
			margin-bottom: 6px;
		}
		
		.form-group input {
			width: 100%;
			padding: 12px;
			background: var(--bg-tertiary);
			border: 1px solid var(--border);
			color: var(--text-primary);
			font-size: 1rem;
			outline: none;
			transition: border-color 0.2s;
		}
		
		.form-group input:focus {
			border-color: var(--accent);
		}
		
		.btn {
			padding: 12px 24px;
			font-size: 0.875rem;
			font-weight: 500;
			border: none;
			cursor: pointer;
			transition: background 0.2s;
		}
		
		.btn-primary {
			background: var(--accent);
			color: white;
			width: 100%;
		}
		
		.btn-primary:hover {
			background: var(--accent-hover);
		}
		
		.error-text {
			color: var(--error);
			font-size: 0.875rem;
			margin-top: 12px;
		}
		
		/* Main Layout */
		.layout {
			display: grid;
			grid-template-columns: 300px 1fr;
			gap: 24px;
			min-height: calc(100vh - 140px);
		}
		
		@media (max-width: 768px) {
			.layout {
				grid-template-columns: 1fr;
			}
		}
		
		/* Sidebar */
		.sidebar {
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			overflow: hidden;
			display: flex;
			flex-direction: column;
		}
		
		.sidebar-header {
			padding: 16px;
			border-bottom: 1px solid var(--border);
			font-weight: 500;
		}
		
		.tree {
			flex: 1;
			overflow-y: auto;
			padding: 8px 0;
		}
		
		.tree-item {
			display: flex;
			align-items: center;
			padding: 8px 16px;
			cursor: pointer;
			transition: background 0.15s;
			gap: 8px;
		}
		
		.tree-item:hover {
			background: var(--bg-tertiary);
		}
		
		.tree-item.active {
			background: var(--accent);
		}
		
		.tree-item.vault {
			font-weight: 500;
		}
		
		.tree-item.record {
			padding-left: 32px;
			color: var(--text-secondary);
		}
		
		.tree-icon {
			width: 16px;
			height: 16px;
			flex-shrink: 0;
		}
		
		/* Content Panel */
		.content {
			background: var(--bg-secondary);
			border: 1px solid var(--border);
			overflow: hidden;
			display: flex;
			flex-direction: column;
		}
		
		.content-header {
			padding: 16px 24px;
			border-bottom: 1px solid var(--border);
		}
		
		.breadcrumb {
			font-size: 0.875rem;
			color: var(--text-secondary);
			margin-bottom: 4px;
		}
		
		.content-title {
			font-size: 1.25rem;
			font-weight: 600;
		}
		
		.content-body {
			flex: 1;
			overflow-y: auto;
			padding: 24px;
		}
		
		/* Metadata Section */
		.section {
			margin-bottom: 32px;
		}
		
		.section-title {
			font-size: 0.75rem;
			text-transform: uppercase;
			letter-spacing: 0.05em;
			color: var(--text-secondary);
			margin-bottom: 12px;
		}
		
		.meta-grid {
			display: grid;
			grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
			gap: 12px;
		}
		
		.meta-item {
			background: var(--bg-tertiary);
			padding: 12px;
		}
		
		.meta-label {
			font-size: 0.75rem;
			color: var(--text-secondary);
			margin-bottom: 4px;
		}
		
		.meta-value {
			font-size: 0.875rem;
			word-break: break-word;
		}
		
		/* Tags */
		.tags {
			display: flex;
			flex-wrap: wrap;
			gap: 8px;
		}
		
		.tag {
			background: var(--bg-tertiary);
			padding: 4px 12px;
			font-size: 0.75rem;
			color: var(--text-secondary);
		}
		
		/* Relationships */
		.rel-list {
			display: flex;
			flex-direction: column;
			gap: 8px;
		}
		
		.rel-item {
			display: flex;
			align-items: center;
			gap: 12px;
			padding: 12px;
			background: var(--bg-tertiary);
		}
		
		.rel-type {
			font-size: 0.75rem;
			text-transform: uppercase;
			color: var(--accent);
			font-weight: 500;
		}
		
		.rel-target {
			font-size: 0.875rem;
			color: var(--text-secondary);
		}
		
		/* Files Grid */
		.files-grid {
			display: grid;
			grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
			gap: 16px;
		}
		
		.file-card {
			background: var(--bg-tertiary);
			border: 1px solid var(--border);
			overflow: hidden;
			cursor: pointer;
			transition: border-color 0.2s;
		}
		
		.file-card:hover {
			border-color: var(--accent);
		}
		
		.file-preview {
			aspect-ratio: 1;
			background: var(--bg-primary);
			display: flex;
			align-items: center;
			justify-content: center;
			overflow: hidden;
		}
		
		.file-preview img {
			width: 100%;
			height: 100%;
			object-fit: cover;
		}
		
		.file-preview video {
			width: 100%;
			height: 100%;
			object-fit: contain;
		}
		
		.file-icon {
			font-size: 2rem;
			color: var(--text-secondary);
		}
		
		.file-info {
			padding: 12px;
		}
		
		.file-name {
			font-size: 0.875rem;
			white-space: nowrap;
			overflow: hidden;
			text-overflow: ellipsis;
			margin-bottom: 4px;
		}
		
		.file-size {
			font-size: 0.75rem;
			color: var(--text-secondary);
		}
		
		/* Empty State */
		.empty-state {
			display: flex;
			flex-direction: column;
			align-items: center;
			justify-content: center;
			height: 100%;
			color: var(--text-secondary);
			text-align: center;
			padding: 40px;
		}
		
		.empty-state h3 {
			font-size: 1.25rem;
			margin-bottom: 8px;
			color: var(--text-primary);
		}
		
		/* Loading */
		.loading {
			display: flex;
			align-items: center;
			justify-content: center;
			height: 100%;
		}
		
		.spinner {
			width: 32px;
			height: 32px;
			border: 3px solid var(--border);
			border-top-color: var(--accent);
			animation: spin 1s linear infinite;
		}
		
		@keyframes spin {
			to { transform: rotate(360deg); }
		}
		
		/* Lightbox */
		.lightbox {
			position: fixed;
			inset: 0;
			background: rgba(0, 0, 0, 0.95);
			display: flex;
			align-items: center;
			justify-content: center;
			z-index: 2000;
			padding: 40px;
		}
		
		.lightbox-content {
			max-width: 100%;
			max-height: 100%;
		}
		
		.lightbox-content img,
		.lightbox-content video {
			max-width: 100%;
			max-height: calc(100vh - 80px);
			object-fit: contain;
		}
		
		.lightbox-close {
			position: absolute;
			top: 20px;
			right: 20px;
			background: none;
			border: none;
			color: white;
			font-size: 2rem;
			cursor: pointer;
			padding: 8px;
			line-height: 1;
		}
		
		.hidden {
			display: none !important;
		}
	</style>
</head>
<body>
	<!-- Password Modal -->
	<div id="passwordModal" class="modal-overlay ''' + ('hidden' if not encrypted else '') + '''">
		<div class="modal">
			<h2>Encrypted Archive</h2>
			<p>This archive is encrypted. Enter the password to view its contents.</p>
			<div class="form-group">
				<label for="password">Password</label>
				<input type="password" id="password" placeholder="Enter password" autocomplete="off">
			</div>
			<button class="btn btn-primary" id="unlockBtn">Unlock Archive</button>
			<div id="passwordError" class="error-text hidden"></div>
		</div>
	</div>
	
	<header>
		<div class="container">
			<div class="header-content">
				<h1>DLFI Archive</h1>
				<div class="status">
					<div class="status-dot''' + (' encrypted' if encrypted else '') + '''" id="statusDot"></div>
					<span id="statusText">''' + ('Encrypted' if encrypted else 'Ready') + '''</span>
				</div>
			</div>
		</div>
	</header>
	
	<main class="container">
		<div id="loadingState" class="loading">
			<div class="spinner"></div>
		</div>
		
		<div id="mainContent" class="layout hidden">
			<aside class="sidebar">
				<div class="sidebar-header">Archive Structure</div>
				<div class="tree" id="treeView"></div>
			</aside>
			
			<section class="content">
				<div id="contentHeader" class="content-header hidden">
					<div class="breadcrumb" id="breadcrumb"></div>
					<h2 class="content-title" id="contentTitle"></h2>
				</div>
				<div class="content-body" id="contentBody">
					<div class="empty-state">
						<h3>Select an item</h3>
						<p>Choose a vault or record from the sidebar to view its details.</p>
					</div>
				</div>
			</section>
		</div>
	</main>
	
	<!-- Lightbox -->
	<div id="lightbox" class="lightbox hidden">
		<button class="lightbox-close" id="lightboxClose">&times;</button>
		<div class="lightbox-content" id="lightboxContent"></div>
	</div>
	
	<script>
		const CONFIG = {
			encrypted: ''' + ('true' if encrypted else 'false') + ''',
			crypto: ''' + (crypto_config if crypto_config else 'null') + '''
		};
		
		let manifest = null;
		let cryptoKey = null;
		
		// Utility functions
		function formatSize(bytes) {
			if (bytes === 0) return '0 B';
			const k = 1024;
			const sizes = ['B', 'KB', 'MB', 'GB'];
			const i = Math.floor(Math.log(bytes) / Math.log(k));
			return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
		}
		
		function getBlobPath(hash) {
			const a = hash.substring(0, 2);
			const b = hash.substring(2, 4);
			return `blobs/${a}/${b}/${hash}`;
		}
		
		// Crypto functions
		async function deriveKey(password) {
			const encoder = new TextEncoder();
			const salt = Uint8Array.from(atob(CONFIG.crypto.salt), c => c.charCodeAt(0));
			
			const keyMaterial = await crypto.subtle.importKey(
				'raw',
				encoder.encode(password),
				'PBKDF2',
				false,
				['deriveKey']
			);
			
			return crypto.subtle.deriveKey(
				{
					name: 'PBKDF2',
					salt: salt,
					iterations: CONFIG.crypto.iterations,
					hash: 'SHA-256'
				},
				keyMaterial,
				{ name: 'AES-GCM', length: 256 },
				false,
				['decrypt']
			);
		}
		
		async function decryptData(encryptedData, key) {
			const nonce = encryptedData.slice(0, CONFIG.crypto.nonceLength);
			const ciphertext = encryptedData.slice(CONFIG.crypto.nonceLength);
			
			return crypto.subtle.decrypt(
				{ name: 'AES-GCM', iv: nonce },
				key,
				ciphertext
			);
		}
		
		async function decryptBlob(hash, parts) {
			try {
				let data;
				
				if (parts > 0) {
					// Fetch and concatenate parts
					const chunks = [];
					for (let i = 1; i <= parts; i++) {
						const partNum = String(i).padStart(3, '0');
						const resp = await fetch(`${getBlobPath(hash)}.${partNum}`);
						if (!resp.ok) throw new Error(`Failed to fetch part ${i}`);
						chunks.push(await resp.arrayBuffer());
					}
					
					// Combine chunks
					const totalLength = chunks.reduce((acc, c) => acc + c.byteLength, 0);
					data = new Uint8Array(totalLength);
					let offset = 0;
					for (const chunk of chunks) {
						data.set(new Uint8Array(chunk), offset);
						offset += chunk.byteLength;
					}
				} else {
					const resp = await fetch(getBlobPath(hash));
					if (!resp.ok) throw new Error('Failed to fetch blob');
					data = new Uint8Array(await resp.arrayBuffer());
				}
				
				if (CONFIG.encrypted && cryptoKey) {
					const decrypted = await decryptData(data, cryptoKey);
					return new Uint8Array(decrypted);
				}
				
				return data;
			} catch (e) {
				console.error('Failed to decrypt blob:', e);
				return null;
			}
		}
		
		// Load manifest
		async function loadManifest(password = null) {
			try {
				const resp = await fetch('manifest.json');
				if (!resp.ok) throw new Error('Failed to load manifest');
				
				if (CONFIG.encrypted) {
					if (!password) {
						document.getElementById('loadingState').classList.add('hidden');
						document.getElementById('passwordModal').classList.remove('hidden');
						return false;
					}
					
					cryptoKey = await deriveKey(password);
					const encryptedData = new Uint8Array(await resp.arrayBuffer());
					
					try {
						const decrypted = await decryptData(encryptedData, cryptoKey);
						const decoder = new TextDecoder();
						manifest = JSON.parse(decoder.decode(decrypted));
					} catch (e) {
						throw new Error('Invalid password');
					}
				} else {
					manifest = await resp.json();
				}
				
				return true;
			} catch (e) {
				console.error('Failed to load manifest:', e);
				throw e;
			}
		}
		
		// Build tree view
		function buildTree() {
			const tree = document.getElementById('treeView');
			tree.innerHTML = '';
			
			// Group nodes by path hierarchy
			const nodes = Object.values(manifest.nodes);
			const rootNodes = nodes.filter(n => !n.parent);
			
			function renderNode(node, depth = 0) {
				const div = document.createElement('div');
				div.className = `tree-item ${node.type.toLowerCase()}`;
				div.style.paddingLeft = `${16 + depth * 16}px`;
				div.dataset.uuid = node.uuid;
				
				const icon = node.type === 'VAULT' ? '📁' : '📄';
				div.innerHTML = `<span class="tree-icon">${icon}</span> ${node.name}`;
				
				div.addEventListener('click', () => selectNode(node.uuid));
				tree.appendChild(div);
				
				// Render children
				const children = nodes.filter(n => n.parent === node.uuid);
				children.sort((a, b) => a.name.localeCompare(b.name));
				children.forEach(child => renderNode(child, depth + 1));
			}
			
			rootNodes.sort((a, b) => a.name.localeCompare(b.name));
			rootNodes.forEach(node => renderNode(node));
		}
		
		// Select and display node
		async function selectNode(uuid) {
			// Update tree selection
			document.querySelectorAll('.tree-item').forEach(el => {
				el.classList.toggle('active', el.dataset.uuid === uuid);
			});
			
			const node = manifest.nodes[uuid];
			if (!node) return;
			
			// Update header
			document.getElementById('contentHeader').classList.remove('hidden');
			document.getElementById('breadcrumb').textContent = node.path;
			document.getElementById('contentTitle').textContent = node.name;
			
			// Build content
			const body = document.getElementById('contentBody');
			body.innerHTML = '';
			
			// Metadata section
			if (Object.keys(node.metadata).length > 0) {
				const section = document.createElement('div');
				section.className = 'section';
				section.innerHTML = '<div class="section-title">Metadata</div><div class="meta-grid"></div>';
				const grid = section.querySelector('.meta-grid');
				
				for (const [key, value] of Object.entries(node.metadata)) {
					const item = document.createElement('div');
					item.className = 'meta-item';
					item.innerHTML = `
						<div class="meta-label">${key}</div>
						<div class="meta-value">${typeof value === 'object' ? JSON.stringify(value) : value}</div>
					`;
					grid.appendChild(item);
				}
				body.appendChild(section);
			}
			
			// Tags section
			if (node.tags && node.tags.length > 0) {
				const section = document.createElement('div');
				section.className = 'section';
				section.innerHTML = '<div class="section-title">Tags</div><div class="tags"></div>';
				const tags = section.querySelector('.tags');
				
				node.tags.forEach(tag => {
					const span = document.createElement('span');
					span.className = 'tag';
					span.textContent = tag;
					tags.appendChild(span);
				});
				body.appendChild(section);
			}
			
			// Relationships section
			if (node.relationships && node.relationships.length > 0) {
				const section = document.createElement('div');
				section.className = 'section';
				section.innerHTML = '<div class="section-title">Relationships</div><div class="rel-list"></div>';
				const list = section.querySelector('.rel-list');
				
				node.relationships.forEach(rel => {
					const item = document.createElement('div');
					item.className = 'rel-item';
					item.innerHTML = `
						<span class="rel-type">${rel.relation}</span>
						<span class="rel-target">${rel.target}</span>
					`;
					list.appendChild(item);
				});
				body.appendChild(section);
			}
			
			// Files section
			if (node.files && node.files.length > 0) {
				const section = document.createElement('div');
				section.className = 'section';
				section.innerHTML = '<div class="section-title">Files</div><div class="files-grid"></div>';
				const grid = section.querySelector('.files-grid');
				
				for (const file of node.files) {
					const card = document.createElement('div');
					card.className = 'file-card';
					
					const blobInfo = manifest.blobs[file.hash] || { parts: 0 };
					const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(file.ext);
					const isVideo = ['mp4', 'webm', 'mov', 'avi'].includes(file.ext);
					
					card.innerHTML = `
						<div class="file-preview">
							<span class="file-icon">${isImage ? '🖼️' : isVideo ? '🎬' : '📎'}</span>
						</div>
						<div class="file-info">
							<div class="file-name" title="${file.name}">${file.name}</div>
							<div class="file-size">${formatSize(file.size)}</div>
						</div>
					`;
					
					// Load preview for images
					if (isImage || isVideo) {
						loadFilePreview(card, file.hash, blobInfo.parts, isImage, isVideo);
					}
					
					card.addEventListener('click', () => openLightbox(file, blobInfo.parts));
					grid.appendChild(card);
				}
				body.appendChild(section);
			}
			
			// Show children for vaults
			if (node.type === 'VAULT') {
				const children = Object.values(manifest.nodes).filter(n => n.parent === uuid);
				if (children.length > 0) {
					const section = document.createElement('div');
					section.className = 'section';
					section.innerHTML = `<div class="section-title">Contents (${children.length})</div><div class="files-grid"></div>`;
					const grid = section.querySelector('.files-grid');
					
					children.sort((a, b) => a.name.localeCompare(b.name));
					children.forEach(child => {
						const card = document.createElement('div');
						card.className = 'file-card';
						const icon = child.type === 'VAULT' ? '📁' : '📄';
						card.innerHTML = `
							<div class="file-preview">
								<span class="file-icon">${icon}</span>
							</div>
							<div class="file-info">
								<div class="file-name" title="${child.name}">${child.name}</div>
								<div class="file-size">${child.type}</div>
							</div>
						`;
						card.addEventListener('click', () => selectNode(child.uuid));
						grid.appendChild(card);
					});
					body.appendChild(section);
				}
			}
		}
		
		async function loadFilePreview(card, hash, parts, isImage, isVideo) {
			const data = await decryptBlob(hash, parts);
			if (!data) return;
			
			const blob = new Blob([data]);
			const url = URL.createObjectURL(blob);
			const preview = card.querySelector('.file-preview');
			
			if (isImage) {
				preview.innerHTML = `<img src="${url}" alt="preview">`;
			} else if (isVideo) {
				preview.innerHTML = `<video src="${url}" muted></video>`;
			}
		}
		
		async function openLightbox(file, parts) {
			const data = await decryptBlob(file.hash, parts);
			if (!data) {
				alert('Failed to load file');
				return;
			}
			
			const blob = new Blob([data]);
			const url = URL.createObjectURL(blob);
			const content = document.getElementById('lightboxContent');
			
			const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(file.ext);
			const isVideo = ['mp4', 'webm', 'mov', 'avi'].includes(file.ext);
			
			if (isImage) {
				content.innerHTML = `<img src="${url}" alt="${file.name}">`;
			} else if (isVideo) {
				content.innerHTML = `<video src="${url}" controls autoplay></video>`;
			} else {
				// Download for other file types
				const a = document.createElement('a');
				a.href = url;
				a.download = file.name;
				a.click();
				return;
			}
			
			document.getElementById('lightbox').classList.remove('hidden');
		}
		
		// Event listeners
		document.getElementById('unlockBtn').addEventListener('click', async () => {
			const password = document.getElementById('password').value;
			const errorEl = document.getElementById('passwordError');
			
			try {
				const success = await loadManifest(password);
				if (success) {
					document.getElementById('passwordModal').classList.add('hidden');
					document.getElementById('statusText').textContent = 'Decrypted';
					init();
				}
			} catch (e) {
				errorEl.textContent = e.message || 'Failed to decrypt archive';
				errorEl.classList.remove('hidden');
			}
		});
		
		document.getElementById('password').addEventListener('keypress', (e) => {
			if (e.key === 'Enter') {
				document.getElementById('unlockBtn').click();
			}
		});
		
		document.getElementById('lightboxClose').addEventListener('click', () => {
			document.getElementById('lightbox').classList.add('hidden');
			document.getElementById('lightboxContent').innerHTML = '';
		});
		
		document.getElementById('lightbox').addEventListener('click', (e) => {
			if (e.target.id === 'lightbox') {
				document.getElementById('lightbox').classList.add('hidden');
				document.getElementById('lightboxContent').innerHTML = '';
			}
		});
		
		// Initialize
		function init() {
			document.getElementById('loadingState').classList.add('hidden');
			document.getElementById('mainContent').classList.remove('hidden');
			buildTree();
		}
		
		// Start
		(async () => {
			try {
				const success = await loadManifest();
				if (success) {
					init();
				}
			} catch (e) {
				document.getElementById('loadingState').innerHTML = `
					<div class="empty-state">
						<h3>Failed to Load Archive</h3>
						<p>${e.message}</p>
					</div>
				`;
			}
		})();
	</script>
</body>
</html>'''