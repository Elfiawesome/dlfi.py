/**
 * DLFI Server - Frontend Application
 */

const App = {
	currentNode: null,
	nodes: [],
	
	/**
	 * Initialize the application
	 */
	async init() {
		this.bindEvents();
		await this.loadNodes();
	},
	
	/**
	 * Bind global event handlers
	 */
	bindEvents() {
		// Close modals on overlay click
		document.querySelectorAll('.modal-overlay').forEach(overlay => {
			overlay.addEventListener('click', (e) => {
				if (e.target === overlay) {
					overlay.classList.add('hidden');
				}
			});
		});
		
		// Lightbox
		const lightbox = document.getElementById('lightbox');
		if (lightbox) {
			document.getElementById('lightboxClose')?.addEventListener('click', () => {
				this.closeLightbox();
			});
			lightbox.addEventListener('click', (e) => {
				if (e.target === lightbox) {
					this.closeLightbox();
				}
			});
		}
		
		// Keyboard shortcuts
		document.addEventListener('keydown', (e) => {
			if (e.key === 'Escape') {
				document.querySelectorAll('.modal-overlay').forEach(m => m.classList.add('hidden'));
				this.closeLightbox();
			}
		});
	},
	
	/**
	 * Load all nodes from the API
	 */
	async loadNodes() {
		try {
			const resp = await fetch('/api/nodes');
			if (!resp.ok) throw new Error('Failed to load nodes');
			
			const data = await resp.json();
			this.nodes = data.nodes;
			this.renderTree();
		} catch (e) {
			console.error('Failed to load nodes:', e);
			this.showError('Failed to load vault contents');
		}
	},
	
	/**
	 * Render the tree view
	 */
	renderTree() {
		const tree = document.getElementById('treeView');
		if (!tree) return;
		
		tree.innerHTML = '';
		
		// Build parent map
		const rootNodes = this.nodes.filter(n => !n.parent);
		
		const renderNode = (node, depth = 0) => {
			const div = document.createElement('div');
			div.className = `tree-item ${node.type.toLowerCase()}`;
			div.style.paddingLeft = `${16 + depth * 16}px`;
			div.dataset.uuid = node.uuid;
			
			const icon = node.type === 'VAULT' ? '📁' : '📄';
			
			div.innerHTML = `
				<span class="tree-icon">${icon}</span>
				<span class="tree-name">${this.escapeHtml(node.name)}</span>
				${node.file_count > 0 ? `<span class="tree-badge">${node.file_count}</span>` : ''}
			`;
			
			div.addEventListener('click', () => this.selectNode(node.uuid));
			tree.appendChild(div);
			
			// Render children
			const children = this.nodes.filter(n => n.parent === node.uuid);
			children.sort((a, b) => {
				if (a.type !== b.type) return a.type === 'VAULT' ? -1 : 1;
				return a.name.localeCompare(b.name);
			});
			children.forEach(child => renderNode(child, depth + 1));
		};
		
		rootNodes.sort((a, b) => {
			if (a.type !== b.type) return a.type === 'VAULT' ? -1 : 1;
			return a.name.localeCompare(b.name);
		});
		rootNodes.forEach(node => renderNode(node));
	},
	
	/**
	 * Select and display a node
	 */
	async selectNode(uuid) {
		// Update tree selection
		document.querySelectorAll('.tree-item').forEach(el => {
			el.classList.toggle('active', el.dataset.uuid === uuid);
		});
		
		try {
			const resp = await fetch(`/api/nodes/${uuid}`);
			if (!resp.ok) throw new Error('Failed to load node');
			
			const node = await resp.json();
			this.currentNode = node;
			this.renderNodeDetail(node);
		} catch (e) {
			console.error('Failed to load node:', e);
			this.showError('Failed to load node details');
		}
	},
	
	/**
	 * Render node details
	 */
	renderNodeDetail(node) {
		const header = document.getElementById('contentHeader');
		const body = document.getElementById('contentBody');
		
		if (!header || !body) return;
		
		header.classList.remove('hidden');
		
		// Breadcrumb and title
		document.getElementById('breadcrumb').textContent = node.path;
		document.getElementById('contentTitle').textContent = node.name;
		
		// Build content
		let html = '';
		
		// Metadata
		if (Object.keys(node.metadata).length > 0) {
			html += `
				<div class="panel">
					<div class="panel-header">
						<span class="panel-title">Metadata</span>
						<button class="btn btn-sm btn-secondary" onclick="App.showEditMetadata()">Edit</button>
					</div>
					<div class="panel-body">
						<div class="meta-grid">
							${Object.entries(node.metadata).map(([k, v]) => `
								<div class="meta-item">
									<div class="meta-label">${this.escapeHtml(k)}</div>
									<div class="meta-value">${this.escapeHtml(typeof v === 'object' ? JSON.stringify(v) : String(v))}</div>
								</div>
							`).join('')}
						</div>
					</div>
				</div>
			`;
		}
		
		// Tags
		html += `
			<div class="panel">
				<div class="panel-header">
					<span class="panel-title">Tags</span>
					<button class="btn btn-sm btn-secondary" onclick="App.showAddTag()">Add Tag</button>
				</div>
				<div class="panel-body">
					${node.tags.length > 0 ? `
						<div class="tags">
							${node.tags.map(t => `
								<span class="tag">
									${this.escapeHtml(t)}
									<span class="tag-remove" onclick="App.removeTag('${t}')">&times;</span>
								</span>
							`).join('')}
						</div>
					` : '<p class="text-muted">No tags</p>'}
				</div>
			</div>
		`;
		
		// Relationships
		if (node.relationships.length > 0) {
			html += `
				<div class="panel">
					<div class="panel-header">
						<span class="panel-title">Relationships</span>
					</div>
					<div class="panel-body">
						<div class="rel-list">
							${node.relationships.map(r => `
								<div class="rel-item">
									<span class="rel-type">${this.escapeHtml(r.relation)}</span>
									<span class="rel-arrow">→</span>
									<span class="rel-target" onclick="App.navigateToPath('${r.target_path}')">${this.escapeHtml(r.target_path)}</span>
								</div>
							`).join('')}
						</div>
					</div>
				</div>
			`;
		}
		
		// Files
		if (node.files.length > 0) {
			html += `
				<div class="panel">
					<div class="panel-header">
						<span class="panel-title">Files (${node.files.length})</span>
						<button class="btn btn-sm btn-secondary" onclick="App.showUploadFile()">Upload</button>
					</div>
					<div class="panel-body">
						<div class="files-grid">
							${node.files.map(f => this.renderFileCard(f)).join('')}
						</div>
					</div>
				</div>
			`;
		} else if (node.type === 'RECORD') {
			html += `
				<div class="panel">
					<div class="panel-header">
						<span class="panel-title">Files</span>
						<button class="btn btn-sm btn-secondary" onclick="App.showUploadFile()">Upload</button>
					</div>
					<div class="panel-body">
						<p class="text-muted">No files attached</p>
					</div>
				</div>
			`;
		}
		
		// Children for vaults
		if (node.children.length > 0) {
			html += `
				<div class="panel">
					<div class="panel-header">
						<span class="panel-title">Contents (${node.children.length})</span>
					</div>
					<div class="panel-body">
						<div class="children-list">
							${node.children.map(c => `
								<div class="child-item" onclick="App.selectNode('${c.uuid}')">
									<span class="child-icon">${c.type === 'VAULT' ? '📁' : '📄'}</span>
									<span class="child-name">${this.escapeHtml(c.name)}</span>
									<span class="child-type">${c.type}</span>
								</div>
							`).join('')}
						</div>
					</div>
				</div>
			`;
		}
		
		body.innerHTML = html || `
			<div class="empty-message">
				<div class="empty-icon">📋</div>
				<div class="empty-title">Empty ${node.type.toLowerCase()}</div>
				<div class="empty-text">This ${node.type.toLowerCase()} has no content yet.</div>
			</div>
		`;
	},
	
	/**
	 * Render a file card
	 */
	renderFileCard(file) {
		const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(file.ext);
		const isVideo = ['mp4', 'webm', 'mov'].includes(file.ext);
		
		let preview = `<span class="file-icon">📎</span>`;
		if (isImage) {
			preview = `<img src="/api/blobs/${file.hash}/thumbnail" alt="" loading="lazy" onerror="this.parentElement.innerHTML='<span class=file-icon>🖼️</span>'">`;
		} else if (isVideo) {
			preview = `<span class="file-icon">🎬</span>`;
		}
		
		return `
			<div class="file-card" onclick="App.openFile('${file.hash}', '${file.ext}', '${this.escapeHtml(file.name)}')">
				<div class="file-preview">${preview}</div>
				<div class="file-info">
					<div class="file-name" title="${this.escapeHtml(file.name)}">${this.escapeHtml(file.name)}</div>
					<div class="file-size">${this.formatSize(file.size)}</div>
				</div>
			</div>
		`;
	},
	
	/**
	 * Open file in lightbox or download
	 */
	async openFile(hash, ext, name) {
		const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(ext);
		const isVideo = ['mp4', 'webm', 'mov'].includes(ext);
		
		if (isImage || isVideo) {
			const lightbox = document.getElementById('lightbox');
			const content = document.getElementById('lightboxContent');
			
			const url = `/api/blobs/${hash}`;
			
			if (isImage) {
				content.innerHTML = `<img src="${url}" alt="${this.escapeHtml(name)}">`;
			} else {
				content.innerHTML = `<video src="${url}" controls autoplay></video>`;
			}
			
			lightbox.classList.remove('hidden');
		} else {
			// Download
			const a = document.createElement('a');
			a.href = `/api/blobs/${hash}`;
			a.download = name;
			a.click();
		}
	},
	
	/**
	 * Close lightbox
	 */
	closeLightbox() {
		const lightbox = document.getElementById('lightbox');
		if (lightbox) {
			lightbox.classList.add('hidden');
			document.getElementById('lightboxContent').innerHTML = '';
		}
	},
	
	/**
	 * Navigate to a path
	 */
	navigateToPath(path) {
		const node = this.nodes.find(n => n.path === path);
		if (node) {
			this.selectNode(node.uuid);
		}
	},
	
	/**
	 * Show create node modal
	 */
	showCreateNode(type = 'RECORD') {
		const modal = document.getElementById('createNodeModal');
		if (modal) {
			document.getElementById('createNodeType').value = type;
			document.getElementById('createNodePath').value = this.currentNode ? this.currentNode.path + '/' : '';
			modal.classList.remove('hidden');
			document.getElementById('createNodePath').focus();
		}
	},
	
	/**
	 * Create a new node
	 */
	async createNode() {
		const path = document.getElementById('createNodePath').value.trim();
		const type = document.getElementById('createNodeType').value;
		
		if (!path) {
			this.showError('Path is required');
			return;
		}
		
		try {
			const resp = await fetch('/api/nodes', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ path, type })
			});
			
			const data = await resp.json();
			
			if (!resp.ok) {
				throw new Error(data.error || 'Failed to create node');
			}
			
			document.getElementById('createNodeModal').classList.add('hidden');
			await this.loadNodes();
			this.selectNode(data.uuid);
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	/**
	 * Show add tag modal
	 */
	showAddTag() {
		const modal = document.getElementById('addTagModal');
		if (modal) {
			document.getElementById('newTagInput').value = '';
			modal.classList.remove('hidden');
			document.getElementById('newTagInput').focus();
		}
	},
	
	/**
	 * Add a tag
	 */
	async addTag() {
		const tag = document.getElementById('newTagInput').value.trim().toLowerCase();
		
		if (!tag || !this.currentNode) return;
		
		try {
			const tags = [...this.currentNode.tags, tag];
			
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ tags })
			});
			
			if (!resp.ok) {
				const data = await resp.json();
				throw new Error(data.error || 'Failed to add tag');
			}
			
			document.getElementById('addTagModal').classList.add('hidden');
			await this.selectNode(this.currentNode.uuid);
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	/**
	 * Remove a tag
	 */
	async removeTag(tag) {
		if (!this.currentNode) return;
		
		try {
			const tags = this.currentNode.tags.filter(t => t !== tag);
			
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ tags })
			});
			
			if (!resp.ok) {
				const data = await resp.json();
				throw new Error(data.error || 'Failed to remove tag');
			}
			
			await this.selectNode(this.currentNode.uuid);
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	/**
	 * Show upload file modal
	 */
	showUploadFile() {
		const input = document.createElement('input');
		input.type = 'file';
		input.multiple = true;
		
		input.addEventListener('change', async () => {
			if (!input.files.length || !this.currentNode) return;
			
			for (const file of input.files) {
				try {
					const formData = new FormData();
					formData.append('file', file);
					
					const resp = await fetch(`/api/nodes/${this.currentNode.uuid}/files`, {
						method: 'POST',
						body: formData
					});
					
					if (!resp.ok) {
						const data = await resp.json();
						throw new Error(data.error || 'Upload failed');
					}
				} catch (e) {
					this.showError(`Failed to upload ${file.name}: ${e.message}`);
				}
			}
			
			await this.loadNodes();
			await this.selectNode(this.currentNode.uuid);
		});
		
		input.click();
	},
	
	/**
	 * Delete current node
	 */
	async deleteNode() {
		if (!this.currentNode) return;
		
		if (!confirm(`Delete "${this.currentNode.name}" and all its contents?`)) {
			return;
		}
		
		try {
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}`, {
				method: 'DELETE'
			});
			
			if (!resp.ok) {
				const data = await resp.json();
				throw new Error(data.error || 'Failed to delete');
			}
			
			this.currentNode = null;
			await this.loadNodes();
			
			// Clear content area
			document.getElementById('contentHeader').classList.add('hidden');
			document.getElementById('contentBody').innerHTML = `
				<div class="empty-message">
					<div class="empty-icon">📂</div>
					<div class="empty-title">Select an item</div>
					<div class="empty-text">Choose a vault or record from the sidebar.</div>
				</div>
			`;
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	/**
	 * Export static site
	 */
	async exportStatic() {
		try {
			const resp = await fetch('/api/export', { method: 'POST' });
			const data = await resp.json();
			
			if (!resp.ok) {
				throw new Error(data.error || 'Export failed');
			}
			
			alert('Static site exported successfully to vault root.');
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	/**
	 * Format file size
	 */
	formatSize(bytes) {
		if (!bytes) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
	},
	
	/**
	 * Escape HTML
	 */
	escapeHtml(str) {
		const div = document.createElement('div');
		div.textContent = str;
		return div.innerHTML;
	},
	
	/**
	 * Show error message
	 */
	showError(message) {
		alert('Error: ' + message);
	}
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
	if (document.getElementById('treeView')) {
		App.init();
	}
});