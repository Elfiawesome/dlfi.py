/**
 * DLFI Server - Frontend Application with Query System
 */

const App = {
	currentNode: null,
	nodes: [],
	queryResults: [],
	autocompleteTimeout: null,
	
	/**
	 * Initialize the application
	 */
	async init() {
		this.bindEvents();
		this.initQueryInput();
		await this.executeQuery('');  // Load all nodes initially
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
				this.hideAutocomplete();
			}
			
			// Focus search with /
			if (e.key === '/' && document.activeElement.tagName !== 'INPUT') {
				e.preventDefault();
				document.getElementById('queryInput')?.focus();
			}
		});
	},
	
	/**
	 * Initialize query input with autocomplete
	 */
	initQueryInput() {
		const input = document.getElementById('queryInput');
		const autocomplete = document.getElementById('autocompleteDropdown');
		
		if (!input) return;
		
		// Input handler with debounce for autocomplete
		input.addEventListener('input', (e) => {
			clearTimeout(this.autocompleteTimeout);
			this.autocompleteTimeout = setTimeout(() => {
				this.fetchAutocomplete(e.target.value, e.target.selectionStart);
			}, 150);
		});
		
		// Execute query on Enter
		input.addEventListener('keydown', (e) => {
			if (e.key === 'Enter' && !e.shiftKey) {
				e.preventDefault();
				this.hideAutocomplete();
				this.executeQuery(input.value);
			}
			
			// Navigate autocomplete
			if (e.key === 'ArrowDown') {
				e.preventDefault();
				this.navigateAutocomplete(1);
			} else if (e.key === 'ArrowUp') {
				e.preventDefault();
				this.navigateAutocomplete(-1);
			} else if (e.key === 'Tab' && autocomplete && !autocomplete.classList.contains('hidden')) {
				e.preventDefault();
				this.selectAutocomplete();
			} else if (e.key === 'Escape') {
				this.hideAutocomplete();
			}
		});
		
		// Hide autocomplete on blur (with delay for click handling)
		input.addEventListener('blur', () => {
			setTimeout(() => this.hideAutocomplete(), 200);
		});
		
		// Show help button
		document.getElementById('queryHelpBtn')?.addEventListener('click', () => {
			this.showQueryHelp();
		});
	},
	
	/**
	 * Fetch autocomplete suggestions
	 */
	async fetchAutocomplete(query, cursorPos) {
		if (!query) {
			this.hideAutocomplete();
			return;
		}
		
		try {
			const resp = await fetch(`/api/autocomplete?q=${encodeURIComponent(query)}&cursor=${cursorPos}`);
			if (!resp.ok) return;
			
			const data = await resp.json();
			this.showAutocomplete(data.suggestions);
		} catch (e) {
			console.error('Autocomplete failed:', e);
		}
	},
	
	/**
	 * Show autocomplete dropdown
	 */
	showAutocomplete(suggestions) {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (!dropdown || !suggestions.length) {
			this.hideAutocomplete();
			return;
		}
		
		dropdown.innerHTML = suggestions.map((s, i) => `
			<div class="autocomplete-item ${i === 0 ? 'active' : ''}" 
				data-index="${i}"
				data-insert="${this.escapeHtml(s.insert_text)}">
				<span class="autocomplete-text">${this.escapeHtml(s.display)}</span>
				<span class="autocomplete-type">${s.type}</span>
				${s.description ? `<span class="autocomplete-desc">${this.escapeHtml(s.description)}</span>` : ''}
			</div>
		`).join('');
		
		dropdown.classList.remove('hidden');
		
		// Add click handlers
		dropdown.querySelectorAll('.autocomplete-item').forEach(item => {
			item.addEventListener('click', () => {
				this.applyAutocomplete(item.dataset.insert);
			});
		});
	},
	
	/**
	 * Hide autocomplete dropdown
	 */
	hideAutocomplete() {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (dropdown) {
			dropdown.classList.add('hidden');
		}
	},
	
	/**
	 * Navigate autocomplete items
	 */
	navigateAutocomplete(direction) {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (!dropdown || dropdown.classList.contains('hidden')) return;
		
		const items = dropdown.querySelectorAll('.autocomplete-item');
		const active = dropdown.querySelector('.autocomplete-item.active');
		let index = active ? parseInt(active.dataset.index) : -1;
		
		index += direction;
		if (index < 0) index = items.length - 1;
		if (index >= items.length) index = 0;
		
		items.forEach(item => item.classList.remove('active'));
		items[index]?.classList.add('active');
	},
	
	/**
	 * Select current autocomplete item
	 */
	selectAutocomplete() {
		const dropdown = document.getElementById('autocompleteDropdown');
		const active = dropdown?.querySelector('.autocomplete-item.active');
		if (active) {
			this.applyAutocomplete(active.dataset.insert);
		}
	},
	
	/**
	 * Apply autocomplete selection
	 */
	applyAutocomplete(insertText) {
		const input = document.getElementById('queryInput');
		if (!input) return;
		
		const cursorPos = input.selectionStart;
		const value = input.value;
		
		// Find the start of the current token
		let tokenStart = cursorPos;
		while (tokenStart > 0 && !/[\s|()]/.test(value[tokenStart - 1])) {
			tokenStart--;
		}
		
		// Replace current token with insert text
		const newValue = value.slice(0, tokenStart) + insertText + value.slice(cursorPos);
		input.value = newValue;
		
		// Position cursor after inserted text
		const newPos = tokenStart + insertText.length;
		input.setSelectionRange(newPos, newPos);
		input.focus();
		
		this.hideAutocomplete();
	},
	
	/**
	 * Execute a query
	 */
	async executeQuery(query) {
		const resultsContainer = document.getElementById('queryResults');
		const statsContainer = document.getElementById('queryStats');
		
		if (resultsContainer) {
			resultsContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
		}
		
		try {
			const resp = await fetch('/api/query', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ query })
			});
			
			const data = await resp.json();
			
			if (!resp.ok) {
				throw new Error(data.error || 'Query failed');
			}
			
			this.queryResults = data.nodes;
			this.renderQueryResults(data);
			
			if (statsContainer) {
				statsContainer.textContent = `${data.total} results (${data.query_time_ms}ms)`;
			}
			
		} catch (e) {
			console.error('Query failed:', e);
			if (resultsContainer) {
				resultsContainer.innerHTML = `
					<div class="query-error">
						<div class="error-icon">⚠️</div>
						<div class="error-message">${this.escapeHtml(e.message)}</div>
					</div>
				`;
			}
		}
	},
	
	/**
	 * Render query results
	 */
	renderQueryResults(data) {
		const container = document.getElementById('queryResults');
		if (!container) return;
		
		if (!data.nodes.length) {
			container.innerHTML = `
				<div class="empty-message">
					<div class="empty-icon">🔍</div>
					<div class="empty-title">No results found</div>
					<div class="empty-text">Try adjusting your query or browse all items.</div>
				</div>
			`;
			return;
		}
		
		container.innerHTML = `
			<div class="results-list">
				${data.nodes.map(node => this.renderResultItem(node)).join('')}
			</div>
		`;
	},
	
	/**
	 * Render a single result item
	 */
	renderResultItem(node) {
		const icon = node.type === 'VAULT' ? '📁' : '📄';
		const tags = node.tags.slice(0, 3).map(t => `<span class="result-tag">${this.escapeHtml(t)}</span>`).join('');
		const moreTags = node.tags.length > 3 ? `<span class="result-tag-more">+${node.tags.length - 3}</span>` : '';
		
		return `
			<div class="result-item" onclick="App.selectNode('${node.uuid}')">
				<div class="result-icon">${icon}</div>
				<div class="result-content">
					<div class="result-name">${this.escapeHtml(node.name)}</div>
					<div class="result-path">${this.escapeHtml(node.path)}</div>
					${node.tags.length ? `<div class="result-tags">${tags}${moreTags}</div>` : ''}
				</div>
				<div class="result-meta">
					${node.file_count > 0 ? `<span class="result-files">${node.file_count} files</span>` : ''}
					${node.child_count > 0 ? `<span class="result-children">${node.child_count} items</span>` : ''}
				</div>
			</div>
		`;
	},
	
	/**
	 * Show query help modal
	 */
	async showQueryHelp() {
		try {
			const resp = await fetch('/api/query/help');
			const data = await resp.json();
			
			const modal = document.getElementById('queryHelpModal');
			const content = document.getElementById('queryHelpContent');
			
			if (modal && content) {
				content.innerHTML = data.syntax.map(cat => `
					<div class="help-category">
						<h3 class="help-category-title">${this.escapeHtml(cat.category)}</h3>
						<div class="help-items">
							${cat.items.map(item => `
								<div class="help-item">
									<code class="help-syntax">${this.escapeHtml(item.syntax)}</code>
									<span class="help-desc">${this.escapeHtml(item.description)}</span>
								</div>
							`).join('')}
						</div>
					</div>
				`).join('');
				
				modal.classList.remove('hidden');
			}
		} catch (e) {
			console.error('Failed to load query help:', e);
		}
	},
	
	/**
	 * Select and display a node
	 */
	async selectNode(uuid) {
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
		const panel = document.getElementById('detailPanel');
		if (!panel) return;
		
		panel.classList.remove('hidden');
		
		const header = document.getElementById('detailHeader');
		const body = document.getElementById('detailBody');
		
		// Header
		if (header) {
			header.innerHTML = `
				<div class="detail-breadcrumb">${this.escapeHtml(node.path)}</div>
				<h2 class="detail-title">${this.escapeHtml(node.name)}</h2>
				<div class="detail-toolbar">
					<button class="btn btn-sm btn-secondary" onclick="App.closeDetail()">Close</button>
					<button class="btn btn-sm btn-danger" onclick="App.deleteNode()">Delete</button>
				</div>
			`;
		}
		
		// Body
		let html = '';
		
		// Type badge
		html += `<div class="detail-type-badge ${node.type.toLowerCase()}">${node.type}</div>`;
		
		// Metadata
		if (Object.keys(node.metadata).length > 0) {
			html += `
				<div class="panel">
					<div class="panel-header"><span class="panel-title">Metadata</span></div>
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
					<button class="btn btn-sm btn-secondary" onclick="App.showAddTag()">Add</button>
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
		if (node.relationships && node.relationships.length > 0) {
			html += `
				<div class="panel">
					<div class="panel-header"><span class="panel-title">Relationships</span></div>
					<div class="panel-body">
						<div class="rel-list">
							${node.relationships.map(r => `
								<div class="rel-item">
									<span class="rel-type">${this.escapeHtml(r.relation)}</span>
									<span class="rel-arrow">→</span>
									<span class="rel-target" onclick="App.queryPath('${r.target_path}')">${this.escapeHtml(r.target_path)}</span>
								</div>
							`).join('')}
						</div>
					</div>
				</div>
			`;
		}
		
		// Files
		if (node.files && node.files.length > 0) {
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
		
		// Children
		if (node.children && node.children.length > 0) {
			html += `
				<div class="panel">
					<div class="panel-header"><span class="panel-title">Contents (${node.children.length})</span></div>
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
		
		if (body) {
			body.innerHTML = html;
		}
	},
	
	/**
	 * Close detail panel
	 */
	closeDetail() {
		const panel = document.getElementById('detailPanel');
		if (panel) {
			panel.classList.add('hidden');
		}
		this.currentNode = null;
	},
	
	/**
	 * Query by path
	 */
	queryPath(path) {
		const input = document.getElementById('queryInput');
		if (input) {
			input.value = `inside:${path}`;
			this.executeQuery(input.value);
		}
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
			this.executeQuery(document.getElementById('queryInput')?.value || '');
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
			
			this.closeDetail();
			this.executeQuery(document.getElementById('queryInput')?.value || '');
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
		if (str === null || str === undefined) return '';
		const div = document.createElement('div');
		div.textContent = String(str);
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
	if (document.getElementById('queryInput')) {
		App.init();
	}
});