/**
 * DLFI Server - Frontend Application with Query System
 */

const App = {
	currentNode: null,
	nodes: [],
	treeNodes: [],
	queryResults: [],
	autocompleteTimeout: null,
	autocompleteIndex: -1,
	
	/**
	 * Initialize the application
	 */
	async init() {
		this.bindEvents();
		this.initQueryInput();
		await Promise.all([
			this.loadTree(),
			this.executeQuery('')
		]);
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
			if (e.key === '/' && document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA') {
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
		
		if (!input) return;
		
		// Show autocomplete on focus (even if empty)
		input.addEventListener('focus', () => {
			clearTimeout(this.autocompleteTimeout);
			this.autocompleteTimeout = setTimeout(() => {
				this.fetchAutocomplete(input.value, input.selectionStart);
			}, 50);
		});
		
		// Input handler with debounce for autocomplete
		input.addEventListener('input', (e) => {
			clearTimeout(this.autocompleteTimeout);
			this.autocompleteTimeout = setTimeout(() => {
				this.fetchAutocomplete(e.target.value, e.target.selectionStart);
			}, 100);
		});
		
		// Handle keyboard navigation
		input.addEventListener('keydown', (e) => {
			const dropdown = document.getElementById('autocompleteDropdown');
			const isDropdownVisible = dropdown && !dropdown.classList.contains('hidden');
			
			if (e.key === 'Enter') {
				if (isDropdownVisible && this.autocompleteIndex >= 0) {
					e.preventDefault();
					this.selectAutocomplete();
				} else {
					e.preventDefault();
					this.hideAutocomplete();
					this.executeQuery(input.value);
				}
			} else if (e.key === 'ArrowDown') {
				e.preventDefault();
				if (!isDropdownVisible) {
					this.fetchAutocomplete(input.value, input.selectionStart);
				} else {
					this.navigateAutocomplete(1);
				}
			} else if (e.key === 'ArrowUp') {
				if (isDropdownVisible) {
					e.preventDefault();
					this.navigateAutocomplete(-1);
				}
			} else if (e.key === 'Tab' && isDropdownVisible) {
				e.preventDefault();
				this.selectAutocomplete();
			} else if (e.key === 'Escape') {
				this.hideAutocomplete();
			}
		});
		
		// Hide on blur with delay
		input.addEventListener('blur', () => {
			setTimeout(() => this.hideAutocomplete(), 150);
		});
		
		// Help button
		document.getElementById('queryHelpBtn')?.addEventListener('click', () => {
			this.showQueryHelp();
		});
	},
	
	/**
	 * Fetch autocomplete suggestions
	 */
	async fetchAutocomplete(query, cursorPos) {
		try {
			const params = new URLSearchParams({
				q: query || '',
				cursor: (cursorPos || 0).toString()
			});
			
			const resp = await fetch(`/api/autocomplete?${params}`);
			if (!resp.ok) {
				console.error('Autocomplete request failed:', resp.status);
				return;
			}
			
			const data = await resp.json();
			
			if (data.suggestions && data.suggestions.length > 0) {
				this.showAutocomplete(data.suggestions);
			} else {
				this.hideAutocomplete();
			}
		} catch (e) {
			console.error('Autocomplete failed:', e);
		}
	},
	

	/**
	 * Show autocomplete dropdown
	 */
	showAutocomplete(suggestions) {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (!dropdown) return;
		
		if (!suggestions || !suggestions.length) {
			this.hideAutocomplete();
			return;
		}
		
		this.autocompleteIndex = 0;
		
		// Group by section
		const grouped = {};
		suggestions.forEach(s => {
			const section = s.section || 'Suggestions';
			if (!grouped[section]) grouped[section] = [];
			grouped[section].push(s);
		});
		
		let html = '';
		let globalIndex = 0;
		
		for (const [section, items] of Object.entries(grouped)) {
			html += `<div class="autocomplete-section">${this.escapeHtml(section)}</div>`;
			for (const item of items) {
				const isActive = globalIndex === 0 ? 'active' : '';
				html += `
					<div class="autocomplete-item ${isActive}" 
						data-index="${globalIndex}"
						data-insert="${this.escapeAttr(item.insert_text || item.text)}">
						<span class="autocomplete-text">${this.escapeHtml(item.display || item.text)}</span>
						<span class="autocomplete-type">${this.escapeHtml(item.type)}</span>
						${item.description ? `<span class="autocomplete-desc">${this.escapeHtml(item.description)}</span>` : ''}
					</div>
				`;
				globalIndex++;
			}
		}
		
		dropdown.innerHTML = html;
		dropdown.classList.remove('hidden');
		
		// Add event handlers
		dropdown.querySelectorAll('.autocomplete-item').forEach(item => {
			item.addEventListener('mousedown', (e) => {
				e.preventDefault();
				e.stopPropagation();
				this.applyAutocomplete(item.dataset.insert);
			});
			
			item.addEventListener('mouseenter', () => {
				dropdown.querySelectorAll('.autocomplete-item').forEach(i => i.classList.remove('active'));
				item.classList.add('active');
				this.autocompleteIndex = parseInt(item.dataset.index);
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
			dropdown.innerHTML = '';
		}
		this.autocompleteIndex = -1;
	},
	
	/**
	 * Navigate autocomplete items
	 */
	navigateAutocomplete(direction) {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (!dropdown || dropdown.classList.contains('hidden')) return;
		
		const items = dropdown.querySelectorAll('.autocomplete-item');
		if (!items.length) return;
		
		// Remove current active
		items.forEach(item => item.classList.remove('active'));
		
		// Calculate new index
		this.autocompleteIndex += direction;
		if (this.autocompleteIndex < 0) this.autocompleteIndex = items.length - 1;
		if (this.autocompleteIndex >= items.length) this.autocompleteIndex = 0;
		
		// Set new active
		const activeItem = items[this.autocompleteIndex];
		if (activeItem) {
			activeItem.classList.add('active');
			activeItem.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
		}
	},
	
	/**
	 * Select current autocomplete item
	 */
	selectAutocomplete() {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (!dropdown) return;
		
		const active = dropdown.querySelector('.autocomplete-item.active');
		if (active && active.dataset.insert) {
			this.applyAutocomplete(active.dataset.insert);
		}
	},
	
	/**
	 * Apply autocomplete selection
	 */
	applyAutocomplete(insertText) {
		const input = document.getElementById('queryInput');
		if (!input || !insertText) return;
		
		const cursorPos = input.selectionStart;
		const value = input.value;
		
		// Find the start of the current token
		let tokenStart = cursorPos;
		while (tokenStart > 0 && !/[\s|()]/.test(value[tokenStart - 1])) {
			tokenStart--;
		}
		
		// Replace current token with insert text
		const before = value.slice(0, tokenStart);
		const after = value.slice(cursorPos);
		const newValue = before + insertText + after;
		
		input.value = newValue;
		
		// Position cursor after inserted text
		const newPos = tokenStart + insertText.length;
		input.setSelectionRange(newPos, newPos);
		input.focus();
		
		this.hideAutocomplete();
		
		// Fetch new suggestions if the insert ends with : or we might need more
		if (insertText.endsWith(':') || insertText.endsWith('=')) {
			setTimeout(() => {
				this.fetchAutocomplete(input.value, input.selectionStart);
			}, 50);
		}
	},
	
	/**
	 * Load tree view
	 */
	async loadTree() {
		try {
			const resp = await fetch('/api/nodes');
			if (!resp.ok) throw new Error('Failed to load nodes');
			
			const data = await resp.json();
			this.treeNodes = data.nodes;
			this.renderTree();
		} catch (e) {
			console.error('Failed to load tree:', e);
			document.getElementById('treeView').innerHTML = `<div class="empty-state"><p>Failed to load</p></div>`;
		}
	},
	
	/**
	 * Refresh tree
	 */
	async refreshTree() {
		await this.loadTree();
	},
	
	/**
	 * Render tree view
	 */
	renderTree() {
		const tree = document.getElementById('treeView');
		if (!tree) return;
		
		if (!this.treeNodes.length) {
			tree.innerHTML = `<div class="tree-empty">No items yet</div>`;
			return;
		}
		
		tree.innerHTML = '';
		
		const rootNodes = this.treeNodes.filter(n => !n.parent);
		
		const renderNode = (node, depth = 0) => {
			const div = document.createElement('div');
			div.className = `tree-item ${node.type.toLowerCase()}`;
			div.style.paddingLeft = `${12 + depth * 14}px`;
			div.dataset.uuid = node.uuid;
			
			const icon = node.type === 'VAULT' ? '📁' : '📄';
			const badge = node.file_count > 0 ? `<span class="tree-badge">${node.file_count}</span>` : '';
			
			div.innerHTML = `<span class="tree-icon">${icon}</span><span class="tree-name">${this.escapeHtml(node.name)}</span>${badge}`;
			
			div.addEventListener('click', (e) => {
				e.stopPropagation();
				this.selectNodeFromTree(node.uuid);
			});
			
			tree.appendChild(div);
			
			const children = this.treeNodes.filter(n => n.parent === node.uuid);
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
	 * Select node from tree and update query
	 */
	selectNodeFromTree(uuid) {
		const node = this.treeNodes.find(n => n.uuid === uuid);
		if (!node) return;
		
		// Update tree selection visually
		document.querySelectorAll('.tree-item').forEach(el => {
			el.classList.toggle('active', el.dataset.uuid === uuid);
		});
		
		// Set query to inside:path and execute
		const input = document.getElementById('queryInput');
		if (input) {
			input.value = `inside:${node.path}`;
			this.executeQuery(input.value);
		}
		
		// Also select the node for detail view
		this.selectNode(uuid);
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
					<div class="empty-text">Try adjusting your query or browse the tree.</div>
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
		const panel = document.getElementById('detailPanel');
		if (!panel) return;
		
		panel.classList.remove('hidden');
		
		const header = document.getElementById('detailHeader');
		const body = document.getElementById('detailBody');
		
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
		
		let html = '';
		
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
									<span class="tag-remove" onclick="App.removeTag('${this.escapeAttr(t)}')">&times;</span>
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
									<span class="rel-target" onclick="App.queryPath('${this.escapeAttr(r.target_path)}')">${this.escapeHtml(r.target_path)}</span>
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
						<div class="files-grid files-grid-small">
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
			<div class="file-card" onclick="App.openFile('${file.hash}', '${file.ext}', '${this.escapeAttr(file.name)}')">
				<div class="file-preview">${preview}</div>
				<div class="file-info">
					<div class="file-name" title="${this.escapeAttr(file.name)}">${this.escapeHtml(file.name)}</div>
					<div class="file-size">${this.formatSize(file.size)}</div>
				</div>
			</div>
		`;
	},
	
	/**
	 * Open file in lightbox or download
	 */
	openFile(hash, ext, name) {
		const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(ext);
		const isVideo = ['mp4', 'webm', 'mov'].includes(ext);
		
		if (isImage || isVideo) {
			const lightbox = document.getElementById('lightbox');
			const content = document.getElementById('lightboxContent');
			
			const url = `/api/blobs/${hash}`;
			
			if (isImage) {
				content.innerHTML = `<img src="${url}" alt="${this.escapeAttr(name)}">`;
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
			await this.loadTree();
			await this.executeQuery(document.getElementById('queryInput')?.value || '');
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
		if (!this.currentNode) {
			this.showError('Select a record first');
			return;
		}
		
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
			await this.loadTree();
			await this.executeQuery(document.getElementById('queryInput')?.value || '');
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
	 * Escape for attribute
	 */
	escapeAttr(str) {
		if (str === null || str === undefined) return '';
		return String(str).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
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