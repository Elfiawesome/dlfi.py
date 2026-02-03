// @ts-nocheck
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
	currentView: 'gallery',
	extractors: [],
	extractorConfigs: {},
	
	// Multi-select
	selectedNodes: new Set(),
	isShiftHeld: false,
	
	/**
	 * Initialize the application
	 */
	async init() {
		this.bindEvents();
		this.initQueryInput();
		this.initMultiSelect();
		await Promise.all([
			this.loadTree(),
			this.executeQuery(''),
			this.loadExtractors()
		]);
	},
	
	/**
	 * Bind global event handlers
	 */
	bindEvents() {
		document.querySelectorAll('.modal-overlay').forEach(overlay => {
			overlay.addEventListener('click', (e) => {
				if (e.target === overlay) {
					overlay.classList.add('hidden');
				}
			});
		});
		
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
		
		document.addEventListener('keydown', (e) => {
			if (e.key === 'Escape') {
				document.querySelectorAll('.modal-overlay').forEach(m => m.classList.add('hidden'));
				this.closeLightbox();
				this.hideAutocomplete();
			}
			
			if (e.key === '/' && document.activeElement.tagName !== 'INPUT' && document.activeElement.tagName !== 'TEXTAREA') {
				e.preventDefault();
				document.getElementById('queryInput')?.focus();
			}
		});
	},
	
	/**
	 * Set view mode (gallery or list)
	 */
	setView(view) {
		this.currentView = view;
		document.querySelectorAll('.view-btn').forEach(btn => {
			btn.classList.toggle('active', btn.dataset.view === view);
		});
		this.renderQueryResults({ nodes: this.queryResults, total: this.queryResults.length });
	},
	
	/**
	 * Initialize query input with autocomplete
	 */
	initQueryInput() {
		const input = document.getElementById('queryInput');
		if (!input) return;
		
		input.addEventListener('focus', () => {
			clearTimeout(this.autocompleteTimeout);
			this.autocompleteTimeout = setTimeout(() => {
				this.fetchAutocomplete(input.value, input.selectionStart);
			}, 50);
		});
		
		input.addEventListener('input', (e) => {
			clearTimeout(this.autocompleteTimeout);
			this.autocompleteTimeout = setTimeout(() => {
				this.fetchAutocomplete(e.target.value, e.target.selectionStart);
			}, 100);
		});
		
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
		
		input.addEventListener('blur', () => {
			setTimeout(() => this.hideAutocomplete(), 150);
		});
		
		document.getElementById('queryHelpBtn')?.addEventListener('click', () => {
			this.showQueryHelp();
		});
	},
	
	async fetchAutocomplete(query, cursorPos) {
		try {
			const params = new URLSearchParams({ q: query || '', cursor: (cursorPos || 0).toString() });
			const resp = await fetch(`/api/autocomplete?${params}`);
			if (!resp.ok) return;
			const data = await resp.json();
			if (data.suggestions && data.suggestions.length > 0) {
				this.showAutocomplete(data.suggestions, query, cursorPos);
			} else {
				this.hideAutocomplete();
			}
		} catch (e) {
			console.error('Autocomplete failed:', e);
		}
	},
	
	showAutocomplete(suggestions, query, cursorPos) {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (!dropdown || !suggestions?.length) {
			this.hideAutocomplete();
			return;
		}
		
		this._autocompleteContext = this._getInsertionContext(query || '', cursorPos || 0);
		this.autocompleteIndex = 0;
		
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
						data-insert="${this.escapeAttr(item.insert_text || item.text)}"
						data-type="${this.escapeAttr(item.type)}">
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
		
		dropdown.querySelectorAll('.autocomplete-item').forEach(item => {
			item.addEventListener('mousedown', (e) => {
				e.preventDefault();
				this.applyAutocomplete(item.dataset.insert, item.dataset.type);
			});
			item.addEventListener('mouseenter', () => {
				dropdown.querySelectorAll('.autocomplete-item').forEach(i => i.classList.remove('active'));
				item.classList.add('active');
				this.autocompleteIndex = parseInt(item.dataset.index);
			});
		});
	},
	
	_getInsertionContext(query, cursorPos) {
		let tokenStart = cursorPos;
		while (tokenStart > 0 && !/[\s|()]/.test(query[tokenStart - 1])) {
			tokenStart--;
		}
		const token = query.slice(tokenStart, cursorPos);
		let operatorPos = -1;
		for (const op of [':', '=']) {
			const pos = token.indexOf(op);
			if (pos !== -1) { operatorPos = pos; break; }
		}
		if (operatorPos !== -1) {
			return {
				replaceStart: tokenStart + operatorPos + 1,
				replaceEnd: cursorPos,
				hasOperator: true,
				prefix: token.slice(0, operatorPos + 1)
			};
		}
		return { replaceStart: tokenStart, replaceEnd: cursorPos, hasOperator: false, prefix: '' };
	},
	
	hideAutocomplete() {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (dropdown) {
			dropdown.classList.add('hidden');
			dropdown.innerHTML = '';
		}
		this.autocompleteIndex = -1;
	},
	
	navigateAutocomplete(direction) {
		const dropdown = document.getElementById('autocompleteDropdown');
		if (!dropdown || dropdown.classList.contains('hidden')) return;
		const items = dropdown.querySelectorAll('.autocomplete-item');
		if (!items.length) return;
		items.forEach(item => item.classList.remove('active'));
		this.autocompleteIndex += direction;
		if (this.autocompleteIndex < 0) this.autocompleteIndex = items.length - 1;
		if (this.autocompleteIndex >= items.length) this.autocompleteIndex = 0;
		const activeItem = items[this.autocompleteIndex];
		if (activeItem) {
			activeItem.classList.add('active');
			activeItem.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
		}
	},
	
	selectAutocomplete() {
		const dropdown = document.getElementById('autocompleteDropdown');
		const active = dropdown?.querySelector('.autocomplete-item.active');
		if (active?.dataset.insert) {
			this.applyAutocomplete(active.dataset.insert, active.dataset.type);
		}
	},
	
	applyAutocomplete(insertText, itemType) {
		const input = document.getElementById('queryInput');
		if (!input || !insertText) return;
		
		const ctx = this._autocompleteContext;
		const value = input.value;
		let newValue, newPos;
		
		if (ctx?.hasOperator) {
			if (insertText.includes(':') || insertText.includes('=')) {
				const tokenStart = ctx.replaceStart - ctx.prefix.length;
				newValue = value.slice(0, tokenStart) + insertText + value.slice(ctx.replaceEnd);
				newPos = tokenStart + insertText.length;
			} else {
				newValue = value.slice(0, ctx.replaceStart) + insertText + value.slice(ctx.replaceEnd);
				newPos = ctx.replaceStart + insertText.length;
			}
		} else if (ctx) {
			newValue = value.slice(0, ctx.replaceStart) + insertText + value.slice(ctx.replaceEnd);
			newPos = ctx.replaceStart + insertText.length;
		} else {
			newValue = insertText;
			newPos = insertText.length;
		}
		
		input.value = newValue;
		input.setSelectionRange(newPos, newPos);
		input.focus();
		this.hideAutocomplete();
		
		if (insertText.endsWith(':') || insertText.endsWith('=')) {
			setTimeout(() => this.fetchAutocomplete(input.value, input.selectionStart), 50);
		}
	},
	
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
	
	async refreshTree() {
		await this.loadTree();
	},
	
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
	
	selectNodeFromTree(uuid) {
		const node = this.treeNodes.find(n => n.uuid === uuid);
		if (!node) return;
		document.querySelectorAll('.tree-item').forEach(el => {
			el.classList.toggle('active', el.dataset.uuid === uuid);
		});
		const input = document.getElementById('queryInput');
		if (input) {
			input.value = `inside:${node.path}`;
			this.executeQuery(input.value);
		}
		this.selectNode(uuid);
	},
	
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
			if (!resp.ok) throw new Error(data.error || 'Query failed');
			
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
		
		if (this.currentView === 'gallery') {
			container.innerHTML = `<div class="gallery-grid">${data.nodes.map(node => this.renderGalleryItem(node)).join('')}</div>`;
		} else {
			container.innerHTML = `<div class="results-list">${data.nodes.map(node => this.renderListItem(node)).join('')}</div>`;
		}
	},
	
	renderGalleryItem(node) {
		const isRecord = node.type === 'RECORD';
		const hasFiles = node.file_count > 0;
		
		let preview = '';
		if (isRecord && hasFiles) {
			preview = `<div class="gallery-preview" data-uuid="${node.uuid}"><div class="loading"><div class="spinner"></div></div></div>`;
			this.loadPreview(node.uuid);
		} else {
			const icon = node.type === 'VAULT' ? '📁' : '📄';
			preview = `<div class="gallery-preview gallery-no-preview"><span class="gallery-icon">${icon}</span></div>`;
		}
		
		const isSelected = this.selectedNodes.has(node.uuid) ? 'selected' : '';
		
		return `
			<div class="gallery-item ${isSelected}" 
				data-uuid="${node.uuid}"
				onclick="App.handleItemClick('${node.uuid}', this, event)"
				oncontextmenu="App.handleItemContextMenu('${node.uuid}', this, event)">
				${preview}
				<div class="gallery-info">
					<div class="gallery-name" title="${this.escapeAttr(node.name)}">${this.escapeHtml(node.name)}</div>
					<div class="gallery-meta">
						${node.file_count > 0 ? `<span>${node.file_count} files</span>` : ''}
						${node.tags.length > 0 ? `<span class="gallery-tag">${this.escapeHtml(node.tags[0])}</span>` : ''}
					</div>
				</div>
			</div>
		`;
	},
	
	async loadPreview(uuid) {
		try {
			const resp = await fetch(`/api/nodes/${uuid}/preview`);
			if (!resp.ok) return;
			const data = await resp.json();
			
			const container = document.querySelector(`.gallery-preview[data-uuid="${uuid}"]`);
			if (!container) return;
			
			if (data.has_preview && data.hash) {
				if (data.is_video) {
					container.innerHTML = `<video src="/api/blobs/${data.hash}" muted loop onmouseenter="this.play()" onmouseleave="this.pause()"></video>`;
				} else {
					container.innerHTML = `<img src="/api/blobs/${data.hash}" alt="" loading="lazy">`;
				}
			} else {
				container.innerHTML = `<span class="gallery-icon">📄</span>`;
				container.classList.add('gallery-no-preview');
			}
		} catch (e) {
			console.error('Failed to load preview:', e);
		}
	},
	
	renderListItem(node) {
		const icon = node.type === 'VAULT' ? '📁' : '📄';
		const tags = node.tags.slice(0, 3).map(t => `<span class="result-tag">${this.escapeHtml(t)}</span>`).join('');
		const moreTags = node.tags.length > 3 ? `<span class="result-tag-more">+${node.tags.length - 3}</span>` : '';
		const isSelected = this.selectedNodes.has(node.uuid) ? 'selected' : '';
		
		return `
			<div class="result-item ${isSelected}"
				data-uuid="${node.uuid}"
				onclick="App.handleItemClick('${node.uuid}', this, event)"
				oncontextmenu="App.handleItemContextMenu('${node.uuid}', this, event)">
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
	
	async selectNode(uuid) {
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
	
	async renderNodeDetail(node) {
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
		
		// Fetch relationship data
		let relData = { outgoing: [], incoming: [] };
		try {
			const relResp = await fetch(`/api/nodes/${node.uuid}/relationships`);
			relData = await relResp.json();
		} catch (e) {
			console.error('Failed to load relationships:', e);
		}
		
		let html = `<div class="detail-type-badge ${node.type.toLowerCase()}">${node.type}</div>`;
		
		// Hero preview
		if (node.files?.length > 0) {
			const firstFile = node.files[0];
			const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(firstFile.ext);
			const isVideo = ['mp4', 'webm', 'mov'].includes(firstFile.ext);
			if (isImage || isVideo) {
				html += `
					<div class="hero-preview" onclick="App.openFile('${firstFile.hash}', '${firstFile.ext}', '${this.escapeAttr(firstFile.name)}')">
						${isImage ? `<img src="/api/blobs/${firstFile.hash}" alt="${this.escapeAttr(firstFile.name)}">` : `<video src="/api/blobs/${firstFile.hash}" controls></video>`}
					</div>
				`;
			}
		}
		
		// Metadata
		html += `
			<div class="panel">
				<div class="panel-header">
					<span class="panel-title">Metadata</span>
					<button class="btn btn-sm btn-secondary" onclick="App.showMetadataEditor()">Edit</button>
				</div>
				<div class="panel-body">
					${Object.keys(node.metadata).length > 0 ? `<div class="meta-tree">${this.renderMetadataTree(node.metadata)}</div>` : '<p class="text-muted">No metadata</p>'}
				</div>
			</div>
		`;
		
		// Tags
		html += `
			<div class="panel">
				<div class="panel-header">
					<span class="panel-title">Tags</span>
					<button class="btn btn-sm btn-secondary" onclick="App.showAddTag()">Add</button>
				</div>
				<div class="panel-body">
					${node.tags.length > 0 ? `<div class="tags">${node.tags.map(t => `<span class="tag">${this.escapeHtml(t)}<span class="tag-remove" onclick="App.removeTag('${this.escapeAttr(t)}')">&times;</span></span>`).join('')}</div>` : '<p class="text-muted">No tags</p>'}
				</div>
			</div>
		`;
		
		// Relationships
		html += `
			<div class="panel">
				<div class="panel-header">
					<span class="panel-title">Relationships</span>
					<button class="btn btn-sm btn-secondary" onclick="App.showAddRelationshipModal()">Add</button>
				</div>
				<div class="panel-body">
		`;
		
		if (relData.outgoing?.length > 0) {
			html += `<div class="rel-section-title">Outgoing</div><div class="rel-list">`;
			for (const r of relData.outgoing) {
				html += `
					<div class="rel-item">
						<span class="rel-type">${this.escapeHtml(r.relation)}</span>
						<span class="rel-arrow">→</span>
						<span class="rel-target" onclick="App.queryPath('${this.escapeAttr(r.target_path)}')">${this.escapeHtml(r.target_path)}</span>
						<div class="rel-actions">
							<button class="rel-remove" onclick="App.removeRelationship('${r.target_uuid}', '${this.escapeAttr(r.relation)}', 'outgoing')" title="Remove">×</button>
						</div>
					</div>
				`;
			}
			html += `</div>`;
		}
		
		if (relData.incoming?.length > 0) {
			html += `<div class="rel-section-title">Incoming</div><div class="rel-list">`;
			for (const r of relData.incoming) {
				html += `
					<div class="rel-item">
						<span class="rel-target" onclick="App.queryPath('${this.escapeAttr(r.source_path)}')">${this.escapeHtml(r.source_path)}</span>
						<span class="rel-arrow">→</span>
						<span class="rel-type">${this.escapeHtml(r.relation)}</span>
						<span class="rel-direction">(incoming)</span>
						<div class="rel-actions">
							<button class="rel-remove" onclick="App.removeRelationship('${r.source_uuid}', '${this.escapeAttr(r.relation)}', 'incoming')" title="Remove">×</button>
						</div>
					</div>
				`;
			}
			html += `</div>`;
		}
		
		if (!relData.outgoing?.length && !relData.incoming?.length) {
			html += `<p class="text-muted">No relationships</p>`;
		}
		
		html += `</div></div>`;
		
		// Files
		if (node.files?.length > 0) {
			const firstFile = node.files[0];
			const isFirstPreviewable = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'mp4', 'webm', 'mov'].includes(firstFile.ext);
			const filesToShow = isFirstPreviewable ? node.files.slice(1) : node.files;
			
			html += `
				<div class="panel">
					<div class="panel-header">
						<span class="panel-title">Files (${node.files.length})</span>
						<button class="btn btn-sm btn-secondary" onclick="App.showUploadFile()">Upload</button>
					</div>
					<div class="panel-body">
						${filesToShow.length > 0 ? `<div class="files-grid">${filesToShow.map(f => this.renderFileCard(f)).join('')}</div>` : (isFirstPreviewable ? '<p class="text-muted">No additional files</p>' : '')}
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
					<div class="panel-body"><p class="text-muted">No files attached</p></div>
				</div>
			`;
		}
		
		// Children
		if (node.children?.length > 0) {
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
		
		if (body) body.innerHTML = html;
	},
	
	renderMetadataTree(obj, depth = 0) {
		if (obj === null || obj === undefined) return '<span class="meta-null">null</span>';
		if (typeof obj !== 'object') {
			if (typeof obj === 'string') return `<span class="meta-string">"${this.escapeHtml(obj)}"</span>`;
			if (typeof obj === 'number') return `<span class="meta-number">${obj}</span>`;
			if (typeof obj === 'boolean') return `<span class="meta-boolean">${obj}</span>`;
			return `<span class="meta-value">${this.escapeHtml(String(obj))}</span>`;
		}
		if (Array.isArray(obj)) {
			if (obj.length === 0) return '<span class="meta-empty">[]</span>';
			return `<div class="meta-array">${obj.map((item, i) => `<div class="meta-array-item" style="padding-left: ${depth * 16}px"><span class="meta-index">[${i}]</span>${this.renderMetadataTree(item, depth + 1)}</div>`).join('')}</div>`;
		}
		const keys = Object.keys(obj);
		if (keys.length === 0) return '<span class="meta-empty">{}</span>';
		return `<div class="meta-object">${keys.map(key => `<div class="meta-property" style="padding-left: ${depth * 16}px"><span class="meta-key">${this.escapeHtml(key)}:</span>${this.renderMetadataTree(obj[key], depth + 1)}</div>`).join('')}</div>`;
	},
	
	showMetadataEditor() {
		if (!this.currentNode) return;
		const modal = document.getElementById('metadataEditorModal');
		const textarea = document.getElementById('metadataJsonEditor');
		if (modal && textarea) {
			textarea.value = JSON.stringify(this.currentNode.metadata, null, 2);
			document.getElementById('metadataEditorError').classList.add('hidden');
			modal.classList.remove('hidden');
			textarea.focus();
		}
	},
	
	async saveMetadata() {
		if (!this.currentNode) return;
		const textarea = document.getElementById('metadataJsonEditor');
		const errorDiv = document.getElementById('metadataEditorError');
		
		let metadata;
		try {
			metadata = JSON.parse(textarea.value);
			if (typeof metadata !== 'object' || Array.isArray(metadata)) throw new Error('Metadata must be a JSON object');
		} catch (e) {
			errorDiv.textContent = `Invalid JSON: ${e.message}`;
			errorDiv.classList.remove('hidden');
			return;
		}
		
		try {
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ metadata })
			});
			if (!resp.ok) {
				const data = await resp.json();
				throw new Error(data.error || 'Failed to save metadata');
			}
			document.getElementById('metadataEditorModal').classList.add('hidden');
			await this.selectNode(this.currentNode.uuid);
			await this.loadTree();
		} catch (e) {
			errorDiv.textContent = e.message;
			errorDiv.classList.remove('hidden');
		}
	},
	
	closeDetail() {
		document.getElementById('detailPanel')?.classList.add('hidden');
		this.currentNode = null;
	},
	
	queryPath(path) {
		const input = document.getElementById('queryInput');
		if (input) {
			input.value = `inside:${path}`;
			this.executeQuery(input.value);
		}
	},
	
	renderFileCard(file) {
		const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(file.ext);
		const isVideo = ['mp4', 'webm', 'mov'].includes(file.ext);
		let preview = `<span class="file-icon">📎</span>`;
		if (isImage) preview = `<img src="/api/blobs/${file.hash}/thumbnail" alt="" loading="lazy" onerror="this.parentElement.innerHTML='<span class=file-icon>🖼️</span>'">`;
		else if (isVideo) preview = `<span class="file-icon">🎬</span>`;
		
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
	
	openFile(hash, ext, name) {
		const isImage = ['jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'].includes(ext);
		const isVideo = ['mp4', 'webm', 'mov'].includes(ext);
		if (isImage || isVideo) {
			const lightbox = document.getElementById('lightbox');
			const content = document.getElementById('lightboxContent');
			const url = `/api/blobs/${hash}`;
			content.innerHTML = isImage ? `<img src="${url}" alt="${this.escapeAttr(name)}">` : `<video src="${url}" controls autoplay></video>`;
			lightbox.classList.remove('hidden');
		} else {
			const a = document.createElement('a');
			a.href = `/api/blobs/${hash}`;
			a.download = name;
			a.click();
		}
	},
	
	closeLightbox() {
		const lightbox = document.getElementById('lightbox');
		if (lightbox) {
			lightbox.classList.add('hidden');
			document.getElementById('lightboxContent').innerHTML = '';
		}
	},
	
	showCreateNode(type = 'RECORD') {
		const modal = document.getElementById('createNodeModal');
		if (modal) {
			document.getElementById('createNodeType').value = type;
			document.getElementById('createNodePath').value = this.currentNode ? this.currentNode.path + '/' : '';
			modal.classList.remove('hidden');
			document.getElementById('createNodePath').focus();
		}
	},
	
	async createNode() {
		const path = document.getElementById('createNodePath').value.trim();
		const type = document.getElementById('createNodeType').value;
		if (!path) { this.showError('Path is required'); return; }
		
		try {
			const resp = await fetch('/api/nodes', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ path, type })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error || 'Failed to create node');
			
			document.getElementById('createNodeModal').classList.add('hidden');
			await this.loadTree();
			await this.executeQuery(document.getElementById('queryInput')?.value || '');
			this.selectNode(data.uuid);
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	showAddTag() {
		const modal = document.getElementById('addTagModal');
		if (modal) {
			document.getElementById('newTagInput').value = '';
			modal.classList.remove('hidden');
			document.getElementById('newTagInput').focus();
		}
	},
	
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
			if (!resp.ok) throw new Error((await resp.json()).error || 'Failed to add tag');
			document.getElementById('addTagModal').classList.add('hidden');
			await this.selectNode(this.currentNode.uuid);
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	async removeTag(tag) {
		if (!this.currentNode) return;
		try {
			const tags = this.currentNode.tags.filter(t => t !== tag);
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}`, {
				method: 'PUT',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ tags })
			});
			if (!resp.ok) throw new Error((await resp.json()).error || 'Failed to remove tag');
			await this.selectNode(this.currentNode.uuid);
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	showUploadFile() {
		if (!this.currentNode) { this.showError('Select a record first'); return; }
		if (this.currentNode.type !== 'RECORD') { this.showError('Can only upload files to records'); return; }
		
		const input = document.createElement('input');
		input.type = 'file';
		input.multiple = true;
		input.addEventListener('change', async () => {
			if (!input.files.length) return;
			for (const file of input.files) {
				try {
					const formData = new FormData();
					formData.append('file', file);
					const resp = await fetch(`/api/nodes/${this.currentNode.uuid}/files`, { method: 'POST', body: formData });
					if (!resp.ok) throw new Error((await resp.json()).error || 'Upload failed');
				} catch (e) {
					this.showError(`Failed to upload ${file.name}: ${e.message}`);
				}
			}
			await this.loadTree();
			await this.selectNode(this.currentNode.uuid);
		});
		input.click();
	},
	
	async deleteNode() {
		if (!this.currentNode) return;
		if (!confirm(`Delete "${this.currentNode.name}" and all its contents?`)) return;
		
		try {
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}`, { method: 'DELETE' });
			if (!resp.ok) throw new Error((await resp.json()).error || 'Failed to delete');
			this.closeDetail();
			await this.loadTree();
			await this.executeQuery(document.getElementById('queryInput')?.value || '');
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	async exportStatic() {
		try {
			const resp = await fetch('/api/export', { method: 'POST' });
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error || 'Export failed');
			alert('Static site exported successfully to vault root.');
		} catch (e) {
			this.showError(e.message);
		}
	},
	
	// ========== SETTINGS ==========
	
	async showSettingsModal() {
		const modal = document.getElementById('settingsModal');
		modal.classList.remove('hidden');
		this.hideSettingsMessages();
		this.hideChangePassword();
		this.hideDisableEncryption();
		
		try {
			const resp = await fetch('/api/settings');
			const data = await resp.json();
			
			const statusDiv = document.getElementById('encryptionStatus');
			const enableDiv = document.getElementById('encryptionEnable');
			const manageDiv = document.getElementById('encryptionManage');
			const partitionInput = document.getElementById('partitionSizeMb');
			
			if (data.encrypted) {
				statusDiv.innerHTML = '<span class="status-badge encrypted">🔒 Encrypted</span>';
				enableDiv.classList.add('hidden');
				manageDiv.classList.remove('hidden');
			} else {
				statusDiv.innerHTML = '<span class="status-badge">🔓 Not Encrypted</span>';
				enableDiv.classList.remove('hidden');
				manageDiv.classList.add('hidden');
			}
			
			partitionInput.value = data.partition_size_mb || 0;
		} catch (e) {
			this.showSettingsError(e.message);
		}
	},
	
	closeSettingsModal() {
		document.getElementById('settingsModal').classList.add('hidden');
	},
	
	hideSettingsMessages() {
		document.getElementById('settingsError').classList.add('hidden');
		document.getElementById('settingsSuccess').classList.add('hidden');
	},
	
	showSettingsError(msg) {
		const el = document.getElementById('settingsError');
		el.textContent = msg;
		el.classList.remove('hidden');
	},
	
	showSettingsSuccess(msg) {
		const el = document.getElementById('settingsSuccess');
		el.textContent = msg;
		el.classList.remove('hidden');
	},
	
	async enableEncryption() {
		this.hideSettingsMessages();
		const pass = document.getElementById('enableEncryptionPassword').value;
		const confirm = document.getElementById('enableEncryptionConfirm').value;
		
		if (!pass) { this.showSettingsError('Password is required'); return; }
		if (pass !== confirm) { this.showSettingsError('Passwords do not match'); return; }
		
		try {
			const resp = await fetch('/api/settings/encryption', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ action: 'enable', new_password: pass })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showSettingsSuccess(data.message);
			setTimeout(() => this.showSettingsModal(), 1000);
		} catch (e) {
			this.showSettingsError(e.message);
		}
	},
	
	showChangePassword() {
		document.getElementById('changePasswordForm').classList.remove('hidden');
		document.getElementById('disableEncryptionForm').classList.add('hidden');
	},
	
	hideChangePassword() {
		document.getElementById('changePasswordForm').classList.add('hidden');
		document.getElementById('currentPassword').value = '';
		document.getElementById('newPassword').value = '';
		document.getElementById('confirmNewPassword').value = '';
	},
	
	async changePassword() {
		this.hideSettingsMessages();
		const current = document.getElementById('currentPassword').value;
		const newPass = document.getElementById('newPassword').value;
		const confirm = document.getElementById('confirmNewPassword').value;
		
		if (!current || !newPass) { this.showSettingsError('All fields are required'); return; }
		if (newPass !== confirm) { this.showSettingsError('New passwords do not match'); return; }
		
		try {
			const resp = await fetch('/api/settings/encryption', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ action: 'change_password', current_password: current, new_password: newPass })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showSettingsSuccess(data.message);
			this.hideChangePassword();
		} catch (e) {
			this.showSettingsError(e.message);
		}
	},
	
	showDisableEncryption() {
		document.getElementById('disableEncryptionForm').classList.remove('hidden');
		document.getElementById('changePasswordForm').classList.add('hidden');
	},
	
	hideDisableEncryption() {
		document.getElementById('disableEncryptionForm').classList.add('hidden');
		document.getElementById('disableEncryptionPassword').value = '';
	},
	
	async disableEncryption() {
		this.hideSettingsMessages();
		const pass = document.getElementById('disableEncryptionPassword').value;
		if (!pass) { this.showSettingsError('Password is required'); return; }
		
		if (!confirm('Are you sure you want to disable encryption? This will decrypt all files.')) return;
		
		try {
			const resp = await fetch('/api/settings/encryption', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ action: 'disable', current_password: pass })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showSettingsSuccess(data.message);
			setTimeout(() => this.showSettingsModal(), 1000);
		} catch (e) {
			this.showSettingsError(e.message);
		}
	},
	
	async updatePartitionSize() {
		this.hideSettingsMessages();
		const sizeMb = parseInt(document.getElementById('partitionSizeMb').value) || 0;
		
		if (sizeMb < 0) { this.showSettingsError('Size cannot be negative'); return; }
		
		try {
			const resp = await fetch('/api/settings/partition', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ size_mb: sizeMb })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showSettingsSuccess(data.message);
		} catch (e) {
			this.showSettingsError(e.message);
		}
	},
	
	// ========== EXTRACTORS ==========
	
	async loadExtractors() {
		try {
			const resp = await fetch('/api/extractors');
			const data = await resp.json();
			this.extractors = data.extractors || [];
			
			const select = document.getElementById('extractorSelect');
			if (select) {
				this.extractors.forEach(ext => {
					const option = document.createElement('option');
					option.value = ext.slug;
					option.textContent = ext.name;
					select.appendChild(option);
				});
			}
		} catch (e) {
			console.error('Failed to load extractors:', e);
		}
	},
	
	showExtractorModal() {
		const modal = document.getElementById('extractorModal');
		document.getElementById('extractorError').classList.add('hidden');
		document.getElementById('extractorSuccess').classList.add('hidden');
		document.getElementById('extractorProgress').classList.add('hidden');
		document.getElementById('extractorUrl').value = '';
		document.getElementById('extractorSelect').value = '';
		document.getElementById('extractorCookies').value = '';
		document.getElementById('extractorConfigSection').classList.add('hidden');
		document.getElementById('runExtractorBtn').disabled = false;
		modal.classList.remove('hidden');
	},
	
	closeExtractorModal() {
		document.getElementById('extractorModal').classList.add('hidden');
	},
	
	async loadExtractorConfig() {
		const slug = document.getElementById('extractorSelect').value;
		const configSection = document.getElementById('extractorConfigSection');
		const configFields = document.getElementById('extractorConfigFields');
		
		if (!slug) {
			configSection.classList.add('hidden');
			return;
		}
		
		try {
			const resp = await fetch(`/api/extractors/${slug}/config`);
			const data = await resp.json();
			
			if (data.config && Object.keys(data.config).length > 0) {
				this.extractorConfigs[slug] = data.config;
				configFields.innerHTML = '';
				
				for (const [key, value] of Object.entries(data.config)) {
					const div = document.createElement('div');
					div.className = 'form-group';
					
					let inputHtml = '';
					if (typeof value === 'boolean') {
						inputHtml = `<select id="extcfg_${key}" class="form-input"><option value="false">No</option><option value="true" ${value ? 'selected' : ''}>Yes</option></select>`;
					} else if (typeof value === 'number') {
						inputHtml = `<input type="number" id="extcfg_${key}" class="form-input" value="${value}">`;
					} else if (Array.isArray(value)) {
						inputHtml = `<input type="text" id="extcfg_${key}" class="form-input" value="${value.join(', ')}" placeholder="Comma-separated values">`;
					} else {
						inputHtml = `<input type="text" id="extcfg_${key}" class="form-input" value="${this.escapeAttr(value || '')}">`;
					}
					
					div.innerHTML = `<label class="form-label">${this.escapeHtml(key)}</label>${inputHtml}`;
					configFields.appendChild(div);
				}
				
				configSection.classList.remove('hidden');
			} else {
				configSection.classList.add('hidden');
			}
		} catch (e) {
			console.error('Failed to load extractor config:', e);
		}
	},
	
	async runExtractor() {
		const url = document.getElementById('extractorUrl').value.trim();
		const slug = document.getElementById('extractorSelect').value;
		const cookies = document.getElementById('extractorCookies').value.trim();
		
		document.getElementById('extractorError').classList.add('hidden');
		document.getElementById('extractorSuccess').classList.add('hidden');
		
		if (!url) {
			document.getElementById('extractorError').textContent = 'URL is required';
			document.getElementById('extractorError').classList.remove('hidden');
			return;
		}
		
		// Build config from form fields
		const config = {};
		const defaultConfig = this.extractorConfigs[slug] || {};
		for (const key of Object.keys(defaultConfig)) {
			const input = document.getElementById(`extcfg_${key}`);
			if (input) {
				let value = input.value;
				if (typeof defaultConfig[key] === 'boolean') {
					value = value === 'true';
				} else if (typeof defaultConfig[key] === 'number') {
					value = parseFloat(value) || 0;
				} else if (Array.isArray(defaultConfig[key])) {
					value = value.split(',').map(s => s.trim()).filter(s => s);
				}
				config[key] = value;
			}
		}
		
		document.getElementById('extractorProgress').classList.remove('hidden');
		document.getElementById('runExtractorBtn').disabled = true;
		
		try {
			const resp = await fetch('/api/extractors/run', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					url,
					config,
					cookies_path: cookies || undefined
				})
			});
			
			const data = await resp.json();
			
			if (!resp.ok) throw new Error(data.error);
			
			let msg = `Created ${data.nodes_created} nodes, added ${data.files_added} files.`;
			if (data.errors?.length > 0) {
				msg += ` ${data.errors.length} errors occurred.`;
			}
			
			document.getElementById('extractorSuccess').textContent = msg;
			document.getElementById('extractorSuccess').classList.remove('hidden');
			
			await this.loadTree();
			await this.executeQuery('');
		} catch (e) {
			document.getElementById('extractorError').textContent = e.message;
			document.getElementById('extractorError').classList.remove('hidden');
		} finally {
			document.getElementById('extractorProgress').classList.add('hidden');
			document.getElementById('runExtractorBtn').disabled = false;
		}
	},
	
	formatSize(bytes) {
		if (!bytes) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
	},
	
	escapeHtml(str) {
		if (str === null || str === undefined) return '';
		const div = document.createElement('div');
		div.textContent = String(str);
		return div.innerHTML;
	},
	
	escapeAttr(str) {
		if (str === null || str === undefined) return '';
		return String(str).replace(/"/g, '&quot;').replace(/'/g, '&#39;');
	},
	
	showError(message) {
		alert('Error: ' + message);
	},

	// ========== MULTI-SELECT ==========

	initMultiSelect() {
		document.addEventListener('keydown', (e) => {
			if (e.key === 'Shift') this.isShiftHeld = true;
		});
		document.addEventListener('keyup', (e) => {
			if (e.key === 'Shift') this.isShiftHeld = false;
		});
		
		// Hide context menu on click elsewhere
		document.addEventListener('click', (e) => {
			if (!e.target.closest('.context-menu')) {
				this.hideContextMenu();
			}
		});
	},

	toggleSelection(uuid, element) {
		if (this.selectedNodes.has(uuid)) {
			this.selectedNodes.delete(uuid);
			element.classList.remove('selected');
		} else {
			this.selectedNodes.add(uuid);
			element.classList.add('selected');
		}
		this.updateSelectionBar();
	},

	clearSelection() {
		this.selectedNodes.clear();
		document.querySelectorAll('.selected').forEach(el => el.classList.remove('selected'));
		this.updateSelectionBar();
	},

	updateSelectionBar() {
		let bar = document.getElementById('selectionBar');
		
		if (this.selectedNodes.size === 0) {
			if (bar) bar.remove();
			return;
		}
		
		if (!bar) {
			bar = document.createElement('div');
			bar.id = 'selectionBar';
			bar.className = 'selection-bar';
			document.body.appendChild(bar);
		}
		
		bar.innerHTML = `
			<span class="selection-count">${this.selectedNodes.size} selected</span>
			<div class="selection-actions">
				<button class="btn btn-sm btn-primary" onclick="App.showBulkEditModal()">Edit Selected</button>
				<button class="btn btn-sm btn-secondary" onclick="App.clearSelection()">Clear</button>
			</div>
		`;
	},

	handleItemClick(uuid, element, event) {
		if (this.isShiftHeld) {
			event.preventDefault();
			event.stopPropagation();
			this.toggleSelection(uuid, element);
		} else if (this.selectedNodes.size > 0) {
			// If there's a selection and not shift-clicking, clear it
			this.clearSelection();
			this.selectNode(uuid);
		} else {
			this.selectNode(uuid);
		}
	},

	handleItemContextMenu(uuid, element, event) {
		event.preventDefault();
		event.stopPropagation();
		
		// If right-clicking on a non-selected item, select only it
		if (!this.selectedNodes.has(uuid)) {
			this.clearSelection();
			this.selectedNodes.add(uuid);
			element.classList.add('selected');
			this.updateSelectionBar();
		}
		
		this.showContextMenu(event.clientX, event.clientY);
	},

	showContextMenu(x, y) {
		const menu = document.getElementById('contextMenu');
		menu.style.left = x + 'px';
		menu.style.top = y + 'px';
		menu.classList.remove('hidden');
		
		// Adjust if off-screen
		const rect = menu.getBoundingClientRect();
		if (rect.right > window.innerWidth) {
			menu.style.left = (x - rect.width) + 'px';
		}
		if (rect.bottom > window.innerHeight) {
			menu.style.top = (y - rect.height) + 'px';
		}
	},

	hideContextMenu() {
		document.getElementById('contextMenu')?.classList.add('hidden');
	},

	contextAction(action) {
		this.hideContextMenu();
		
		if (this.selectedNodes.size === 0) return;
		
		if (action === 'open' && this.selectedNodes.size === 1) {
			const uuid = Array.from(this.selectedNodes)[0];
			this.selectNode(uuid);
		} else if (action === 'addTag') {
			if (this.selectedNodes.size === 1) {
				this.selectNode(Array.from(this.selectedNodes)[0]);
				setTimeout(() => this.showAddTag(), 100);
			} else {
				this.showBulkEditModal();
			}
		} else if (action === 'addRelationship') {
			if (this.selectedNodes.size === 1) {
				this.selectNode(Array.from(this.selectedNodes)[0]);
				setTimeout(() => this.showAddRelationshipModal(), 100);
			} else {
				this.showBulkEditModal();
			}
		} else if (action === 'delete') {
			if (this.selectedNodes.size === 1) {
				this.selectNode(Array.from(this.selectedNodes)[0]);
				setTimeout(() => this.deleteNode(), 100);
			} else {
				this.showBulkEditModal();
			}
		}
	},

	// ========== BULK EDIT ==========

	showBulkEditModal() {
		document.getElementById('bulkEditCount').textContent = this.selectedNodes.size;
		document.getElementById('bulkEditError').classList.add('hidden');
		document.getElementById('bulkEditSuccess').classList.add('hidden');
		document.getElementById('bulkAddTagsInput').value = '';
		document.getElementById('bulkRemoveTagsInput').value = '';
		document.getElementById('bulkRelationType').value = '';
		document.getElementById('bulkRelationTarget').value = '';
		document.getElementById('bulkMetadataInput').value = '';
		
		// Load relation types and paths for datalists
		this.loadRelationshipDataLists('bulk');
		
		document.getElementById('bulkEditModal').classList.remove('hidden');
	},

	closeBulkEditModal() {
		document.getElementById('bulkEditModal').classList.add('hidden');
	},

	showBulkError(msg) {
		const el = document.getElementById('bulkEditError');
		el.textContent = msg;
		el.classList.remove('hidden');
	},

	showBulkSuccess(msg) {
		const el = document.getElementById('bulkEditSuccess');
		el.textContent = msg;
		el.classList.remove('hidden');
	},

	async bulkAddTags() {
		const input = document.getElementById('bulkAddTagsInput').value;
		const tags = input.split(',').map(t => t.trim().toLowerCase()).filter(t => t);
		
		if (tags.length === 0) {
			this.showBulkError('Enter at least one tag');
			return;
		}
		
		try {
			const resp = await fetch('/api/bulk/tags', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ uuids: Array.from(this.selectedNodes), tags })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showBulkSuccess(`Added ${tags.length} tag(s) to ${data.count} items`);
			await this.refreshAfterBulk();
		} catch (e) {
			this.showBulkError(e.message);
		}
	},

	async bulkRemoveTags() {
		const input = document.getElementById('bulkRemoveTagsInput').value;
		const tags = input.split(',').map(t => t.trim().toLowerCase()).filter(t => t);
		
		if (tags.length === 0) {
			this.showBulkError('Enter at least one tag');
			return;
		}
		
		try {
			const resp = await fetch('/api/bulk/tags', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ uuids: Array.from(this.selectedNodes), tags })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showBulkSuccess(`Removed ${tags.length} tag(s) from ${data.count} items`);
			await this.refreshAfterBulk();
		} catch (e) {
			this.showBulkError(e.message);
		}
	},

	async bulkAddRelationship() {
		const relation = document.getElementById('bulkRelationType').value.trim().toUpperCase();
		const targetPath = document.getElementById('bulkRelationTarget').value.trim();
		
		if (!relation || !targetPath) {
			this.showBulkError('Relationship type and target path required');
			return;
		}
		
		try {
			const resp = await fetch('/api/bulk/relationships', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					source_uuids: Array.from(this.selectedNodes),
					target_path: targetPath,
					relation
				})
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showBulkSuccess(`Added relationship to ${data.count} items`);
			await this.refreshAfterBulk();
		} catch (e) {
			this.showBulkError(e.message);
		}
	},

	async bulkAddMetadata() {
		const input = document.getElementById('bulkMetadataInput').value.trim();
		
		let metadata;
		try {
			metadata = JSON.parse(input);
			if (typeof metadata !== 'object' || Array.isArray(metadata)) {
				throw new Error('Must be a JSON object');
			}
		} catch (e) {
			this.showBulkError(`Invalid JSON: ${e.message}`);
			return;
		}
		
		try {
			const resp = await fetch('/api/bulk/metadata', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ uuids: Array.from(this.selectedNodes), metadata })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			this.showBulkSuccess(`Merged metadata into ${data.count} items`);
			await this.refreshAfterBulk();
		} catch (e) {
			this.showBulkError(e.message);
		}
	},

	async bulkDelete() {
		if (!confirm(`Delete ${this.selectedNodes.size} items? This cannot be undone.`)) {
			return;
		}
		
		try {
			const resp = await fetch('/api/bulk/delete', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ uuids: Array.from(this.selectedNodes) })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			
			this.closeBulkEditModal();
			this.clearSelection();
			await this.loadTree();
			await this.executeQuery(document.getElementById('queryInput')?.value || '');
		} catch (e) {
			this.showBulkError(e.message);
		}
	},

	async refreshAfterBulk() {
		await this.loadTree();
		if (this.currentNode) {
			await this.selectNode(this.currentNode.uuid);
		}
	},

	// ========== RELATIONSHIPS ==========

	async showAddRelationshipModal() {
		if (!this.currentNode) return;
		
		document.getElementById('addRelError').classList.add('hidden');
		document.getElementById('relationType').value = '';
		document.getElementById('relationTarget').value = '';
		
		await this.loadRelationshipDataLists('');
		
		document.getElementById('addRelationshipModal').classList.remove('hidden');
	},

	closeAddRelationshipModal() {
		document.getElementById('addRelationshipModal').classList.add('hidden');
	},

	async loadRelationshipDataLists(prefix) {
		try {
			// Load relation types
			const typesResp = await fetch('/api/relationships/types');
			const typesData = await typesResp.json();
			
			const typesList = document.getElementById(prefix ? `${prefix}RelationTypesList` : 'relationTypesList');
			if (typesList) {
				typesList.innerHTML = typesData.types.map(t => `<option value="${this.escapeAttr(t)}">`).join('');
			}
			
			// Load paths
			const pathsList = document.getElementById(prefix ? `${prefix}RelationTargetsList` : 'relationTargetsList');
			if (pathsList) {
				const paths = this.treeNodes.map(n => n.path);
				pathsList.innerHTML = paths.map(p => `<option value="${this.escapeAttr(p)}">`).join('');
			}
		} catch (e) {
			console.error('Failed to load relationship datalists:', e);
		}
	},

	async addRelationship() {
		if (!this.currentNode) return;
		
		const relation = document.getElementById('relationType').value.trim().toUpperCase();
		const targetPath = document.getElementById('relationTarget').value.trim();
		const errorEl = document.getElementById('addRelError');
		
		if (!relation || !targetPath) {
			errorEl.textContent = 'Relationship type and target path are required';
			errorEl.classList.remove('hidden');
			return;
		}
		
		try {
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}/relationships`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ target_path: targetPath, relation })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			
			this.closeAddRelationshipModal();
			await this.selectNode(this.currentNode.uuid);
		} catch (e) {
			errorEl.textContent = e.message;
			errorEl.classList.remove('hidden');
		}
	},

	async removeRelationship(targetUuid, relation, direction) {
		if (!this.currentNode) return;
		
		if (!confirm(`Remove this relationship?`)) return;
		
		try {
			const resp = await fetch(`/api/nodes/${this.currentNode.uuid}/relationships`, {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ target_uuid: targetUuid, relation, direction })
			});
			const data = await resp.json();
			if (!resp.ok) throw new Error(data.error);
			
			await this.selectNode(this.currentNode.uuid);
		} catch (e) {
			this.showError(e.message);
		}
	},
};

document.addEventListener('DOMContentLoaded', () => {
	if (document.getElementById('queryInput')) {
		App.init();
	}
});