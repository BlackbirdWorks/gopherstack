<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import {
		Upload, Download, Trash2, RefreshCw, Search,
		File as FileIcon, Info, ChevronUp, Film, Copy, Check,
		BarChart2, Edit2, X
	} from 'lucide-svelte';

	type MsdObject = {
		path: string;
		contentType: string;
		cacheControl: string;
		storageClass: string;
		etag: string;
		sha256: string;
		contentLength: number;
		lastModified: number;
	};

	type SortField = 'path' | 'contentLength' | 'lastModified';
	type SortDir = 'asc' | 'desc';

	let loading = $state(false);
	let objects = $state<MsdObject[]>([]);
	let searchQuery = $state('');
	let expandedPath = $state<string | null>(null);
	let sortField = $state<SortField>('path');
	let sortDir = $state<SortDir>('asc');
	let deletingPaths = $state<Set<string>>(new Set());
	let copiedKey = $state<string | null>(null);

	// Stats
	let stats = $state({ objectCount: 0, totalBytes: 0 });

	// Upload form
	let showUpload = $state(false);
	let uploadPath = $state('');
	let uploadContentType = $state('');
	let uploadCacheControl = $state('');
	let uploadStorageClass = $state('TEMPORAL');
	let uploading = $state(false);
	// Typed as globalThis.File to avoid collision with lucide FileIcon import.
	let uploadFileBrowser = $state<globalThis.File | null>(null);

	// Metadata edit
	let editingPath = $state<string | null>(null);
	let editContentType = $state('');
	let editCacheControl = $state('');
	let saving = $state(false);

	const filteredObjects = $derived(
		[...objects]
			.filter(o =>
				o.path.toLowerCase().includes(searchQuery.toLowerCase()) ||
				o.contentType.toLowerCase().includes(searchQuery.toLowerCase())
			)
			.toSorted((a, b) => {
				let cmp = 0;
				if (sortField === 'path') cmp = a.path.localeCompare(b.path);
				else if (sortField === 'contentLength') cmp = a.contentLength - b.contentLength;
				else cmp = a.lastModified - b.lastModified;
				return sortDir === 'asc' ? cmp : -cmp;
			})
	);

	const totalSize = $derived(objects.reduce((s, o) => s + o.contentLength, 0));

	async function loadObjects() {
		loading = true;
		try {
			const [objRes, statsRes] = await Promise.all([
				fetch('/dashboard/api/mediastoredata/objects'),
				fetch('/dashboard/api/mediastoredata/stats')
			]);

			if (!objRes.ok) throw new Error(`status ${objRes.status}`);
			const data = await objRes.json() as { objects?: MsdObject[] };
			objects = data.objects ?? [];

			if (statsRes.ok) {
				stats = await statsRes.json() as typeof stats;
			}
		} catch (err: unknown) {
			toast.error(`Failed to load objects: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function uploadObject() {
		if (!uploadFileBrowser) { toast.error('Select a file to upload'); return; }
		if (!uploadPath.trim()) { toast.error('Object path is required'); return; }

		uploading = true;
		try {
			const form = new FormData();
			form.append('path', uploadPath.trim());
			form.append('file', uploadFileBrowser);
			if (uploadContentType.trim()) form.append('content_type', uploadContentType.trim());
			if (uploadCacheControl.trim()) form.append('cache_control', uploadCacheControl.trim());
			if (uploadStorageClass) form.append('storage_class', uploadStorageClass);

			const res = await fetch('/dashboard/api/mediastoredata/upload', { method: 'POST', body: form });
			if (!res.ok) {
				const err = await res.json().catch(() => ({ error: `status ${res.status}` })) as { error?: string };
				throw new Error(err.error ?? `status ${res.status}`);
			}

			toast.success(`Uploaded ${uploadPath.trim()}`);
			uploadPath = '';
			uploadFileBrowser = null;
			uploadContentType = '';
			uploadCacheControl = '';
			uploadStorageClass = 'TEMPORAL';
			showUpload = false;
			await loadObjects();
		} catch (err: unknown) {
			toast.error(`Upload failed: ${(err as Error).message}`);
		} finally {
			uploading = false;
		}
	}

	async function downloadObject(obj: MsdObject) {
		try {
			const res = await fetch(`/dashboard/api/mediastoredata/download?path=${encodeURIComponent(obj.path)}`);
			if (!res.ok) throw new Error(`status ${res.status}`);
			const blob = await res.blob();
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = obj.path.split('/').pop() ?? obj.path;
			a.click();
			URL.revokeObjectURL(url);
		} catch (err: unknown) {
			toast.error(`Download failed: ${(err as Error).message}`);
		}
	}

	async function deleteObject(obj: MsdObject) {
		const confirmed = await confirmDestructive({ message: `Delete object "${obj.path}"?`, dangerous: true });
		if (!confirmed) return;

		deletingPaths = new Set([...deletingPaths, obj.path]);
		try {
			const res = await fetch(`/dashboard/api/mediastoredata/objects?path=${encodeURIComponent(obj.path)}`, { method: 'DELETE' });
			if (!res.ok) {
				const err = await res.json().catch(() => ({ error: `status ${res.status}` })) as { error?: string };
				throw new Error(err.error ?? `status ${res.status}`);
			}
			toast.success(`Deleted ${obj.path}`);
			await loadObjects();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		} finally {
			deletingPaths = new Set([...deletingPaths].filter(p => p !== obj.path));
		}
	}

	function startEdit(obj: MsdObject) {
		editingPath = obj.path;
		editContentType = obj.contentType;
		editCacheControl = obj.cacheControl;
	}

	async function saveMetadata() {
		if (!editingPath) return;
		saving = true;
		try {
			const res = await fetch(`/dashboard/api/mediastoredata/objects?path=${encodeURIComponent(editingPath)}`, {
				method: 'PATCH',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ contentType: editContentType, cacheControl: editCacheControl })
			});
			if (!res.ok) throw new Error(`status ${res.status}`);
			toast.success('Metadata updated');
			editingPath = null;
			await loadObjects();
		} catch (err: unknown) {
			toast.error(`Save failed: ${(err as Error).message}`);
		} finally {
			saving = false;
		}
	}

	async function copyToClipboard(value: string, key: string) {
		try {
			await navigator.clipboard.writeText(value);
			copiedKey = key;
			setTimeout(() => { copiedKey = null; }, 1500);
		} catch {
			toast.error('Copy failed');
		}
	}

	function toggleSort(field: SortField) {
		if (sortField === field) {
			sortDir = sortDir === 'asc' ? 'desc' : 'asc';
		} else {
			sortField = field;
			sortDir = 'asc';
		}
	}

	function formatBytes(n: number): string {
		if (n < 1024) return `${n} B`;
		if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
		return `${(n / 1024 / 1024).toFixed(2)} MB`;
	}

	function formatDate(unix: number): string {
		return new Date(unix * 1000).toLocaleString();
	}

	function handleFileChange(e: Event) {
		const input = e.target as HTMLInputElement;
		uploadFileBrowser = input.files?.[0] ?? null;
		if (uploadFileBrowser) {
			if (!uploadContentType) uploadContentType = uploadFileBrowser.type || 'application/octet-stream';
			if (!uploadPath.trim()) uploadPath = '/' + uploadFileBrowser.name;
		}
	}

	onMount(() => { void loadObjects(); });
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Film class="w-7 h-7 text-blue-700 dark:text-blue-300" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">MediaStore Data</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Object browser — upload, download, and inspect media objects</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => (showUpload = !showUpload)}
				class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium"
			>
				<Upload class="w-4 h-4" /> Upload
			</button>
			<button
				onclick={loadObjects}
				title="Refresh"
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- Stats bar -->
	<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3 flex items-center gap-3">
			<BarChart2 class="w-5 h-5 text-blue-500 shrink-0" />
			<div>
				<p class="text-xs text-gray-500 dark:text-gray-400">Objects</p>
				<p class="text-lg font-bold text-gray-900 dark:text-white">{stats.objectCount}</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3 flex items-center gap-3">
			<FileIcon class="w-5 h-5 text-green-500 shrink-0" />
			<div>
				<p class="text-xs text-gray-500 dark:text-gray-400">Total Size</p>
				<p class="text-lg font-bold text-gray-900 dark:text-white">{formatBytes(totalSize)}</p>
			</div>
		</div>
	</div>

	<!-- Upload panel -->
	{#if showUpload}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-blue-200 dark:border-blue-700 p-5 space-y-4">
			<h2 class="text-base font-semibold text-gray-900 dark:text-white flex items-center gap-2">
				<Upload class="w-4 h-4 text-blue-600" /> Upload Object
			</h2>
			<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
				<div>
					<label for="upload-file" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">File *</label>
					<input
						id="upload-file"
						type="file"
						onchange={handleFileChange}
						class="w-full text-sm text-gray-700 dark:text-gray-300 file:mr-3 file:py-1 file:px-3 file:rounded file:border-0 file:bg-blue-50 dark:file:bg-blue-900/30 file:text-blue-700 dark:file:text-blue-300 file:text-sm"
					/>
				</div>
				<div>
					<label for="upload-path" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Object Path *</label>
					<input
						id="upload-path"
						bind:value={uploadPath}
						placeholder="/folder/filename.mp4"
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="upload-ct" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Content-Type</label>
					<input
						id="upload-ct"
						bind:value={uploadContentType}
						placeholder="video/mp4"
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="upload-cc" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Cache-Control</label>
					<input
						id="upload-cc"
						bind:value={uploadCacheControl}
						placeholder="max-age=3600"
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="upload-sc" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Storage Class</label>
					<select
						id="upload-sc"
						bind:value={uploadStorageClass}
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					>
						<option value="TEMPORAL">TEMPORAL</option>
						<option value="STANDARD">STANDARD</option>
					</select>
				</div>
			</div>
			<div class="flex justify-end gap-2">
				<button
					onclick={() => (showUpload = false)}
					class="px-3 py-2 text-sm rounded-lg border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700"
				>
					Cancel
				</button>
				<button
					onclick={uploadObject}
					disabled={uploading}
					class="px-4 py-2 text-sm rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-medium disabled:opacity-50"
				>
					{uploading ? 'Uploading...' : 'Upload'}
				</button>
			</div>
		</div>
	{/if}

	<!-- Objects list -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-wrap justify-between items-center gap-2">
			<div class="flex items-center gap-3">
				<h2 class="text-base font-semibold text-gray-900 dark:text-white">
					Objects {#if objects.length > 0}<span class="text-gray-400 font-normal text-sm">({filteredObjects.length}/{objects.length})</span>{/if}
				</h2>
				<!-- Sort controls -->
				<div class="flex items-center gap-1 text-xs">
					{#each ([['path', 'Name'], ['contentLength', 'Size'], ['lastModified', 'Date']] as const) as [field, label]}
						<button
							onclick={() => toggleSort(field as SortField)}
							class="px-2 py-1 rounded {sortField === field ? 'bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300 font-medium' : 'text-gray-500 hover:bg-gray-100 dark:hover:bg-gray-700'}"
						>
							{label}{sortField === field ? (sortDir === 'asc' ? ' ↑' : ' ↓') : ''}
						</button>
					{/each}
				</div>
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input
					bind:value={searchQuery}
					placeholder="Search by path or type..."
					class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64"
				/>
			</div>
		</div>

		<div class="p-4">
			{#if loading && objects.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400 text-sm">Loading objects...</div>
			{:else if filteredObjects.length === 0}
				<div class="text-center py-10 text-gray-500 dark:text-gray-400">
					<FileIcon class="w-10 h-10 mx-auto mb-3 opacity-40" />
					<p class="text-sm">{searchQuery ? 'No objects match your search.' : 'No objects stored. Upload one to get started.'}</p>
				</div>
			{:else}
				<div class="space-y-2">
					{#each filteredObjects as obj (obj.path)}
						<div class="rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
							<!-- Object row -->
							<div class="flex items-center justify-between p-3 gap-3">
								<div class="flex items-center gap-2 min-w-0 flex-1">
									<FileIcon class="w-4 h-4 shrink-0 text-blue-500" />
									<div class="min-w-0">
										<p class="font-medium text-sm text-gray-900 dark:text-white truncate font-mono">{obj.path}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
											{obj.contentType || 'unknown'} · {formatBytes(obj.contentLength)} · {formatDate(obj.lastModified)}
											{#if obj.storageClass}<span class="ml-1 text-[10px] bg-slate-100 dark:bg-slate-700 px-1.5 py-0.5 rounded font-mono">{obj.storageClass}</span>{/if}
										</p>
									</div>
								</div>
								<div class="flex items-center gap-1 shrink-0">
									<button
										onclick={() => (expandedPath = expandedPath === obj.path ? null : obj.path)}
										title="Details"
										class="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 dark:text-gray-400"
									>
										{#if expandedPath === obj.path}
											<ChevronUp class="w-4 h-4" />
										{:else}
											<Info class="w-4 h-4" />
										{/if}
									</button>
									<button
										onclick={() => startEdit(obj)}
										title="Edit metadata"
										class="p-1.5 rounded hover:bg-yellow-50 dark:hover:bg-yellow-900/30 text-yellow-600 dark:text-yellow-400"
									>
										<Edit2 class="w-4 h-4" />
									</button>
									<button
										onclick={() => downloadObject(obj)}
										title="Download"
										class="p-1.5 rounded hover:bg-blue-50 dark:hover:bg-blue-900/30 text-blue-600 dark:text-blue-400"
									>
										<Download class="w-4 h-4" />
									</button>
									<button
										onclick={() => deleteObject(obj)}
										disabled={deletingPaths.has(obj.path)}
										title="Delete"
										class="p-1.5 rounded hover:bg-red-50 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400 disabled:opacity-40"
									>
										<Trash2 class="w-4 h-4 {deletingPaths.has(obj.path) ? 'animate-pulse' : ''}" />
									</button>
								</div>
							</div>

							<!-- Inline metadata editor -->
							{#if editingPath === obj.path}
								<div class="border-t border-blue-200 dark:border-blue-700 bg-blue-50 dark:bg-blue-900/20 p-4 space-y-3">
									<h3 class="text-xs font-semibold text-blue-700 dark:text-blue-300 uppercase tracking-wide">Edit Metadata</h3>
									<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
										<div>
											<label for="edit-ct-{obj.path}" class="block text-xs text-gray-600 dark:text-gray-400 mb-1">Content-Type</label>
											<input
												id="edit-ct-{obj.path}"
												bind:value={editContentType}
												class="w-full rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-2 py-1.5 text-sm text-gray-900 dark:text-white"
											/>
										</div>
										<div>
											<label for="edit-cc-{obj.path}" class="block text-xs text-gray-600 dark:text-gray-400 mb-1">Cache-Control</label>
											<input
												id="edit-cc-{obj.path}"
												bind:value={editCacheControl}
												class="w-full rounded border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-2 py-1.5 text-sm text-gray-900 dark:text-white"
											/>
										</div>
									</div>
									<div class="flex gap-2">
										<button onclick={saveMetadata} disabled={saving}
											class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
											{saving ? 'Saving...' : 'Save'}
										</button>
										<button onclick={() => (editingPath = null)}
											class="px-3 py-1.5 text-sm rounded border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700">
											<X class="w-4 h-4 inline" />
										</button>
									</div>
								</div>
							{/if}

							<!-- Expanded metadata -->
							{#if expandedPath === obj.path}
								<div class="border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/50 p-4">
									<div class="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-3 text-sm">
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">ETag</span>
											<div class="flex items-center gap-1 mt-0.5">
												<p class="font-mono text-gray-800 dark:text-gray-200 break-all text-xs flex-1">{obj.etag}</p>
												<button
													onclick={() => copyToClipboard(obj.etag, obj.path + ':etag')}
													class="shrink-0 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
													title="Copy ETag"
												>
													{#if copiedKey === obj.path + ':etag'}
														<Check class="w-3.5 h-3.5 text-green-500" />
													{:else}
														<Copy class="w-3.5 h-3.5" />
													{/if}
												</button>
											</div>
										</div>
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">SHA-256</span>
											<div class="flex items-center gap-1 mt-0.5">
												<p class="font-mono text-gray-800 dark:text-gray-200 break-all text-xs flex-1">{obj.sha256}</p>
												<button
													onclick={() => copyToClipboard(obj.sha256, obj.path + ':sha')}
													class="shrink-0 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
													title="Copy SHA-256"
												>
													{#if copiedKey === obj.path + ':sha'}
														<Check class="w-3.5 h-3.5 text-green-500" />
													{:else}
														<Copy class="w-3.5 h-3.5" />
													{/if}
												</button>
											</div>
										</div>
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Storage Class</span>
											<p class="text-gray-800 dark:text-gray-200">{obj.storageClass || '—'}</p>
										</div>
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Cache-Control</span>
											<p class="text-gray-800 dark:text-gray-200">{obj.cacheControl || '—'}</p>
										</div>
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Content-Length</span>
											<p class="text-gray-800 dark:text-gray-200">{obj.contentLength} bytes ({formatBytes(obj.contentLength)})</p>
										</div>
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">Last Modified</span>
											<p class="text-gray-800 dark:text-gray-200">{formatDate(obj.lastModified)}</p>
										</div>
									</div>
								</div>
							{/if}
						</div>
					{/each}
				</div>
			{/if}
		</div>
	</div>
</div>
