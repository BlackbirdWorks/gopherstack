<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import {
		Upload, Download, Trash2, RefreshCw, Search,
		File as FileIcon, Info, ChevronDown, ChevronUp, Film
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

	let loading = $state(false);
	let objects = $state<MsdObject[]>([]);
	let searchQuery = $state('');
	let expandedPath = $state<string | null>(null);

	// Upload form
	let showUpload = $state(false);
	let uploadPath = $state('');
	let uploadContentType = $state('');
	let uploadCacheControl = $state('');
	let uploading = $state(false);
	// Explicitly typed as browser File to avoid collision with lucide FileIcon import.
	let uploadFileBrowser = $state<globalThis.File | null>(null);

	const filteredObjects = $derived(
		objects.filter(o =>
			o.path.toLowerCase().includes(searchQuery.toLowerCase()) ||
			o.contentType.toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadObjects() {
		loading = true;
		try {
			const res = await fetch('/dashboard/api/mediastoredata/objects');
			if (!res.ok) throw new Error(`status ${res.status}`);
			const data = await res.json() as { objects?: MsdObject[] };
			objects = data.objects ?? [];
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
		if (uploadFileBrowser && !uploadContentType) {
			uploadContentType = uploadFileBrowser.type || 'application/octet-stream';
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

	<!-- Upload panel -->
	{#if showUpload}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-blue-200 dark:border-blue-700 p-5 space-y-4">
			<h2 class="text-base font-semibold text-gray-900 dark:text-white flex items-center gap-2">
				<Upload class="w-4 h-4 text-blue-600" /> Upload Object
			</h2>
			<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Object Path *</label>
					<input
						bind:value={uploadPath}
						placeholder="/folder/filename.mp4"
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Content-Type</label>
					<input
						bind:value={uploadContentType}
						placeholder="video/mp4"
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Cache-Control</label>
					<input
						bind:value={uploadCacheControl}
						placeholder="max-age=3600"
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">File *</label>
					<input
						type="file"
						onchange={handleFileChange}
						class="w-full text-sm text-gray-700 dark:text-gray-300 file:mr-3 file:py-1 file:px-3 file:rounded file:border-0 file:bg-blue-50 dark:file:bg-blue-900/30 file:text-blue-700 dark:file:text-blue-300 file:text-sm"
					/>
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
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex justify-between items-center">
			<h2 class="text-base font-semibold text-gray-900 dark:text-white">
				Objects {#if objects.length > 0}<span class="text-gray-400 font-normal text-sm">({objects.length})</span>{/if}
			</h2>
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
										onclick={() => downloadObject(obj)}
										title="Download"
										class="p-1.5 rounded hover:bg-blue-50 dark:hover:bg-blue-900/30 text-blue-600 dark:text-blue-400"
									>
										<Download class="w-4 h-4" />
									</button>
									<button
										onclick={() => deleteObject(obj)}
										title="Delete"
										class="p-1.5 rounded hover:bg-red-50 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>

							<!-- Expanded metadata -->
							{#if expandedPath === obj.path}
								<div class="border-t border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900/50 p-4">
									<div class="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-2 text-sm">
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">ETag</span>
											<p class="font-mono text-gray-800 dark:text-gray-200 break-all">{obj.etag}</p>
										</div>
										<div>
											<span class="text-xs text-gray-500 dark:text-gray-400 uppercase tracking-wide">SHA-256</span>
											<p class="font-mono text-gray-800 dark:text-gray-200 break-all text-xs">{obj.sha256}</p>
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
