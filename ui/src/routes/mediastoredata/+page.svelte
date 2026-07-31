<script lang="ts">
	// MediaStore Data is the MediaStore *data plane*: PutObject, GetObject,
	// DeleteObject, DescribeObject, ListItems (confirmed exhaustive against
	// services/mediastoredata/handler.go's GetSupportedOperations() and the
	// installed @aws-sdk/client-mediastore-data command list -- both directions
	// clean, no fabricated ops, nothing missing). Unlike the pure query-shaped
	// services handled earlier in this sweep (redshiftdata, sts), this one
	// genuinely creates and deletes objects, so a browser/CRUD-ish shape is
	// closer to right here. But it is still not a resource-management page:
	//
	//   - There is no Update. services/mediastoredata/objects.go DOES have an
	//     UpdateObjectMetadata() backend method (content-type/cache-control
	//     in place, tested in objects_test.go) -- but handler.go's Handler()
	//     dispatch only ever calls handlePutObject/handleGetObject/
	//     handleDeleteObject/handleListItems/handleDescribeObject; nothing
	//     wires UpdateObjectMetadata to any HTTP route, and there is no
	//     UpdateObject-shaped command in the real SDK either. It is dead code
	//     on the wire. The PREVIOUS version of this page had an "Edit
	//     metadata" panel that PATCHed a `/dashboard/api/mediastoredata/objects`
	//     endpoint that does not exist anywhere in this codebase (no matching
	//     +server.ts, confirmed by search) -- so that feature could never have
	//     worked even against the old custom-API architecture, let alone the
	//     real SDK. It is not reintroduced here.
	//   - There is no folder-create op. ListItems' FOLDER entries are
	//     synthesized purely from "/"-nesting in real object Paths (see
	//     services/mediastoredata/items.go: a FOLDER item only ever comes from
	//     `strings.Cut(rest, "/")` finding a nested object) -- there is no
	//     "create empty folder" primitive the way S3's zero-byte
	//     trailing-slash-key trick has a real analogue. A folder appears the
	//     moment an object is uploaded under it, and this page relies on
	//     exactly that (type the nested path directly into the upload form),
	//     rather than adding a "New Folder" button the backend has no op for.
	//
	// So the shape is: a Path-prefix object/folder browser (ListItems drives
	// navigation, matching real MediaStore's folder semantics) with real
	// Upload (PutObject), Download (GetObject), Delete (DeleteObject), and
	// Details/Range-preview (DescribeObject + GetObject Range) -- no forced
	// Update.
	//
	// ---------------------------------------------------------------------
	// Container control-plane connection: checked, and there isn't one
	// ---------------------------------------------------------------------
	// Real AWS MediaStore Data is reached through a *per-container* data
	// endpoint (obtained from mediastore:DescribeContainer) -- a request never
	// carries a container name/ARN as a parameter, the container is implied
	// entirely by which endpoint you send it to. gopherstack does not model
	// per-container endpoints or isolation at all: services/mediastoredata's
	// InMemoryBackend keys objects only by region (see store_setup.go /
	// getRegion), with no notion of a container anywhere in
	// objects.go/items.go/models.go. Grepping services/mediastore (the
	// separate control-plane service that owns Container/CORS/lifecycle/
	// metric policies) for any reference to the mediastoredata package finds
	// nothing, and the reverse search is equally empty -- the two backends
	// are entirely unaware of each other. Concretely: PutObject/GetObject/etc.
	// here work against a single flat per-region object store regardless of
	// whether any container was ever created via the mediastore service, and
	// ContainerNotFoundException (a real, always-present modeled error on
	// every op) can never actually be returned -- confirmed in
	// services/mediastoredata/PARITY.md's gaps section. This is a milder
	// disconnect than appconfigdata's (there the data-plane op 404s until an
	// internal admin endpoint seeds it -- here every real op just works,
	// unconditionally, without ever needing a container). So this page does
	// not pretend to select or require a container; it is honest that it
	// talks to gopherstack's single region-wide object store.
	//
	// ---------------------------------------------------------------------
	// Object bodies, content types, and Range requests -- verified for real
	// ---------------------------------------------------------------------
	// PutObject stores the exact bytes given, computes a real SHA-256
	// (contentSHA256) and quotes it as ETag, and honors Content-Type/
	// Cache-Control headers verbatim (objects.go PutObject). GetObject/
	// DescribeObject return those same Content-Type/Content-Length/
	// Cache-Control/ETag/Last-Modified values back -- both were checked
	// against the real SDK's response shape and match. Range requests are
	// REAL: handler.go's handleRangeGet supports single closed ("bytes=a-b"),
	// suffix ("bytes=-N"), and open ("bytes=N-") ranges, returning 206 +
	// Content-Range on success and 416 (RequestedRangeNotSatisfiableException)
	// when unsatisfiable -- so the Range field below is wired to something
	// real, not decorative. What is NOT real: StorageClass has exactly one
	// legal value on the wire (`TEMPORAL` -- confirmed against the installed
	// SDK's StorageClass enum, which does not include `STANDARD` at all;
	// `STANDARD` is a value of the unrelated UploadAvailability enum, and the
	// PREVIOUS version of this page's Storage Class dropdown conflated the
	// two by offering "STANDARD" as a storage class option). Also: neither
	// GetObjectResponse, DescribeObjectResponse, nor ListItems' Item include
	// StorageClass or UploadAvailability at all (verified against the
	// installed SDK's models_0.d.ts) -- PutObject's response is the ONLY
	// place StorageClass is ever visible again after upload, so this page
	// does not pretend to show a persistent "storage class" column/field
	// anywhere else. Chunked/progressive delivery for STREAMING uploads is
	// also not modeled server-side (PARITY.md gaps: "an object is only ever
	// visible after PutObject fully returns") -- UploadAvailability is still
	// exposed as a real request field since it round-trips into storage, but
	// this page does not claim a live-streaming-while-uploading experience
	// the backend cannot produce.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMediaStoreDataClient } from '$lib/aws-client';
	import {
		PutObjectCommand,
		GetObjectCommand,
		DeleteObjectCommand,
		DescribeObjectCommand,
		ListItemsCommand,
		type Item,
		type DescribeObjectCommandOutput
	} from '@aws-sdk/client-mediastore-data';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate, formatBytes } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { Film, Upload, Download, Trash2, Folder, File as FileIcon, Home, ChevronRight, Info, Scissors } from 'lucide-svelte';

	const client = regionalClient(getMediaStoreDataClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	// Replaces the JSON.stringify(x) antipattern: search only over the named
	// fields that are actually meaningful for each resource.
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	// ==================== Folder navigation (ListItems) ====================
	//
	// currentPath never has a leading slash and always has a trailing slash
	// once non-root, matching real MediaStore Path semantics: the wire URI
	// template is literally "/{Path+}" (confirmed in the installed SDK's
	// bundled operation schema), so the *value* bound to Path must NOT itself
	// start with "/" -- the template already supplies that slash. A value
	// that did start with "/" would serialize to a double-slash URL. Real
	// AWS's own CLI examples for mediastore-data likewise never show a
	// leading slash on --path.

	let currentPath = $state('');
	let items = $state<Item[]>([]);
	let nextToken = $state<string | undefined>();
	let loadingMore = $state(false);
	let searchQuery = $state('');

	const breadcrumbs = $derived(currentPath.split('/').filter(Boolean));

	// `currentPath`/`nextToken` are read with `untrack` for the same reason
	// tab-loader.svelte.ts's `load()` untracks `s.loaded`: this function runs
	// synchronously up to its first `await`, and when it is invoked from
	// inside the `onRegionChange` effect below (via `tabLoader.refresh`), any
	// reactive read that happens in that synchronous prefix becomes a tracked
	// dependency of THAT effect -- not just of this call. Without `untrack`,
	// `currentPath` would become a dependency of the `onRegionChange` effect,
	// and since that same effect also *writes* `currentPath` (resetting to
	// root), navigating into a folder (which writes `currentPath`) would
	// immediately re-trigger the effect and snap the user back to root.
	// Confirmed with a console.log repro before adding this: the effect fired
	// a second time, "resetting currentPath from clips/" back to "", right
	// after a folder click. `untrack` makes this an ordinary imperative read
	// of whatever `currentPath`/`nextToken` are AT CALL TIME, with no
	// dependency created on whoever happened to call it.
	async function fetchItems(reset: boolean): Promise<void> {
		const path = untrack(() => currentPath);
		const token = untrack(() => nextToken);
		const resp = await client().send(
			new ListItemsCommand({
				Path: path || undefined,
				NextToken: reset ? undefined : token
			})
		);
		items = reset ? (resp.Items ?? []) : [...items, ...(resp.Items ?? [])];
		nextToken = resp.NextToken;
	}

	async function loadMoreItems(): Promise<void> {
		loadingMore = true;
		try {
			await fetchItems(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	function navigateInto(item: Item): void {
		if (item.Type !== 'FOLDER' || !item.Name) return;
		currentPath = `${currentPath}${item.Name}/`;
		searchQuery = '';
		tabLoader.refresh('browse');
	}

	function navigateToBreadcrumb(index: number): void {
		currentPath = index < 0 ? '' : breadcrumbs.slice(0, index + 1).join('/') + '/';
		searchQuery = '';
		tabLoader.refresh('browse');
	}

	function fullPath(item: Item): string {
		return `${currentPath}${item.Name ?? ''}`;
	}

	// ==================== Upload (PutObject) ====================

	let showUpload = $state(false);
	let uploadFile = $state<globalThis.File | null>(null);
	let uploadPath = $state('');
	let uploadContentType = $state('');
	let uploadCacheControl = $state('');
	let uploadAvailability = $state<'STANDARD' | 'STREAMING'>('STANDARD');
	let uploading = $state(false);

	function handleFileChange(e: Event): void {
		const input = e.target as HTMLInputElement;
		uploadFile = input.files?.[0] ?? null;
		if (uploadFile) {
			if (!uploadContentType) uploadContentType = uploadFile.type || 'application/octet-stream';
			if (!uploadPath.trim()) uploadPath = `${currentPath}${uploadFile.name}`;
		}
	}

	async function uploadObject(): Promise<void> {
		if (!uploadFile) { toast.error('Select a file to upload'); return; }
		if (!uploadPath.trim()) { toast.error('Object path is required'); return; }

		uploading = true;
		try {
			const buf = await uploadFile.arrayBuffer();
			// StorageClass is intentionally omitted: TEMPORAL is the only
			// legal value (see script-top comment) and is exactly what the
			// backend defaults to when the field is unset (objects.go
			// PutObject: `if storageClass == "" { storageClass = "TEMPORAL" }`).
			await client().send(
				new PutObjectCommand({
					Path: uploadPath.trim(),
					Body: new Uint8Array(buf),
					ContentType: uploadContentType.trim() || undefined,
					CacheControl: uploadCacheControl.trim() || undefined,
					UploadAvailability: uploadAvailability
				})
			);
			toast.success(`Uploaded ${uploadPath.trim()}`);
			uploadFile = null;
			uploadPath = '';
			uploadContentType = '';
			uploadCacheControl = '';
			uploadAvailability = 'STANDARD';
			showUpload = false;
			await tabLoader.refresh('browse');
		} catch (e) {
			toast.error(`Upload failed: ${describeError(e)}`);
		} finally {
			uploading = false;
		}
	}

	// ==================== Download (GetObject) ====================

	async function downloadObject(item: Item): Promise<void> {
		try {
			const res = await client().send(new GetObjectCommand({ Path: fullPath(item) }));
			const bytes = await res.Body?.transformToByteArray();
			const blob = new Blob([bytes ? (bytes as BlobPart) : new Uint8Array()], {
				type: res.ContentType ?? 'application/octet-stream'
			});
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = item.Name ?? 'object';
			a.click();
			URL.revokeObjectURL(url);
		} catch (e) {
			toast.error(`Download failed: ${describeError(e)}`);
		}
	}

	// ==================== Delete (DeleteObject) ====================

	let deletingNames = $state<Set<string>>(new Set());

	async function deleteObject(item: Item): Promise<void> {
		if (!item.Name) return;
		const confirmed = await confirmDestructive({
			message: `Delete object "${fullPath(item)}"?`,
			dangerous: true
		});
		if (!confirmed) return;

		deletingNames = new Set([...deletingNames, item.Name]);
		try {
			await client().send(new DeleteObjectCommand({ Path: fullPath(item) }));
			toast.success(`Deleted ${fullPath(item)}`);
			await tabLoader.refresh('browse');
		} catch (e) {
			toast.error(`Delete failed: ${describeError(e)}`);
		} finally {
			deletingNames = new Set([...deletingNames].filter((n) => n !== item.Name));
		}
	}

	// ==================== Details modal (DescribeObject + GetObject Range) ====================
	//
	// DescribeObject (HEAD) is used for the metadata panel rather than
	// GetObject, so opening "Details" never downloads the object body -- this
	// is the one real op the previous version of this page never called at
	// all (it only ever hit its own nonexistent custom API routes). The
	// Range field below sends a real GetObject Range request and shows the
	// resulting StatusCode/Content-Range, demonstrating actual 206/416
	// behavior instead of a decorative input.

	let detailsModal = $state<Modal | null>(null);
	let detailsItem = $state<Item | null>(null);
	let detailsLoading = $state(false);
	let detailsResult = $state<DescribeObjectCommandOutput | null>(null);
	let detailsError = $state<string | null>(null);

	let rangeInput = $state('');
	let rangeLoading = $state(false);
	let rangeResult = $state<{ status?: number; contentRange?: string; bytes: number; text?: string } | null>(null);
	let rangeError = $state<string | null>(null);

	async function openDetails(item: Item): Promise<void> {
		detailsItem = item;
		detailsResult = null;
		detailsError = null;
		rangeInput = '';
		rangeResult = null;
		rangeError = null;
		detailsModal?.open();
		detailsLoading = true;
		try {
			detailsResult = await client().send(new DescribeObjectCommand({ Path: fullPath(item) }));
		} catch (e) {
			detailsError = describeError(e);
		} finally {
			detailsLoading = false;
		}
	}

	async function fetchRange(): Promise<void> {
		if (!detailsItem) return;
		rangeLoading = true;
		rangeError = null;
		rangeResult = null;
		try {
			const res = await client().send(
				new GetObjectCommand({ Path: fullPath(detailsItem), Range: rangeInput.trim() || undefined })
			);
			const bytes = await res.Body?.transformToByteArray();
			const isTextish = (res.ContentType ?? '').startsWith('text/') || res.ContentType === 'application/json';
			rangeResult = {
				status: res.StatusCode,
				contentRange: res.ContentRange,
				bytes: bytes?.length ?? 0,
				text: isTextish && bytes ? new TextDecoder().decode(bytes) : undefined
			};
		} catch (e) {
			rangeError = describeError(e);
		} finally {
			rangeLoading = false;
		}
	}

	// ==================== Loader ====================

	const tabLoader = createTabLoader<'browse'>({
		browse: () => fetchItems(true).catch(rethrowDescribed)
	});

	onRegionChange(() => {
		currentPath = '';
		searchQuery = '';
		tabLoader.refresh('browse');
	});

	function handleRefresh(): void {
		tabLoader.refresh('browse');
	}

	const filteredItems = $derived(items.filter((i) => matches(searchQuery, i.Name, i.ContentType)));
	const folderCount = $derived(filteredItems.filter((i) => i.Type === 'FOLDER').length);
	const objectCount = $derived(filteredItems.filter((i) => i.Type === 'OBJECT').length);
	const totalBytes = $derived(
		filteredItems.reduce((sum, i) => sum + (i.Type === 'OBJECT' ? (i.ContentLength ?? 0) : 0), 0)
	);
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Film}
		title="MediaStore Data"
		description="Browse, upload, download, and delete objects in the MediaStore data plane."
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			<button
				onclick={() => (showUpload = !showUpload)}
				class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium"
			>
				<Upload class="w-4 h-4" /> Upload
			</button>
		{/snippet}
	</PageHeader>

	<div
		role="note"
		class="rounded-lg border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/20 px-4 py-2 text-xs text-blue-800 dark:text-blue-300"
	>
		This is a single, region-wide object store, not scoped to any MediaStore container — the MediaStore Data
		API never takes a container name (a real client is pointed at a per-container endpoint instead), and this
		emulator does not connect objects here to containers created via the separate MediaStore service.
	</div>

	<!-- Breadcrumbs -->
	<nav aria-label="Breadcrumb" class="flex items-center gap-1 text-sm">
		<button
			onclick={() => navigateToBreadcrumb(-1)}
			class="flex items-center gap-1 px-2 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300"
		>
			<Home class="w-3.5 h-3.5" /> root
		</button>
		{#each breadcrumbs as segment, i (i)}
			<ChevronRight class="w-3.5 h-3.5 text-gray-400 shrink-0" />
			<button
				onclick={() => navigateToBreadcrumb(i)}
				class="px-2 py-1 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300 font-mono"
			>
				{segment}
			</button>
		{/each}
	</nav>

	<!-- Stats -->
	<div class="flex flex-wrap gap-2 text-xs text-gray-500 dark:text-gray-400">
		<span>{objectCount} object{objectCount === 1 ? '' : 's'}</span>
		<span>·</span>
		<span>{folderCount} folder{folderCount === 1 ? '' : 's'}</span>
		<span>·</span>
		<span>{formatBytes(totalBytes)} in this folder</span>
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
						placeholder="folder/filename.mp4"
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm font-mono text-gray-900 dark:text-white"
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
					<label for="upload-avail" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">
						Upload Availability
					</label>
					<select
						id="upload-avail"
						bind:value={uploadAvailability}
						class="w-full rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-2 text-sm text-gray-900 dark:text-white"
					>
						<option value="STANDARD">STANDARD</option>
						<option value="STREAMING">STREAMING</option>
					</select>
					<p class="text-[11px] text-gray-400 mt-1">
						Stored and echoed back on PutObject's response only; this emulator does not model progressive
						download of a still-uploading STREAMING object.
					</p>
				</div>
				<div>
					<span class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Storage Class</span>
					<p class="px-3 py-2 text-sm text-gray-500 dark:text-gray-400 font-mono">TEMPORAL (only legal value)</p>
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

	<!-- Search -->
	<div class="flex justify-end">
		<SearchInput bind:value={searchQuery} placeholder="Search by name or content type..." />
	</div>

	{#if tabLoader.getError('browse')}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load items</p>
			<p>{tabLoader.getError('browse')}</p>
		</div>
	{/if}

	<!-- Browser -->
	{#snippet nameCell(item: Item)}
		{#if item.Type === 'FOLDER'}
			<button
				onclick={() => navigateInto(item)}
				class="flex items-center gap-2 text-blue-600 dark:text-blue-400 hover:underline font-medium"
			>
				<Folder class="w-4 h-4 shrink-0" /> {item.Name}
			</button>
		{:else}
			<span class="flex items-center gap-2 font-mono text-xs text-gray-900 dark:text-white">
				<FileIcon class="w-4 h-4 shrink-0 text-gray-400" /> {item.Name}
			</span>
		{/if}
	{/snippet}
	{#snippet typeCell(item: Item)}
		<span class="text-xs px-2 py-0.5 rounded-full {item.Type === 'FOLDER' ? 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300' : 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300'}">
			{item.Type}
		</span>
	{/snippet}
	{#snippet contentTypeCell(item: Item)}
		{item.Type === 'OBJECT' ? (item.ContentType || 'unknown') : '—'}
	{/snippet}
	{#snippet sizeCell(item: Item)}
		{item.Type === 'OBJECT' ? formatBytes(item.ContentLength) : '—'}
	{/snippet}
	{#snippet modifiedCell(item: Item)}
		{item.Type === 'OBJECT' ? formatDate(item.LastModified) : '—'}
	{/snippet}
	{#snippet actionsCell(item: Item)}
		{#if item.Type === 'OBJECT'}
			<div class="flex items-center gap-1 justify-end">
				<button
					onclick={() => openDetails(item)}
					title="Details"
					aria-label="Details for {item.Name}"
					class="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500 dark:text-gray-400"
				>
					<Info class="w-4 h-4" />
				</button>
				<button
					onclick={() => downloadObject(item)}
					title="Download"
					aria-label="Download {item.Name}"
					class="p-1.5 rounded hover:bg-blue-50 dark:hover:bg-blue-900/30 text-blue-600 dark:text-blue-400"
				>
					<Download class="w-4 h-4" />
				</button>
				<button
					onclick={() => deleteObject(item)}
					disabled={deletingNames.has(item.Name ?? '')}
					title="Delete"
					aria-label="Delete {item.Name}"
					class="p-1.5 rounded hover:bg-red-50 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400 disabled:opacity-40"
				>
					<Trash2 class="w-4 h-4 {deletingNames.has(item.Name ?? '') ? 'animate-pulse' : ''}" />
				</button>
			</div>
		{/if}
	{/snippet}
	<DataTable
		rows={filteredItems}
		rowKey={(i) => i.Name ?? ''}
		columns={defineColumns<Item>([
			{ key: 'Name', label: 'Name', render: nameCell },
			{ key: 'Type', label: 'Type', render: typeCell },
			{ key: 'ContentType', label: 'Content-Type', render: contentTypeCell },
			{ key: 'ContentLength', label: 'Size', render: sizeCell },
			{ key: 'LastModified', label: 'Last Modified', render: modifiedCell },
			{ key: 'actions', label: '', render: actionsCell }
		])}
		loading={tabLoader.isLoading('browse')}
		emptyMessage={searchQuery ? 'No items match your search.' : 'No objects or folders here. Upload a file to get started.'}
	/>
	<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMoreItems} />
</div>

<!-- Details modal -->
<Modal bind:this={detailsModal} title={detailsItem ? `Details — ${detailsItem.Name}` : 'Details'}>
	{#snippet children()}
		{#if detailsLoading}
			<div class="flex justify-center py-8">
				<div class="animate-spin w-6 h-6 border-4 border-blue-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if detailsError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailsError}</p>
		{:else if detailsResult}
			<div class="space-y-3 text-sm max-h-[65vh] overflow-y-auto pr-1">
				{#each [
					['ETag', detailsResult.ETag],
					['Content-Type', detailsResult.ContentType],
					['Content-Length', detailsResult.ContentLength !== undefined ? `${detailsResult.ContentLength} bytes (${formatBytes(detailsResult.ContentLength)})` : undefined],
					['Cache-Control', detailsResult.CacheControl],
					['Last Modified', formatDate(detailsResult.LastModified)]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}

				<div class="pt-2">
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2 flex items-center gap-1">
						<Scissors class="w-3.5 h-3.5" /> Range preview (GetObject)
					</h4>
					<div class="flex gap-2">
						<input
							bind:value={rangeInput}
							placeholder="bytes=0-99 (leave empty for full object)"
							aria-label="Range header value"
							class="flex-1 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-700 px-3 py-1.5 text-xs font-mono text-gray-900 dark:text-white"
						/>
						<button
							onclick={fetchRange}
							disabled={rangeLoading}
							class="px-3 py-1.5 text-xs rounded-lg bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50"
						>
							{rangeLoading ? 'Fetching…' : 'Fetch'}
						</button>
					</div>
					{#if rangeError}
						<p class="text-xs text-red-600 dark:text-red-400 mt-2">{rangeError}</p>
					{:else if rangeResult}
						<div class="mt-2 text-xs space-y-1 bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
							<p><span class="text-gray-500">Status:</span> <span class="font-mono">{rangeResult.status ?? '—'}</span></p>
							<p><span class="text-gray-500">Content-Range:</span> <span class="font-mono">{rangeResult.contentRange ?? '—'}</span></p>
							<p><span class="text-gray-500">Bytes received:</span> <span class="font-mono">{rangeResult.bytes}</span></p>
							{#if rangeResult.text !== undefined}
								<pre class="mt-1 whitespace-pre-wrap break-all font-mono text-[11px] text-gray-700 dark:text-gray-300">{rangeResult.text}</pre>
							{/if}
						</div>
					{/if}
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => detailsModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>
