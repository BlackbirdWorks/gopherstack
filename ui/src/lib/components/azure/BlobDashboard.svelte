<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { HardDrive, Plus, Trash2, Upload, Download, Folder, File as FileIcon, RefreshCw, ChevronRight, Copy, X, Box, Camera } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import MetadataEditor from '#lib/components/azure/MetadataEditor.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		ACCESS_TIERS,
		BlobError,
		DEFAULT_ACCOUNT,
		MAX_PAGE,
		blobUrl,
		breadcrumbs,
		createContainer,
		createSnapshot,
		deleteBlob,
		deleteContainer,
		downloadBlob,
		formatBytes,
		getBlobProperties,
		getContainerAcl,
		getContainerProperties,
		listBlobs,
		listContainers,
		listSnapshots,
		metadataToRows,
		rowsToMetadata,
		setBlobMetadata,
		setBlobProperties,
		setBlobTier,
		setContainerAcl,
		setContainerMetadata,
		uploadBlob,
		validateBlobName,
		validateContainerName,
		type AccessTier,
		type BlobItem,
		type BlobProperties,
		type BrowserEntry,
		type ContainerInfo,
		type MetaRow,
		type PublicAccess,
		type SignedIdentifier
	} from '#lib/azureblob.ts';

	const account = DEFAULT_ACCOUNT;
	const accessLabels: Record<PublicAccess, string> = {
		'': 'Private (no anonymous access)',
		blob: 'Blob (anonymous read of blobs)',
		container: 'Container (anonymous read and list)'
	};

	let containers = $state<ContainerInfo[]>([]);
	let current = $state<string | null>(null);
	let entries = $state<BrowserEntry[]>([]);
	let nextMarker = $state('');
	let prefix = $state('');
	let selectedBlob = $state<BlobItem | null>(null);
	let props = $state<BlobProperties | null>(null);
	let snapshots = $state<BlobItem[]>([]);
	let loading = $state(true);
	let loadingBlobs = $state(false);
	let loadError = $state('');
	let uploading = $state(false);
	let dragging = $state(false);

	let accessChoice = $state<PublicAccess>('');
	let aclIdentifiers = $state<SignedIdentifier[]>([]);
	let containerMeta = $state<MetaRow[]>([]);
	let blobMeta = $state<MetaRow[]>([]);
	let contentForm = $state({ contentType: '', contentEncoding: '', contentLanguage: '', contentDisposition: '', cacheControl: '' });
	let tierChoice = $state<AccessTier>('Hot');
	let saving = $state('');

	let modal = $state<Modal | null>(null);
	let formName = $state('');
	let formAccess = $state<PublicAccess>('');
	let touched = $state(false);
	let creating = $state(false);
	let fileInput = $state<HTMLInputElement | null>(null);

	const crumbs = $derived(breadcrumbs(prefix));
	const nameError = $derived(validateContainerName(formName.trim()));

	function describe(e: unknown): string {
		if (e instanceof BlobError) return e.code ? `${e.code}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	async function loadContainers() {
		loading = true;
		loadError = '';
		try {
			containers = (await listContainers(account)).toSorted((a, b) => a.name.localeCompare(b.name));
			if (current && !containers.some((c) => c.name === current)) selectContainer(null);
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load containers: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadBlobs(more = false) {
		if (!current) return;
		loadingBlobs = true;
		try {
			const page = await listBlobs(account, current, {
				prefix,
				marker: more ? nextMarker : '',
				maxResults: MAX_PAGE
			});
			entries = more ? [...entries, ...page.entries] : page.entries;
			nextMarker = page.nextMarker;
		} catch (e) {
			toast.error(`Failed to list blobs: ${describe(e)}`);
		} finally {
			loadingBlobs = false;
		}
	}

	async function loadContainerSettings() {
		if (!current) return;
		try {
			const [p, acl] = await Promise.all([getContainerProperties(account, current), getContainerAcl(account, current)]);
			accessChoice = p.publicAccess;
			containerMeta = metadataToRows(p.metadata);
			aclIdentifiers = acl.identifiers;
		} catch (e) {
			toast.error(`Failed to read container settings: ${describe(e)}`);
		}
	}

	async function refresh() {
		await loadContainers();
		await Promise.all([loadBlobs(), loadContainerSettings()]);
		if (selectedBlob) await showBlob(selectedBlob);
	}

	function selectContainer(name: string | null) {
		current = name;
		prefix = '';
		entries = [];
		nextMarker = '';
		clearBlob();
		if (name) {
			void loadBlobs();
			void loadContainerSettings();
		}
	}

	function navigate(p: string) {
		prefix = p;
		clearBlob();
		void loadBlobs();
	}

	function clearBlob() {
		selectedBlob = null;
		props = null;
		snapshots = [];
	}

	async function showBlob(b: BlobItem) {
		selectedBlob = b;
		props = null;
		if (!current) return;
		try {
			const [p, snaps] = await Promise.all([getBlobProperties(account, current, b.name), listSnapshots(account, current, b.name)]);
			props = p;
			snapshots = snaps;
			blobMeta = metadataToRows(p.metadata);
			contentForm = {
				contentType: p.contentType,
				contentEncoding: p.contentEncoding,
				contentLanguage: p.contentLanguage,
				contentDisposition: p.contentDisposition,
				cacheControl: p.cacheControl
			};
			tierChoice = ACCESS_TIERS.includes(p.accessTier as AccessTier) ? (p.accessTier as AccessTier) : 'Hot';
		} catch (e) {
			toast.error(`Failed to read properties: ${describe(e)}`);
		}
	}

	async function run(label: string, action: () => Promise<void>, success: string, failure: string) {
		saving = label;
		try {
			await action();
			toast.success(success);
		} catch (e) {
			toast.error(`${failure}: ${describe(e)}`);
		} finally {
			saving = '';
		}
	}

	async function saveAccess() {
		const c = current;
		if (!c) return;
		await run('access', () => setContainerAcl(account, c, accessChoice, aclIdentifiers), 'Saved access level', 'Failed to save access level');
		await loadContainerSettings();
	}

	async function saveContainerMeta() {
		const c = current;
		if (!c) return;
		await run('container-meta', () => setContainerMetadata(account, c, rowsToMetadata(containerMeta)), 'Saved container metadata', 'Failed to save container metadata');
		await loadContainerSettings();
	}

	async function saveBlobMeta() {
		const c = current;
		const b = selectedBlob;
		if (!c || !b) return;
		await run('blob-meta', () => setBlobMetadata(account, c, b.name, rowsToMetadata(blobMeta)), 'Saved blob metadata', 'Failed to save blob metadata');
		await showBlob(b);
	}

	async function saveContent() {
		const c = current;
		const b = selectedBlob;
		if (!c || !b) return;
		await run('content', () => setBlobProperties(account, c, b.name, { ...contentForm, contentMd5: props?.contentMd5 }), 'Saved content properties', 'Failed to save content properties');
		await showBlob(b);
		await loadBlobs();
	}

	async function applyTier() {
		const c = current;
		const b = selectedBlob;
		if (!c || !b) return;
		await run('tier', () => setBlobTier(account, c, b.name, tierChoice), `Updated access tier to ${tierChoice}`, 'Failed to set access tier');
		await showBlob(b);
		await loadBlobs();
	}

	async function snapshotBlob() {
		const c = current;
		const b = selectedBlob;
		if (!c || !b) return;
		await run('snapshot', async () => { await createSnapshot(account, c, b.name); }, 'Created snapshot', 'Failed to create snapshot');
		await showBlob(b);
	}

	async function removeSnapshot(snap: BlobItem) {
		const c = current;
		const b = selectedBlob;
		if (!c || !b) return;
		if (!(await confirmDestructive({ title: 'Delete snapshot', message: `Delete snapshot ${snap.snapshot}? This cannot be undone.` }))) return;
		try {
			await deleteBlob(account, c, b.name, { snapshot: snap.snapshot });
			toast.success('Deleted snapshot');
			await showBlob(b);
		} catch (e) {
			toast.error(`Failed to delete snapshot: ${describe(e)}`);
		}
	}

	function openNew() {
		formName = '';
		formAccess = '';
		touched = false;
		modal?.open();
	}

	async function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (nameError) return;
		creating = true;
		const name = formName.trim();
		try {
			await createContainer(account, name, { publicAccess: formAccess });
			toast.success(`Created container ${name}`);
			modal?.close('created');
			await loadContainers();
			selectContainer(name);
		} catch (e) {
			toast.error(`Failed to create ${name}: ${describe(e)}`);
		} finally {
			creating = false;
		}
	}

	async function removeContainer(c: ContainerInfo) {
		if (!(await confirmDestructive({ title: 'Delete container', message: `Delete container "${c.name}" and all of its blobs? This cannot be undone.` }))) return;
		try {
			await deleteContainer(account, c.name);
			toast.success(`Deleted container ${c.name}`);
			if (current === c.name) selectContainer(null);
			await loadContainers();
		} catch (e) {
			toast.error(`Failed to delete ${c.name}: ${describe(e)}`);
		}
	}

	async function upload(files: File[]) {
		if (!current || files.length === 0) return;
		uploading = true;
		let ok = 0;
		for (const f of files) {
			const name = `${prefix}${f.name}`;
			const invalid = validateBlobName(name);
			if (invalid) {
				toast.error(`Cannot upload ${f.name}: ${invalid}`);
				continue;
			}
			try {
				await uploadBlob(account, current, name, f, f.type);
				ok++;
			} catch (e) {
				toast.error(`Failed to upload ${f.name}: ${describe(e)}`);
			}
		}
		uploading = false;
		if (fileInput) fileInput.value = '';
		if (ok > 0) toast.success(`Uploaded ${ok} blob${ok === 1 ? '' : 's'}`);
		await loadBlobs();
	}

	function onDrop(e: DragEvent) {
		e.preventDefault();
		dragging = false;
		void upload(Array.from(e.dataTransfer?.files ?? []));
	}

	async function download(b: BlobItem) {
		if (!current) return;
		try {
			const data = await downloadBlob(account, current, b.name, b.snapshot);
			const url = URL.createObjectURL(data);
			const a = document.createElement('a');
			a.href = url;
			a.download = b.name.split('/').pop() ?? b.name;
			document.body.append(a);
			a.click();
			a.remove();
			URL.revokeObjectURL(url);
		} catch (e) {
			toast.error(`Failed to download ${b.name}: ${describe(e)}`);
		}
	}

	async function removeBlob(b: BlobItem) {
		if (!current) return;
		if (!(await confirmDestructive({ title: 'Delete blob', message: `Delete blob "${b.name}"? This cannot be undone.` }))) return;
		try {
			try {
				await deleteBlob(account, current, b.name);
			} catch (e) {
				if (!(e instanceof BlobError && e.code === 'SnapshotsPresent')) throw e;
				if (!(await confirmDestructive({ title: 'Delete snapshots too', message: `"${b.name}" has snapshots. Delete the blob and all of its snapshots?` }))) return;
				await deleteBlob(account, current, b.name, { deleteSnapshots: 'include' });
			}
			toast.success(`Deleted ${b.name}`);
			if (selectedBlob?.name === b.name) clearBlob();
			await loadBlobs();
		} catch (e) {
			toast.error(`Failed to delete ${b.name}: ${describe(e)}`);
		}
	}

	async function copy(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const btn =
		'rounded-md bg-blue-600 px-3 py-1 text-sm text-white hover:bg-blue-700 disabled:opacity-50';

	onMount(loadContainers);
</script>

<div class="space-y-6 pb-20" data-testid="blob-dashboard">
	<PageHeader
		icon={HardDrive}
		title="Blob Storage"
		description="Azure Blob Storage: containers and blobs"
		color="blue"
		onRefresh={refresh}
	>
		{#snippet actions()}
			<button
				class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
				onclick={openNew}
				disabled={loading || Boolean(loadError)}
			>
				<Plus class="h-4 w-4" /> New container
			</button>
		{/snippet}
	</PageHeader>

	<p class="text-xs text-gray-500 dark:text-gray-400">
		Storage account <span class="font-mono" data-testid="blob-account">{account}</span>
	</p>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="blob-error">
			<p class="font-medium">Could not reach Azure Blob Storage</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadContainers}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<section aria-label="Containers" class="space-y-2">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Containers</h2>
				{#if loading}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if containers.length === 0}
					<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="blob-no-containers">
						<Box class="h-10 w-10 mb-2 opacity-30" />
						<p class="text-sm">No containers</p>
						<p class="text-xs">Create a container to get started</p>
					</div>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
						{#each containers as c (c.name)}
							<li class="flex items-center {current === c.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="blob-container-row">
								<button class="flex-1 truncate px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectContainer(c.name)}>
									{c.name}
									{#if c.publicAccess}<span class="ml-2 rounded bg-amber-100 dark:bg-amber-900/40 px-1.5 py-0.5 text-[10px] font-normal uppercase text-amber-800 dark:text-amber-300" data-testid="blob-container-access">{c.publicAccess}</span>{/if}
								</button>
								<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {c.name}" aria-label="Delete container {c.name}" onclick={() => removeContainer(c)}>
									<Trash2 class="h-4 w-4" />
								</button>
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<section aria-label="Blobs" class="space-y-3 min-w-0">
				{#if !current}
					<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="blob-select-hint">
						<HardDrive class="h-12 w-12 mb-3 opacity-30" />
						<p>Select a container to browse its blobs</p>
					</div>
				{:else}
					<div class="flex flex-wrap items-center justify-between gap-2">
						<nav class="flex flex-wrap items-center gap-1 text-sm" aria-label="Blob path" data-testid="blob-breadcrumbs">
							<button class="font-medium text-blue-600 dark:text-blue-400 hover:underline" onclick={() => navigate('')}>{current}</button>
							{#each crumbs as c (c.prefix)}
								<ChevronRight class="h-3.5 w-3.5 text-gray-400" />
								<button class="text-blue-600 dark:text-blue-400 hover:underline" onclick={() => navigate(c.prefix)}>{c.label}</button>
							{/each}
						</nav>
						<div class="flex items-center gap-2">
							<input bind:this={fileInput} type="file" multiple class="hidden" data-testid="blob-file-input" onchange={(e) => upload(Array.from(e.currentTarget.files ?? []))} />
							<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={() => fileInput?.click()} disabled={uploading}>
								<Upload class="h-4 w-4" /> {uploading ? 'Uploading…' : 'Upload'}
							</button>
						</div>
					</div>

					<details class="rounded-lg border border-gray-200 dark:border-gray-700 p-4" data-testid="container-settings">
						<summary class="cursor-pointer text-sm font-semibold text-gray-700 dark:text-gray-300">Container settings</summary>
						<div class="mt-3 grid gap-6 md:grid-cols-2">
							<div class="space-y-2">
								<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Public access level</h4>
								<select class={input} bind:value={accessChoice} aria-label="Public access level" data-testid="container-access">
									{#each Object.entries(accessLabels) as [value, label] (value)}
										<option {value}>{label}</option>
									{/each}
								</select>
								<p class="text-xs text-gray-500 dark:text-gray-400">
									Anonymous requests are only restricted when the server runs with --azure-blob-enforce-public-access.
								</p>
								<button type="button" class={btn} disabled={saving === 'access'} onclick={saveAccess} data-testid="container-access-save">Save access level</button>
							</div>
							<MetadataEditor bind:rows={containerMeta} label="Container metadata" testId="container-meta" saving={saving === 'container-meta'} onsave={saveContainerMeta} />
						</div>
					</details>

					<div
						role="region"
						aria-label="Drop files to upload"
						class="rounded-lg border-2 border-dashed px-4 py-3 text-center text-xs {dragging ? 'border-blue-500 bg-blue-50 dark:bg-blue-950/40 text-blue-700 dark:text-blue-300' : 'border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400'}"
						data-testid="blob-dropzone"
						ondragover={(e) => { e.preventDefault(); dragging = true; }}
						ondragleave={() => (dragging = false)}
						ondrop={onDrop}
					>
						Drag and drop files here to upload{prefix ? ` into ${prefix}` : ''}
					</div>

					{#if loadingBlobs && entries.length === 0}
						<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
					{:else if entries.length === 0}
						<div class="flex flex-col items-center py-10 text-gray-500 dark:text-gray-400" data-testid="blob-empty">
							<FileIcon class="h-10 w-10 mb-2 opacity-30" />
							<p class="text-sm">{prefix ? 'This folder is empty' : 'This container is empty'}</p>
							<p class="text-xs">Upload a file to create a blob</p>
						</div>
					{:else}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
							<table class="w-full text-sm">
								<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
									<tr>
										<th class="px-4 py-3 font-medium">Name</th>
										<th class="px-4 py-3 font-medium">Size</th>
										<th class="px-4 py-3 font-medium">Content type</th>
										<th class="px-4 py-3 font-medium">Tier</th>
										<th class="px-4 py-3 font-medium">Last modified</th>
										<th class="px-4 py-3 text-right font-medium">Actions</th>
									</tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each entries as e (e.kind === 'folder' ? `d:${e.prefix}` : `b:${e.blob.name}`)}
										{#if e.kind === 'folder'}
											<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50" data-testid="blob-folder-row" onclick={() => navigate(e.prefix)}>
												<td class="px-4 py-3 font-medium text-gray-900 dark:text-gray-100"><span class="flex items-center gap-2"><Folder class="h-4 w-4 text-amber-500" />{e.name}/</span></td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td></td>
											</tr>
										{:else}
											<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {selectedBlob?.name === e.blob.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="blob-row" onclick={() => showBlob(e.blob)}>
												<td class="px-4 py-3 font-medium text-gray-900 dark:text-gray-100"><span class="flex items-center gap-2"><FileIcon class="h-4 w-4 text-sky-500" />{e.name}</span></td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{formatBytes(e.blob.contentLength)}</td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{e.blob.contentType || '—'}</td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{e.blob.accessTier || '—'}</td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{e.blob.lastModified || '—'}</td>
												<td class="px-4 py-3 text-right whitespace-nowrap">
													<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" title="Download {e.name}" aria-label="Download {e.name}" onclick={(ev) => { ev.stopPropagation(); void download(e.blob); }}><Download class="h-4 w-4" /></button>
													<button class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {e.name}" aria-label="Delete blob {e.name}" onclick={(ev) => { ev.stopPropagation(); void removeBlob(e.blob); }}><Trash2 class="h-4 w-4" /></button>
												</td>
											</tr>
										{/if}
									{/each}
								</tbody>
							</table>
						</div>
						{#if nextMarker}
							<div class="flex justify-center">
								<button class="rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={loadingBlobs} onclick={() => loadBlobs(true)} data-testid="blob-load-more">
									{loadingBlobs ? 'Loading…' : 'Load more'}
								</button>
							</div>
						{/if}
					{/if}

					{#if selectedBlob}
						<section class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-4" data-testid="blob-detail" aria-label="Properties of {selectedBlob.name}">
							<div class="flex items-start justify-between gap-4">
								<div class="min-w-0">
									<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{selectedBlob.name}</h3>
									<p class="text-xs text-gray-500 dark:text-gray-400">Blob properties</p>
								</div>
								<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={clearBlob} aria-label="Close details"><X class="h-4 w-4" /></button>
							</div>
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
								<dt class={dt}>URL</dt>
								<dd class="{dd} flex items-center gap-2 font-mono text-xs">
									{`${location.origin}${blobUrl(account, current, selectedBlob.name)}`}
									<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy blob URL" onclick={() => copy(`${location.origin}${blobUrl(account, current ?? '', selectedBlob?.name ?? '')}`)}><Copy class="h-3.5 w-3.5" /></button>
								</dd>
								<dt class={dt}>Blob type</dt><dd class={dd}>{props?.blobType || '—'}</dd>
								<dt class={dt}>Size</dt><dd class={dd}>{formatBytes(props?.contentLength ?? selectedBlob.contentLength)} ({props?.contentLength ?? selectedBlob.contentLength} bytes)</dd>
								<dt class={dt}>Content type</dt><dd class={dd}>{props?.contentType || selectedBlob.contentType || '—'}</dd>
								<dt class={dt}>Last modified</dt><dd class={dd}>{props?.lastModified || selectedBlob.lastModified || '—'}</dd>
								<dt class={dt}>Access tier</dt><dd class={dd} data-testid="blob-tier-current">{props?.accessTier || selectedBlob.accessTier || '—'}{props?.accessTierInferred ? ' (inferred)' : ''}</dd>
								<dt class={dt}>ETag</dt><dd class="{dd} font-mono text-xs">{props?.etag || selectedBlob.etag || '—'}</dd>
							</dl>
							<div class="grid gap-6 md:grid-cols-2">
								<div class="space-y-2" data-testid="blob-content-props">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Content properties</h4>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Type<input class={input} bind:value={contentForm.contentType} aria-label="Content-Type" /></label>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Encoding<input class={input} bind:value={contentForm.contentEncoding} aria-label="Content-Encoding" /></label>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Language<input class={input} bind:value={contentForm.contentLanguage} aria-label="Content-Language" /></label>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Disposition<input class={input} bind:value={contentForm.contentDisposition} aria-label="Content-Disposition" /></label>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Cache-Control<input class={input} bind:value={contentForm.cacheControl} aria-label="Cache-Control" /></label>
									<button type="button" class={btn} disabled={saving === 'content'} onclick={saveContent} data-testid="blob-content-save">Save content properties</button>
								</div>
								<div class="space-y-6">
									<MetadataEditor bind:rows={blobMeta} label="Blob metadata" testId="blob-meta" saving={saving === 'blob-meta'} onsave={saveBlobMeta} />
									<div class="space-y-2" data-testid="blob-tier">
										<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Access tier</h4>
										<div class="flex items-center gap-2">
											<select class={input} bind:value={tierChoice} aria-label="Access tier" data-testid="blob-tier-select">
												{#each ACCESS_TIERS as t (t)}<option value={t}>{t}</option>{/each}
											</select>
											<button type="button" class={btn} disabled={saving === 'tier'} onclick={applyTier} data-testid="blob-tier-apply">Apply</button>
										</div>
										{#if tierChoice === 'Archive'}<p class="text-xs text-amber-700 dark:text-amber-400">Archived blobs cannot be read or downloaded until moved to another tier.</p>{/if}
									</div>
								</div>
							</div>
							<div class="space-y-2" data-testid="blob-snapshots">
								<div class="flex items-center justify-between">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Snapshots ({snapshots.length})</h4>
									<button type="button" class="flex items-center gap-1 rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={saving === 'snapshot'} onclick={snapshotBlob} data-testid="blob-snapshot-create"><Camera class="h-3.5 w-3.5" /> Create snapshot</button>
								</div>
								{#if snapshots.length === 0}
									<p class="text-xs text-gray-500 dark:text-gray-400">No snapshots</p>
								{:else}
									<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
										{#each snapshots as snap (snap.snapshot)}
											<li class="flex items-center justify-between gap-2 px-3 py-2 text-xs" data-testid="blob-snapshot-row">
												<span class="font-mono break-all">{snap.snapshot}</span>
												<span class="whitespace-nowrap">
													<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" title="Download snapshot" aria-label="Download snapshot {snap.snapshot}" onclick={() => download(snap)}><Download class="h-4 w-4" /></button>
													<button class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete snapshot" aria-label="Delete snapshot {snap.snapshot}" onclick={() => removeSnapshot(snap)}><Trash2 class="h-4 w-4" /></button>
												</span>
											</li>
										{/each}
									</ul>
								{/if}
							</div>
							<div class="flex gap-2">
								<button class="flex items-center gap-2 rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => selectedBlob && download(selectedBlob)}><Download class="h-4 w-4" /> Download</button>
								<button class="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950" onclick={() => selectedBlob && removeBlob(selectedBlob)}><Trash2 class="h-4 w-4" /> Delete</button>
							</div>
						</section>
					{/if}
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={modal} title="New container" titleId="blob-create-title">
	{#snippet children()}
		<form id="blob-create-form" class="space-y-3" onsubmit={submit} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Name</span>
				<input class={input} bind:value={formName} placeholder="my-container" autocomplete="off" aria-label="Name" />
				{#if touched && nameError}<span class="block text-xs text-red-600 dark:text-red-400">{nameError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Public access level</span>
				<select class={input} bind:value={formAccess} aria-label="Public access level for new container">
					{#each Object.entries(accessLabels) as [value, label] (value)}
						<option {value}>{label}</option>
					{/each}
				</select>
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="blob-create-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={creating}>
			{creating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>
