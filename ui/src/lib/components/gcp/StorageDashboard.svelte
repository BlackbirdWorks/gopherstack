<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { HardDrive, Plus, Trash2, Upload, Download, Folder, File as FileIcon, RefreshCw, ChevronRight, Copy, X, Box } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		DELIMITER,
		LOCATIONS,
		MAX_PAGE,
		STORAGE_CLASSES,
		GcsError,
		base64ToHex,
		breadcrumbs,
		createBucket,
		deleteBucket,
		deleteObject,
		diffMap,
		downloadObject,
		downloadUrl,
		formatBytes,
		getBucket,
		getIamPolicy,
		getObject,
		gsUri,
		listBuckets,
		listObjects,
		listVersions,
		metadataToRows,
		patchBucket,
		patchObject,
		readStoredProject,
		rowsToMap,
		uploadObject,
		validateBucketName,
		validateLabelKey,
		validateMetadataKey,
		validateObjectName,
		validateRows,
		writeStoredProject,
		type BrowserEntry,
		type BucketInfo,
		type IamPolicy,
		type MetaRow,
		type ObjectInfo
	} from '#lib/gcpstorage.ts';

	let project = $state(readStoredProject());
	let projectInput = $state(readStoredProject());
	let buckets = $state<BucketInfo[]>([]);
	let current = $state<string | null>(null);
	let bucket = $state<BucketInfo | null>(null);
	let entries = $state<BrowserEntry[]>([]);
	let nextToken = $state('');
	let prefix = $state('');
	let selected = $state<ObjectInfo | null>(null);
	let detail = $state<ObjectInfo | null>(null);
	let versions = $state<ObjectInfo[]>([]);
	let loading = $state(true);
	let loadingObjects = $state(false);
	let loadError = $state('');
	let uploading = $state(false);
	let dragging = $state(false);

	let iam = $state<IamPolicy | null>(null);
	let bucketLabels = $state<MetaRow[]>([]);
	let versioningChoice = $state(false);
	let objectMeta = $state<MetaRow[]>([]);
	let contentForm = $state({ contentType: '', contentEncoding: '', contentLanguage: '', contentDisposition: '', cacheControl: '' });
	let saving = $state('');

	let modal = $state<Modal | null>(null);
	let formName = $state('');
	let formLocation = $state('US');
	let formClass = $state('STANDARD');
	let formVersioning = $state(false);
	let formLabels = $state<MetaRow[]>([]);
	let touched = $state(false);
	let creating = $state(false);
	let fileInput = $state<HTMLInputElement | null>(null);

	const crumbs = $derived(breadcrumbs(prefix));
	const nameError = $derived(validateBucketName(formName.trim()));

	function describe(e: unknown): string {
		if (e instanceof GcsError) return e.reason ? `${e.reason}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	async function loadBuckets() {
		loading = true;
		loadError = '';
		try {
			buckets = (await listBuckets(project)).toSorted((a, b) => a.name.localeCompare(b.name));
			if (current && !buckets.some((b) => b.name === current)) selectBucket(null);
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load buckets: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadObjects(more = false) {
		if (!current) return;
		loadingObjects = true;
		try {
			const res = await listObjects(current, {
				prefix,
				delimiter: DELIMITER,
				pageToken: more ? nextToken : '',
				maxResults: MAX_PAGE
			});
			entries = more ? [...entries, ...res.entries] : res.entries;
			nextToken = res.nextToken;
		} catch (e) {
			toast.error(`Failed to list objects: ${describe(e)}`);
		} finally {
			loadingObjects = false;
		}
	}

	async function loadBucketSettings() {
		if (!current) return;
		try {
			const [b, policy] = await Promise.all([getBucket(current), getIamPolicy(current)]);
			bucket = b;
			iam = policy;
			bucketLabels = metadataToRows(b.labels);
			versioningChoice = b.versioning;
		} catch (e) {
			toast.error(`Failed to read bucket settings: ${describe(e)}`);
		}
	}

	async function refresh() {
		await loadBuckets();
		await Promise.all([loadObjects(), loadBucketSettings()]);
		if (selected) await showObject(selected);
	}

	function selectBucket(name: string | null) {
		current = name;
		bucket = null;
		iam = null;
		prefix = '';
		entries = [];
		nextToken = '';
		clearObject();
		if (name) {
			void loadObjects();
			void loadBucketSettings();
		}
	}

	function navigate(p: string) {
		prefix = p;
		clearObject();
		void loadObjects();
	}

	function clearObject() {
		selected = null;
		detail = null;
		versions = [];
	}

	async function showObject(o: ObjectInfo) {
		selected = o;
		detail = null;
		if (!current) return;
		try {
			const [d, vers] = await Promise.all([
				getObject(current, o.name),
				bucket?.versioning ? listVersions(current, o.name) : Promise.resolve([] as ObjectInfo[])
			]);
			detail = d;
			versions = vers;
			objectMeta = metadataToRows(d.metadata);
			contentForm = {
				contentType: d.contentType,
				contentEncoding: d.contentEncoding,
				contentLanguage: d.contentLanguage,
				contentDisposition: d.contentDisposition,
				cacheControl: d.cacheControl
			};
		} catch (e) {
			toast.error(`Failed to read object: ${describe(e)}`);
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

	function applyProject(event: Event) {
		event.preventDefault();
		const next = projectInput.trim();
		if (!next) return;
		project = next;
		writeStoredProject(next);
		selectBucket(null);
		void loadBuckets();
	}

	async function saveLabels() {
		const c = current;
		const b = bucket;
		if (!c || !b) return;
		await run('labels', async () => { await patchBucket(c, { labels: diffMap(b.labels, rowsToMap(bucketLabels)) }); }, 'Saved bucket labels', 'Failed to save labels');
		await loadBucketSettings();
		await loadBuckets();
	}

	async function saveVersioning() {
		const c = current;
		if (!c) return;
		await run('versioning', async () => { await patchBucket(c, { versioning: { enabled: versioningChoice } }); }, `Versioning ${versioningChoice ? 'enabled' : 'disabled'}`, 'Failed to update versioning');
		await loadBucketSettings();
		if (selected) await showObject(selected);
	}

	async function saveObjectMeta() {
		const c = current;
		const d = detail;
		if (!c || !d) return;
		await run('object-meta', async () => { await patchObject(c, d.name, { metadata: diffMap(d.metadata, rowsToMap(objectMeta)) }); }, 'Saved object metadata', 'Failed to save metadata');
		await showObject(d);
	}

	async function saveContent() {
		const c = current;
		const d = detail;
		if (!c || !d) return;
		await run('content', async () => { await patchObject(c, d.name, { ...contentForm }); }, 'Saved content properties', 'Failed to save content properties');
		await showObject(d);
		await loadObjects();
	}

	function openNew() {
		formName = '';
		formLocation = 'US';
		formClass = 'STANDARD';
		formVersioning = false;
		formLabels = [];
		touched = false;
		modal?.open();
	}

	async function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (nameError) return;
		const labelError = validateRows(formLabels, validateLabelKey);
		if (labelError) {
			toast.error(`Invalid label ${labelError}`);
			return;
		}
		creating = true;
		const name = formName.trim();
		try {
			await createBucket(project, name, {
				location: formLocation,
				storageClass: formClass,
				versioning: formVersioning,
				labels: rowsToMap(formLabels)
			});
			toast.success(`Created bucket ${name}`);
			modal?.close('created');
			await loadBuckets();
			selectBucket(name);
		} catch (e) {
			toast.error(`Failed to create ${name}: ${describe(e)}`);
		} finally {
			creating = false;
		}
	}

	async function removeBucket(b: BucketInfo) {
		if (!(await confirmDestructive({ title: 'Delete bucket', message: `Delete bucket "${b.name}"? It must be empty. This cannot be undone.` }))) return;
		try {
			await deleteBucket(b.name);
			toast.success(`Deleted bucket ${b.name}`);
			if (current === b.name) selectBucket(null);
			await loadBuckets();
		} catch (e) {
			toast.error(`Failed to delete ${b.name}: ${describe(e)}`);
		}
	}

	async function upload(files: File[]) {
		if (!current || files.length === 0) return;
		uploading = true;
		let ok = 0;
		for (const f of files) {
			const name = `${prefix}${f.name}`;
			const invalid = validateObjectName(name);
			if (invalid) {
				toast.error(`Cannot upload ${f.name}: ${invalid}`);
				continue;
			}
			try {
				await uploadObject(current, name, f, f.type);
				ok++;
			} catch (e) {
				toast.error(`Failed to upload ${f.name}: ${describe(e)}`);
			}
		}
		uploading = false;
		if (fileInput) fileInput.value = '';
		if (ok > 0) toast.success(`Uploaded ${ok} object${ok === 1 ? '' : 's'}`);
		await loadObjects();
		if (selected) await showObject(selected);
	}

	function onDrop(e: DragEvent) {
		e.preventDefault();
		dragging = false;
		void upload(Array.from(e.dataTransfer?.files ?? []));
	}

	async function download(o: ObjectInfo, generation = '') {
		if (!current) return;
		try {
			const data = await downloadObject(current, o.name, generation);
			const url = URL.createObjectURL(data);
			const a = document.createElement('a');
			a.href = url;
			a.download = o.name.split('/').pop() ?? o.name;
			document.body.append(a);
			a.click();
			a.remove();
			URL.revokeObjectURL(url);
		} catch (e) {
			toast.error(`Failed to download ${o.name}: ${describe(e)}`);
		}
	}

	async function removeObject(o: ObjectInfo, generation = '') {
		if (!current) return;
		const what = generation ? `generation ${generation} of "${o.name}"` : `object "${o.name}"`;
		if (!(await confirmDestructive({ title: 'Delete object', message: `Delete ${what}? This cannot be undone.` }))) return;
		try {
			await deleteObject(current, o.name, generation);
			toast.success(`Deleted ${o.name}`);
			if (selected?.name === o.name) {
				if (generation && versions.length > 1) await showObject(o);
				else clearObject();
			}
			await loadObjects();
		} catch (e) {
			toast.error(`Failed to delete ${o.name}: ${describe(e)}`);
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
	const btn = 'rounded-md bg-blue-600 px-3 py-1 text-sm text-white hover:bg-blue-700 disabled:opacity-50';

	onMount(loadBuckets);
</script>

<div class="space-y-6 pb-20" data-testid="gcs-dashboard">
	<PageHeader
		icon={HardDrive}
		title="Cloud Storage"
		description="Google Cloud Storage: buckets and objects"
		color="blue"
		onRefresh={refresh}
	>
		{#snippet actions()}
			<button
				class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
				onclick={openNew}
				disabled={loading || Boolean(loadError)}
				data-testid="gcs-new-bucket"
			>
				<Plus class="h-4 w-4" /> New bucket
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyProject}>
		<label for="gcs-project">Project</label>
		<input id="gcs-project" class="w-56 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="gcs-project" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || projectInput.trim() === project}>Switch</button>
		<span>Buckets are listed per project.</span>
	</form>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="gcs-error">
			<p class="font-medium">Could not reach Google Cloud Storage</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadBuckets}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<section aria-label="Buckets" class="space-y-2">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Buckets</h2>
				{#if loading}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if buckets.length === 0}
					<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="gcs-no-buckets">
						<Box class="h-10 w-10 mb-2 opacity-30" />
						<p class="text-sm">No buckets in {project}</p>
						<p class="text-xs">Create a bucket to get started</p>
					</div>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
						{#each buckets as b (b.name)}
							<li class="flex items-center {current === b.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="gcs-bucket-row">
								<button class="flex-1 min-w-0 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectBucket(b.name)}>
									<span class="block truncate">{b.name}</span>
									<span class="block text-[11px] font-normal text-gray-500 dark:text-gray-400">{b.location} · {b.storageClass}{#if b.versioning} · versioned{/if}</span>
								</button>
								<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {b.name}" aria-label="Delete bucket {b.name}" onclick={() => removeBucket(b)}>
									<Trash2 class="h-4 w-4" />
								</button>
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<section aria-label="Objects" class="space-y-3 min-w-0">
				{#if !current}
					<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="gcs-select-hint">
						<HardDrive class="h-12 w-12 mb-3 opacity-30" />
						<p>Select a bucket to browse its objects</p>
					</div>
				{:else}
					<div class="flex flex-wrap items-center justify-between gap-2">
						<nav class="flex flex-wrap items-center gap-1 text-sm" aria-label="Object path" data-testid="gcs-breadcrumbs">
							<button class="font-medium text-blue-600 dark:text-blue-400 hover:underline" onclick={() => navigate('')}>{current}</button>
							{#each crumbs as c (c.prefix)}
								<ChevronRight class="h-3.5 w-3.5 text-gray-400" />
								<button class="text-blue-600 dark:text-blue-400 hover:underline" onclick={() => navigate(c.prefix)}>{c.label}</button>
							{/each}
						</nav>
						<div class="flex items-center gap-2">
							<input bind:this={fileInput} type="file" multiple class="hidden" data-testid="gcs-file-input" onchange={(e) => upload(Array.from(e.currentTarget.files ?? []))} />
							<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={() => fileInput?.click()} disabled={uploading}>
								<Upload class="h-4 w-4" /> {uploading ? 'Uploading…' : 'Upload'}
							</button>
						</div>
					</div>

					<details class="rounded-lg border border-gray-200 dark:border-gray-700 p-4" data-testid="gcs-bucket-settings">
						<summary class="cursor-pointer text-sm font-semibold text-gray-700 dark:text-gray-300">Bucket settings</summary>
						{#if bucket}
							<div class="mt-3 grid gap-6 md:grid-cols-2">
								<dl class="grid grid-cols-[max-content_1fr] content-start gap-x-6 gap-y-2 text-sm">
									<dt class={dt}>URI</dt>
									<dd class="{dd} flex items-center gap-2 font-mono text-xs">{gsUri(bucket.name)}<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy bucket URI" onclick={() => copy(gsUri(bucket?.name ?? ''))}><Copy class="h-3.5 w-3.5" /></button></dd>
									<dt class={dt}>Location</dt><dd class={dd}>{bucket.location} ({bucket.locationType || '—'})</dd>
									<dt class={dt}>Storage class</dt><dd class={dd}>{bucket.storageClass}</dd>
									<dt class={dt}>Created</dt><dd class={dd}>{bucket.timeCreated || '—'}</dd>
									<dt class={dt}>Metageneration</dt><dd class={dd}>{bucket.metageneration}</dd>
								</dl>
								<div class="space-y-2" data-testid="gcs-versioning">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Object versioning</h4>
									<label class="flex items-center gap-2 text-sm">
										<input type="checkbox" bind:checked={versioningChoice} data-testid="gcs-versioning-toggle" /> Keep noncurrent versions of overwritten or deleted objects
									</label>
									<button type="button" class={btn} disabled={saving === 'versioning' || versioningChoice === bucket.versioning} onclick={saveVersioning} data-testid="gcs-versioning-save">Save versioning</button>
								</div>
								<KeyValueEditor bind:rows={bucketLabels} label="Labels" testId="gcs-labels" validateKey={validateLabelKey} saving={saving === 'labels'} onsave={saveLabels} />
								<div class="space-y-2" data-testid="gcs-iam">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">IAM policy (read-only)</h4>
									{#if !iam || iam.bindings.length === 0}
										<p class="text-xs text-gray-500 dark:text-gray-400">No bindings</p>
									{:else}
										<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 text-xs">
											{#each iam.bindings as binding (binding.role)}
												<li class="px-3 py-2" data-testid="gcs-iam-binding">
													<p class="font-mono font-medium">{binding.role}</p>
													<p class="font-mono text-gray-500 dark:text-gray-400 break-all">{binding.members.join(', ')}</p>
												</li>
											{/each}
										</ul>
									{/if}
									{#if iam}<p class="text-[11px] text-gray-500 dark:text-gray-400">etag {iam.etag} · version {iam.version}</p>{/if}
								</div>
							</div>
						{:else}
							<div class="flex justify-center py-4"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
						{/if}
					</details>

					<div
						role="region"
						aria-label="Drop files to upload"
						class="rounded-lg border-2 border-dashed px-4 py-3 text-center text-xs {dragging ? 'border-blue-500 bg-blue-50 dark:bg-blue-950/40 text-blue-700 dark:text-blue-300' : 'border-gray-200 dark:border-gray-700 text-gray-500 dark:text-gray-400'}"
						data-testid="gcs-dropzone"
						ondragover={(e) => { e.preventDefault(); dragging = true; }}
						ondragleave={() => (dragging = false)}
						ondrop={onDrop}
					>
						Drag and drop files here to upload{prefix ? ` into ${prefix}` : ''}
					</div>

					{#if loadingObjects && entries.length === 0}
						<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
					{:else if entries.length === 0}
						<div class="flex flex-col items-center py-10 text-gray-500 dark:text-gray-400" data-testid="gcs-empty">
							<FileIcon class="h-10 w-10 mb-2 opacity-30" />
							<p class="text-sm">{prefix ? 'This folder is empty' : 'This bucket is empty'}</p>
							<p class="text-xs">Upload a file to create an object</p>
						</div>
					{:else}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
							<table class="w-full text-sm">
								<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
									<tr>
										<th class="px-4 py-3 font-medium">Name</th>
										<th class="px-4 py-3 font-medium">Size</th>
										<th class="px-4 py-3 font-medium">Content type</th>
										<th class="px-4 py-3 font-medium">Class</th>
										<th class="px-4 py-3 font-medium">Updated</th>
										<th class="px-4 py-3 text-right font-medium">Actions</th>
									</tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each entries as e (e.kind === 'folder' ? `d:${e.prefix}` : `o:${e.object.name}#${e.object.generation}`)}
										{#if e.kind === 'folder'}
											<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50" data-testid="gcs-folder-row" onclick={() => navigate(e.prefix)}>
												<td class="px-4 py-3 font-medium text-gray-900 dark:text-gray-100"><span class="flex items-center gap-2"><Folder class="h-4 w-4 text-amber-500" />{e.name}/</span></td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td class="px-4 py-3 text-gray-400">—</td>
												<td></td>
											</tr>
										{:else}
											<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {selected?.name === e.object.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="gcs-object-row" onclick={() => showObject(e.object)}>
												<td class="px-4 py-3 font-medium text-gray-900 dark:text-gray-100"><span class="flex items-center gap-2"><FileIcon class="h-4 w-4 text-sky-500" />{e.name}</span></td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{formatBytes(e.object.size)}</td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{e.object.contentType || '—'}</td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{e.object.storageClass || '—'}</td>
												<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{e.object.updated || '—'}</td>
												<td class="px-4 py-3 text-right whitespace-nowrap">
													<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" title="Download {e.name}" aria-label="Download {e.name}" onclick={(ev) => { ev.stopPropagation(); void download(e.object); }}><Download class="h-4 w-4" /></button>
													<button class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {e.name}" aria-label="Delete object {e.name}" onclick={(ev) => { ev.stopPropagation(); void removeObject(e.object); }}><Trash2 class="h-4 w-4" /></button>
												</td>
											</tr>
										{/if}
									{/each}
								</tbody>
							</table>
						</div>
						{#if nextToken}
							<div class="flex justify-center">
								<button class="rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={loadingObjects} onclick={() => loadObjects(true)} data-testid="gcs-load-more">
									{loadingObjects ? 'Loading…' : 'Load more'}
								</button>
							</div>
						{/if}
					{/if}

					{#if selected}
						{@const o = detail ?? selected}
						<section class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-4" data-testid="gcs-object-detail" aria-label="Details of {selected.name}">
							<div class="flex items-start justify-between gap-4">
								<div class="min-w-0">
									<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{selected.name}</h3>
									<p class="text-xs text-gray-500 dark:text-gray-400">Object details</p>
								</div>
								<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={clearObject} aria-label="Close details"><X class="h-4 w-4" /></button>
							</div>
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
								<dt class={dt}>URI</dt>
								<dd class="{dd} flex items-center gap-2 font-mono text-xs">
									{gsUri(current, o.name)}
									<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy object URI" onclick={() => copy(gsUri(current ?? '', o.name))}><Copy class="h-3.5 w-3.5" /></button>
								</dd>
								<dt class={dt}>Size</dt><dd class={dd} data-testid="gcs-detail-size">{formatBytes(o.size)} ({o.size} bytes)</dd>
								<dt class={dt}>Content type</dt><dd class={dd}>{o.contentType || '—'}</dd>
								<dt class={dt}>MD5 (base64)</dt><dd class="{dd} font-mono text-xs" data-testid="gcs-detail-md5">{o.md5Hash || '—'}</dd>
								<dt class={dt}>MD5 (hex)</dt><dd class="{dd} font-mono text-xs" data-testid="gcs-detail-md5hex">{base64ToHex(o.md5Hash) || '—'}</dd>
								<dt class={dt}>CRC32C</dt><dd class="{dd} font-mono text-xs" data-testid="gcs-detail-crc32c">{o.crc32c || '—'}</dd>
								<dt class={dt}>Generation</dt><dd class={dd} data-testid="gcs-detail-generation">{o.generation}</dd>
								<dt class={dt}>Metageneration</dt><dd class={dd} data-testid="gcs-detail-metageneration">{o.metageneration}</dd>
								<dt class={dt}>Storage class</dt><dd class={dd}>{o.storageClass || '—'}</dd>
								<dt class={dt}>Created</dt><dd class={dd}>{o.timeCreated || '—'}</dd>
								<dt class={dt}>Updated</dt><dd class={dd}>{o.updated || '—'}</dd>
								<dt class={dt}>ETag</dt><dd class="{dd} font-mono text-xs">{o.etag || '—'}</dd>
							</dl>
							{#if detail}
								<div class="grid gap-6 md:grid-cols-2">
									<div class="space-y-2" data-testid="gcs-content-props">
										<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Content properties</h4>
										<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Type<input class={input} bind:value={contentForm.contentType} aria-label="Content-Type" /></label>
										<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Encoding<input class={input} bind:value={contentForm.contentEncoding} aria-label="Content-Encoding" /></label>
										<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Language<input class={input} bind:value={contentForm.contentLanguage} aria-label="Content-Language" /></label>
										<label class="block text-xs text-gray-500 dark:text-gray-400">Content-Disposition<input class={input} bind:value={contentForm.contentDisposition} aria-label="Content-Disposition" /></label>
										<label class="block text-xs text-gray-500 dark:text-gray-400">Cache-Control<input class={input} bind:value={contentForm.cacheControl} aria-label="Cache-Control" /></label>
										<button type="button" class={btn} disabled={saving === 'content'} onclick={saveContent} data-testid="gcs-content-save">Save content properties</button>
									</div>
									<KeyValueEditor bind:rows={objectMeta} label="Custom metadata" testId="gcs-object-meta" validateKey={validateMetadataKey} saving={saving === 'object-meta'} onsave={saveObjectMeta} />
								</div>
							{/if}
							{#if bucket?.versioning}
								<div class="space-y-2" data-testid="gcs-versions">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Versions ({versions.length})</h4>
									{#if versions.length === 0}
										<p class="text-xs text-gray-500 dark:text-gray-400">No versions</p>
									{:else}
										<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
											{#each versions as v (v.generation)}
												<li class="flex items-center justify-between gap-2 px-3 py-2 text-xs" data-testid="gcs-version-row">
													<span class="font-mono break-all">
														{v.generation} · {formatBytes(v.size)} · {v.timeDeleted ? `noncurrent (deleted ${v.timeDeleted})` : 'live'}
													</span>
													<span class="whitespace-nowrap">
														<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" title="Download version" aria-label="Download generation {v.generation}" onclick={() => download(v, v.generation)}><Download class="h-4 w-4" /></button>
														<button class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete version" aria-label="Delete generation {v.generation}" onclick={() => removeObject(v, v.generation)}><Trash2 class="h-4 w-4" /></button>
													</span>
												</li>
											{/each}
										</ul>
									{/if}
								</div>
							{/if}
							<div class="flex gap-2">
								<button class="flex items-center gap-2 rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => download(o)} data-testid="gcs-detail-download"><Download class="h-4 w-4" /> Download</button>
								<a class="flex items-center gap-2 rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" href={downloadUrl(current, o.name)} target="_blank" rel="noopener" data-testid="gcs-detail-open">Open raw</a>
								<button class="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950" onclick={() => removeObject(o)} data-testid="gcs-detail-delete"><Trash2 class="h-4 w-4" /> Delete</button>
							</div>
						</section>
					{/if}
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={modal} title="New bucket" titleId="gcs-create-title">
	{#snippet children()}
		<form id="gcs-create-form" class="space-y-3" onsubmit={submit} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Name</span>
				<input class={input} bind:value={formName} placeholder="my-bucket" autocomplete="off" aria-label="Name" />
				{#if touched && nameError}<span class="block text-xs text-red-600 dark:text-red-400">{nameError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Location</span>
				<select class={input} bind:value={formLocation} aria-label="Location">
					{#each LOCATIONS as l (l)}<option value={l}>{l}</option>{/each}
				</select>
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Storage class</span>
				<select class={input} bind:value={formClass} aria-label="Storage class">
					{#each STORAGE_CLASSES as sc (sc)}<option value={sc}>{sc}</option>{/each}
				</select>
			</label>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={formVersioning} aria-label="Enable versioning" /> Enable object versioning
			</label>
			<KeyValueEditor bind:rows={formLabels} label="Labels" testId="gcs-create-labels" validateKey={validateLabelKey} hideSave />
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="gcs-create-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={creating} data-testid="gcs-create-submit">
			{creating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>
