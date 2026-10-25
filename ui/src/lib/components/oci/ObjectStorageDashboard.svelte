<script lang="ts">
	import { onMount } from 'svelte';
	import { Archive, Download, Folder, RefreshCw, Trash2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { formatBytes } from '#lib/format.ts';
	import { attempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		DELIMITER,
		deleteBucket,
		deleteObject,
		getLifecycle,
		getNamespace,
		listBuckets,
		listObjects,
		listPars,
		listRetention,
		objectDownloadUrl,
		prefixCrumbs,
		type BucketInfo,
		type Lifecycle,
		type ObjectEntry,
		type ObjectSummary,
		type ParInfo,
		type RetentionRule
	} from '#lib/ociobjectstorage.ts';

	const TABS = [
		{ id: 'objects', label: 'Objects' },
		{ id: 'pars', label: 'Pre-authenticated requests' },
		{ id: 'policies', label: 'Lifecycle & retention' }
	];

	let namespace = $state('');
	let buckets = $state<BucketInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let tab = $state('objects');
	let prefix = $state('');
	let entries = $state<ObjectEntry[]>([]);
	let nextStart = $state('');
	let childLoading = $state(false);
	let childError = $state('');
	let detail = $state<ObjectSummary | null>(null);
	let pars = $state<ParInfo[]>([]);
	let lifecycle = $state<Lifecycle>({ configured: false, rules: [] });
	let retention = $state<RetentionRule[]>([]);

	async function load() {
		try {
			[namespace, buckets] = await Promise.all([getNamespace(), listBuckets()]);
			error = '';
			if (!buckets.some((b) => b.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadObjects(append = false) {
		childLoading = true;
		childError = '';
		try {
			const page = await listObjects(selected, { prefix, start: append ? nextStart : undefined });
			entries = append ? [...entries, ...page.entries] : page.entries;
			nextStart = page.nextStartWith;
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	async function loadPolicies() {
		childLoading = true;
		childError = '';
		try {
			[pars, lifecycle, retention] = await Promise.all([listPars(selected), getLifecycle(selected), listRetention(selected)]);
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	function refreshChild() {
		detail = null;
		if (!selected) return;
		if (tab === 'objects') void loadObjects();
		else void loadPolicies();
	}

	function selectBucket(row: ResourceRow) {
		selected = selected === row.id ? '' : row.id;
		prefix = '';
		tab = 'objects';
		entries = [];
		refreshChild();
	}

	function selectTab(id: string) {
		tab = id;
		refreshChild();
	}

	function openFolder(p: string) {
		prefix = p;
		entries = [];
		refreshChild();
	}

	async function removeBucket(row: ResourceRow) {
		if (!(await confirmDestructive({ title: 'Delete bucket', message: `Delete bucket "${row.id}"? It must be empty.` }))) return;
		if (await attempt(() => deleteBucket(row.id), `Deleted ${row.id}`, `Failed to delete ${row.id}`)) {
			if (selected === row.id) selected = '';
			await load();
		}
	}

	async function removeObject(o: ObjectSummary) {
		if (!(await confirmDestructive({ title: 'Delete object', message: `Delete object "${o.name}"?` }))) return;
		if (await attempt(() => deleteObject(selected, o.name), `Deleted ${o.name}`, `Failed to delete ${o.name}`)) {
			detail = null;
			await loadObjects();
			await load();
		}
	}

	const rows = $derived(
		buckets.map((b) => ({
			id: b.name,
			cells: [b.name, b.storageTier, b.publicAccessType, b.versioning, `${b.objectCount} objects, ${formatBytes(b.sizeBytes)}`],
			raw: b.raw
		}))
	);
	const crumbs = $derived(prefixCrumbs(prefix));
	const cell = 'px-4 py-2';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="oci-os-dashboard">
	<PageHeader icon={Archive} title="Object Storage" description={`Oracle Cloud Object Storage${namespace ? ` (namespace ${namespace})` : ''}: buckets, objects, pre-authenticated requests and policies`} color="red" onRefresh={load} />
	<GcpResourceTable testId="oci-bucket" noun="buckets" columns={['Bucket', 'Tier', 'Public access', 'Versioning', 'Contents']} {rows} {loading} {error} {selected} onRetry={load} onSelect={selectBucket} onDelete={removeBucket} />

	{#if selected}
		<div class="space-y-3" data-testid="oci-os-detail">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selected}</h2>
			<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="red" />
			{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="oci-os-error">{childError}</p>{/if}

			{#if tab === 'objects'}
				<nav class="flex flex-wrap items-center gap-1 text-sm" aria-label="Prefix" data-testid="oci-os-crumbs">
					<button class="underline decoration-dotted" onclick={() => openFolder('')}>{selected}</button>
					{#each crumbs as c (c.prefix)}
						<span>{DELIMITER}</span><button class="underline decoration-dotted" onclick={() => openFolder(c.prefix)}>{c.label}</button>
					{/each}
				</nav>
				{#if childLoading && entries.length === 0}
					<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
				{:else if entries.length === 0}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="oci-os-empty">No objects{prefix ? ` under ${prefix}` : ''}</p>
				{:else}
					<table class="w-full text-sm" data-testid="oci-os-objects">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Size</th><th class={cell}>Tier</th><th class={cell}>Modified</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each entries as e (e.kind === 'folder' ? `d:${e.prefix}` : `o:${e.object.name}`)}
								{#if e.kind === 'folder'}
									<tr data-testid="oci-os-folder">
										<td class="{cell} font-mono text-xs" colspan="5"><button class="inline-flex items-center gap-1 underline decoration-dotted" onclick={() => openFolder(e.prefix)} data-testid="oci-os-open-folder"><Folder class="h-3.5 w-3.5" />{e.name}/</button></td>
									</tr>
								{:else}
									<tr data-testid="oci-os-object">
										<td class="{cell} font-mono text-xs break-all">{e.name}</td>
										<td class="{cell} text-xs">{formatBytes(e.object.size)}</td>
										<td class="{cell} text-xs">{e.object.storageTier || 'Standard'}</td>
										<td class="{cell} text-xs">{e.object.timeModified}</td>
										<td class="{cell} text-right whitespace-nowrap space-x-1">
											<button class={GCP_MINI} onclick={() => (detail = detail?.name === e.object.name ? null : e.object)} data-testid="oci-os-details">Details</button>
											<a class="{GCP_MINI} inline-block" href={objectDownloadUrl(selected, e.object.name)} download data-testid="oci-os-download"><Download class="inline h-3.5 w-3.5" /> Download</a>
											<button class="{GCP_MINI} text-red-600" onclick={() => removeObject(e.object)} aria-label="Delete {e.object.name}" data-testid="oci-os-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
										</td>
									</tr>
									{#if detail?.name === e.object.name}
										<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="oci-os-object-detail">
											<td colspan="5" class="px-4 py-3 text-xs">
												<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
													<dt class="font-medium text-gray-500 dark:text-gray-400">Content type</dt><dd>{detail.contentType || '—'}</dd>
													<dt class="font-medium text-gray-500 dark:text-gray-400">ETag</dt><dd class="font-mono">{detail.etag}</dd>
													<dt class="font-medium text-gray-500 dark:text-gray-400">MD5</dt><dd class="font-mono">{detail.md5}</dd>
													<dt class="font-medium text-gray-500 dark:text-gray-400">Version</dt><dd class="font-mono">{detail.versionId || '—'}</dd>
													<dt class="font-medium text-gray-500 dark:text-gray-400">Created</dt><dd>{detail.timeCreated}</dd>
													<dt class="font-medium text-gray-500 dark:text-gray-400">Archival</dt><dd>{detail.archivalState || 'n/a'}</dd>
													<dt class="font-medium text-gray-500 dark:text-gray-400">Metadata</dt><dd class="font-mono">{Object.keys(detail.metadata).length ? JSON.stringify(detail.metadata) : '—'}</dd>
												</dl>
											</td>
										</tr>
									{/if}
								{/if}
							{/each}
						</tbody>
					</table>
					{#if nextStart}
						<button class={GCP_MINI} onclick={() => loadObjects(true)} disabled={childLoading} data-testid="oci-os-more">Load more</button>
					{/if}
				{/if}
			{:else if childLoading}
				<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
			{:else if tab === 'pars'}
				{#if pars.length === 0}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="oci-os-pars-empty">No pre-authenticated requests</p>
				{:else}
					<table class="w-full text-sm" data-testid="oci-os-pars">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Access</th><th class={cell}>Object</th><th class={cell}>Expires</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each pars as p (p.id)}
								<tr data-testid="oci-os-par"><td class="{cell} text-xs">{p.name}</td><td class="{cell} text-xs">{p.accessType}</td><td class="{cell} font-mono text-xs">{p.objectName || '(bucket)'}</td><td class="{cell} text-xs">{p.timeExpires}</td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{:else}
				<h3 class="font-medium text-gray-900 dark:text-white">Lifecycle rules</h3>
				{#if lifecycle.rules.length === 0}
					<p class="py-2 text-gray-500 dark:text-gray-400" data-testid="oci-os-lifecycle-empty">No lifecycle policy</p>
				{:else}
					<table class="w-full text-sm" data-testid="oci-os-lifecycle">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Rule</th><th class={cell}>Action</th><th class={cell}>After</th><th class={cell}>Applies to</th><th class={cell}>Enabled</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each lifecycle.rules as r (r.name)}
								<tr data-testid="oci-os-lifecycle-rule"><td class="{cell} text-xs">{r.name}</td><td class="{cell} text-xs">{r.action}</td><td class="{cell} text-xs">{r.amount} {r.unit.toLowerCase()}</td><td class="{cell} text-xs">{r.target}{r.prefixes.length ? ` (${r.prefixes.join(', ')})` : ''}</td><td class="{cell} text-xs">{r.enabled ? 'yes' : 'no'}</td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
				<h3 class="mt-4 font-medium text-gray-900 dark:text-white">Retention rules</h3>
				{#if retention.length === 0}
					<p class="py-2 text-gray-500 dark:text-gray-400" data-testid="oci-os-retention-empty">No retention rules</p>
				{:else}
					<table class="w-full text-sm" data-testid="oci-os-retention">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Duration</th><th class={cell}>Locked</th><th class={cell}>Created</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each retention as r (r.id)}
								<tr data-testid="oci-os-retention-rule"><td class="{cell} text-xs">{r.displayName}</td><td class="{cell} text-xs">{r.duration}</td><td class="{cell} text-xs">{r.locked ? 'yes' : 'no'}</td><td class="{cell} text-xs">{r.timeCreated}</td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{/if}
		</div>
	{/if}
</div>
