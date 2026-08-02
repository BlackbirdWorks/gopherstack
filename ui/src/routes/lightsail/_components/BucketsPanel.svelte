<script lang="ts">
	// Buckets -- family S (10 ops). Lightsail's own object storage product,
	// distinct from and NOT backed by this repo's real services/s3
	// (services/lightsail/models.go's Bucket doc comment).
	import {
		GetBucketsCommand,
		CreateBucketCommand,
		DeleteBucketCommand,
		UpdateBucketCommand,
		UpdateBucketBundleCommand,
		SetResourceAccessForBucketCommand,
		GetBucketAccessKeysCommand,
		CreateBucketAccessKeyCommand,
		DeleteBucketAccessKeyCommand,
		GetBucketMetricDataCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Bucket,
		type AccessKey,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, tagsToRecord } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let buckets = $state<Bucket[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchBuckets(reset: boolean): Promise<void> {
		const resp = await client().send(new GetBucketsCommand({ pageToken: reset ? undefined : nextToken }));
		buckets = reset ? (resp.buckets ?? []) : [...buckets, ...(resp.buckets ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchBuckets(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchBuckets(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		buckets.filter((b) => (b.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createBundleId = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createBundleId = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(new CreateBucketCommand({ bucketName: createName, bundleId: createBundleId }));
			toast.success('Bucket created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteBucket(b: Bucket): Promise<void> {
		if (!b.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete bucket',
			message: `Delete bucket ${b.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteBucketCommand({ bucketName: b.name, forceDelete: true }));
			toast.success('Bucket deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Bucket | null>(null);
	let accessKeys = $state<AccessKey[]>([]);
	let newBundleId = $state('');
	let resourceAccessName = $state('');
	let metricsChecked = $state(false);
	let metricsEmpty = $state(true);

	async function openDetail(b: Bucket): Promise<void> {
		viewed = b;
		accessKeys = [];
		newBundleId = b.bundleId ?? '';
		resourceAccessName = '';
		metricsChecked = false;
		metricsEmpty = true;
		detailModal?.open();
		if (b.name) {
			try {
				const resp = await client().send(new GetBucketAccessKeysCommand({ bucketName: b.name }));
				accessKeys = resp.accessKeys ?? [];
			} catch (e) {
				toast.error(describeError(e));
			}
		}
	}

	async function updateBundle(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new UpdateBucketBundleCommand({ bucketName: viewed.name, bundleId: newBundleId }));
			toast.success('Bucket bundle updated');
			viewed = { ...viewed, bundleId: newBundleId };
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function toggleVersioning(enabled: boolean): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new UpdateBucketCommand({ bucketName: viewed.name, versioning: enabled ? 'Enabled' : 'Suspended' })
			);
			toast.success('Bucket versioning updated');
			viewed = { ...viewed, objectVersioning: enabled ? 'Enabled' : 'Suspended' };
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function grantResourceAccess(): Promise<void> {
		if (!viewed?.name || !resourceAccessName) return;
		try {
			await client().send(
				new SetResourceAccessForBucketCommand({ bucketName: viewed.name, resourceName: resourceAccessName, access: 'allow' })
			);
			toast.success('Resource access granted');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function createAccessKey(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(new CreateBucketAccessKeyCommand({ bucketName: viewed.name }));
			toast.success(`Access key created -- secret shown once: ${resp.accessKey?.secretAccessKey ?? ''}`);
			const list = await client().send(new GetBucketAccessKeysCommand({ bucketName: viewed.name }));
			accessKeys = list.accessKeys ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteAccessKey(k: AccessKey): Promise<void> {
		if (!viewed?.name || !k.accessKeyId) return;
		try {
			await client().send(new DeleteBucketAccessKeyCommand({ bucketName: viewed.name, accessKeyId: k.accessKeyId }));
			toast.success('Access key deleted');
			accessKeys = accessKeys.filter((a) => a.accessKeyId !== k.accessKeyId);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// GetBucketMetricData is a real emulator call that always returns an
	// empty MetricData series (no real underlying telemetry signal) -- shown
	// as an honest empty state, never a fabricated chart.
	async function checkMetrics(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(
				new GetBucketMetricDataCommand({
					bucketName: viewed.name,
					metricName: 'BucketSizeBytes',
					startTime: new Date(Date.now() - 3600_000),
					endTime: new Date(),
					period: 300,
					statistics: ['Average'],
					unit: 'Bytes'
				})
			);
			metricsEmpty = (resp.metricData ?? []).length === 0;
			metricsChecked = true;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new TagResourceCommand({ resourceName: viewed.name, tags: [{ key, value }] }));
		viewed = { ...viewed, tags: [...(viewed.tags ?? []).filter((t) => t.key !== key), { key, value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new UntagResourceCommand({ resourceName: viewed.name, tagKeys: [key] }));
		viewed = { ...viewed, tags: (viewed.tags ?? []).filter((t) => t.key !== key) };
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create bucket
	</button>
</div>

{#snippet createdCell(b: Bucket)}
	{formatDate(b.createdAt)}
{/snippet}
{#snippet stateCell(b: Bucket)}
	{b.state?.code ?? '—'}
{/snippet}
{#snippet rowActions(b: Bucket)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(b)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteBucket(b)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(b) => b.name ?? ''}
	columns={defineColumns<Bucket>([
		{ key: 'name', label: 'Name' },
		{ key: 'bundleId', label: 'Bundle' },
		{ key: 'url', label: 'URL' },
		{ key: 'state', label: 'State', render: stateCell },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No buckets found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create bucket">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-bucket-name">
				Name
				<input id="ls-bucket-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-bucket-bundle">
				Bundle ID -- see the Reference Data tab for valid values
				<input id="ls-bucket-bundle" bind:value={createBundleId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createBundleId} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Bucket {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">URL</dt><dd class="break-all">{viewed.url ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Versioning</dt><dd>{viewed.objectVersioning ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Bundle</p>
					<div class="flex items-center gap-2">
						<input bind:value={newBundleId} aria-label="New bundle ID" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<button onclick={updateBundle} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Update bundle</button>
					</div>
					<div class="flex gap-2">
						<button onclick={() => toggleVersioning(true)} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Enable versioning</button>
						<button onclick={() => toggleVersioning(false)} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Suspend versioning</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Grant another resource access to this bucket</p>
					<div class="flex items-center gap-2">
						<input bind:value={resourceAccessName} placeholder="Resource name (e.g. instance name)" aria-label="Resource name" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 flex-1" />
						<button onclick={grantResourceAccess} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Grant</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Access keys</p>
					<button onclick={createAccessKey} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Create access key</button>
					<ul class="text-sm space-y-1">
						{#each accessKeys as k (k.accessKeyId)}
							<li class="flex items-center justify-between">
								<span>{k.accessKeyId} ({k.status})</span>
								<button onclick={() => deleteAccessKey(k)} class="text-red-600 hover:underline text-xs">Delete</button>
							</li>
						{:else}
							<li class="text-slate-500">No access keys</li>
						{/each}
					</ul>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Metrics</p>
					<button onclick={checkMetrics} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Check bucket size metric</button>
					{#if metricsChecked && metricsEmpty}
						<p class="text-xs text-slate-500">
							This emulator does not produce real metric datapoints -- GetBucketMetricData returned an honest empty series.
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={tagsToRecord(viewed.tags)} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
