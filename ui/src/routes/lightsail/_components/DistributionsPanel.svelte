<script lang="ts">
	// Distributions -- family T (10 ops), Lightsail's CloudFront-backed CDN.
	// Modeled as global (no region segment in its ARN), but its own SDK doc
	// comment confirms every distribution's Location always literally
	// reports "us-east-1" regardless of the backend's configured region
	// (services/lightsail/consts.go's distributionRegion) -- shown as-is,
	// not corrected to the current region.
	import {
		GetDistributionsCommand,
		CreateDistributionCommand,
		UpdateDistributionCommand,
		DeleteDistributionCommand,
		UpdateDistributionBundleCommand,
		ResetDistributionCacheCommand,
		GetDistributionLatestCacheResetCommand,
		AttachCertificateToDistributionCommand,
		DetachCertificateFromDistributionCommand,
		GetDistributionMetricDataCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type LightsailDistribution,
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

	let distributions = $state<LightsailDistribution[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchDistributions(reset: boolean): Promise<void> {
		const resp = await client().send(new GetDistributionsCommand({ pageToken: reset ? undefined : nextToken }));
		distributions = reset ? (resp.distributions ?? []) : [...distributions, ...(resp.distributions ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchDistributions(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchDistributions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		distributions.filter((d) => (d.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createOriginName = $state('');
	let createBundleId = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createOriginName = '';
		createBundleId = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateDistributionCommand({
					distributionName: createName,
					origin: { name: createOriginName, regionName: 'us-east-1' },
					defaultCacheBehavior: { behavior: 'cache' },
					bundleId: createBundleId
				})
			);
			toast.success('Distribution creation started');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteDistribution(d: LightsailDistribution): Promise<void> {
		if (!d.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete distribution',
			message: `Delete distribution ${d.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDistributionCommand({ distributionName: d.name }));
			toast.success('Distribution deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<LightsailDistribution | null>(null);
	let newBundleId = $state('');
	let attachCertName = $state('');
	let lastCacheReset = $state<string | null>(null);
	let metricsChecked = $state(false);
	let metricsEmpty = $state(true);

	function openDetail(d: LightsailDistribution): void {
		viewed = d;
		newBundleId = d.bundleId ?? '';
		attachCertName = '';
		lastCacheReset = null;
		metricsChecked = false;
		metricsEmpty = true;
		detailModal?.open();
	}

	async function toggleEnabled(enabled: boolean): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new UpdateDistributionCommand({ distributionName: viewed.name, isEnabled: enabled }));
			toast.success('Distribution updated');
			viewed = { ...viewed, isEnabled: enabled };
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function updateBundle(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new UpdateDistributionBundleCommand({ distributionName: viewed.name, bundleId: newBundleId }));
			toast.success('Distribution bundle updated');
			viewed = { ...viewed, bundleId: newBundleId };
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function resetCache(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new ResetDistributionCacheCommand({ distributionName: viewed.name }));
			toast.success('Cache reset requested');
			const resp = await client().send(new GetDistributionLatestCacheResetCommand({ distributionName: viewed.name }));
			lastCacheReset = resp.status ? `${resp.status} at ${formatDate(resp.createTime)}` : null;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function attachCert(): Promise<void> {
		if (!viewed?.name || !attachCertName) return;
		try {
			await client().send(new AttachCertificateToDistributionCommand({ distributionName: viewed.name, certificateName: attachCertName }));
			toast.success('Certificate attached');
			viewed = { ...viewed, certificateName: attachCertName };
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function detachCert(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new DetachCertificateFromDistributionCommand({ distributionName: viewed.name }));
			toast.success('Certificate detached');
			viewed = { ...viewed, certificateName: undefined };
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// GetDistributionMetricData always returns an honest empty MetricData
	// series in this emulator -- never a fabricated chart.
	async function checkMetrics(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(
				new GetDistributionMetricDataCommand({
					distributionName: viewed.name,
					metricName: 'Requests',
					period: 300,
					startTime: new Date(Date.now() - 3600_000),
					endTime: new Date(),
					unit: 'Count',
					statistics: ['Sum']
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
		Create distribution
	</button>
</div>

{#snippet createdCell(d: LightsailDistribution)}
	{formatDate(d.createdAt)}
{/snippet}
{#snippet rowActions(d: LightsailDistribution)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(d)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteDistribution(d)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(d) => d.name ?? ''}
	columns={defineColumns<LightsailDistribution>([
		{ key: 'name', label: 'Name' },
		{ key: 'domainName', label: 'Domain' },
		{ key: 'bundleId', label: 'Bundle' },
		{ key: 'status', label: 'Status' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No distributions found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create distribution">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-dist-name">
				Name
				<input id="ls-dist-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-dist-origin">
				Origin resource name (e.g. a bucket or instance name)
				<input id="ls-dist-origin" bind:value={createOriginName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-dist-bundle">
				Bundle ID -- see the Reference Data tab
				<input id="ls-dist-bundle" bind:value={createBundleId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createOriginName || !createBundleId} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Distribution {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Status</dt><dd>{viewed.status ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Enabled?</dt><dd>{viewed.isEnabled ? 'Yes' : 'No'}</dd></div>
					<div><dt class="text-slate-500">Location</dt><dd>{viewed.location?.regionName ?? '—'} (always us-east-1, real AWS behavior)</dd></div>
					<div><dt class="text-slate-500">Certificate</dt><dd>{viewed.certificateName ?? '(none)'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<div class="flex gap-2">
						<button onclick={() => toggleEnabled(true)} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Enable</button>
						<button onclick={() => toggleEnabled(false)} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Disable</button>
					</div>
					<div class="flex items-center gap-2">
						<input bind:value={newBundleId} aria-label="New bundle ID" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<button onclick={updateBundle} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Update bundle</button>
					</div>
					<div class="flex items-center gap-2">
						<button onclick={resetCache} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Reset cache</button>
						{#if lastCacheReset}<span class="text-xs text-slate-500">{lastCacheReset}</span>{/if}
					</div>
					<div class="flex items-center gap-2">
						<input bind:value={attachCertName} placeholder="Certificate name" aria-label="Certificate to attach" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<button onclick={attachCert} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Attach cert</button>
						<button onclick={detachCert} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Detach cert</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Metrics</p>
					<button onclick={checkMetrics} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Check requests metric</button>
					{#if metricsChecked && metricsEmpty}
						<p class="text-xs text-slate-500">
							This emulator does not produce real metric datapoints -- GetDistributionMetricData returned an honest empty series.
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
