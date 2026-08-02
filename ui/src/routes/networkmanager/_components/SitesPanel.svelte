<script lang="ts">
	// Sites within a Global Network (services/networkmanager/PARITY.md family
	// B, 4 ops).
	import {
		GetSitesCommand,
		CreateSiteCommand,
		UpdateSiteCommand,
		DeleteSiteCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Site,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import GlobalNetworkSelect from './GlobalNetworkSelect.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let globalNetworkId = $state('');
	let sites = $state<Site[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchSites(reset: boolean): Promise<void> {
		if (!globalNetworkId) {
			sites = [];
			return;
		}
		const resp = await client().send(
			new GetSitesCommand({
				GlobalNetworkId: globalNetworkId,
				MaxResults: 50,
				NextToken: reset ? undefined : nextToken
			})
		);
		sites = reset ? (resp.Sites ?? []) : [...sites, ...(resp.Sites ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchSites(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchSites(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	// Reacts to the global network picker's own onRegionChange-driven load
	// (which sets `globalNetworkId`) as well as the user choosing a
	// different one -- no separate onRegionChange is needed here since every
	// path that can change `globalNetworkId` already funnels through this.
	$effect(() => {
		void refresh();
	});

	const filtered = $derived(
		sites.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(s.SiteId ?? '').toLowerCase().includes(q) ||
				(s.Description ?? '').toLowerCase().includes(q) ||
				(s.Location?.Address ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createDescription = $state('');
	let createAddress = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createDescription = '';
		createAddress = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!globalNetworkId) return;
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateSiteCommand({
					GlobalNetworkId: globalNetworkId,
					Description: createDescription || undefined,
					Location: createAddress ? { Address: createAddress } : undefined
				})
			);
			toast.success('Site created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteSite(s: Site): Promise<void> {
		if (!s.SiteId || !globalNetworkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete site',
			message: `Delete site ${s.SiteId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSiteCommand({ GlobalNetworkId: globalNetworkId, SiteId: s.SiteId }));
			toast.success('Site deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Site | null>(null);
	let editDescription = $state('');
	let editAddress = $state('');
	let detailBusy = $state(false);
	let detailError = $state<string | null>(null);

	function openDetail(s: Site): void {
		viewed = s;
		editDescription = s.Description ?? '';
		editAddress = s.Location?.Address ?? '';
		detailError = null;
		detailModal?.open();
	}

	async function saveSite(): Promise<void> {
		if (!viewed?.SiteId || !globalNetworkId) return;
		detailBusy = true;
		detailError = null;
		try {
			const resp = await client().send(
				new UpdateSiteCommand({
					GlobalNetworkId: globalNetworkId,
					SiteId: viewed.SiteId,
					Description: editDescription || undefined,
					Location: editAddress ? { Address: editAddress } : undefined
				})
			);
			viewed = resp.Site ?? viewed;
			toast.success('Site updated');
			await refresh();
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailBusy = false;
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.SiteId) return;
		const arn = taggableArn('site', viewed.SiteId, globalNetworkId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.SiteId) return;
		const arn = taggableArn('site', viewed.SiteId, globalNetworkId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}
</script>

<div class="flex flex-wrap items-end justify-between gap-3">
	<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-sites-gn" />
	<button
		onclick={openCreate}
		disabled={!globalNetworkId}
		class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
	>
		Create site
	</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet addressCell(s: Site)}
	{s.Location?.Address ?? '—'}
{/snippet}
{#snippet createdCell(s: Site)}
	{formatDate(s.CreatedAt)}
{/snippet}
{#snippet rowActions(s: Site)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(s)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteSite(s)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(s) => s.SiteId ?? ''}
	columns={defineColumns<Site>([
		{ key: 'SiteId', label: 'ID' },
		{ key: 'Description', label: 'Description' },
		{ key: 'Address', label: 'Address', render: addressCell },
		{ key: 'State', label: 'State' },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No sites found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create site">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-site-desc">
				Description
				<input id="nm-site-desc" bind:value={createDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-site-addr">
				Address
				<input id="nm-site-addr" bind:value={createAddress} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Site {viewed?.SiteId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.SiteArn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<label class="flex flex-col gap-1 text-sm" for="nm-site-edit-desc">
						Description
						<input id="nm-site-edit-desc" bind:value={editDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<label class="flex flex-col gap-1 text-sm" for="nm-site-edit-addr">
						Address
						<input id="nm-site-edit-addr" bind:value={editAddress} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<button onclick={saveSite} disabled={detailBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
					{#if detailError}<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>{/if}
				</div>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={viewed.Tags ?? []} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
