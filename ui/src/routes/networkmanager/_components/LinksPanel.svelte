<script lang="ts">
	// Links within a Global Network (services/networkmanager/PARITY.md family
	// D, 4 ops). Every link belongs to a Site (see the Sites tab for IDs).
	import {
		GetLinksCommand,
		CreateLinkCommand,
		UpdateLinkCommand,
		DeleteLinkCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Link,
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
	let links = $state<Link[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchLinks(reset: boolean): Promise<void> {
		if (!globalNetworkId) {
			links = [];
			return;
		}
		const resp = await client().send(
			new GetLinksCommand({
				GlobalNetworkId: globalNetworkId,
				MaxResults: 50,
				NextToken: reset ? undefined : nextToken
			})
		);
		links = reset ? (resp.Links ?? []) : [...links, ...(resp.Links ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchLinks(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchLinks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	$effect(() => {
		void refresh();
	});

	const filtered = $derived(
		links.filter((l) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(l.LinkId ?? '').toLowerCase().includes(q) ||
				(l.Description ?? '').toLowerCase().includes(q) ||
				(l.Provider ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createSiteId = $state('');
	let createDescription = $state('');
	let createType = $state('');
	let createProvider = $state('');
	let createUploadSpeed = $state(100);
	let createDownloadSpeed = $state(100);
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createSiteId = '';
		createDescription = '';
		createType = '';
		createProvider = '';
		createUploadSpeed = 100;
		createDownloadSpeed = 100;
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!globalNetworkId || !createSiteId.trim()) {
			createError = 'Site ID is required.';
			return;
		}
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateLinkCommand({
					GlobalNetworkId: globalNetworkId,
					SiteId: createSiteId.trim(),
					Bandwidth: { UploadSpeed: createUploadSpeed, DownloadSpeed: createDownloadSpeed },
					Description: createDescription || undefined,
					Type: createType || undefined,
					Provider: createProvider || undefined
				})
			);
			toast.success('Link created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteLink(l: Link): Promise<void> {
		if (!l.LinkId || !globalNetworkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete link',
			message: `Delete link ${l.LinkId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLinkCommand({ GlobalNetworkId: globalNetworkId, LinkId: l.LinkId }));
			toast.success('Link deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Link | null>(null);
	let editDescription = $state('');
	let editProvider = $state('');
	let detailBusy = $state(false);
	let detailError = $state<string | null>(null);

	function openDetail(l: Link): void {
		viewed = l;
		editDescription = l.Description ?? '';
		editProvider = l.Provider ?? '';
		detailError = null;
		detailModal?.open();
	}

	async function saveLink(): Promise<void> {
		if (!viewed?.LinkId || !globalNetworkId) return;
		detailBusy = true;
		detailError = null;
		try {
			const resp = await client().send(
				new UpdateLinkCommand({
					GlobalNetworkId: globalNetworkId,
					LinkId: viewed.LinkId,
					Description: editDescription || undefined,
					Provider: editProvider || undefined
				})
			);
			viewed = resp.Link ?? viewed;
			toast.success('Link updated');
			await refresh();
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailBusy = false;
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.LinkId) return;
		const arn = taggableArn('link', viewed.LinkId, globalNetworkId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.LinkId) return;
		const arn = taggableArn('link', viewed.LinkId, globalNetworkId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}

	function bandwidthLabel(l: Link): string {
		if (!l.Bandwidth) return '—';
		return `${l.Bandwidth.UploadSpeed ?? '?'} / ${l.Bandwidth.DownloadSpeed ?? '?'} Mbps (up/down)`;
	}
</script>

<div class="flex flex-wrap items-end justify-between gap-3">
	<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-links-gn" />
	<button
		onclick={openCreate}
		disabled={!globalNetworkId}
		class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
	>
		Create link
	</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet bwCell(l: Link)}
	{bandwidthLabel(l)}
{/snippet}
{#snippet createdCell(l: Link)}
	{formatDate(l.CreatedAt)}
{/snippet}
{#snippet rowActions(l: Link)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(l)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteLink(l)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(l) => l.LinkId ?? ''}
	columns={defineColumns<Link>([
		{ key: 'LinkId', label: 'ID' },
		{ key: 'SiteId', label: 'Site' },
		{ key: 'Provider', label: 'Provider' },
		{ key: 'Bandwidth', label: 'Bandwidth', render: bwCell },
		{ key: 'State', label: 'State' },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No links found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create link">
	{#snippet children()}
		<div class="grid grid-cols-2 gap-3">
			<label class="flex flex-col gap-1 text-sm col-span-2" for="nm-link-site">
				Site ID *
				<input id="nm-link-site" bind:value={createSiteId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm col-span-2" for="nm-link-desc">
				Description
				<input id="nm-link-desc" bind:value={createDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-link-type">
				Type
				<input id="nm-link-type" bind:value={createType} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-link-provider">
				Provider
				<input id="nm-link-provider" bind:value={createProvider} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-link-up">
				Upload speed (Mbps)
				<input id="nm-link-up" type="number" min="0" bind:value={createUploadSpeed} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-link-down">
				Download speed (Mbps)
				<input id="nm-link-down" type="number" min="0" bind:value={createDownloadSpeed} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="col-span-2 text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Link {viewed?.LinkId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.LinkArn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Bandwidth</dt><dd>{bandwidthLabel(viewed)}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<label class="flex flex-col gap-1 text-sm" for="nm-link-edit-desc">
						Description
						<input id="nm-link-edit-desc" bind:value={editDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<label class="flex flex-col gap-1 text-sm" for="nm-link-edit-provider">
						Provider
						<input id="nm-link-edit-provider" bind:value={editProvider} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<button onclick={saveLink} disabled={detailBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
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
