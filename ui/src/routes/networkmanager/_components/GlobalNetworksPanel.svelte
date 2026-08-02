<script lang="ts">
	// Global Networks -- the root container every other Global-Networks-side
	// resource (Site/Device/Link/Connection/association) is scoped under via
	// GlobalNetworkId (services/networkmanager/PARITY.md family A, 4 ops).
	import {
		DescribeGlobalNetworksCommand,
		CreateGlobalNetworkCommand,
		UpdateGlobalNetworkCommand,
		DeleteGlobalNetworkCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type GlobalNetwork,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let networks = $state<GlobalNetwork[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchNetworks(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeGlobalNetworksCommand({ MaxResults: 50, NextToken: reset ? undefined : nextToken })
		);
		networks = reset ? (resp.GlobalNetworks ?? []) : [...networks, ...(resp.GlobalNetworks ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchNetworks(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchNetworks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		networks.filter((n) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(n.GlobalNetworkId ?? '').toLowerCase().includes(q) ||
				(n.Description ?? '').toLowerCase().includes(q) ||
				(n.State ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createDescription = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createDescription = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(new CreateGlobalNetworkCommand({ Description: createDescription || undefined }));
			toast.success('Global network created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteNetwork(n: GlobalNetwork): Promise<void> {
		if (!n.GlobalNetworkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete global network',
			message: `Delete global network ${n.GlobalNetworkId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteGlobalNetworkCommand({ GlobalNetworkId: n.GlobalNetworkId }));
			toast.success('Global network deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<GlobalNetwork | null>(null);
	let editDescription = $state('');
	let detailBusy = $state(false);
	let detailError = $state<string | null>(null);

	function openDetail(n: GlobalNetwork): void {
		viewed = n;
		editDescription = n.Description ?? '';
		detailError = null;
		detailModal?.open();
	}

	async function saveDescription(): Promise<void> {
		if (!viewed?.GlobalNetworkId) return;
		detailBusy = true;
		detailError = null;
		try {
			const resp = await client().send(
				new UpdateGlobalNetworkCommand({
					GlobalNetworkId: viewed.GlobalNetworkId,
					Description: editDescription || undefined
				})
			);
			viewed = resp.GlobalNetwork ?? viewed;
			toast.success('Global network updated');
			await refresh();
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailBusy = false;
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.GlobalNetworkId) return;
		const arn = taggableArn('global-network', viewed.GlobalNetworkId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.GlobalNetworkId) return;
		const arn = taggableArn('global-network', viewed.GlobalNetworkId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}

	function stateClass(state: string | undefined): string {
		if (state === 'AVAILABLE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'DELETING') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
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
		Create global network
	</button>
</div>

{#snippet stateCell(n: GlobalNetwork)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(n.State)}">{n.State ?? '—'}</span>
{/snippet}
{#snippet createdCell(n: GlobalNetwork)}
	{formatDate(n.CreatedAt)}
{/snippet}
{#snippet rowActions(n: GlobalNetwork)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(n)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteNetwork(n)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(n) => n.GlobalNetworkId ?? ''}
	columns={defineColumns<GlobalNetwork>([
		{ key: 'GlobalNetworkId', label: 'ID' },
		{ key: 'Description', label: 'Description' },
		{ key: 'State', label: 'State', render: stateCell },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No global networks found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create global network">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-gn-desc">
				Description
				<input
					id="nm-gn-desc"
					bind:value={createDescription}
					class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Global network {viewed?.GlobalNetworkId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.GlobalNetworkArn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<label class="flex flex-col gap-1 text-sm" for="nm-gn-edit-desc">
						Description
						<input
							id="nm-gn-edit-desc"
							bind:value={editDescription}
							class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</label>
					<button onclick={saveDescription} disabled={detailBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
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
