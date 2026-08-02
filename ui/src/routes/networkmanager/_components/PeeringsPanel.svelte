<script lang="ts">
	// Peerings (services/networkmanager/PARITY.md family R, 4 ops). PeeringType
	// currently has exactly one real value, TRANSIT_GATEWAY, so create/get use
	// the transit-gateway-specific ops while delete/list are generic across
	// peering types (future-proofing for a peering kind that doesn't exist
	// yet in this SDK version).
	import {
		ListPeeringsCommand,
		CreateTransitGatewayPeeringCommand,
		GetTransitGatewayPeeringCommand,
		DeletePeeringCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Peering,
		type TransitGatewayPeering,
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

	let peerings = $state<Peering[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchPeerings(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListPeeringsCommand({ MaxResults: 50, NextToken: reset ? undefined : nextToken })
		);
		peerings = reset ? (resp.Peerings ?? []) : [...peerings, ...(resp.Peerings ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchPeerings(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchPeerings(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		peerings.filter((p) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(p.PeeringId ?? '').toLowerCase().includes(q) ||
				(p.CoreNetworkId ?? '').toLowerCase().includes(q) ||
				(p.EdgeLocation ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createCoreNetworkId = $state('');
	let createTransitGatewayArn = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createCoreNetworkId = '';
		createTransitGatewayArn = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!createCoreNetworkId.trim() || !createTransitGatewayArn.trim()) {
			createError = 'Core network ID and transit gateway ARN are required.';
			return;
		}
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateTransitGatewayPeeringCommand({
					CoreNetworkId: createCoreNetworkId.trim(),
					TransitGatewayArn: createTransitGatewayArn.trim()
				})
			);
			toast.success('Transit gateway peering created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deletePeering(p: Peering): Promise<void> {
		if (!p.PeeringId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete peering',
			message: `Delete peering ${p.PeeringId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeletePeeringCommand({ PeeringId: p.PeeringId }));
			toast.success('Peering deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Peering | null>(null);
	let viewedDetail = $state<TransitGatewayPeering | null>(null);
	let detailError = $state<string | null>(null);

	async function openDetail(p: Peering): Promise<void> {
		if (!p.PeeringId) return;
		viewed = p;
		viewedDetail = null;
		detailError = null;
		detailModal?.open();
		if (p.PeeringType === 'TRANSIT_GATEWAY') {
			try {
				const resp = await client().send(new GetTransitGatewayPeeringCommand({ PeeringId: p.PeeringId }));
				viewedDetail = resp.TransitGatewayPeering ?? null;
			} catch (e) {
				detailError = describeError(e);
			}
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.PeeringId) return;
		const arn = taggableArn('peering', viewed.PeeringId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.PeeringId) return;
		const arn = taggableArn('peering', viewed.PeeringId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
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
		Create transit gateway peering
	</button>
</div>

{#snippet createdCell(p: Peering)}
	{formatDate(p.CreatedAt)}
{/snippet}
{#snippet rowActions(p: Peering)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(p)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deletePeering(p)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(p) => p.PeeringId ?? ''}
	columns={defineColumns<Peering>([
		{ key: 'PeeringId', label: 'ID' },
		{ key: 'CoreNetworkId', label: 'Core network' },
		{ key: 'PeeringType', label: 'Type' },
		{ key: 'EdgeLocation', label: 'Edge' },
		{ key: 'State', label: 'State' },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No peerings found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create transit gateway peering">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-peer-cn">
				Core network ID *
				<input id="nm-peer-cn" bind:value={createCoreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-peer-tgw">
				Transit gateway ARN * <span class="text-xs text-slate-500">(must already be registered to the global network)</span>
				<input id="nm-peer-tgw" bind:value={createTransitGatewayArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Peering {viewed?.PeeringId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				{#if detailError}<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>{/if}
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Type</dt><dd>{viewed.PeeringType ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Owner account</dt><dd>{viewed.OwnerAccountId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
					{#if viewedDetail}
						<div><dt class="text-slate-500">Transit gateway ARN</dt><dd class="break-all">{viewedDetail.TransitGatewayArn ?? '—'}</dd></div>
						<div><dt class="text-slate-500">Peering attachment ID</dt><dd>{viewedDetail.TransitGatewayPeeringAttachmentId ?? '—'}</dd></div>
					{/if}
				</dl>
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
