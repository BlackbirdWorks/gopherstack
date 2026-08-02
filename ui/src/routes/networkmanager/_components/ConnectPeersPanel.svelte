<script lang="ts">
	// Cloud WAN Connect Peers -- the BGP/GRE peer terminating a Connect
	// attachment (services/networkmanager/PARITY.md family K, 4 ops).
	// Distinct from the Global-Networks-side ConnectPeerAssociation (see the
	// Associations tab's "Connect Peer" kind, which binds one of these to an
	// on-prem Device/Link).
	import {
		ListConnectPeersCommand,
		CreateConnectPeerCommand,
		DeleteConnectPeerCommand,
		GetConnectPeerCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type ConnectPeerSummary,
		type ConnectPeer,
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

	let peers = $state<ConnectPeerSummary[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchPeers(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListConnectPeersCommand({ MaxResults: 50, NextToken: reset ? undefined : nextToken })
		);
		peers = reset ? (resp.ConnectPeers ?? []) : [...peers, ...(resp.ConnectPeers ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchPeers(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchPeers(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		peers.filter((p) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(p.ConnectPeerId ?? '').toLowerCase().includes(q) ||
				(p.ConnectAttachmentId ?? '').toLowerCase().includes(q) ||
				(p.CoreNetworkId ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createAttachmentId = $state('');
	let createPeerAddress = $state('');
	let createSubnetArn = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createAttachmentId = '';
		createPeerAddress = '';
		createSubnetArn = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!createAttachmentId.trim() || !createPeerAddress.trim()) {
			createError = 'Connect attachment ID and peer address are required.';
			return;
		}
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateConnectPeerCommand({
					ConnectAttachmentId: createAttachmentId.trim(),
					PeerAddress: createPeerAddress.trim(),
					SubnetArn: createSubnetArn || undefined
				})
			);
			toast.success('Connect peer created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deletePeer(p: ConnectPeerSummary): Promise<void> {
		if (!p.ConnectPeerId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Connect peer',
			message: `Delete Connect peer ${p.ConnectPeerId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteConnectPeerCommand({ ConnectPeerId: p.ConnectPeerId }));
			toast.success('Connect peer deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<ConnectPeer | null>(null);
	let detailError = $state<string | null>(null);

	async function openDetail(p: ConnectPeerSummary): Promise<void> {
		if (!p.ConnectPeerId) return;
		detailError = null;
		viewed = null;
		detailModal?.open();
		try {
			const resp = await client().send(new GetConnectPeerCommand({ ConnectPeerId: p.ConnectPeerId }));
			viewed = resp.ConnectPeer ?? { ConnectPeerId: p.ConnectPeerId };
		} catch (e) {
			detailError = describeError(e);
			viewed = { ConnectPeerId: p.ConnectPeerId };
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.ConnectPeerId) return;
		const arn = taggableArn('connect-peer', viewed.ConnectPeerId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.ConnectPeerId) return;
		const arn = taggableArn('connect-peer', viewed.ConnectPeerId);
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
		Create Connect peer
	</button>
</div>

{#snippet createdCell(p: ConnectPeerSummary)}
	{formatDate(p.CreatedAt)}
{/snippet}
{#snippet rowActions(p: ConnectPeerSummary)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(p)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deletePeer(p)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(p) => p.ConnectPeerId ?? ''}
	columns={defineColumns<ConnectPeerSummary>([
		{ key: 'ConnectPeerId', label: 'ID' },
		{ key: 'ConnectAttachmentId', label: 'Connect attachment' },
		{ key: 'CoreNetworkId', label: 'Core network' },
		{ key: 'EdgeLocation', label: 'Edge' },
		{ key: 'ConnectPeerState', label: 'State' },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No Connect peers found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create Connect peer">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-cp-attachment">
				Connect attachment ID *
				<input id="nm-cp-attachment" bind:value={createAttachmentId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-cp-peer-addr">
				Peer address *
				<input id="nm-cp-peer-addr" bind:value={createPeerAddress} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-cp-subnet">
				Subnet ARN (NO_ENCAP only)
				<input id="nm-cp-subnet" bind:value={createSubnetArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Connect peer {viewed?.ConnectPeerId ?? ''}">
	{#snippet children()}
		{#if detailError}<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>{/if}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Core network</dt><dd>{viewed.CoreNetworkId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Edge location</dt><dd>{viewed.EdgeLocation ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Protocol</dt><dd>{viewed.Configuration?.Protocol ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Peer address</dt><dd>{viewed.Configuration?.PeerAddress ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Core network address</dt><dd>{viewed.Configuration?.CoreNetworkAddress ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
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
