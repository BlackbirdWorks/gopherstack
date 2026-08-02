<script lang="ts">
	// Connections -- an on-prem device-to-device logical/physical link over a
	// Link (services/networkmanager/PARITY.md family F, 4 ops). Distinct
	// from a Cloud WAN "Connect attachment"/"Connect peer" despite sharing
	// the English word "connection" -- see the Cloud WAN tab group for
	// those.
	import {
		GetConnectionsCommand,
		CreateConnectionCommand,
		UpdateConnectionCommand,
		DeleteConnectionCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Connection,
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
	let connections = $state<Connection[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchConnections(reset: boolean): Promise<void> {
		if (!globalNetworkId) {
			connections = [];
			return;
		}
		const resp = await client().send(
			new GetConnectionsCommand({
				GlobalNetworkId: globalNetworkId,
				MaxResults: 50,
				NextToken: reset ? undefined : nextToken
			})
		);
		connections = reset ? (resp.Connections ?? []) : [...connections, ...(resp.Connections ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchConnections(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchConnections(false);
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
		connections.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(c.ConnectionId ?? '').toLowerCase().includes(q) ||
				(c.Description ?? '').toLowerCase().includes(q) ||
				(c.DeviceId ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createDeviceId = $state('');
	let createConnectedDeviceId = $state('');
	let createLinkId = $state('');
	let createConnectedLinkId = $state('');
	let createDescription = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createDeviceId = '';
		createConnectedDeviceId = '';
		createLinkId = '';
		createConnectedLinkId = '';
		createDescription = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!globalNetworkId || !createDeviceId.trim() || !createConnectedDeviceId.trim()) {
			createError = 'Device ID and connected device ID are required.';
			return;
		}
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateConnectionCommand({
					GlobalNetworkId: globalNetworkId,
					DeviceId: createDeviceId.trim(),
					ConnectedDeviceId: createConnectedDeviceId.trim(),
					LinkId: createLinkId || undefined,
					ConnectedLinkId: createConnectedLinkId || undefined,
					Description: createDescription || undefined
				})
			);
			toast.success('Connection created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteConnection(c: Connection): Promise<void> {
		if (!c.ConnectionId || !globalNetworkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete connection',
			message: `Delete connection ${c.ConnectionId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteConnectionCommand({ GlobalNetworkId: globalNetworkId, ConnectionId: c.ConnectionId })
			);
			toast.success('Connection deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Connection | null>(null);
	let editDescription = $state('');
	let detailBusy = $state(false);
	let detailError = $state<string | null>(null);

	function openDetail(c: Connection): void {
		viewed = c;
		editDescription = c.Description ?? '';
		detailError = null;
		detailModal?.open();
	}

	async function saveConnection(): Promise<void> {
		if (!viewed?.ConnectionId || !globalNetworkId) return;
		detailBusy = true;
		detailError = null;
		try {
			const resp = await client().send(
				new UpdateConnectionCommand({
					GlobalNetworkId: globalNetworkId,
					ConnectionId: viewed.ConnectionId,
					Description: editDescription || undefined
				})
			);
			viewed = resp.Connection ?? viewed;
			toast.success('Connection updated');
			await refresh();
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailBusy = false;
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.ConnectionId) return;
		const arn = taggableArn('connection', viewed.ConnectionId, globalNetworkId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.ConnectionId) return;
		const arn = taggableArn('connection', viewed.ConnectionId, globalNetworkId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}
</script>

<div class="flex flex-wrap items-end justify-between gap-3">
	<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-connections-gn" />
	<button
		onclick={openCreate}
		disabled={!globalNetworkId}
		class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
	>
		Create connection
	</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet createdCell(c: Connection)}
	{formatDate(c.CreatedAt)}
{/snippet}
{#snippet rowActions(c: Connection)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(c)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteConnection(c)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(c) => c.ConnectionId ?? ''}
	columns={defineColumns<Connection>([
		{ key: 'ConnectionId', label: 'ID' },
		{ key: 'DeviceId', label: 'Device' },
		{ key: 'ConnectedDeviceId', label: 'Connected device' },
		{ key: 'State', label: 'State' },
		{ key: 'CreatedAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No connections found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create connection">
	{#snippet children()}
		<div class="grid grid-cols-2 gap-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-conn-device">
				Device ID *
				<input id="nm-conn-device" bind:value={createDeviceId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-conn-connected-device">
				Connected device ID *
				<input id="nm-conn-connected-device" bind:value={createConnectedDeviceId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-conn-link">
				Link ID
				<input id="nm-conn-link" bind:value={createLinkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-conn-connected-link">
				Connected link ID
				<input id="nm-conn-connected-link" bind:value={createConnectedLinkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm col-span-2" for="nm-conn-desc">
				Description
				<input id="nm-conn-desc" bind:value={createDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="col-span-2 text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Connection {viewed?.ConnectionId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.ConnectionArn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Link / connected link</dt><dd>{viewed.LinkId ?? '—'} / {viewed.ConnectedLinkId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<label class="flex flex-col gap-1 text-sm" for="nm-conn-edit-desc">
						Description
						<input id="nm-conn-edit-desc" bind:value={editDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<button onclick={saveConnection} disabled={detailBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
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
