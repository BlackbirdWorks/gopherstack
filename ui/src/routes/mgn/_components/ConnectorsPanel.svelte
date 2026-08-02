<script lang="ts">
	// Connectors (services/mgn/PARITY.md family G, 4 ops): Create/Update/
	// Delete/List. A Connector represents an SSM Managed Instance bridging an
	// on-prem vCenter environment to AWS -- no AccountID field on any op
	// (PARITY.md: connectors are not delegated-account-scoped).
	import {
		ListConnectorsCommand,
		CreateConnectorCommand,
		UpdateConnectorCommand,
		DeleteConnectorCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Connector,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client, searchQuery }: Props = $props();

	let connectors = $state<Connector[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchConnectors(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListConnectorsCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		connectors = reset ? (resp.items ?? []) : [...connectors, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchConnectors(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchConnectors(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		connectors.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (c.name ?? '').toLowerCase().includes(q) || (c.connectorID ?? '').toLowerCase().includes(q) || (c.ssmInstanceID ?? '').toLowerCase().includes(q);
		})
	);

	let createModal = $state<Modal | null>(null);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let newName = $state('');
	let newSsmInstanceID = $state('');

	function openCreate(): void {
		createError = null;
		newName = '';
		newSsmInstanceID = '';
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!newName.trim() || !newSsmInstanceID.trim()) {
			createError = 'Name and SSM instance ID are required.';
			return;
		}
		creating = true;
		createError = null;
		try {
			await client().send(new CreateConnectorCommand({ name: newName.trim(), ssmInstanceID: newSsmInstanceID.trim() }));
			toast.success('Connector created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
			toast.error(createError);
		} finally {
			creating = false;
		}
	}

	let editModal = $state<Modal | null>(null);
	let editing = $state(false);
	let editError = $state<string | null>(null);
	let editID = $state('');
	let editName = $state('');

	function openEdit(c: Connector): void {
		editError = null;
		editID = c.connectorID ?? '';
		editName = c.name ?? '';
		editModal?.open();
	}

	async function submitEdit(): Promise<void> {
		if (!editID) return;
		editing = true;
		editError = null;
		try {
			await client().send(new UpdateConnectorCommand({ connectorID: editID, name: editName.trim() || undefined }));
			toast.success('Connector updated');
			editModal?.close();
			await refresh();
		} catch (e) {
			editError = describeError(e);
			toast.error(editError);
		} finally {
			editing = false;
		}
	}

	async function deleteConnector(c: Connector): Promise<void> {
		if (!c.connectorID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete connector',
			message: `Delete connector ${c.name ?? c.connectorID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteConnectorCommand({ connectorID: c.connectorID }));
			toast.success('Connector deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Connector | null>(null);

	function openDetail(c: Connector): void {
		viewed = c;
		detailModal?.open();
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.connectorID) return;
		const arn = taggableArn('connector', viewed.connectorID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.connectorID) return;
		const arn = taggableArn('connector', viewed.connectorID, currentRegion());
		await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
		const rest = { ...viewed.tags };
		delete rest[key];
		viewed = { ...viewed, tags: rest };
	}

	const columns = defineColumns<Connector>([
		{ key: 'name', label: 'Name' },
		{ key: 'connectorID', label: 'Connector ID' },
		{ key: 'ssmInstanceID', label: 'SSM Instance ID' }
	]);
</script>

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Create connector</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet rowActions(c: Connector)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(c)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => openEdit(c)} class="text-blue-600 hover:underline text-sm">Edit</button>
		<button onclick={() => deleteConnector(c)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable rows={filtered} rowKey={(c) => c.connectorID ?? ''} columns={[...columns, { key: 'actions', label: '', render: rowActions }]} {loading} emptyMessage="No connectors found" />
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create Connector">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">Name
				<input bind:value={newName} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">SSM instance ID
				<input bind:value={newSsmInstanceID} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={creating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{creating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editModal} title="Edit Connector">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">Name
				<input bind:value={editName} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEdit} disabled={editing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Connector {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Connector ID</dt><dd>{viewed.connectorID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">SSM instance ID</dt><dd>{viewed.ssmInstanceID ?? '—'}</dd></div>
				</dl>
				<TagEditor tags={viewed.tags ?? {}} onAdd={addTag} onRemove={removeTag} />
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
