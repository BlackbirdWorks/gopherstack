<script lang="ts">
	// Relational database snapshots -- family P (4 ops).
	import {
		GetRelationalDatabaseSnapshotsCommand,
		CreateRelationalDatabaseSnapshotCommand,
		DeleteRelationalDatabaseSnapshotCommand,
		CreateRelationalDatabaseFromSnapshotCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type RelationalDatabaseSnapshot,
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

	let snapshots = $state<RelationalDatabaseSnapshot[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchSnapshots(reset: boolean): Promise<void> {
		const resp = await client().send(
			new GetRelationalDatabaseSnapshotsCommand({ pageToken: reset ? undefined : nextToken })
		);
		snapshots = reset
			? (resp.relationalDatabaseSnapshots ?? [])
			: [...snapshots, ...(resp.relationalDatabaseSnapshots ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchSnapshots(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchSnapshots(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		snapshots.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (s.name ?? '').toLowerCase().includes(q) || (s.fromRelationalDatabaseName ?? '').toLowerCase().includes(q);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createDbName = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createDbName = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateRelationalDatabaseSnapshotCommand({
					relationalDatabaseSnapshotName: createName,
					relationalDatabaseName: createDbName
				})
			);
			toast.success('Database snapshot creation started');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteSnapshot(s: RelationalDatabaseSnapshot): Promise<void> {
		if (!s.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete database snapshot',
			message: `Delete database snapshot ${s.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteRelationalDatabaseSnapshotCommand({ relationalDatabaseSnapshotName: s.name }));
			toast.success('Database snapshot deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Restore ---------------------------------

	let restoreModal = $state<Modal | null>(null);
	let restoreTarget = $state<RelationalDatabaseSnapshot | null>(null);
	let restoreDbName = $state('');
	let restoreBusy = $state(false);
	let restoreError = $state<string | null>(null);

	function openRestore(s: RelationalDatabaseSnapshot): void {
		restoreTarget = s;
		restoreDbName = '';
		restoreError = null;
		restoreModal?.open();
	}

	async function submitRestore(): Promise<void> {
		if (!restoreTarget?.name) return;
		restoreBusy = true;
		restoreError = null;
		try {
			await client().send(
				new CreateRelationalDatabaseFromSnapshotCommand({
					relationalDatabaseName: restoreDbName,
					relationalDatabaseSnapshotName: restoreTarget.name
				})
			);
			toast.success('Restore from snapshot started -- see the Databases tab');
			restoreModal?.close();
		} catch (e) {
			restoreError = describeError(e);
		} finally {
			restoreBusy = false;
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<RelationalDatabaseSnapshot | null>(null);

	function openDetail(s: RelationalDatabaseSnapshot): void {
		viewed = s;
		detailModal?.open();
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
		Create database snapshot
	</button>
</div>

{#snippet createdCell(s: RelationalDatabaseSnapshot)}
	{formatDate(s.createdAt)}
{/snippet}
{#snippet rowActions(s: RelationalDatabaseSnapshot)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openRestore(s)} class="text-emerald-600 hover:underline text-sm">Restore</button>
		<button onclick={() => openDetail(s)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteSnapshot(s)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(s) => s.name ?? ''}
	columns={defineColumns<RelationalDatabaseSnapshot>([
		{ key: 'name', label: 'Name' },
		{ key: 'fromRelationalDatabaseName', label: 'Source database' },
		{ key: 'engine', label: 'Engine' },
		{ key: 'state', label: 'State' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No database snapshots found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create database snapshot">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-dbsnap-name">
				Snapshot name
				<input id="ls-dbsnap-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-dbsnap-db">
				Source database name
				<input id="ls-dbsnap-db" bind:value={createDbName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createDbName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={restoreModal} title="Restore {restoreTarget?.name ?? ''} to a new database">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-dbsnap-restore-name">
				New database name
				<input id="ls-dbsnap-restore-name" bind:value={restoreDbName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if restoreError}<p class="text-sm text-red-600 dark:text-red-400">{restoreError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => restoreModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitRestore} disabled={restoreBusy || !restoreDbName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Restore</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Database snapshot {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Engine</dt><dd>{viewed.engine ?? '—'} {viewed.engineVersion ?? ''}</dd></div>
					<div><dt class="text-slate-500">Size</dt><dd>{viewed.sizeInGb ?? '—'} GB</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>
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
