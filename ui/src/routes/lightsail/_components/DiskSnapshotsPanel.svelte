<script lang="ts">
	// Disk snapshots -- family J (5 ops), including CopySnapshot, the one
	// cross-region op in this entire 161-op surface (services/lightsail/
	// PARITY.md). CopySnapshot can target either a disk snapshot or an
	// instance snapshot by name; this panel exposes it for disk snapshots
	// since that's the tab it's mounted from.
	import {
		GetDiskSnapshotsCommand,
		CreateDiskSnapshotCommand,
		DeleteDiskSnapshotCommand,
		CopySnapshotCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type DiskSnapshot,
		type RegionName,
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

	let snapshots = $state<DiskSnapshot[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchSnapshots(reset: boolean): Promise<void> {
		const resp = await client().send(new GetDiskSnapshotsCommand({ pageToken: reset ? undefined : nextToken }));
		snapshots = reset ? (resp.diskSnapshots ?? []) : [...snapshots, ...(resp.diskSnapshots ?? [])];
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
			return (
				(s.name ?? '').toLowerCase().includes(q) ||
				(s.fromDiskName ?? '').toLowerCase().includes(q) ||
				(s.state ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createDiskName = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createDiskName = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(new CreateDiskSnapshotCommand({ diskSnapshotName: createName, diskName: createDiskName }));
			toast.success('Disk snapshot creation started');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteSnapshot(s: DiskSnapshot): Promise<void> {
		if (!s.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete disk snapshot',
			message: `Delete disk snapshot ${s.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDiskSnapshotCommand({ diskSnapshotName: s.name }));
			toast.success('Disk snapshot deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Copy (cross-region) ----------------------

	let copyModal = $state<Modal | null>(null);
	let copyTarget = $state<DiskSnapshot | null>(null);
	let copyTargetName = $state('');
	let copySourceRegion = $state('');
	let copyBusy = $state(false);
	let copyError = $state<string | null>(null);

	function openCopy(s: DiskSnapshot): void {
		copyTarget = s;
		copyTargetName = '';
		copySourceRegion = '';
		copyError = null;
		copyModal?.open();
	}

	async function submitCopy(): Promise<void> {
		if (!copyTarget?.name) return;
		copyBusy = true;
		copyError = null;
		try {
			await client().send(
				new CopySnapshotCommand({
					sourceSnapshotName: copyTarget.name,
					targetSnapshotName: copyTargetName,
					sourceRegion: copySourceRegion as RegionName
				})
			);
			toast.success('Snapshot copy started');
			copyModal?.close();
			await refresh();
		} catch (e) {
			copyError = describeError(e);
		} finally {
			copyBusy = false;
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<DiskSnapshot | null>(null);

	function openDetail(s: DiskSnapshot): void {
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
		Create disk snapshot
	</button>
</div>

{#snippet createdCell(s: DiskSnapshot)}
	{formatDate(s.createdAt)}
{/snippet}
{#snippet rowActions(s: DiskSnapshot)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openCopy(s)} class="text-emerald-600 hover:underline text-sm">Copy</button>
		<button onclick={() => openDetail(s)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteSnapshot(s)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(s) => s.name ?? ''}
	columns={defineColumns<DiskSnapshot>([
		{ key: 'name', label: 'Name' },
		{ key: 'fromDiskName', label: 'Source disk' },
		{ key: 'state', label: 'State' },
		{ key: 'sizeInGb', label: 'Size (GB)' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No disk snapshots found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create disk snapshot">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-disksnap-name">
				Snapshot name
				<input id="ls-disksnap-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-disksnap-disk">
				Source disk name
				<input id="ls-disksnap-disk" bind:value={createDiskName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createDiskName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={copyModal} title="Copy {copyTarget?.name ?? ''} to another region">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-disksnap-copy-name">
				Target snapshot name
				<input id="ls-disksnap-copy-name" bind:value={copyTargetName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-disksnap-copy-region">
				Source region (e.g. us-west-2)
				<input id="ls-disksnap-copy-region" bind:value={copySourceRegion} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if copyError}<p class="text-sm text-red-600 dark:text-red-400">{copyError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => copyModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCopy} disabled={copyBusy || !copyTargetName || !copySourceRegion} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Copy</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Disk snapshot {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Progress</dt><dd>{viewed.progress ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Size</dt><dd>{viewed.sizeInGb ?? '—'} GB</dd></div>
					<div><dt class="text-slate-500">Auto snapshot?</dt><dd>{viewed.isFromAutoSnapshot ? 'Yes' : 'No'}</dd></div>
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
