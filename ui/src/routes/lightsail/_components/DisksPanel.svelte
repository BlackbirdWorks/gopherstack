<script lang="ts">
	// Disks -- family I (7 ops), plus family F's add-ons/auto-snapshots
	// folded into the detail view (the same AddOnType/AutoSnapshot machinery
	// Instances use, DiskState IS a real 5-value typed SDK enum unlike
	// InstanceState).
	import {
		GetDisksCommand,
		CreateDiskCommand,
		CreateDiskFromSnapshotCommand,
		AttachDiskCommand,
		DetachDiskCommand,
		DeleteDiskCommand,
		GetAutoSnapshotsCommand,
		DeleteAutoSnapshotCommand,
		EnableAddOnCommand,
		DisableAddOnCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Disk,
		type AutoSnapshotDetails,
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

	let disks = $state<Disk[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchDisks(reset: boolean): Promise<void> {
		const resp = await client().send(new GetDisksCommand({ pageToken: reset ? undefined : nextToken }));
		disks = reset ? (resp.disks ?? []) : [...disks, ...(resp.disks ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchDisks(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchDisks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		disks.filter((d) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(d.name ?? '').toLowerCase().includes(q) ||
				(d.attachedTo ?? '').toLowerCase().includes(q) ||
				(d.state ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createAvailabilityZone = $state('');
	let createSizeInGb = $state(8);
	let createFromSnapshot = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createAvailabilityZone = '';
		createSizeInGb = 8;
		createFromSnapshot = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			if (createFromSnapshot.trim()) {
				await client().send(
					new CreateDiskFromSnapshotCommand({
						diskName: createName,
						diskSnapshotName: createFromSnapshot,
						availabilityZone: createAvailabilityZone,
						sizeInGb: createSizeInGb
					})
				);
			} else {
				await client().send(
					new CreateDiskCommand({ diskName: createName, availabilityZone: createAvailabilityZone, sizeInGb: createSizeInGb })
				);
			}
			toast.success('Disk creation started');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Attach/detach ---------------------------

	let attachModal = $state<Modal | null>(null);
	let attachTarget = $state<Disk | null>(null);
	let attachInstanceName = $state('');
	let attachDiskPath = $state('');
	let attachBusy = $state(false);
	let attachError = $state<string | null>(null);

	function openAttach(d: Disk): void {
		attachTarget = d;
		attachInstanceName = '';
		attachDiskPath = '';
		attachError = null;
		attachModal?.open();
	}

	async function submitAttach(): Promise<void> {
		if (!attachTarget?.name) return;
		attachBusy = true;
		attachError = null;
		try {
			await client().send(
				new AttachDiskCommand({ diskName: attachTarget.name, instanceName: attachInstanceName, diskPath: attachDiskPath })
			);
			toast.success('Disk attach requested');
			attachModal?.close();
			await refresh();
		} catch (e) {
			attachError = describeError(e);
		} finally {
			attachBusy = false;
		}
	}

	async function detach(d: Disk): Promise<void> {
		if (!d.name) return;
		try {
			await client().send(new DetachDiskCommand({ diskName: d.name }));
			toast.success('Disk detach requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteDisk(d: Disk): Promise<void> {
		if (!d.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete disk',
			message: `Delete disk ${d.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDiskCommand({ diskName: d.name }));
			toast.success('Disk deletion started');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Disk | null>(null);
	let autoSnapshots = $state<AutoSnapshotDetails[]>([]);

	async function openDetail(d: Disk): Promise<void> {
		viewed = d;
		autoSnapshots = [];
		detailModal?.open();
		if (d.name) {
			try {
				const resp = await client().send(new GetAutoSnapshotsCommand({ resourceName: d.name }));
				autoSnapshots = resp.autoSnapshots ?? [];
			} catch (e) {
				toast.error(describeError(e));
			}
		}
	}

	async function enableAutoSnapshot(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new EnableAddOnCommand({
					resourceName: viewed.name,
					addOnRequest: { addOnType: 'AutoSnapshot', autoSnapshotAddOnRequest: { snapshotTimeOfDay: '06:00' } }
				})
			);
			toast.success('AutoSnapshot add-on enabled');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function disableAutoSnapshot(): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new DisableAddOnCommand({ resourceName: viewed.name, addOnType: 'AutoSnapshot' }));
			toast.success('AutoSnapshot add-on disabled');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteOneAutoSnapshot(date: string | undefined): Promise<void> {
		if (!viewed?.name || !date) return;
		try {
			await client().send(new DeleteAutoSnapshotCommand({ resourceName: viewed.name, date }));
			toast.success('Auto snapshot deleted');
			const resp = await client().send(new GetAutoSnapshotsCommand({ resourceName: viewed.name }));
			autoSnapshots = resp.autoSnapshots ?? [];
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

	function stateClass(state: string | undefined): string {
		if (state === 'available' || state === 'in-use')
			return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'error') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
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
		Create disk
	</button>
</div>

{#snippet stateCell(d: Disk)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(d.state)}">{d.state ?? '—'}</span>
{/snippet}
{#snippet createdCell(d: Disk)}
	{formatDate(d.createdAt)}
{/snippet}
{#snippet rowActions(d: Disk)}
	<div class="flex items-center gap-3 justify-end">
		{#if d.isAttached}
			<button onclick={() => detach(d)} class="text-amber-600 hover:underline text-sm">Detach</button>
		{:else}
			<button onclick={() => openAttach(d)} class="text-emerald-600 hover:underline text-sm">Attach</button>
		{/if}
		<button onclick={() => openDetail(d)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteDisk(d)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(d) => d.name ?? ''}
	columns={defineColumns<Disk>([
		{ key: 'name', label: 'Name' },
		{ key: 'sizeInGb', label: 'Size (GB)' },
		{ key: 'attachedTo', label: 'Attached to' },
		{ key: 'state', label: 'State', render: stateCell },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No disks found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create disk">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-disk-name">
				Name
				<input id="ls-disk-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-disk-az">
				Availability zone
				<input id="ls-disk-az" bind:value={createAvailabilityZone} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-disk-size">
				Size (GB)
				<input id="ls-disk-size" type="number" bind:value={createSizeInGb} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-disk-snap">
				From disk snapshot (optional)
				<input id="ls-disk-snap" bind:value={createFromSnapshot} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={attachModal} title="Attach {attachTarget?.name ?? ''} to an instance">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-disk-attach-instance">
				Instance name
				<input id="ls-disk-attach-instance" bind:value={attachInstanceName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-disk-attach-path">
				Disk path (e.g. /dev/xvdf)
				<input id="ls-disk-attach-path" bind:value={attachDiskPath} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if attachError}<p class="text-sm text-red-600 dark:text-red-400">{attachError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => attachModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitAttach} disabled={attachBusy || !attachInstanceName || !attachDiskPath} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Attach</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Disk {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Size</dt><dd>{viewed.sizeInGb ?? '—'} GB</dd></div>
					<div><dt class="text-slate-500">IOPS</dt><dd>{viewed.iops ?? '—'}</dd></div>
					<div><dt class="text-slate-500">System disk?</dt><dd>{viewed.isSystemDisk ? 'Yes' : 'No'}</dd></div>
					<div><dt class="text-slate-500">Auto-mount</dt><dd>{viewed.autoMountStatus ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Add-ons &amp; auto snapshots</p>
					<div class="flex gap-2">
						<button onclick={enableAutoSnapshot} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Enable AutoSnapshot</button>
						<button onclick={disableAutoSnapshot} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Disable AutoSnapshot</button>
					</div>
					<ul class="text-sm space-y-1">
						{#each autoSnapshots as s (s.date)}
							<li class="flex items-center justify-between">
								<span>{s.date} -- {s.status}</span>
								<button onclick={() => deleteOneAutoSnapshot(s.date)} class="text-red-600 hover:underline text-xs">Delete</button>
							</li>
						{:else}
							<li class="text-slate-500">No automatic snapshots</li>
						{/each}
					</ul>
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
