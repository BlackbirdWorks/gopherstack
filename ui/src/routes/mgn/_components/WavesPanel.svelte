<script lang="ts">
	// Waves (services/mgn/PARITY.md family F, 8 ops): Create/Update/Delete/
	// List/Archive/Unarchive/AssociateApplications/DisassociateApplications.
	// A Wave groups Applications (see Applications tab for the
	// Application<->SourceServer half of the hierarchy).
	import {
		ListWavesCommand,
		CreateWaveCommand,
		UpdateWaveCommand,
		DeleteWaveCommand,
		ArchiveWaveCommand,
		UnarchiveWaveCommand,
		AssociateApplicationsCommand,
		DisassociateApplicationsCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Wave,
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

	let waves = $state<Wave[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchWaves(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListWavesCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		waves = reset ? (resp.items ?? []) : [...waves, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchWaves(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchWaves(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		waves.filter((w) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (w.name ?? '').toLowerCase().includes(q) || (w.description ?? '').toLowerCase().includes(q);
		})
	);

	let createModal = $state<Modal | null>(null);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let newName = $state('');
	let newDescription = $state('');

	function openCreate(): void {
		createError = null;
		newName = '';
		newDescription = '';
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!newName.trim()) {
			createError = 'Name is required.';
			return;
		}
		creating = true;
		createError = null;
		try {
			await client().send(new CreateWaveCommand({ name: newName.trim(), description: newDescription.trim() || undefined }));
			toast.success('Wave created');
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
	let editDescription = $state('');

	function openEdit(w: Wave): void {
		editError = null;
		editID = w.waveID ?? '';
		editName = w.name ?? '';
		editDescription = w.description ?? '';
		editModal?.open();
	}

	async function submitEdit(): Promise<void> {
		if (!editID) return;
		editing = true;
		editError = null;
		try {
			await client().send(new UpdateWaveCommand({ waveID: editID, name: editName.trim() || undefined, description: editDescription.trim() || undefined }));
			toast.success('Wave updated');
			editModal?.close();
			await refresh();
		} catch (e) {
			editError = describeError(e);
			toast.error(editError);
		} finally {
			editing = false;
		}
	}

	async function toggleArchive(w: Wave): Promise<void> {
		if (!w.waveID) return;
		try {
			if (w.isArchived) {
				await client().send(new UnarchiveWaveCommand({ waveID: w.waveID }));
				toast.success('Wave unarchived');
			} else {
				await client().send(new ArchiveWaveCommand({ waveID: w.waveID }));
				toast.success('Wave archived');
			}
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteWave(w: Wave): Promise<void> {
		if (!w.waveID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete wave',
			message: `Delete wave ${w.name ?? w.waveID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteWaveCommand({ waveID: w.waveID }));
			toast.success('Wave deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Wave | null>(null);
	let assocError = $state<string | null>(null);
	let assocAppIDs = $state('');
	let disassocAppIDs = $state('');

	function openDetail(w: Wave): void {
		viewed = w;
		assocError = null;
		assocAppIDs = '';
		disassocAppIDs = '';
		detailModal?.open();
	}

	function parseIDs(text: string): string[] {
		return text
			.split(/[\n,]/)
			.map((s) => s.trim())
			.filter(Boolean);
	}

	async function submitAssociate(): Promise<void> {
		const ids = parseIDs(assocAppIDs);
		if (!viewed?.waveID || ids.length === 0) return;
		try {
			await client().send(new AssociateApplicationsCommand({ waveID: viewed.waveID, applicationIDs: ids }));
			toast.success('Applications associated');
			assocAppIDs = '';
			await refresh();
		} catch (e) {
			assocError = describeError(e);
			toast.error(assocError);
		}
	}

	async function submitDisassociate(): Promise<void> {
		const ids = parseIDs(disassocAppIDs);
		if (!viewed?.waveID || ids.length === 0) return;
		try {
			await client().send(new DisassociateApplicationsCommand({ waveID: viewed.waveID, applicationIDs: ids }));
			toast.success('Applications disassociated');
			disassocAppIDs = '';
			await refresh();
		} catch (e) {
			assocError = describeError(e);
			toast.error(assocError);
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.waveID) return;
		const arn = taggableArn('wave', viewed.waveID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.waveID) return;
		const arn = taggableArn('wave', viewed.waveID, currentRegion());
		await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
		const rest = { ...viewed.tags };
		delete rest[key];
		viewed = { ...viewed, tags: rest };
	}

	function archivedClass(archived: boolean | undefined): string {
		return archived
			? 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
			: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
	}
</script>

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Create wave</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet archivedCell(w: Wave)}
	<span class="text-xs px-2 py-1 rounded-full {archivedClass(w.isArchived)}">{w.isArchived ? 'Archived' : 'Active'}</span>
{/snippet}
{#snippet rowActions(w: Wave)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(w)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => openEdit(w)} class="text-blue-600 hover:underline text-sm">Edit</button>
		<button onclick={() => toggleArchive(w)} class="text-slate-600 hover:underline text-sm">{w.isArchived ? 'Unarchive' : 'Archive'}</button>
		<button onclick={() => deleteWave(w)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(w) => w.waveID ?? ''}
	columns={defineColumns<Wave>([
		{ key: 'name', label: 'Name' },
		{ key: 'description', label: 'Description' },
		{ key: 'isArchived', label: 'Status', render: archivedCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No waves found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create Wave">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">Name
				<input bind:value={newName} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Description
				<input bind:value={newDescription} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={creating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{creating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editModal} title="Edit Wave">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">Name
				<input bind:value={editName} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Description
				<input bind:value={editDescription} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEdit} disabled={editing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Wave {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Wave ID</dt><dd>{viewed.waveID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Health</dt><dd>{viewed.waveAggregatedStatus?.healthStatus ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Progress</dt><dd>{viewed.waveAggregatedStatus?.progressStatus ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Total applications</dt><dd>{viewed.waveAggregatedStatus?.totalApplications ?? 0}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Application association</p>
					<label class="flex flex-col gap-1 text-sm">Associate (comma or newline separated application IDs)
						<textarea bind:value={assocAppIDs} rows="2" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
					</label>
					<button onclick={submitAssociate} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Associate</button>
					<label class="flex flex-col gap-1 text-sm">Disassociate (comma or newline separated application IDs)
						<textarea bind:value={disassocAppIDs} rows="2" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
					</label>
					<button onclick={submitDisassociate} class="px-3 py-1 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700">Disassociate</button>
					{#if assocError}<p class="text-sm text-red-600 dark:text-red-400">{assocError}</p>{/if}
				</div>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={viewed.tags ?? {}} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
