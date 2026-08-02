<script lang="ts">
	// Applications (services/mgn/PARITY.md family E, 8 ops): Create/Update/
	// Delete/List/Archive/Unarchive/AssociateSourceServers/
	// DisassociateSourceServers. Applications group SourceServers; Waves (a
	// separate tab) group Applications -- see PARITY.md's Wave/Application/
	// SourceServer hierarchy note.
	import {
		ListApplicationsCommand,
		CreateApplicationCommand,
		UpdateApplicationCommand,
		DeleteApplicationCommand,
		ArchiveApplicationCommand,
		UnarchiveApplicationCommand,
		AssociateSourceServersCommand,
		DisassociateSourceServersCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Application,
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

	let apps = $state<Application[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchApps(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListApplicationsCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		apps = reset ? (resp.items ?? []) : [...apps, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchApps(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchApps(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		apps.filter((a) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(a.name ?? '').toLowerCase().includes(q) ||
				(a.applicationID ?? '').toLowerCase().includes(q) ||
				(a.description ?? '').toLowerCase().includes(q)
			);
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
			await client().send(new CreateApplicationCommand({ name: newName.trim(), description: newDescription.trim() || undefined }));
			toast.success('Application created');
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

	function openEdit(a: Application): void {
		editError = null;
		editID = a.applicationID ?? '';
		editName = a.name ?? '';
		editDescription = a.description ?? '';
		editModal?.open();
	}

	async function submitEdit(): Promise<void> {
		if (!editID) return;
		editing = true;
		editError = null;
		try {
			await client().send(
				new UpdateApplicationCommand({ applicationID: editID, name: editName.trim() || undefined, description: editDescription.trim() || undefined })
			);
			toast.success('Application updated');
			editModal?.close();
			await refresh();
		} catch (e) {
			editError = describeError(e);
			toast.error(editError);
		} finally {
			editing = false;
		}
	}

	async function toggleArchive(a: Application): Promise<void> {
		if (!a.applicationID) return;
		try {
			if (a.isArchived) {
				await client().send(new UnarchiveApplicationCommand({ applicationID: a.applicationID }));
				toast.success('Application unarchived');
			} else {
				await client().send(new ArchiveApplicationCommand({ applicationID: a.applicationID }));
				toast.success('Application archived');
			}
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteApp(a: Application): Promise<void> {
		if (!a.applicationID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete application',
			message: `Delete application ${a.name ?? a.applicationID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteApplicationCommand({ applicationID: a.applicationID }));
			toast.success('Application deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --------------------------- Detail / associations ---------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Application | null>(null);
	let assocError = $state<string | null>(null);
	let assocServerIDs = $state('');
	let disassocServerIDs = $state('');

	function openDetail(a: Application): void {
		viewed = a;
		assocError = null;
		assocServerIDs = '';
		disassocServerIDs = '';
		detailModal?.open();
	}

	function parseIDs(text: string): string[] {
		return text
			.split(/[\n,]/)
			.map((s) => s.trim())
			.filter(Boolean);
	}

	async function submitAssociate(): Promise<void> {
		const ids = parseIDs(assocServerIDs);
		if (!viewed?.applicationID || ids.length === 0) return;
		try {
			await client().send(new AssociateSourceServersCommand({ applicationID: viewed.applicationID, sourceServerIDs: ids }));
			toast.success('Source servers associated');
			assocServerIDs = '';
			await refresh();
		} catch (e) {
			assocError = describeError(e);
			toast.error(assocError);
		}
	}

	async function submitDisassociate(): Promise<void> {
		const ids = parseIDs(disassocServerIDs);
		if (!viewed?.applicationID || ids.length === 0) return;
		try {
			await client().send(new DisassociateSourceServersCommand({ applicationID: viewed.applicationID, sourceServerIDs: ids }));
			toast.success('Source servers disassociated');
			disassocServerIDs = '';
			await refresh();
		} catch (e) {
			assocError = describeError(e);
			toast.error(assocError);
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.applicationID) return;
		const arn = taggableArn('application', viewed.applicationID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.applicationID) return;
		const arn = taggableArn('application', viewed.applicationID, currentRegion());
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
	<button onclick={openCreate} class="px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Create application</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet archivedCell(a: Application)}
	<span class="text-xs px-2 py-1 rounded-full {archivedClass(a.isArchived)}">{a.isArchived ? 'Archived' : 'Active'}</span>
{/snippet}
{#snippet rowActions(a: Application)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(a)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => openEdit(a)} class="text-blue-600 hover:underline text-sm">Edit</button>
		<button onclick={() => toggleArchive(a)} class="text-slate-600 hover:underline text-sm">{a.isArchived ? 'Unarchive' : 'Archive'}</button>
		<button onclick={() => deleteApp(a)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(a) => a.applicationID ?? ''}
	columns={defineColumns<Application>([
		{ key: 'name', label: 'Name' },
		{ key: 'description', label: 'Description' },
		{ key: 'waveID', label: 'Wave' },
		{ key: 'isArchived', label: 'Status', render: archivedCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No applications found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create Application">
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

<Modal bind:this={editModal} title="Edit Application">
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

<Modal bind:this={detailModal} title="Application {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Application ID</dt><dd>{viewed.applicationID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Wave</dt><dd>{viewed.waveID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Health</dt><dd>{viewed.applicationAggregatedStatus?.healthStatus ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Progress</dt><dd>{viewed.applicationAggregatedStatus?.progressStatus ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Total source servers</dt><dd>{viewed.applicationAggregatedStatus?.totalSourceServers ?? 0}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Source server association</p>
					<label class="flex flex-col gap-1 text-sm">Associate (comma or newline separated source server IDs)
						<textarea bind:value={assocServerIDs} rows="2" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
					</label>
					<button onclick={submitAssociate} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Associate</button>
					<label class="flex flex-col gap-1 text-sm">Disassociate (comma or newline separated source server IDs)
						<textarea bind:value={disassocServerIDs} rows="2" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
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
