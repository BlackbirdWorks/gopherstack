<script lang="ts">
	// Launch Configuration Templates (services/mgn/PARITY.md family C's
	// account-level half, 4 ops: Create/Delete/Describe/Update) plus the
	// template-scoped post-launch custom actions (family J's other half:
	// PutTemplateAction/ListTemplateActions/RemoveTemplateAction) -- 7 ops.
	// Distinct from the per-source-server Launch Configuration surfaced on the
	// Source Servers tab: no SDK op links a template to a server (PARITY.md's
	// "how template -> per-server configuration application actually
	// happens is not exposed by any op" gap).
	import {
		DescribeLaunchConfigurationTemplatesCommand,
		CreateLaunchConfigurationTemplateCommand,
		UpdateLaunchConfigurationTemplateCommand,
		DeleteLaunchConfigurationTemplateCommand,
		ListTemplateActionsCommand,
		PutTemplateActionCommand,
		RemoveTemplateActionCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type LaunchConfigurationTemplate,
		type TemplateActionDocument,
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

	let templates = $state<LaunchConfigurationTemplate[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchTemplates(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeLaunchConfigurationTemplatesCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		templates = reset ? (resp.items ?? []) : [...templates, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchTemplates(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchTemplates(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		templates.filter((t) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (t.launchConfigurationTemplateID ?? '').toLowerCase().includes(q);
		})
	);

	let createModal = $state<Modal | null>(null);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let newBootMode = $state<'LEGACY_BIOS' | 'UEFI' | 'USE_SOURCE'>('USE_SOURCE');
	let newCopyPrivateIp = $state(false);
	let newCopyTags = $state(false);

	function openCreate(): void {
		createError = null;
		newBootMode = 'USE_SOURCE';
		newCopyPrivateIp = false;
		newCopyTags = false;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		creating = true;
		createError = null;
		try {
			await client().send(
				new CreateLaunchConfigurationTemplateCommand({
					bootMode: newBootMode,
					copyPrivateIp: newCopyPrivateIp,
					copyTags: newCopyTags
				})
			);
			toast.success('Launch configuration template created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
			toast.error(createError);
		} finally {
			creating = false;
		}
	}

	async function deleteTemplate(t: LaunchConfigurationTemplate): Promise<void> {
		if (!t.launchConfigurationTemplateID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete launch configuration template',
			message: `Delete template ${t.launchConfigurationTemplateID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLaunchConfigurationTemplateCommand({ launchConfigurationTemplateID: t.launchConfigurationTemplateID }));
			toast.success('Template deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --------------------------- Detail / edit / actions --------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<LaunchConfigurationTemplate | null>(null);
	let editError = $state<string | null>(null);
	let editBootMode = $state<'LEGACY_BIOS' | 'UEFI' | 'USE_SOURCE'>('USE_SOURCE');
	let editCopyPrivateIp = $state(false);
	let editCopyTags = $state(false);

	let templateActions = $state<TemplateActionDocument[]>([]);
	let templateActionsError = $state<string | null>(null);
	let newActionID = $state('');
	let newActionName = $state('');
	let newActionDoc = $state('');
	let newActionOrder = $state(100);

	async function openDetail(t: LaunchConfigurationTemplate): Promise<void> {
		viewed = t;
		editError = null;
		editBootMode = (t.bootMode as typeof editBootMode) ?? 'USE_SOURCE';
		editCopyPrivateIp = t.copyPrivateIp ?? false;
		editCopyTags = t.copyTags ?? false;
		templateActionsError = null;
		newActionID = '';
		newActionName = '';
		newActionDoc = '';
		detailModal?.open();
		await loadTemplateActions();
	}

	async function loadTemplateActions(): Promise<void> {
		if (!viewed?.launchConfigurationTemplateID) return;
		try {
			const resp = await client().send(
				new ListTemplateActionsCommand({ launchConfigurationTemplateID: viewed.launchConfigurationTemplateID })
			);
			templateActions = resp.items ?? [];
		} catch (e) {
			templateActionsError = describeError(e);
		}
	}

	async function submitEdit(): Promise<void> {
		if (!viewed?.launchConfigurationTemplateID) return;
		editError = null;
		try {
			viewed = await client().send(
				new UpdateLaunchConfigurationTemplateCommand({
					launchConfigurationTemplateID: viewed.launchConfigurationTemplateID,
					bootMode: editBootMode,
					copyPrivateIp: editCopyPrivateIp,
					copyTags: editCopyTags
				})
			);
			toast.success('Template updated');
			await refresh();
		} catch (e) {
			editError = describeError(e);
			toast.error(editError);
		}
	}

	async function submitNewAction(): Promise<void> {
		if (!viewed?.launchConfigurationTemplateID || !newActionID.trim() || !newActionName.trim() || !newActionDoc.trim()) {
			templateActionsError = 'Action ID, name and document identifier are required.';
			return;
		}
		try {
			await client().send(
				new PutTemplateActionCommand({
					launchConfigurationTemplateID: viewed.launchConfigurationTemplateID,
					actionID: newActionID.trim(),
					actionName: newActionName.trim(),
					documentIdentifier: newActionDoc.trim(),
					order: newActionOrder
				})
			);
			toast.success('Template action saved');
			newActionID = '';
			newActionName = '';
			newActionDoc = '';
			await loadTemplateActions();
		} catch (e) {
			templateActionsError = describeError(e);
			toast.error(templateActionsError);
		}
	}

	async function removeTemplateAction(actionID: string | undefined): Promise<void> {
		if (!viewed?.launchConfigurationTemplateID || !actionID) return;
		try {
			await client().send(
				new RemoveTemplateActionCommand({ launchConfigurationTemplateID: viewed.launchConfigurationTemplateID, actionID })
			);
			toast.success('Template action removed');
			await loadTemplateActions();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.launchConfigurationTemplateID) return;
		const arn = taggableArn('launch-configuration-template', viewed.launchConfigurationTemplateID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.launchConfigurationTemplateID) return;
		const arn = taggableArn('launch-configuration-template', viewed.launchConfigurationTemplateID, currentRegion());
		await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
		const rest = { ...viewed.tags };
		delete rest[key];
		viewed = { ...viewed, tags: rest };
	}

	const columns = defineColumns<LaunchConfigurationTemplate>([
		{ key: 'launchConfigurationTemplateID', label: 'Template ID' },
		{ key: 'bootMode', label: 'Boot Mode' },
		{ key: 'launchDisposition', label: 'Launch Disposition' }
	]);
	const actionColumns = defineColumns<TemplateActionDocument>([
		{ key: 'order', label: 'Order' },
		{ key: 'actionID', label: 'Action ID' },
		{ key: 'actionName', label: 'Name' },
		{ key: 'documentIdentifier', label: 'Document' }
	]);
</script>

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Create template</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet rowActions(t: LaunchConfigurationTemplate)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(t)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteTemplate(t)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(t) => t.launchConfigurationTemplateID ?? ''}
	columns={[...columns, { key: 'actions', label: '', render: rowActions }]}
	{loading}
	emptyMessage="No launch configuration templates found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create Launch Configuration Template">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">Boot mode
				<select bind:value={newBootMode} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="USE_SOURCE">USE_SOURCE</option>
					<option value="LEGACY_BIOS">LEGACY_BIOS</option>
					<option value="UEFI">UEFI</option>
				</select>
			</label>
			<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={newCopyPrivateIp} /> Copy private IP</label>
			<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={newCopyTags} /> Copy tags</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={creating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{creating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Template {viewed?.launchConfigurationTemplateID ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<div class="grid grid-cols-2 gap-2 text-sm">
					<label class="flex flex-col gap-1">Boot mode
						<select bind:value={editBootMode} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							<option value="USE_SOURCE">USE_SOURCE</option>
							<option value="LEGACY_BIOS">LEGACY_BIOS</option>
							<option value="UEFI">UEFI</option>
						</select>
					</label>
					<label class="flex items-center gap-2"><input type="checkbox" bind:checked={editCopyPrivateIp} /> Copy private IP</label>
					<label class="flex items-center gap-2"><input type="checkbox" bind:checked={editCopyTags} /> Copy tags</label>
				</div>
				<button onclick={submitEdit} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Save</button>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Template post-launch actions</p>
					{#if templateActionsError}<p class="text-sm text-red-600 dark:text-red-400">{templateActionsError}</p>{/if}
					{#snippet actionRowActions(a: TemplateActionDocument)}
						<button onclick={() => removeTemplateAction(a.actionID)} class="text-red-600 hover:underline text-sm">Remove</button>
					{/snippet}
					<DataTable
						rows={templateActions}
						rowKey={(a) => a.actionID ?? ''}
						columns={[...actionColumns, { key: 'remove', label: '', render: actionRowActions }]}
						loading={false}
						emptyMessage="No template actions configured"
					/>
					<div class="grid grid-cols-4 gap-2">
						<input bind:value={newActionID} placeholder="Action ID" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input bind:value={newActionName} placeholder="Action name" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input bind:value={newActionDoc} placeholder="SSM document identifier" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input type="number" bind:value={newActionOrder} placeholder="Order" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
					<button onclick={submitNewAction} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Add action</button>
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
