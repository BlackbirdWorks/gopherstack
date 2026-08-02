<script lang="ts">
	// Network Migration -- Definitions (services/mgn/PARITY.md family M's
	// definition-CRUD half, 5 of its 13 ops): CreateNetworkMigrationDefinition,
	// GetNetworkMigrationDefinition, UpdateNetworkMigrationDefinition,
	// DeleteNetworkMigrationDefinition, ListNetworkMigrationDefinitions.
	//
	// This is a structurally separate sub-product from the 70-op replication
	// surface: it analyzes exported on-prem network configuration, maps it
	// onto a target AWS topology, generates infrastructure code, and deploys
	// it -- see the other five Network Migration tabs. A NetworkMigrationExecution
	// is scoped to one definition; no op creates one directly (this backend
	// auto-vivifies one the first time any Start* op references an unseen
	// (definition, execution) pair -- see the Mappings/Analysis/Code
	// Generation/Deployment tabs), so this panel's detail view lists whatever
	// executions already exist for the selected definition via
	// ListNetworkMigrationExecutions.
	import {
		ListNetworkMigrationDefinitionsCommand,
		CreateNetworkMigrationDefinitionCommand,
		GetNetworkMigrationDefinitionCommand,
		UpdateNetworkMigrationDefinitionCommand,
		DeleteNetworkMigrationDefinitionCommand,
		ListNetworkMigrationExecutionsCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type NetworkMigrationDefinitionSummary,
		type NetworkMigrationDefinition,
		type NetworkMigrationExecution,
		type SourceEnvironment,
		type TargetNetworkTopology,
		type TargetDeployment,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client, searchQuery }: Props = $props();

	let definitions = $state<NetworkMigrationDefinitionSummary[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchDefinitions(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListNetworkMigrationDefinitionsCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		definitions = reset ? (resp.items ?? []) : [...definitions, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchDefinitions(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchDefinitions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		definitions.filter((d) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (d.name ?? '').toLowerCase().includes(q) || (d.networkMigrationDefinitionID ?? '').toLowerCase().includes(q);
		})
	);

	let createModal = $state<Modal | null>(null);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let newName = $state('');
	let newDescription = $state('');
	let newTargetBucket = $state('');
	let newTargetBucketOwner = $state('');
	let newTopology = $state<TargetNetworkTopology>('ISOLATED_VPC');
	let newTargetDeployment = $state<TargetDeployment>('SINGLE_ACCOUNT');
	let newSourceEnvironment = $state<SourceEnvironment | ''>('');
	let newSourceBucket = $state('');
	let newSourceBucketOwner = $state('');
	let newSourceKey = $state('');

	function openCreate(): void {
		createError = null;
		newName = '';
		newDescription = '';
		newTargetBucket = '';
		newTargetBucketOwner = '';
		newTopology = 'ISOLATED_VPC';
		newTargetDeployment = 'SINGLE_ACCOUNT';
		newSourceEnvironment = '';
		newSourceBucket = '';
		newSourceBucketOwner = '';
		newSourceKey = '';
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!newName.trim() || !newTargetBucket.trim() || !newTargetBucketOwner.trim()) {
			createError = 'Name and target S3 bucket/owner are required.';
			return;
		}
		creating = true;
		createError = null;
		try {
			await client().send(
				new CreateNetworkMigrationDefinitionCommand({
					name: newName.trim(),
					description: newDescription.trim() || undefined,
					targetS3Configuration: { s3Bucket: newTargetBucket.trim(), s3BucketOwner: newTargetBucketOwner.trim() },
					targetNetwork: { topology: newTopology },
					targetDeployment: newTargetDeployment,
					sourceConfigurations:
						newSourceEnvironment && newSourceBucket.trim() && newSourceBucketOwner.trim() && newSourceKey.trim()
							? [
									{
										sourceEnvironment: newSourceEnvironment,
										sourceS3Configuration: {
											s3Bucket: newSourceBucket.trim(),
											s3BucketOwner: newSourceBucketOwner.trim(),
											s3Key: newSourceKey.trim()
										}
									}
								]
							: undefined
				})
			);
			toast.success('Network migration definition created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
			toast.error(createError);
		} finally {
			creating = false;
		}
	}

	async function deleteDefinition(d: NetworkMigrationDefinitionSummary): Promise<void> {
		if (!d.networkMigrationDefinitionID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete network migration definition',
			message: `Delete definition ${d.name ?? d.networkMigrationDefinitionID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteNetworkMigrationDefinitionCommand({ networkMigrationDefinitionID: d.networkMigrationDefinitionID }));
			toast.success('Definition deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// -------------------------------- Detail --------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<NetworkMigrationDefinition | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let editError = $state<string | null>(null);
	let editName = $state('');
	let editDescription = $state('');

	let executions = $state<NetworkMigrationExecution[]>([]);
	let executionsError = $state<string | null>(null);
	let executionsLoading = $state(false);

	async function openDetail(d: NetworkMigrationDefinitionSummary): Promise<void> {
		if (!d.networkMigrationDefinitionID) return;
		viewed = null;
		detailError = null;
		editError = null;
		executions = [];
		executionsError = null;
		detailModal?.open();
		detailLoading = true;
		try {
			viewed = await client().send(new GetNetworkMigrationDefinitionCommand({ networkMigrationDefinitionID: d.networkMigrationDefinitionID }));
			editName = viewed.name ?? '';
			editDescription = viewed.description ?? '';
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
		await loadExecutions();
	}

	async function loadExecutions(): Promise<void> {
		if (!viewed?.networkMigrationDefinitionID) return;
		executionsLoading = true;
		try {
			const resp = await client().send(
				new ListNetworkMigrationExecutionsCommand({ networkMigrationDefinitionID: viewed.networkMigrationDefinitionID })
			);
			executions = resp.items ?? [];
		} catch (e) {
			executionsError = describeError(e);
		} finally {
			executionsLoading = false;
		}
	}

	async function submitEdit(): Promise<void> {
		if (!viewed?.networkMigrationDefinitionID) return;
		editError = null;
		try {
			viewed = await client().send(
				new UpdateNetworkMigrationDefinitionCommand({
					networkMigrationDefinitionID: viewed.networkMigrationDefinitionID,
					name: editName.trim() || undefined,
					description: editDescription.trim() || undefined
				})
			);
			toast.success('Definition updated');
			await refresh();
		} catch (e) {
			editError = describeError(e);
			toast.error(editError);
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.networkMigrationDefinitionID) return;
		const arn = taggableArn('network-migration-definition', viewed.networkMigrationDefinitionID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.networkMigrationDefinitionID) return;
		const arn = taggableArn('network-migration-definition', viewed.networkMigrationDefinitionID, currentRegion());
		await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
		const rest = { ...viewed.tags };
		delete rest[key];
		viewed = { ...viewed, tags: rest };
	}

	const columns = defineColumns<NetworkMigrationDefinitionSummary>([
		{ key: 'name', label: 'Name' },
		{ key: 'networkMigrationDefinitionID', label: 'Definition ID' },
		{ key: 'sourceEnvironment', label: 'Source Environment' }
	]);
	const executionColumns = defineColumns<NetworkMigrationExecution>([
		{ key: 'networkMigrationExecutionID', label: 'Execution ID' },
		{ key: 'stage', label: 'Stage' },
		{ key: 'activity', label: 'Activity' },
		{ key: 'status', label: 'Status' }
	]);
</script>

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">Create definition</button>
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet rowActions(d: NetworkMigrationDefinitionSummary)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(d)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteDefinition(d)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(d) => d.networkMigrationDefinitionID ?? ''}
	columns={[...columns, { key: 'actions', label: '', render: rowActions }]}
	{loading}
	emptyMessage="No network migration definitions found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create Network Migration Definition">
	{#snippet children()}
		<div class="space-y-3 max-h-[60vh] overflow-y-auto pr-1">
			<label class="flex flex-col gap-1 text-sm">Name
				<input bind:value={newName} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Description
				<input bind:value={newDescription} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Target network topology
				<select bind:value={newTopology} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="ISOLATED_VPC">ISOLATED_VPC</option>
					<option value="HUB_AND_SPOKE">HUB_AND_SPOKE</option>
				</select>
			</label>
			<label class="flex flex-col gap-1 text-sm">Target deployment
				<select bind:value={newTargetDeployment} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="SINGLE_ACCOUNT">SINGLE_ACCOUNT</option>
					<option value="MULTI_ACCOUNT">MULTI_ACCOUNT</option>
				</select>
			</label>
			<label class="flex flex-col gap-1 text-sm">Target S3 bucket
				<input bind:value={newTargetBucket} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Target S3 bucket owner (account ID)
				<input bind:value={newTargetBucketOwner} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<p class="text-xs text-slate-500 dark:text-slate-400 pt-2">Optional source configuration (leave the source environment unset to skip):</p>
			<label class="flex flex-col gap-1 text-sm">Source environment
				<select bind:value={newSourceEnvironment} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">(none)</option>
					<option value="VSPHERE">VSPHERE</option>
					<option value="NSX">NSX</option>
					<option value="FORTIGATE_FIREWALL">FORTIGATE_FIREWALL</option>
					<option value="PALO_ALTO_FIREWALL">PALO_ALTO_FIREWALL</option>
					<option value="CISCO_ACI">CISCO_ACI</option>
					<option value="LOGICAL_MODEL">LOGICAL_MODEL</option>
					<option value="MODELIZE_IT">MODELIZE_IT</option>
					<option value="AWS_DISCOVERY_COLLECTOR">AWS_DISCOVERY_COLLECTOR</option>
				</select>
			</label>
			{#if newSourceEnvironment}
				<label class="flex flex-col gap-1 text-sm">Source S3 bucket
					<input bind:value={newSourceBucket} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm">Source S3 bucket owner
					<input bind:value={newSourceBucketOwner} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm">Source S3 key
					<input bind:value={newSourceKey} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
			{/if}
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={creating} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Definition {viewed?.name ?? ''}">
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<div class="grid grid-cols-2 gap-2 text-sm">
					<label class="flex flex-col gap-1">Name
						<input bind:value={editName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<label class="flex flex-col gap-1">Description
						<input bind:value={editDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
				</div>
				<button onclick={submitEdit} class="px-3 py-1 text-xs rounded-lg bg-violet-600 text-white hover:bg-violet-700">Save</button>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}

				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Topology</dt><dd>{viewed.targetNetwork?.topology ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Deployment</dt><dd>{viewed.targetDeployment ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
					<div><dt class="text-slate-500">Updated</dt><dd>{formatDate(viewed.updatedAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Executions</p>
					<p class="text-xs text-slate-500 dark:text-slate-400 mb-2">
						No operation creates an execution directly -- one is auto-vivified the
						first time a Mapping/Analysis/Code Generation/Deployment op
						references an unseen execution ID for this definition (see those
						tabs).
					</p>
					{#if executionsError}<p class="text-sm text-red-600 dark:text-red-400">{executionsError}</p>{/if}
					<DataTable rows={executions} rowKey={(e) => e.networkMigrationExecutionID ?? ''} columns={executionColumns} loading={executionsLoading} emptyMessage="No executions yet" />
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
