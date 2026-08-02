<script lang="ts">
	// Network Migration -- Mappings (services/mgn/PARITY.md family M's
	// mapping-job half, 4 ops): ListNetworkMigrationMappings,
	// ListNetworkMigrationMappingUpdates, StartNetworkMigrationMapping,
	// StartNetworkMigrationMappingUpdate.
	//
	// StartNetworkMigrationMapping is one of the 5 Start* ops that
	// auto-vivifies a NetworkMigrationExecution the first time this backend
	// sees an (definitionID, executionID) pair it hasn't seen before
	// (PARITY.md's documented convention for the "no op creates an
	// execution" gap) -- so this is a legitimate way to bring a new
	// execution into existence, not just to progress one.
	import {
		ListNetworkMigrationMappingsCommand,
		ListNetworkMigrationMappingUpdatesCommand,
		StartNetworkMigrationMappingCommand,
		StartNetworkMigrationMappingUpdateCommand,
		type NetworkMigrationMappingJobDetails,
		type NetworkMigrationMappingUpdateJobDetails,
		type SecurityGroupMappingStrategy,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import { describeError } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client }: Props = $props();

	let definitionID = $state('');
	let executionID = $state('');

	let mappings = $state<NetworkMigrationMappingJobDetails[]>([]);
	let mappingsError = $state<string | null>(null);
	let mappingsLoading = $state(false);

	let mappingUpdates = $state<NetworkMigrationMappingUpdateJobDetails[]>([]);
	let mappingUpdatesError = $state<string | null>(null);
	let mappingUpdatesLoading = $state(false);

	let strategy = $state<SecurityGroupMappingStrategy>('MAP');
	let startError = $state<string | null>(null);
	let starting = $state(false);

	let updateConstructID = $state('');
	let updateConstructType = $state('');
	let updateSegmentID = $state('');
	let updateOperationKind = $state<'delete' | 'update'>('delete');
	let updateNewName = $state('');
	let updateStartError = $state<string | null>(null);
	let updateStarting = $state(false);

	function requireScope(): boolean {
		return Boolean(definitionID.trim() && executionID.trim());
	}

	async function listMappings(): Promise<void> {
		if (!requireScope()) {
			mappingsError = 'Definition ID and Execution ID are required.';
			return;
		}
		mappingsLoading = true;
		mappingsError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationMappingsCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			mappings = resp.items ?? [];
		} catch (e) {
			mappingsError = describeError(e);
		} finally {
			mappingsLoading = false;
		}
	}

	async function listMappingUpdates(): Promise<void> {
		if (!requireScope()) {
			mappingUpdatesError = 'Definition ID and Execution ID are required.';
			return;
		}
		mappingUpdatesLoading = true;
		mappingUpdatesError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationMappingUpdatesCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			mappingUpdates = resp.items ?? [];
		} catch (e) {
			mappingUpdatesError = describeError(e);
		} finally {
			mappingUpdatesLoading = false;
		}
	}

	async function submitStartMapping(): Promise<void> {
		if (!requireScope()) {
			startError = 'Definition ID and Execution ID are required.';
			return;
		}
		starting = true;
		startError = null;
		try {
			const resp = await client().send(
				new StartNetworkMigrationMappingCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim(),
					securityGroupMappingStrategy: strategy
				})
			);
			toast.success(`Mapping job started: ${resp.jobID ?? '?'}`);
			await listMappings();
		} catch (e) {
			startError = describeError(e);
			toast.error(startError);
		} finally {
			starting = false;
		}
	}

	async function submitStartMappingUpdate(): Promise<void> {
		if (!requireScope() || !updateConstructID.trim() || !updateConstructType.trim() || !updateSegmentID.trim()) {
			updateStartError = 'Definition/Execution IDs and construct ID/type/segment ID are all required.';
			return;
		}
		updateStarting = true;
		updateStartError = null;
		try {
			const resp = await client().send(
				new StartNetworkMigrationMappingUpdateCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim(),
					constructs: [
						{
							constructID: updateConstructID.trim(),
							constructType: updateConstructType.trim(),
							segmentID: updateSegmentID.trim(),
							operation:
								updateOperationKind === 'delete' ? { delete: {} } : { update: { name: updateNewName.trim() || undefined } }
						}
					]
				})
			);
			toast.success(`Mapping update job started: ${resp.jobID ?? '?'}`);
			await listMappingUpdates();
		} catch (e) {
			updateStartError = describeError(e);
			toast.error(updateStartError);
		} finally {
			updateStarting = false;
		}
	}

	const mappingColumns = defineColumns<NetworkMigrationMappingJobDetails>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'status', label: 'Status' }
	]);
	const mappingUpdateColumns = defineColumns<NetworkMigrationMappingUpdateJobDetails>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'status', label: 'Status' }
	]);
</script>

<div class="grid grid-cols-2 gap-3">
	<label class="flex flex-col gap-1 text-sm">Network migration definition ID
		<input bind:value={definitionID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
	<label class="flex flex-col gap-1 text-sm">Network migration execution ID
		<span class="text-xs font-normal text-slate-500 dark:text-slate-400">(a new ID here is auto-vivified into a real execution the first time Start Mapping runs)</span>
		<input bind:value={executionID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
</div>

<div class="space-y-2 border-t border-slate-200 dark:border-slate-700 pt-3">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Start network migration mapping</p>
	<label class="flex flex-col gap-1 text-sm w-64">Security group mapping strategy
		<select bind:value={strategy} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
			<option value="MAP">MAP</option>
			<option value="MAP_DHCP">MAP_DHCP</option>
			<option value="SKIP">SKIP</option>
		</select>
	</label>
	<button onclick={submitStartMapping} disabled={starting} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">Start mapping</button>
	<button onclick={listMappings} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List mapping jobs</button>
	{#if startError}<p class="text-sm text-red-600 dark:text-red-400">{startError}</p>{/if}
	{#if mappingsError}<p class="text-sm text-red-600 dark:text-red-400">{mappingsError}</p>{/if}
	<DataTable rows={mappings} rowKey={(m) => m.jobID ?? ''} columns={mappingColumns} loading={mappingsLoading} emptyMessage="No mapping jobs yet" />
</div>

<div class="space-y-2 border-t border-slate-200 dark:border-slate-700 pt-3">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Start mapping update</p>
	<div class="grid grid-cols-3 gap-2">
		<input bind:value={updateSegmentID} placeholder="Segment ID" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
		<input bind:value={updateConstructID} placeholder="Construct ID" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
		<input bind:value={updateConstructType} placeholder="Construct type" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</div>
	<label class="flex flex-col gap-1 text-sm w-64">Operation
		<select bind:value={updateOperationKind} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
			<option value="delete">Delete construct</option>
			<option value="update">Rename construct</option>
		</select>
	</label>
	{#if updateOperationKind === 'update'}
		<input bind:value={updateNewName} placeholder="New name" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	{/if}
	<div>
		<button onclick={submitStartMappingUpdate} disabled={updateStarting} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">Start mapping update</button>
		<button onclick={listMappingUpdates} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List mapping update jobs</button>
	</div>
	{#if updateStartError}<p class="text-sm text-red-600 dark:text-red-400">{updateStartError}</p>{/if}
	{#if mappingUpdatesError}<p class="text-sm text-red-600 dark:text-red-400">{mappingUpdatesError}</p>{/if}
	<DataTable rows={mappingUpdates} rowKey={(m) => m.jobID ?? ''} columns={mappingUpdateColumns} loading={mappingUpdatesLoading} emptyMessage="No mapping update jobs yet" />
</div>
