<script lang="ts">
	// Network Migration -- Deployment (services/mgn/PARITY.md family N, 3 of
	// its 10 ops, plus the definition-scoped executions list lives on the
	// Definitions tab): StartNetworkMigrationDeployment,
	// ListNetworkMigrationDeployments, ListNetworkMigrationDeployedStacks.
	//
	// Real AWS deploys generated infrastructure as actual CloudFormation
	// stacks (types.NetworkMigrationDeployedStackDetails's own doc comment:
	// "Details about a CloudFormation stack that has been deployed"). This
	// emulator has no such deployment engine -- the job progresses honestly,
	// deployed-stack details always come back empty, matching the Analysis/
	// Code Generation tabs' identical honesty pattern.
	import {
		StartNetworkMigrationDeploymentCommand,
		ListNetworkMigrationDeploymentsCommand,
		ListNetworkMigrationDeployedStacksCommand,
		type NetworkMigrationDeployerJobDetails,
		type NetworkMigrationDeployedStackDetails,
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

	let starting = $state(false);
	let startError = $state<string | null>(null);

	let jobs = $state<NetworkMigrationDeployerJobDetails[]>([]);
	let jobsError = $state<string | null>(null);
	let jobsLoading = $state(false);

	let stacks = $state<NetworkMigrationDeployedStackDetails[]>([]);
	let stacksError = $state<string | null>(null);
	let stacksLoading = $state(false);

	function requireScope(): boolean {
		return Boolean(definitionID.trim() && executionID.trim());
	}

	async function submitStart(): Promise<void> {
		if (!requireScope()) {
			startError = 'Definition ID and Execution ID are required.';
			return;
		}
		starting = true;
		startError = null;
		try {
			const resp = await client().send(
				new StartNetworkMigrationDeploymentCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			toast.success(`Deployment job started: ${resp.jobID ?? '?'}`);
			await listJobs();
		} catch (e) {
			startError = describeError(e);
			toast.error(startError);
		} finally {
			starting = false;
		}
	}

	async function listJobs(): Promise<void> {
		if (!requireScope()) {
			jobsError = 'Definition ID and Execution ID are required.';
			return;
		}
		jobsLoading = true;
		jobsError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationDeploymentsCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			jobs = resp.items ?? [];
		} catch (e) {
			jobsError = describeError(e);
		} finally {
			jobsLoading = false;
		}
	}

	async function listStacks(): Promise<void> {
		if (!requireScope()) {
			stacksError = 'Definition ID and Execution ID are required.';
			return;
		}
		stacksLoading = true;
		stacksError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationDeployedStacksCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			stacks = resp.items ?? [];
		} catch (e) {
			stacksError = describeError(e);
		} finally {
			stacksLoading = false;
		}
	}

	const jobColumns = defineColumns<NetworkMigrationDeployerJobDetails>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'status', label: 'Status' }
	]);
	const stackColumns = defineColumns<NetworkMigrationDeployedStackDetails>([
		{ key: 'stackPhysicalID', label: 'Stack ID' },
		{ key: 'stackLogicalID', label: 'Logical ID' },
		{ key: 'status', label: 'Status' }
	]);
</script>

<div class="grid grid-cols-2 gap-3">
	<label class="flex flex-col gap-1 text-sm">Network migration definition ID
		<input bind:value={definitionID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
	<label class="flex flex-col gap-1 text-sm">Network migration execution ID
		<input bind:value={executionID} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
	</label>
</div>

<div class="flex gap-2">
	<button onclick={submitStart} disabled={starting} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">Start deployment</button>
	<button onclick={listJobs} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List deployment jobs</button>
	<button onclick={listStacks} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List deployed stacks</button>
</div>
{#if startError}<p class="text-sm text-red-600 dark:text-red-400">{startError}</p>{/if}

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Jobs</p>
	{#if jobsError}<p class="text-sm text-red-600 dark:text-red-400">{jobsError}</p>{/if}
	<DataTable rows={jobs} rowKey={(j) => j.jobID ?? ''} columns={jobColumns} loading={jobsLoading} emptyMessage="No deployment jobs yet" />
</div>

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">
		Deployed stacks
		<span class="ml-1 text-xs font-normal text-amber-600 dark:text-amber-400">(always empty -- no CloudFormation deployment engine exists for this feature)</span>
	</p>
	{#if stacksError}<p class="text-sm text-red-600 dark:text-red-400">{stacksError}</p>{/if}
	<DataTable rows={stacks} rowKey={(s) => s.stackPhysicalID ?? ''} columns={stackColumns} loading={stacksLoading} emptyMessage="No deployed stacks -- expected, see the note above" />
</div>
