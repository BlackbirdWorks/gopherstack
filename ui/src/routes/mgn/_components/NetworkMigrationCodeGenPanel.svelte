<script lang="ts">
	// Network Migration -- Code generation (services/mgn/PARITY.md family N,
	// 3 of its 10 ops): StartNetworkMigrationCodeGeneration,
	// ListNetworkMigrationCodeGenerations, ListNetworkMigrationCodeGenerationSegments.
	//
	// Same honesty pattern as the Analysis tab: the JOB progresses for real
	// (PENDING -> STARTED -> SUCCEEDED), but generated-code segments/artifacts
	// always come back empty -- there is no code-generation engine in this
	// repo, and PARITY.md is explicit that inventing infrastructure-as-code
	// output would misrepresent what actually happened.
	import {
		StartNetworkMigrationCodeGenerationCommand,
		ListNetworkMigrationCodeGenerationsCommand,
		ListNetworkMigrationCodeGenerationSegmentsCommand,
		type NetworkMigrationCodeGenerationJobDetails,
		type NetworkMigrationCodeGenerationSegment,
		type CodeGenerationOutputFormatType,
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
	let outputFormat = $state<CodeGenerationOutputFormatType | ''>('');

	let starting = $state(false);
	let startError = $state<string | null>(null);

	let jobs = $state<NetworkMigrationCodeGenerationJobDetails[]>([]);
	let jobsError = $state<string | null>(null);
	let jobsLoading = $state(false);

	let segments = $state<NetworkMigrationCodeGenerationSegment[]>([]);
	let segmentsError = $state<string | null>(null);
	let segmentsLoading = $state(false);

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
				new StartNetworkMigrationCodeGenerationCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim(),
					codeGenerationOutputFormatTypes: outputFormat ? [outputFormat] : undefined
				})
			);
			toast.success(`Code generation job started: ${resp.jobID ?? '?'}`);
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
				new ListNetworkMigrationCodeGenerationsCommand({
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

	async function listSegments(): Promise<void> {
		if (!requireScope()) {
			segmentsError = 'Definition ID and Execution ID are required.';
			return;
		}
		segmentsLoading = true;
		segmentsError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationCodeGenerationSegmentsCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			segments = resp.items ?? [];
		} catch (e) {
			segmentsError = describeError(e);
		} finally {
			segmentsLoading = false;
		}
	}

	const jobColumns = defineColumns<NetworkMigrationCodeGenerationJobDetails>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'status', label: 'Status' }
	]);
	const segmentColumns = defineColumns<NetworkMigrationCodeGenerationSegment>([
		{ key: 'segmentID', label: 'Segment ID' },
		{ key: 'segmentType', label: 'Type' }
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

<div class="flex flex-wrap items-end gap-2">
	<label class="flex flex-col gap-1 text-sm w-56">Output format (optional)
		<select bind:value={outputFormat} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
			<option value="">(default)</option>
			<option value="CDK_L1">CDK_L1</option>
			<option value="CDK_L2">CDK_L2</option>
			<option value="LZA">LZA</option>
			<option value="TERRAFORM">TERRAFORM</option>
		</select>
	</label>
	<button onclick={submitStart} disabled={starting} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">Start code generation</button>
	<button onclick={listJobs} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List code generation jobs</button>
	<button onclick={listSegments} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List code generation segments</button>
</div>
{#if startError}<p class="text-sm text-red-600 dark:text-red-400">{startError}</p>{/if}

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Jobs</p>
	{#if jobsError}<p class="text-sm text-red-600 dark:text-red-400">{jobsError}</p>{/if}
	<DataTable rows={jobs} rowKey={(j) => j.jobID ?? ''} columns={jobColumns} loading={jobsLoading} emptyMessage="No code generation jobs yet" />
</div>

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">
		Segments
		<span class="ml-1 text-xs font-normal text-amber-600 dark:text-amber-400">(always empty -- no code-generation engine exists)</span>
	</p>
	{#if segmentsError}<p class="text-sm text-red-600 dark:text-red-400">{segmentsError}</p>{/if}
	<DataTable rows={segments} rowKey={(s) => s.segmentID ?? ''} columns={segmentColumns} loading={segmentsLoading} emptyMessage="No code generation segments -- expected, see the note above" />
</div>
