<script lang="ts">
	// Network Migration -- Analysis (services/mgn/PARITY.md family N, 3 of
	// its 10 ops): StartNetworkMigrationAnalysis, ListNetworkMigrationAnalyses,
	// ListNetworkMigrationAnalysisResults.
	//
	// The analysis JOB progresses honestly (PENDING -> STARTED -> SUCCEEDED,
	// a shared internal bookkeeping record backing all 5 Network Migration
	// job-details shapes -- PARITY.md's Implementation summary), but
	// analysis RESULT CONTENT always comes back empty: no real
	// network-analysis engine exists to produce findings from, and
	// fabricating "AnalysisResult" free-text would misrepresent what this
	// emulator actually did (PARITY.md's own explicit warning). This panel
	// shows the job progressing for real while being honest that results
	// stay empty.
	import {
		StartNetworkMigrationAnalysisCommand,
		ListNetworkMigrationAnalysesCommand,
		ListNetworkMigrationAnalysisResultsCommand,
		type NetworkMigrationAnalysisJobDetails,
		type NetworkMigrationAnalysisResult,
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

	let jobs = $state<NetworkMigrationAnalysisJobDetails[]>([]);
	let jobsError = $state<string | null>(null);
	let jobsLoading = $state(false);

	let results = $state<NetworkMigrationAnalysisResult[]>([]);
	let resultsError = $state<string | null>(null);
	let resultsLoading = $state(false);

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
				new StartNetworkMigrationAnalysisCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			toast.success(`Analysis job started: ${resp.jobID ?? '?'}`);
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
				new ListNetworkMigrationAnalysesCommand({
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

	async function listResults(): Promise<void> {
		if (!requireScope()) {
			resultsError = 'Definition ID and Execution ID are required.';
			return;
		}
		resultsLoading = true;
		resultsError = null;
		try {
			const resp = await client().send(
				new ListNetworkMigrationAnalysisResultsCommand({
					networkMigrationDefinitionID: definitionID.trim(),
					networkMigrationExecutionID: executionID.trim()
				})
			);
			results = resp.items ?? [];
		} catch (e) {
			resultsError = describeError(e);
		} finally {
			resultsLoading = false;
		}
	}

	const jobColumns = defineColumns<NetworkMigrationAnalysisJobDetails>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'status', label: 'Status' }
	]);
	const resultColumns = defineColumns<NetworkMigrationAnalysisResult>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'analyzerType', label: 'Analyzer' },
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
	<button onclick={submitStart} disabled={starting} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">Start analysis</button>
	<button onclick={listJobs} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List analysis jobs</button>
	<button onclick={listResults} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 text-slate-700 dark:text-slate-200 hover:bg-slate-100 dark:hover:bg-slate-800">List analysis results</button>
</div>
{#if startError}<p class="text-sm text-red-600 dark:text-red-400">{startError}</p>{/if}

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Jobs</p>
	{#if jobsError}<p class="text-sm text-red-600 dark:text-red-400">{jobsError}</p>{/if}
	<DataTable rows={jobs} rowKey={(j) => j.jobID ?? ''} columns={jobColumns} loading={jobsLoading} emptyMessage="No analysis jobs yet" />
</div>

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">
		Results
		<span class="ml-1 text-xs font-normal text-amber-600 dark:text-amber-400">(always empty -- no real analysis engine exists; see the note above)</span>
	</p>
	{#if resultsError}<p class="text-sm text-red-600 dark:text-red-400">{resultsError}</p>{/if}
	<DataTable rows={results} rowKey={(r) => r.jobID ?? ''} columns={resultColumns} loading={resultsLoading} emptyMessage="No analysis results -- expected, see the note above" />
</div>
