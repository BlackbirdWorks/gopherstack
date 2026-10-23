<script lang="ts">
	import { onMount } from 'svelte';
	import { Play, Plus, RefreshCw, Trash2 } from 'lucide-svelte';
	import RunExecutions from '#lib/components/gcp/RunExecutions.svelte';
	import RunJobForm from '#lib/components/gcp/RunJobForm.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_MINI, GCP_PRIMARY, attempt } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { createJob, deleteJob, listJobs, runJob, type JobInfo, type NewJob, type RunScope } from '#lib/gcprun.ts';

	type Props = { scope: RunScope; tick: number; autoRefresh: boolean; onRuntime: (down: boolean) => void };
	let { scope, tick, autoRefresh, onRuntime }: Props = $props();

	const REFRESH_MS = 3000;
	let jobs = $state<JobInfo[]>([]);
	let expanded = $state<string | null>(null);
	let loading = $state(true);
	let loadError = $state('');
	let busy = $state('');
	let form = $state<RunJobForm | null>(null);
	const cell = 'px-4 py-3';

	async function load() {
		try {
			jobs = (await listJobs(scope)).toSorted((a, b) => a.id.localeCompare(b.id));
			loadError = '';
		} catch (e) {
			loadError = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function act(label: string, fn: () => Promise<unknown>, ok: string, fail: string): Promise<boolean> {
		busy = label;
		const done = await attempt(fn, ok, fail);
		busy = '';
		if (done) await load();
		return done;
	}

	async function create(job: NewJob) {
		const ok = await act('create', () => createJob(scope, job), `Created job ${job.id}`, `Failed to create ${job.id}`);
		if (ok) form?.close();
	}

	async function run(job: JobInfo) {
		const ok = await act(`run-${job.id}`, () => runJob(scope, job.id), `Started ${job.id}`, `Failed to run ${job.id}`);
		if (ok) expanded = job.id;
	}

	async function remove(job: JobInfo) {
		if (!(await confirmDestructive({ title: 'Delete job', message: `Delete job "${job.id}" and its executions?` }))) return;
		if (expanded === job.id) expanded = null;
		await act(`del-${job.id}`, () => deleteJob(scope, job.id), `Deleted ${job.id}`, `Failed to delete ${job.id}`);
	}

	$effect(() => {
		void tick;
		void scope;
		void load();
	});

	$effect(() => {
		if (!autoRefresh) return;
		const timer = setInterval(() => void load(), REFRESH_MS);
		return () => clearInterval(timer);
	});

	onMount(() => () => onRuntime(false));
</script>

<div class="space-y-4" data-testid="run-jobs">
	<div class="flex justify-end">
		<button class="flex items-center gap-2 {GCP_PRIMARY}" onclick={() => form?.open()} disabled={loading || Boolean(loadError)} data-testid="run-new-job"><Plus class="h-4 w-4" /> Create job</button>
	</div>

	{#if loadError && jobs.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="run-jobs-error">
			<p class="font-medium">Could not reach Cloud Run</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if jobs.length === 0}
		<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="run-no-jobs">No jobs in {scope.location}</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700" data-testid="run-job-list">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Job</th><th class={cell}>Image</th><th class={cell}>Tasks</th><th class={cell}>Parallelism</th><th class={cell}>Max retries</th><th class={cell}>Executions</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each jobs as j (j.name)}
						<tr data-testid="run-job-row">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (expanded = expanded === j.id ? null : j.id)} aria-expanded={expanded === j.id} data-testid="run-job-toggle">{j.id}</button></td>
							<td class="{cell} break-all font-mono text-xs">{j.image}</td>
							<td class="{cell} text-xs">{j.taskCount}</td>
							<td class="{cell} text-xs">{j.parallelism || 'all'}</td>
							<td class="{cell} text-xs">{j.maxRetries}</td>
							<td class="{cell} text-xs">{j.executionCount}</td>
							<td class="{cell} space-x-1 whitespace-nowrap text-right">
								<button class={GCP_MINI} disabled={busy === `run-${j.id}`} onclick={() => run(j)} data-testid="run-job-run"><Play class="inline h-3.5 w-3.5" /> Run</button>
								<button class="{GCP_MINI} text-red-600" disabled={busy === `del-${j.id}`} onclick={() => remove(j)} aria-label="Delete job {j.id}" data-testid="run-job-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
							</td>
						</tr>
						{#if expanded === j.id}
							<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="run-job-detail">
								<td colspan="7" class="px-4 py-3"><RunExecutions {scope} job={j.id} {tick} {autoRefresh} {onRuntime} /></td>
							</tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>

<RunJobForm bind:this={form} busy={busy === 'create'} onCreate={create} />
