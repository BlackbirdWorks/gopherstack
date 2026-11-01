<script lang="ts">
	import { onMount } from 'svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listEntries, severityTone, type LogEntry } from '#lib/gcplogging.ts';
	import { isSettled, jobLogFilter, listTasks, statePill, type BatchJob, type BatchTask } from '#lib/gcpbatch.ts';

	const POLL_MS = 2000;

	let { job }: { job: BatchJob } = $props();

	let tasks = $state<BatchTask[]>([]);
	let openTask = $state('');
	let logs = $state<LogEntry[]>([]);
	let error = $state('');
	let logError = $state('');

	const cell = 'px-3 py-2';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';

	async function load() {
		try {
			tasks = await listTasks(job.name);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		}
		try {
			logs = await listEntries(job.project, jobLogFilter(job.project, job.id), 100);
			logError = '';
		} catch (e) {
			logs = [];
			logError = describeGcpError(e);
		}
	}

	$effect(() => {
		void job.name;
		void job.state;
		void load();
	});

	onMount(() => {
		const timer = setInterval(() => {
			if (!isSettled(job)) void load();
		}, POLL_MS);
		return () => clearInterval(timer);
	});
</script>

<section class="space-y-5 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="batch-detail">
	<h2 class="text-lg font-semibold">{job.id} <span class="rounded-full px-2 py-0.5 text-xs {statePill(job.state)}" data-testid="batch-detail-state">{job.state}</span></h2>

	{#each job.groups as g (g.name)}
		<div class="space-y-2" data-testid="batch-group">
			<h3 class="text-sm font-semibold">Task group <span class="font-mono">{g.name.split('/').pop()}</span></h3>
			<p class="text-xs text-gray-500 dark:text-gray-400">taskCount {g.taskCount} · parallelism {g.parallelism || 'unbounded'} · max retries {g.maxRetryCount}{g.maxRunDuration ? ` · max run ${g.maxRunDuration}` : ''}</p>
			{#each g.runnables as r, i (i)}
				<div class="rounded border border-gray-100 p-3 text-xs dark:border-gray-800" data-testid="batch-runnable">
					<p class="font-medium">Runnable {i + 1}: {r.kind}{r.background ? ' (background)' : ''}{r.alwaysRun ? ' (always run)' : ''}</p>
					{#if r.kind === 'container'}
						<p class="font-mono break-all" data-testid="batch-runnable-image">{r.image}</p>
						{#if r.commands.length}<pre class="mt-1 overflow-x-auto rounded bg-gray-50 p-2 font-mono dark:bg-slate-900" data-testid="batch-runnable-commands">{r.commands.join(' ')}</pre>{/if}
					{:else if r.kind === 'script'}
						{#if r.scriptPath}<p class="font-mono">{r.scriptPath}</p>{/if}
						<pre class="mt-1 overflow-x-auto rounded bg-gray-50 p-2 font-mono dark:bg-slate-900" data-testid="batch-runnable-script">{r.scriptText}</pre>
					{/if}
					{#each Object.entries(r.variables) as [k, v] (k)}<p class="font-mono">{k}={v}</p>{/each}
					{#each r.secretNames as k (k)}<p class="font-mono" data-testid="batch-secret">{k}=(secret)</p>{/each}
				</div>
			{/each}
			{#if Object.keys(g.variables).length || g.secretNames.length}
				<div class="text-xs" data-testid="batch-env">
					<p class="font-medium">Environment</p>
					{#each Object.entries(g.variables) as [k, v] (k)}<p class="font-mono">{k}={v}</p>{/each}
					{#each g.secretNames as k (k)}<p class="font-mono" data-testid="batch-secret">{k}=(secret)</p>{/each}
				</div>
			{/if}
		</div>
	{/each}

	<div>
		<h3 class="mb-1 text-sm font-semibold">Status events</h3>
		<ol class="space-y-1 text-xs" data-testid="batch-events">
			{#each job.events as e, i (i)}
				<li class="flex gap-3"><span class="shrink-0 font-mono text-gray-500">{e.time}</span><span>{e.description || e.type}</span></li>
			{:else}
				<li class="text-gray-500">No events.</li>
			{/each}
		</ol>
	</div>

	<div>
		<h3 class="mb-1 text-sm font-semibold">Tasks</h3>
		{#if error}<p class="text-sm text-red-600" role="alert">{error}</p>{/if}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="batch-task-table">
				<thead class={thead}><tr><th class={cell}>Task</th><th class={cell}>State</th><th class={cell}>Exit code</th><th class={cell}>Attempts</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each tasks as t (t.name)}
						<tr data-testid="batch-task">
							<td class="{cell} font-mono text-xs"><button class="text-blue-600 underline dark:text-blue-400" onclick={() => (openTask = openTask === t.name ? '' : t.name)}>{t.index}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {statePill(t.state)}" data-testid="batch-task-state">{t.state}</span></td>
							<td class="{cell} text-xs" data-testid="batch-task-exit">{t.exitCode ?? '—'}</td>
							<td class="{cell} text-xs" data-testid="batch-task-attempts">{t.attempts}</td>
						</tr>
						{#if openTask === t.name}
							<tr><td colspan="4" class="{cell} text-xs"><ol class="space-y-1" data-testid="batch-task-events">{#each t.events as e, i (i)}<li class="flex gap-3"><span class="shrink-0 font-mono text-gray-500">{e.time}</span><span>{e.description}{e.exitCode !== null ? ` (exit ${e.exitCode})` : ''}</span></li>{/each}</ol></td></tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
	</div>

	<div>
		<h3 class="mb-1 text-sm font-semibold">Task logs <span class="font-normal text-gray-500">(Cloud Logging, log {'batch_task_logs'})</span></h3>
		{#if logError}
			<p class="text-xs text-gray-500" data-testid="batch-logs-unavailable">Task logs could not be read: {logError}</p>
		{:else if logs.length === 0}
			<p class="text-xs text-gray-500" data-testid="batch-logs-empty">No log entries. Tasks write logs only when the docker engine runs them.</p>
		{:else}
			<ul class="max-h-64 overflow-auto rounded bg-gray-50 p-2 font-mono text-xs dark:bg-slate-900" data-testid="batch-logs">
				{#each logs as l (l.insertId)}
					<li><span class="mr-2 rounded px-1 {severityTone(l.severity)}">{l.severity}</span>{l.message}</li>
				{/each}
			</ul>
		{/if}
	</div>
</section>
