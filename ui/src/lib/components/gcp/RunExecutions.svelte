<script lang="ts">
	import { Ban, Trash2 } from 'lucide-svelte';
	import RunBadge from '#lib/components/gcp/RunBadge.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_MINI, attempt } from '#lib/gcp-action.ts';
	import { cancelExecution, deleteExecution, listExecutions, listTasks, runtimeUnavailable, taskMessages, type ExecutionInfo, type ExecutionState, type RunScope, type TaskInfo, type TaskState } from '#lib/gcprun.ts';

	type Props = { scope: RunScope; job: string; tick: number; autoRefresh: boolean; onRuntime: (down: boolean) => void };
	let { scope, job, tick, autoRefresh, onRuntime }: Props = $props();

	const REFRESH_MS = 2000;
	let executions = $state<ExecutionInfo[]>([]);
	let tasks = $state<TaskInfo[]>([]);
	let open = $state<string | null>(null);
	let busy = $state('');
	let loaded = $state(false);

	const when = (iso: string) => (iso ? new Date(iso).toLocaleString() : '—');
	const TONE: Record<ExecutionState | TaskState, 'ok' | 'failed' | 'pending'> = {
		succeeded: 'ok', failed: 'failed', cancelled: 'failed', running: 'pending', pending: 'pending'
	};

	async function load() {
		try {
			executions = (await listExecutions(scope, job)).toSorted((a, b) => b.createTime.localeCompare(a.createTime));
			tasks = open ? (await listTasks(scope, job, open)).toSorted((a, b) => a.index - b.index) : [];
			onRuntime(runtimeUnavailable(tasks.flatMap((t) => taskMessages(t))) || runtimeUnavailable(executions.map((e) => e.message)));
		} catch {
			executions = [];
		} finally {
			loaded = true;
		}
	}

	async function act(label: string, fn: () => Promise<unknown>, ok: string, fail: string) {
		busy = label;
		await attempt(fn, ok, fail);
		busy = '';
		await load();
	}

	async function cancel(e: ExecutionInfo) {
		await act(`c-${e.id}`, () => cancelExecution(scope, job, e.id), `Cancelling ${e.id}`, `Failed to cancel ${e.id}`);
	}

	async function remove(e: ExecutionInfo) {
		if (!(await confirmDestructive({ title: 'Delete execution', message: `Delete execution "${e.id}"?` }))) return;
		if (open === e.id) open = null;
		await act(`d-${e.id}`, () => deleteExecution(scope, job, e.id), `Deleted ${e.id}`, `Failed to delete ${e.id}`);
	}

	function toggle(e: ExecutionInfo) {
		open = open === e.id ? null : e.id;
		tasks = [];
		void load();
	}

	$effect(() => {
		void tick;
		void job;
		void load();
	});

	$effect(() => {
		if (!autoRefresh) return;
		const timer = setInterval(() => void load(), REFRESH_MS);
		return () => clearInterval(timer);
	});
</script>

<div class="space-y-2" data-testid="run-executions">
	<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 dark:text-gray-400">Executions</h4>
	{#if loaded && executions.length === 0}
		<p class="text-xs text-gray-500" data-testid="run-no-executions">No executions yet. Use Run to start one.</p>
	{:else}
		<table class="w-full text-xs">
			<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="py-1 pr-3">Execution</th><th class="pr-3">Status</th><th class="pr-3">Succeeded</th><th class="pr-3">Failed</th><th class="pr-3">Running</th><th class="pr-3">Tasks</th><th class="pr-3">Started</th><th></th></tr></thead>
			<tbody>
				{#each executions as e (e.name)}
					<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="run-execution-row">
						<td class="py-1 pr-3"><button class="font-mono underline decoration-dotted" onclick={() => toggle(e)} aria-expanded={open === e.id} data-testid="run-execution-toggle">{e.id}</button></td>
						<td class="pr-3"><RunBadge tone={TONE[e.state]} label={e.state} testId="run-execution-state" /></td>
						<td class="pr-3 text-green-700 dark:text-green-400" data-testid="run-count-succeeded">{e.succeeded}</td>
						<td class="pr-3 text-red-700 dark:text-red-400" data-testid="run-count-failed">{e.failed}</td>
						<td class="pr-3" data-testid="run-count-running">{e.running}</td>
						<td class="pr-3">{e.taskCount}</td>
						<td class="pr-3">{when(e.createTime)}</td>
						<td class="space-x-1 whitespace-nowrap text-right">
							{#if e.state === 'running'}
								<button class={GCP_MINI} disabled={busy === `c-${e.id}`} onclick={() => cancel(e)} data-testid="run-execution-cancel"><Ban class="inline h-3.5 w-3.5" /> Cancel</button>
							{/if}
							<button class="{GCP_MINI} text-red-600" disabled={busy === `d-${e.id}`} onclick={() => remove(e)} aria-label="Delete execution {e.id}" data-testid="run-execution-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
						</td>
					</tr>
					{#if open === e.id}
						<tr data-testid="run-tasks-row">
							<td colspan="8" class="bg-white px-3 py-2 dark:bg-slate-900">
								<table class="w-full" data-testid="run-tasks">
									<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="py-1 pr-3">Task</th><th class="pr-3">Status</th><th class="pr-3">Exit code</th><th class="pr-3">Retries</th><th class="pr-3">Completed</th><th>Message</th></tr></thead>
									<tbody>
										{#each tasks as t (t.name)}
											<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="run-task-row">
												<td class="py-1 pr-3 font-mono">#{t.index}</td>
												<td class="pr-3"><RunBadge tone={TONE[t.state]} label={t.state} /></td>
												<td class="pr-3 font-mono" data-testid="run-task-exit">{t.exitCode ?? '—'}</td>
												<td class="pr-3">{t.retried}</td>
												<td class="pr-3">{when(t.completionTime)}</td>
												<td class="break-all text-gray-600 dark:text-gray-300">{t.message || '—'}</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	{/if}
</div>
