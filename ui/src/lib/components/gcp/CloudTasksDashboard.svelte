<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { ListChecks, Plus, RefreshCw, Pause, Play, Trash2, Eraser, X, Zap } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { rowsToMap, validateRows, type MetaRow } from '#lib/gcpstorage.ts';
	import {
		HTTP_METHODS,
		attemptOk,
		createQueue,
		createTask,
		deleteQueue,
		deleteTask,
		describeAttempt,
		listQueues,
		listTasks,
		optionalNumber,
		queueAction,
		readStoredTasksLocation,
		readStoredTasksProject,
		runTask,
		validateHeaderKey,
		validateQueueId,
		validateTaskId,
		validateTaskUrl,
		writeStoredTasksScope,
		type HttpMethod,
		type QueueAction,
		type QueueInfo,
		type TaskInfo
	} from '#lib/gcpcloudtasks.ts';

	const REFRESH_MS = 3000;

	let project = $state(readStoredTasksProject());
	let location = $state(readStoredTasksLocation());
	let projectInput = $state(readStoredTasksProject());
	let locationInput = $state(readStoredTasksLocation());
	let queues = $state<QueueInfo[]>([]);
	let current = $state<QueueInfo | null>(null);
	let tasks = $state<TaskInfo[]>([]);
	let loading = $state(true);
	let loadError = $state('');
	let busy = $state('');
	let autoRefresh = $state(false);
	let expanded = $state<string | null>(null);

	let queueModal = $state<Modal | null>(null);
	let fQueueId = $state('');
	let fRate = $state('');
	let fConcurrent = $state('');
	let fAttempts = $state('');
	let queueTouched = $state(false);
	const queueIdError = $derived(validateQueueId(fQueueId.trim()));

	let taskModal = $state<Modal | null>(null);
	let fTaskId = $state('');
	let fUrl = $state('');
	let fMethod = $state<HttpMethod>('POST');
	let fHeaders = $state<MetaRow[]>([]);
	let fBody = $state('');
	let fDelay = $state('');
	let taskTouched = $state(false);
	const urlError = $derived(validateTaskUrl(fUrl.trim()));
	const taskIdError = $derived(validateTaskId(fTaskId.trim()));
	const bodyError = $derived(fBody && ['GET', 'HEAD', 'OPTIONS'].includes(fMethod) ? `${fMethod} requests cannot have a body` : null);

	const describe = describeGcpError;
	const scope = $derived({ project, location });
	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const mini = 'rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const cell = 'px-4 py-3';

	const fmt = (v: number | null, unit = '') => (v === null ? '—' : `${v}${unit}`);
	const when = (iso: string) => (iso ? new Date(iso).toLocaleString() : '—');
	const stateClass = (st: string) =>
		st === 'RUNNING' ? 'text-green-700 dark:text-green-400' : st === 'PAUSED' ? 'text-amber-700 dark:text-amber-400' : 'text-gray-500 dark:text-gray-400';

	async function loadQueues() {
		try {
			queues = (await listQueues(scope)).toSorted((a, b) => a.id.localeCompare(b.id));
			loadError = '';
			const keep = current ? queues.find((q) => q.name === current?.name) : undefined;
			current = keep ?? null;
			if (!keep) tasks = [];
		} catch (e) {
			loadError = describe(e);
			if (!autoRefresh) toast.error(`Failed to load queues: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadTasks() {
		const q = current;
		if (!q) return;
		try {
			tasks = (await listTasks(scope, q.id)).toSorted((a, b) => a.scheduleTime.localeCompare(b.scheduleTime));
		} catch (e) {
			if (!autoRefresh) toast.error(`Failed to load tasks: ${describe(e)}`);
		}
	}

	async function refresh() {
		await loadQueues();
		await loadTasks();
	}

	async function run(label: string, action: () => Promise<void>, success: string, failure: string): Promise<boolean> {
		busy = label;
		try {
			await action();
			if (success) toast.success(success);
			return true;
		} catch (e) {
			toast.error(`${failure}: ${describe(e)}`);
			return false;
		} finally {
			busy = '';
		}
	}

	async function selectQueue(q: QueueInfo) {
		current = q;
		tasks = [];
		expanded = null;
		await loadTasks();
	}

	function openQueue() {
		fQueueId = '';
		fRate = '';
		fConcurrent = '';
		fAttempts = '';
		queueTouched = false;
		queueModal?.open();
	}

	async function submitQueue(event: Event) {
		event.preventDefault();
		queueTouched = true;
		if (queueIdError) return;
		const rate = optionalNumber(fRate, { min: 0 });
		const conc = optionalNumber(fConcurrent, { integer: true, min: 1 });
		const att = optionalNumber(fAttempts, { integer: true, min: -1 });
		for (const [label, r] of [['Max dispatches/second', rate], ['Max concurrent dispatches', conc], ['Max attempts', att]] as const) {
			if ('error' in r) return void toast.error(`${label}: ${r.error}`);
		}
		const id = fQueueId.trim();
		const ok = await run('create-queue', async () => {
			await createQueue(scope, { id, maxDispatchesPerSecond: fRate, maxConcurrentDispatches: fConcurrent, maxAttempts: fAttempts });
		}, `Created queue ${id}`, `Failed to create ${id}`);
		if (ok) {
			queueModal?.close('created');
			await loadQueues();
			const made = queues.find((q) => q.id === id);
			if (made) await selectQueue(made);
		}
	}

	async function doQueueAction(action: QueueAction) {
		const q = current;
		if (!q) return;
		if (action === 'purge' && !(await confirmDestructive({ title: 'Purge queue', message: `Delete all tasks in "${q.id}"? This cannot be undone.`, confirmLabel: 'Purge' }))) return;
		const done = { pause: 'Paused', resume: 'Resumed', purge: 'Purged' }[action];
		const ok = await run(action, async () => { await queueAction(scope, q.id, action); }, `${done} ${q.id}`, `Failed to ${action} ${q.id}`);
		if (ok) await refresh();
	}

	async function removeQueue() {
		const q = current;
		if (!q) return;
		if (!(await confirmDestructive({ title: 'Delete queue', message: `Delete queue "${q.id}" and all of its tasks?` }))) return;
		const ok = await run('delete-queue', async () => { await deleteQueue(scope, q.id); }, `Deleted queue ${q.id}`, `Failed to delete ${q.id}`);
		if (ok) {
			current = null;
			tasks = [];
			await loadQueues();
		}
	}

	function openTask() {
		fTaskId = '';
		fUrl = '';
		fMethod = 'POST';
		fHeaders = [];
		fBody = '';
		fDelay = '';
		taskTouched = false;
		taskModal?.open();
	}

	async function submitTask(event: Event) {
		event.preventDefault();
		taskTouched = true;
		const q = current;
		if (!q || urlError || taskIdError || bodyError) return;
		const bad = validateRows(fHeaders, validateHeaderKey);
		if (bad) return void toast.error(`Invalid header ${bad}`);
		const delay = optionalNumber(fDelay, { min: 0 });
		if ('error' in delay) return void toast.error(`Schedule delay: ${delay.error}`);
		const ok = await run('create-task', async () => {
			await createTask(scope, q.id, { id: fTaskId, url: fUrl, method: fMethod, headers: rowsToMap(fHeaders), body: fBody, delaySeconds: fDelay });
		}, 'Task created', 'Failed to create task');
		if (ok) {
			taskModal?.close('created');
			await loadTasks();
		}
	}

	async function runNow(t: TaskInfo) {
		const q = current;
		if (!q) return;
		const ok = await run(`run-${t.id}`, async () => { await runTask(scope, q.id, t.id); }, `Dispatching ${t.id}`, `Failed to run ${t.id}`);
		if (ok) await loadTasks();
	}

	async function removeTask(t: TaskInfo) {
		const q = current;
		if (!q) return;
		if (!(await confirmDestructive({ title: 'Delete task', message: `Delete task "${t.id}"?` }))) return;
		const ok = await run(`del-${t.id}`, async () => { await deleteTask(scope, q.id, t.id); }, `Deleted task ${t.id}`, `Failed to delete ${t.id}`);
		if (ok) await loadTasks();
	}

	function applyScope(event: Event) {
		event.preventDefault();
		const p = projectInput.trim();
		const l = locationInput.trim();
		if (!p || !l) return;
		project = p;
		location = l;
		writeStoredTasksScope(p, l);
		current = null;
		tasks = [];
		loading = true;
		void loadQueues();
	}

	$effect(() => {
		if (!autoRefresh) return;
		const timer = setInterval(() => void refresh(), REFRESH_MS);
		return () => clearInterval(timer);
	});

	onMount(loadQueues);
</script>

<div class="space-y-6 pb-20" data-testid="cloudtasks-dashboard">
	<PageHeader icon={ListChecks} title="Cloud Tasks" description="Google Cloud Tasks: queues and HTTP tasks" color="blue" onRefresh={refresh}>
		{#snippet actions()}
			<label class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
				<input type="checkbox" bind:checked={autoRefresh} data-testid="tasks-auto-refresh" /> Auto-refresh
			</label>
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={openQueue} disabled={loading || Boolean(loadError)} data-testid="tasks-new-queue">
				<Plus class="h-4 w-4" /> New queue
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyScope}>
		<label for="tasks-project">Project</label>
		<input id="tasks-project" class="w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="tasks-project" />
		<label for="tasks-location">Location</label>
		<input id="tasks-location" class="w-32 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={locationInput} data-testid="tasks-location" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || !locationInput.trim() || (projectInput.trim() === project && locationInput.trim() === location)}>Switch</button>
		<span>Queues are listed per project and location.</span>
	</form>

	{#if loadError && queues.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="tasks-error">
			<p class="font-medium">Could not reach Cloud Tasks</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadQueues}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<section aria-label="Queues" class="space-y-2">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Queues</h2>
				{#if loading}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if queues.length === 0}
					<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="tasks-no-queues">No queues in {location}</p>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
						{#each queues as q (q.name)}
							<li class={current?.name === q.name ? 'bg-blue-50 dark:bg-blue-950/30' : ''} data-testid="tasks-queue-row">
								<button class="w-full min-w-0 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectQueue(q)}>
									<span class="block truncate">{q.id}</span>
									<span class="block truncate text-[11px] font-normal {stateClass(q.state)}" data-testid="tasks-queue-state">{q.state}</span>
								</button>
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<section aria-label="Queue details" class="min-w-0">
				{#if !current}
					<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="tasks-select-hint">Select a queue to see its tasks</p>
				{:else}
					{@const q = current}
					<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-5" data-testid="tasks-detail">
						<div class="flex items-start justify-between gap-4">
							<div class="min-w-0">
								<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{q.id} <span class="ml-2 text-sm font-medium {stateClass(q.state)}" data-testid="tasks-detail-state">{q.state}</span></h3>
								<p class="font-mono text-xs text-gray-500 dark:text-gray-400 break-all">{q.name}</p>
							</div>
							<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (current = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
						</div>

						<div class="flex flex-wrap gap-2">
							{#if q.state === 'PAUSED'}
								<button class={mini} disabled={busy === 'resume'} onclick={() => doQueueAction('resume')} data-testid="tasks-resume"><Play class="inline h-3.5 w-3.5" /> Resume</button>
							{:else}
								<button class={mini} disabled={busy === 'pause'} onclick={() => doQueueAction('pause')} data-testid="tasks-pause"><Pause class="inline h-3.5 w-3.5" /> Pause</button>
							{/if}
							<button class={mini} disabled={busy === 'purge'} onclick={() => doQueueAction('purge')} data-testid="tasks-purge"><Eraser class="inline h-3.5 w-3.5" /> Purge</button>
							<button class="{mini} text-red-600" disabled={busy === 'delete-queue'} onclick={removeQueue} data-testid="tasks-delete-queue"><Trash2 class="inline h-3.5 w-3.5" /> Delete</button>
						</div>

						<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm md:grid-cols-[max-content_1fr_max-content_1fr]" data-testid="tasks-config">
							<dt class={dt}>Max dispatches/sec</dt><dd class={dd}>{fmt(q.maxDispatchesPerSecond)}</dd>
							<dt class={dt}>Max burst size</dt><dd class={dd}>{fmt(q.maxBurstSize)}</dd>
							<dt class={dt}>Max concurrent</dt><dd class={dd}>{fmt(q.maxConcurrentDispatches)}</dd>
							<dt class={dt}>Max attempts</dt><dd class={dd}>{q.maxAttempts === -1 ? 'unlimited' : fmt(q.maxAttempts)}</dd>
							<dt class={dt}>Min backoff</dt><dd class={dd}>{q.minBackoff || '—'}</dd>
							<dt class={dt}>Max backoff</dt><dd class={dd}>{q.maxBackoff || '—'}</dd>
							<dt class={dt}>Max doublings</dt><dd class={dd}>{fmt(q.maxDoublings)}</dd>
							<dt class={dt}>Max retry duration</dt><dd class={dd}>{q.maxRetryDuration || 'unlimited'}</dd>
						</dl>

						<div class="space-y-2" data-testid="tasks-list">
							<div class="flex items-center justify-between">
								<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Tasks ({tasks.length})</h4>
								<button class={mini} onclick={openTask} data-testid="tasks-new-task"><Plus class="inline h-3.5 w-3.5" /> New task</button>
							</div>
							{#if tasks.length === 0}
								<p class="py-6 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="tasks-no-tasks">No tasks in this queue</p>
							{:else}
								<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
									<table class="w-full text-sm">
										<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Task</th><th class={cell}>Target</th><th class={cell}>Scheduled</th><th class={cell}>Attempts</th><th class={cell}>Last attempt</th><th class="{cell} text-right">Actions</th></tr></thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each tasks as t (t.name)}
												{@const ok = attemptOk(t.lastAttempt)}
												<tr data-testid="tasks-task-row">
													<td class="{cell} font-mono text-xs max-w-[10rem]">
														<button class="block max-w-full truncate text-left underline decoration-dotted" title={t.id} onclick={() => (expanded = expanded === t.name ? null : t.name)} aria-expanded={expanded === t.name} data-testid="tasks-task-toggle">{t.id}</button>
													</td>
													<td class="{cell} text-xs break-all"><span class="font-semibold">{t.method}</span> {t.url}</td>
													<td class="{cell} whitespace-nowrap text-xs">{when(t.scheduleTime)}</td>
													<td class="{cell} font-mono" data-testid="tasks-attempts">{t.dispatchCount}</td>
													<td class="{cell} text-xs {ok === true ? 'text-green-700 dark:text-green-400' : ok === false ? 'text-red-700 dark:text-red-400' : 'text-gray-500 dark:text-gray-400'}" data-testid="tasks-last-attempt">{describeAttempt(t.lastAttempt)}</td>
													<td class="{cell} text-right whitespace-nowrap space-x-1">
														<button class={mini} disabled={busy === `run-${t.id}`} onclick={() => runNow(t)} data-testid="tasks-run"><Zap class="inline h-3.5 w-3.5" /> Run now</button>
														<button class="{mini} text-red-600" disabled={busy === `del-${t.id}`} onclick={() => removeTask(t)} data-testid="tasks-delete-task" aria-label="Delete task {t.id}"><Trash2 class="inline h-3.5 w-3.5" /></button>
													</td>
												</tr>
												{#if expanded === t.name}
													<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="tasks-task-detail">
														<td colspan="6" class="px-4 py-3 text-xs space-y-1">
															<p><span class={dt}>Created</span> {when(t.createTime)} · <span class={dt}>Responses</span> {t.responseCount}</p>
															{#if t.lastAttempt}<p><span class={dt}>Last dispatch</span> {when(t.lastAttempt.dispatchTime)} · <span class={dt}>Response</span> {when(t.lastAttempt.responseTime)}</p>{/if}
															<p><span class={dt}>Headers</span> {Object.entries(t.headers).map(([hk, hv]) => `${hk}: ${hv}`).join(', ') || '—'}</p>
															<p class={dt}>Body</p>
															<pre class="whitespace-pre-wrap break-all rounded-md bg-white dark:bg-slate-900 p-2 font-mono">{t.body === null ? '(binary)' : t.body || '(empty)'}</pre>
														</td>
													</tr>
												{/if}
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
						</div>
					</div>
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={queueModal} title="New queue" titleId="tasks-queue-title">
	{#snippet children()}
		<form id="tasks-queue-form" class="space-y-3" onsubmit={submitQueue} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Queue ID</span>
				<input class={input} bind:value={fQueueId} placeholder="my-queue" autocomplete="off" aria-label="Queue ID" />
				{#if queueTouched && queueIdError}<span class="block text-xs text-red-600 dark:text-red-400">{queueIdError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Max dispatches per second (optional)</span>
				<input class={input} bind:value={fRate} placeholder="500" aria-label="Max dispatches per second" />
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Max concurrent dispatches (optional)</span>
				<input class={input} bind:value={fConcurrent} placeholder="1000" aria-label="Max concurrent dispatches" />
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Max attempts (optional, -1 for unlimited)</span>
				<input class={input} bind:value={fAttempts} placeholder="100" aria-label="Max attempts" />
			</label>
			<p class="text-xs text-gray-500 dark:text-gray-400">Created in {project} / {location}.</p>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => queueModal?.close()}>Cancel</button>
		<button type="submit" form="tasks-queue-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'create-queue'} data-testid="tasks-queue-submit">{busy === 'create-queue' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={taskModal} title="New HTTP task" titleId="tasks-task-title">
	{#snippet children()}
		<form id="tasks-task-form" class="space-y-3" onsubmit={submitTask} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Target URL</span>
				<input class={input} bind:value={fUrl} placeholder="http://localhost:9000/hook" autocomplete="off" aria-label="Target URL" />
				{#if taskTouched && urlError}<span class="block text-xs text-red-600 dark:text-red-400">{urlError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">HTTP method</span>
				<select class={input} bind:value={fMethod} aria-label="HTTP method">
					{#each HTTP_METHODS as m (m)}<option value={m}>{m}</option>{/each}
				</select>
			</label>
			<KeyValueEditor bind:rows={fHeaders} label="Headers" testId="tasks-task-headers" validateKey={validateHeaderKey} hideSave />
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Body (optional)</span>
				<textarea class="{input} font-mono" rows="3" bind:value={fBody} aria-label="Body"></textarea>
				{#if taskTouched && bodyError}<span class="block text-xs text-red-600 dark:text-red-400">{bodyError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Schedule delay in seconds (optional)</span>
				<input class={input} bind:value={fDelay} placeholder="0" aria-label="Schedule delay seconds" />
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Task ID (optional)</span>
				<input class={input} bind:value={fTaskId} placeholder="generated if blank" autocomplete="off" aria-label="Task ID" />
				{#if taskTouched && taskIdError}<span class="block text-xs text-red-600 dark:text-red-400">{taskIdError}</span>{/if}
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => taskModal?.close()}>Cancel</button>
		<button type="submit" form="tasks-task-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'create-task'} data-testid="tasks-task-submit">{busy === 'create-task' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>
