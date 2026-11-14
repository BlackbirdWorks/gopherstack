<script lang="ts">
	import { onMount, untrack } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { RefreshCw } from 'lucide-svelte';
	import JsonView from './JsonView.svelte';
	import { confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		buildTimeline,
		cancelRun,
		formatDuration,
		durationMs,
		getContent,
		getRun,
		isRunning,
		listActions,
		listRuns,
		statusTone,
		type ActionRun,
		type ContentScope,
		type Run,
		type RunTarget
	} from '#lib/azurelogic.ts';

	let { target, focus = '' }: { target: RunTarget; focus?: string } = $props();

	const POLL_MS = 2000;

	let runs = $state<Run[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let run = $state<Run>();
	let actions = $state<ActionRun[]>([]);
	let detailError = $state('');
	let contents = $state<Record<string, string>>({});
	let contentError = $state<Record<string, string>>({});

	const timeline = $derived(buildTimeline(actions));
	const busy = $derived(runs.some((r) => isRunning(r.status)) || (run ? isRunning(run.status) : false));

	async function loadRuns() {
		try {
			runs = await listRuns(target);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadDetail() {
		if (!selected) {
			run = undefined;
			actions = [];
			return;
		}
		try {
			[run, actions] = await Promise.all([getRun(target, selected), listActions(target, selected)]);
			detailError = '';
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	async function refresh() {
		await Promise.all([loadRuns(), loadDetail()]);
	}

	async function open(name: string) {
		selected = name;
		contents = {};
		contentError = {};
		await Promise.all([loadRuns(), loadDetail()]);
	}

	async function select(name: string) {
		selected = selected === name ? '' : name;
		contents = {};
		contentError = {};
		await loadDetail();
	}

	async function cancel(r: Run) {
		const ok = await confirmedAttempt(
			{ title: 'Cancel run', message: `Cancel run ${r.name}?`, confirmLabel: 'Cancel run' },
			() => cancelRun(target, r.name),
			`Cancelling ${r.name}`,
			`Failed to cancel ${r.name}`
		);
		if (ok) await refresh();
	}

	async function showContent(key: string, scope: ContentScope, kind: 'inputs' | 'outputs') {
		if (contents[key] !== undefined) {
			const { [key]: _gone, ...rest } = contents;
			contents = rest;
			return;
		}
		try {
			contents = { ...contents, [key]: await getContent(target, selected, scope, kind) };
			contentError = { ...contentError, [key]: '' };
		} catch (e) {
			contentError = { ...contentError, [key]: describeGcpError(e) };
			toast.error(`Failed to read ${kind}: ${describeGcpError(e)}`);
		}
	}

	const tones = {
		good: 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300',
		bad: 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300',
		busy: 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300',
		muted: 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300'
	};
	const bars = {
		good: 'bg-green-500',
		bad: 'bg-red-500',
		busy: 'bg-blue-500',
		muted: 'bg-gray-400'
	};
	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');

	$effect(() => {
		const f = focus;
		if (f) untrack(() => void open(f));
	});

	onMount(() => {
		void loadRuns();
		const timer = setInterval(() => {
			if (busy) void refresh();
		}, POLL_MS);
		return () => clearInterval(timer);
	});
</script>

<div class="space-y-4" data-testid="logic-runs">
	<div class="flex items-center gap-3">
		<button class={GCP_MINI} onclick={refresh} data-testid="logic-runs-refresh">Refresh runs</button>
	</div>
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="logic-runs-error">Could not load runs: {error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="logic-run-table">
				<thead class={thead}><tr><th class={cell}>Run</th><th class={cell}>Status</th><th class={cell}>Trigger</th><th class={cell}>Started</th><th class={cell}>Duration</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each runs as r (r.name)}
						<tr class={selected === r.name ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="logic-run">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(r.name)} aria-expanded={selected === r.name} data-testid="logic-run-select">{r.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {tones[statusTone(r.status)]}" data-testid="logic-run-status">{r.status}</span></td>
							<td class="{cell} text-xs">{r.trigger || '—'}</td>
							<td class="{cell} text-xs">{when(r.start)}</td>
							<td class="{cell} text-xs">{formatDuration(durationMs(r.start, r.end))}</td>
						</tr>
					{:else}
						<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="logic-runs-empty">No runs yet. Run a trigger to start one.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if run}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="logic-run-detail">
				<div class="flex flex-wrap items-center gap-3">
					<h2 class="font-mono text-sm font-semibold text-gray-900 dark:text-white">{run.name}</h2>
					<span class="rounded-full px-2 py-0.5 text-xs {tones[statusTone(run.status)]}" data-testid="logic-detail-status">{run.status}</span>
					{#if isRunning(run.status)}<button class={GCP_MINI} onclick={() => cancel(run!)} data-testid="logic-run-cancel">Cancel run</button>{/if}
				</div>
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
					<dt class="text-gray-500">Trigger</dt><dd>{run.trigger} ({run.triggerStatus || '—'})</dd>
					<dt class="text-gray-500">Started</dt><dd>{when(run.start)}</dd>
					<dt class="text-gray-500">Ended</dt><dd>{when(run.end)}</dd>
				</dl>
				{#if run.error}<p class="text-sm text-red-600" data-testid="logic-run-error">{run.error.code}: {run.error.message}</p>{/if}
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="logic-detail-error">{detailError}</p>{/if}

				{#if run.hasTriggerInputs || run.hasTriggerOutputs}
					<div class="space-y-2" data-testid="logic-trigger-io">
						<p class="text-xs font-medium text-gray-700 dark:text-gray-300">Trigger {run.trigger}</p>
						<div class="flex gap-2">
							{#if run.hasTriggerInputs}<button class={GCP_MINI} onclick={() => showContent('trigger:inputs', { trigger: true }, 'inputs')} data-testid="logic-trigger-inputs">{contents['trigger:inputs'] === undefined ? 'Show inputs' : 'Hide inputs'}</button>{/if}
							{#if run.hasTriggerOutputs}<button class={GCP_MINI} onclick={() => showContent('trigger:outputs', { trigger: true }, 'outputs')} data-testid="logic-trigger-outputs">{contents['trigger:outputs'] === undefined ? 'Show outputs' : 'Hide outputs'}</button>{/if}
						</div>
						{#each ['trigger:inputs', 'trigger:outputs'] as key (key)}
							{#if contents[key] !== undefined}<JsonView value={contents[key]} testid="logic-content-{key.replace(':', '-')}" />{/if}
						{/each}
					</div>
				{/if}

				<h3 class="font-medium text-gray-900 dark:text-white">Actions</h3>
				<ol class="space-y-3" data-testid="logic-timeline">
					{#each timeline as a (a.name)}
						<li class="space-y-2 rounded-md border border-gray-200 p-3 dark:border-gray-700" data-testid="logic-action">
							<div class="flex flex-wrap items-center gap-3 text-sm">
								<span class="font-mono text-xs font-medium" data-testid="logic-action-name">{a.name}</span>
								<span class="rounded-full px-2 py-0.5 text-xs {tones[statusTone(a.status)]}" data-testid="logic-action-status">{a.status}</span>
								<span class="text-xs text-gray-500">{when(a.start)} → {when(a.end)} · {formatDuration(a.duration)}</span>
							</div>
							<div class="h-2 rounded bg-gray-100 dark:bg-slate-800" aria-hidden="true">
								<div class="h-2 rounded {bars[statusTone(a.status)]}" style="margin-left: {a.offsetPct}%; width: {a.widthPct}%"></div>
							</div>
							{#if a.error}<p class="text-xs text-red-600" data-testid="logic-action-error">{a.error.code}: {a.error.message}</p>{/if}
							<div class="flex gap-2">
								{#if a.hasInputs}<button class={GCP_MINI} onclick={() => showContent(`${a.name}:inputs`, { action: a.name }, 'inputs')} data-testid="logic-action-inputs">{contents[`${a.name}:inputs`] === undefined ? `Show inputs (${a.inputsSize} B)` : 'Hide inputs'}</button>{/if}
								{#if a.hasOutputs}<button class={GCP_MINI} onclick={() => showContent(`${a.name}:outputs`, { action: a.name }, 'outputs')} data-testid="logic-action-outputs">{contents[`${a.name}:outputs`] === undefined ? `Show outputs (${a.outputsSize} B)` : 'Hide outputs'}</button>{/if}
							</div>
							{#each ['inputs', 'outputs'] as kind (kind)}
								{#if contents[`${a.name}:${kind}`] !== undefined}<JsonView value={contents[`${a.name}:${kind}`]} testid="logic-content-{a.name}-{kind}" />{/if}
								{#if contentError[`${a.name}:${kind}`]}<p class="text-xs text-red-600">{contentError[`${a.name}:${kind}`]}</p>{/if}
							{/each}
						</li>
					{:else}
						<li class="text-sm text-gray-500" data-testid="logic-actions-empty">No actions have started.</li>
					{/each}
				</ol>
			</div>
		{/if}
	{/if}
</div>
