<script lang="ts">
	import { onMount } from 'svelte';
	import { Puzzle, AlertCircle, RefreshCw, RotateCcw, Terminal } from 'lucide-svelte';
	import {
		claimLabels,
		hookLabels,
		needsFastPoll,
		statusStyle,
		type ExtensionInfo
	} from '#lib/extensions.ts';

	const API = '/_gopherstack/extensions';
	const POLL_MS = 3000;
	const FAST_POLL_MS = 500;

	let extensions = $state<ExtensionInfo[]>([]);
	let loaded = $state(false);
	let error = $state<string | null>(null);
	let busy = $state<string | null>(null);
	let outputFor = $state<string | null>(null);
	let outputLines = $state<string[]>([]);
	let outputError = $state<string | null>(null);

	async function load() {
		try {
			const res = await fetch(API);
			if (!res.ok) throw new Error(`Failed to load extensions: ${res.status}`);
			const data = await res.json();
			extensions = data.extensions ?? [];
			error = null;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		} finally {
			loaded = true;
		}
	}

	async function loadOutput(name: string) {
		try {
			const res = await fetch(`${API}/${encodeURIComponent(name)}/output`);
			if (!res.ok) throw new Error(`Failed to load output: ${res.status}`);
			const data = await res.json();
			outputLines = data.lines ?? [];
			outputError = null;
		} catch (e) {
			outputError = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function showOutput(name: string) {
		outputFor = name;
		outputLines = [];
		await loadOutput(name);
	}

	async function restart(name: string) {
		busy = name;
		try {
			const res = await fetch(`${API}/${encodeURIComponent(name)}/restart`, { method: 'POST' });
			if (!res.ok) throw new Error(`Failed to restart ${name}: ${res.status}`);
			error = null;
			await load();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		} finally {
			busy = null;
		}
	}

	onMount(() => {
		void load();
		let timer: ReturnType<typeof setTimeout>;
		const tick = async () => {
			await load();
			if (outputFor) await loadOutput(outputFor);
			timer = setTimeout(tick, needsFastPoll(extensions) ? FAST_POLL_MS : POLL_MS);
		};
		timer = setTimeout(tick, POLL_MS);
		return () => clearTimeout(timer);
	});
</script>

<div class="space-y-6">
	<div>
		<div class="flex items-center gap-3 mb-2">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg">
				<Puzzle class="w-6 h-6 text-indigo-600 dark:text-indigo-400" />
			</div>
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Extensions</h1>
		</div>
		<p class="text-slate-600 dark:text-slate-300">Out-of-process extensions: lifecycle, claimed routes and hooks</p>
	</div>

	{#if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg flex gap-3">
			<AlertCircle class="w-5 h-5 text-red-600 dark:text-red-400 flex-shrink-0 mt-0.5" />
			<div class="text-sm text-red-700 dark:text-red-300" role="alert">{error}</div>
		</div>
	{/if}

	{#if loaded && extensions.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center text-slate-500 dark:text-slate-400">
			<Puzzle class="w-8 h-8 mx-auto mb-2 opacity-50" />
			<p>No extensions configured</p>
			<p class="text-xs mt-1">Declare them with <code>--extension</code> or <code>--extensions-config</code>.</p>
		</div>
	{/if}

	<div class="space-y-4">
		{#each extensions as ext (ext.name)}
			{@const claims = claimLabels(ext.registration?.routes)}
			{@const hooks = hookLabels(ext.registration?.hooks)}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6" data-testid="extension-{ext.name}">
				<div class="flex flex-wrap items-start justify-between gap-3 mb-4">
					<div class="flex items-center gap-3">
						<h2 class="text-lg font-semibold text-slate-900 dark:text-white">{ext.name}</h2>
						<span class="px-2 py-0.5 rounded text-xs font-medium {statusStyle(ext.status)}" data-testid="status-{ext.name}">{ext.status}</span>
						<span class="px-2 py-0.5 rounded text-xs font-medium bg-slate-100 text-slate-700 dark:bg-slate-700 dark:text-slate-300">{ext.type}</span>
						{#if ext.registration?.version}
							<span class="text-xs text-slate-500 dark:text-slate-400">v{ext.registration.version}</span>
						{/if}
					</div>
					<div class="flex gap-2">
						<button
							onclick={() => showOutput(ext.name)}
							class="px-3 py-1.5 text-sm font-medium text-slate-700 dark:text-slate-200 bg-slate-100 dark:bg-slate-700 rounded-lg hover:bg-slate-200 dark:hover:bg-slate-600 transition-colors inline-flex items-center gap-1.5"
						>
							<Terminal class="w-4 h-4" /> Output
						</button>
						<button
							onclick={() => restart(ext.name)}
							disabled={busy === ext.name}
							class="px-3 py-1.5 text-sm font-medium text-white bg-indigo-600 rounded-lg hover:bg-indigo-700 disabled:opacity-50 transition-colors inline-flex items-center gap-1.5"
						>
							<RotateCcw class="w-4 h-4" /> Restart
						</button>
					</div>
				</div>

				<dl class="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-3 text-sm">
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Restarts</dt>
						<dd class="text-slate-900 dark:text-white" data-testid="restarts-{ext.name}">{ext.restarts}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Endpoint</dt>
						<dd class="text-slate-900 dark:text-white font-mono">{ext.endpoint || '—'}</dd>
					</div>
					<div class="md:col-span-2">
						<dt class="text-slate-500 dark:text-slate-400">Last error</dt>
						<dd class="text-slate-900 dark:text-white {ext.last_error ? 'text-red-600 dark:text-red-400' : ''}">{ext.last_error || '—'}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Claimed routes</dt>
						<dd class="flex flex-wrap gap-1.5 mt-1">
							{#each claims as c}
								<span class="px-2 py-0.5 rounded bg-slate-100 dark:bg-slate-700 font-mono text-xs text-slate-800 dark:text-slate-200">{c}</span>
							{:else}
								<span class="text-slate-500 dark:text-slate-400">none</span>
							{/each}
						</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Hooks</dt>
						<dd class="flex flex-wrap gap-1.5 mt-1">
							{#each hooks as h}
								<span class="px-2 py-0.5 rounded bg-slate-100 dark:bg-slate-700 font-mono text-xs text-slate-800 dark:text-slate-200">{h}</span>
							{:else}
								<span class="text-slate-500 dark:text-slate-400">none</span>
							{/each}
						</dd>
					</div>
				</dl>

				{#if outputFor === ext.name}
					<div class="mt-4">
						<div class="flex items-center justify-between mb-1">
							<h3 class="text-sm font-semibold text-slate-900 dark:text-white">Recent output</h3>
							<button onclick={() => loadOutput(ext.name)} class="text-xs text-slate-500 hover:text-slate-900 dark:hover:text-white inline-flex items-center gap-1" aria-label="Refresh output">
								<RefreshCw class="w-3 h-3" /> Refresh
							</button>
						</div>
						{#if outputError}
							<p class="text-sm text-red-600 dark:text-red-400">{outputError}</p>
						{/if}
						<pre class="max-h-64 overflow-auto p-3 rounded-lg bg-slate-900 text-slate-100 text-xs font-mono whitespace-pre-wrap" data-testid="output-{ext.name}">{outputLines.length ? outputLines.join('\n') : 'No output captured.'}</pre>
					</div>
				{/if}
			</div>
		{/each}
	</div>
</div>
