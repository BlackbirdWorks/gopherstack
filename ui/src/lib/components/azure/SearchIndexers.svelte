<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';
	import { attempt, confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		executionTone,
		getIndexerStatus,
		listIndexers,
		resetIndexer,
		runIndexer,
		type Indexer,
		type IndexerStatus,
		type SearchTarget
	} from '#lib/azuresearch.ts';

	let { target }: { target: SearchTarget } = $props();

	let indexers = $state<Indexer[]>([]);
	let statuses = $state<Record<string, IndexerStatus>>({});
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');

	const tones: Record<string, string> = {
		good: 'text-emerald-700 dark:text-emerald-300',
		bad: 'text-red-700 dark:text-red-300',
		busy: 'text-amber-700 dark:text-amber-300',
		muted: 'text-gray-500'
	};
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');

	async function load() {
		try {
			indexers = await listIndexers(target);
			const entries = await Promise.all(indexers.map(async (i) => [i.name, await getIndexerStatus(target, i.name)] as const));
			statuses = Object.fromEntries(entries);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function run(name: string) {
		if (await attempt(() => runIndexer(target, name), `Started ${name}`, `Failed to run ${name}`)) await load();
	}

	async function reset(name: string) {
		const ok = await confirmedAttempt(
			{ title: 'Reset indexer', message: `Reset "${name}"? The next run re-processes every document.`, confirmLabel: 'Reset' },
			() => resetIndexer(target, name),
			`Reset ${name}`,
			`Failed to reset ${name}`
		);
		if (ok) await load();
	}

	onMount(load);

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
</script>

<div class="space-y-4" data-testid="search-indexers">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="search-indexers-error">Could not load indexers: {error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="search-indexer-table">
				<thead class={thead}><tr><th class={cell}>Indexer</th><th class={cell}>Data source</th><th class={cell}>Target index</th><th class={cell}>Schedule</th><th class={cell}>Last result</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each indexers as ix (ix.name)}
						{@const st = statuses[ix.name]}
						<tr data-testid="search-indexer">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = selected === ix.name ? '' : ix.name)} aria-expanded={selected === ix.name} data-testid="search-indexer-select">{ix.name}</button>{ix.disabled ? ' (disabled)' : ''}</td>
							<td class="{cell} font-mono text-xs">{ix.dataSource}</td>
							<td class="{cell} font-mono text-xs">{ix.target}</td>
							<td class="{cell} text-xs">{ix.schedule || 'on demand'}</td>
							<td class="{cell} text-xs" data-testid="search-indexer-last">
								{#if st?.last}
									<span class={tones[executionTone(st.last.status)]} data-testid="search-indexer-status">{st.last.status}</span> · {st.last.processed} processed{st.last.failed > 0 ? ` · ${st.last.failed} failed` : ''}
								{:else}<span data-testid="search-indexer-status">never run</span>{/if}
							</td>
							<td class="{cell} text-right whitespace-nowrap">
								<button class={GCP_MINI} disabled={ix.disabled} onclick={() => run(ix.name)} data-testid="search-indexer-run">Run</button>
								<button class={GCP_MINI} onclick={() => reset(ix.name)} data-testid="search-indexer-reset">Reset</button>
							</td>
						</tr>
						{#if selected === ix.name}
							<tr>
								<td colspan="6" class="{cell}" data-testid="search-indexer-history">
									{#if !st || st.history.length === 0}
										<p class="text-xs text-gray-500">No executions yet.</p>
									{:else}
										<table class="w-full text-xs">
											<thead class={thead}><tr><th class={cell}>Status</th><th class={cell}>Started</th><th class={cell}>Ended</th><th class={cell}>Processed</th><th class={cell}>Failed</th><th class={cell}>Error</th></tr></thead>
											<tbody>
												{#each st.history as ex, i (i)}
													<tr data-testid="search-execution"><td class="{cell} {tones[executionTone(ex.status)]}">{ex.status}</td><td class={cell}>{when(ex.start)}</td><td class={cell}>{when(ex.end)}</td><td class={cell}>{ex.processed}</td><td class={cell}>{ex.failed}</td><td class={cell}>{ex.error || '—'}</td></tr>
												{/each}
											</tbody>
										</table>
									{/if}
								</td>
							</tr>
						{/if}
					{:else}
						<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="search-indexers-empty">No indexers on this service.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
