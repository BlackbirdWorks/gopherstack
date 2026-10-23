<script lang="ts">
	import { History, Play } from 'lucide-svelte';
	import BigQueryResultGrid from '#lib/components/gcp/BigQueryResultGrid.svelte';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		pushHistory,
		runQuery,
		summarizeQuery,
		type HistoryEntry,
		type QueryResult
	} from '#lib/gcpbigquery.ts';

	let {
		project,
		datasets,
		defaultDataset = $bindable(''),
		sql = $bindable('SELECT 1 AS one'),
		onchanged
	}: { project: string; datasets: string[]; defaultDataset: string; sql: string; onchanged: () => void } = $props();

	let running = $state(false);
	let result = $state<QueryResult | null>(null);
	let error = $state('');
	let history = $state<HistoryEntry[]>([]);

	function record(ok: boolean, summary: string) {
		history = pushHistory(history, { sql: sql.trim(), at: Date.now(), ok, summary });
	}

	async function run() {
		if (!sql.trim() || running) return;
		running = true;
		error = '';
		try {
			result = await runQuery(project, sql, defaultDataset);
			record(true, summarizeQuery(result));
			onchanged();
		} catch (e) {
			result = null;
			error = describeGcpError(e);
			record(false, error);
		} finally {
			running = false;
		}
	}

	function onKey(event: KeyboardEvent) {
		if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
			event.preventDefault();
			void run();
		}
	}

	const stats = $derived(
		result
			? [
					['Job', result.jobId || '—'],
					['Duration', `${result.elapsedMs} ms`],
					['Bytes processed', result.bytesProcessed],
					['Cache', result.cacheHit ? 'hit' : 'miss'],
					...(result.dmlAffected ? [['Rows affected', result.dmlAffected]] : [])
				]
			: []
	);
</script>

<div class="space-y-4" data-testid="bq-query">
	<div class="flex flex-wrap items-center gap-3 text-sm">
		<label class="flex items-center gap-2 text-gray-600 dark:text-gray-400">
			Default dataset
			<select class="{GCP_INPUT} !w-auto !py-1" bind:value={defaultDataset} aria-label="Default dataset" data-testid="bq-default-dataset">
				<option value="">(none)</option>
				{#each datasets as d (d)}<option value={d}>{d}</option>{/each}
			</select>
		</label>
		<span class="text-xs text-gray-400">Ctrl+Enter to run</span>
	</div>

	<textarea class="{GCP_INPUT} font-mono" rows="7" bind:value={sql} onkeydown={onKey} spellcheck="false" aria-label="SQL query" data-testid="bq-sql"></textarea>
	<button class="flex items-center gap-2 rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={running || !sql.trim()} onclick={run} data-testid="bq-run">
		<Play class="h-4 w-4" /> {running ? 'Running…' : 'Run query'}
	</button>

	{#if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="bq-query-error">
			<p class="font-medium">Query failed</p>
			<p class="font-mono text-xs whitespace-pre-wrap break-words">{error}</p>
		</div>
	{/if}

	{#if result}
		<dl class="flex flex-wrap gap-x-6 gap-y-1 text-xs text-gray-500 dark:text-gray-400" data-testid="bq-stats">
			{#each stats as [k, v] (k)}<div><dt class="inline">{k}:</dt> <dd class="inline font-mono text-gray-800 dark:text-gray-200">{v}</dd></div>{/each}
		</dl>
		{#if result.schema.length > 0}
			<BigQueryResultGrid schema={result.schema} rows={result.rows} testId="bq-result" />
			<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="bq-result-count">{result.totalRows} row(s)</p>
		{:else}
			<p class="text-sm text-gray-600 dark:text-gray-300" data-testid="bq-result-none">{summarizeQuery(result)}</p>
		{/if}
	{/if}

	<div class="space-y-2" data-testid="bq-history">
		<h4 class="flex items-center gap-1 text-sm font-semibold text-gray-700 dark:text-gray-300"><History class="h-4 w-4" /> History (this session)</h4>
		{#if history.length === 0}
			<p class="text-xs text-gray-500 dark:text-gray-400">No queries yet</p>
		{:else}
			<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
				{#each history as h (h.sql)}
					<li>
						<button class="w-full px-3 py-2 text-left hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => (sql = h.sql)} data-testid="bq-history-item">
							<span class="block truncate font-mono text-xs text-gray-900 dark:text-gray-100">{h.sql}</span>
							<span class="block text-[11px] {h.ok ? 'text-green-700 dark:text-green-400' : 'text-red-600 dark:text-red-400'}">{new Date(h.at).toLocaleTimeString()} · {h.summary}</span>
						</button>
					</li>
				{/each}
			</ul>
		{/if}
	</div>
</div>
