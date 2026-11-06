<script lang="ts">
	import { attempt, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { runQuery, validateQuery, type SqlResult, type SqlServer } from '#lib/azuresql.ts';

	let { server, databases }: { server: SqlServer; databases: string[] } = $props();

	let database = $state('');
	let query = $state('SELECT name, database_id FROM sys.databases ORDER BY name');
	let running = $state(false);
	let result = $state<SqlResult | undefined>();

	const dbName = $derived(database || (databases.includes('master') ? 'master' : (databases[0] ?? 'master')));
	const invalid = $derived(validateQuery(query));

	async function run(e: Event) {
		e.preventDefault();
		if (invalid) return;
		running = true;
		const ok = await attempt(async () => {
			result = await runQuery(server, dbName, query);
		}, '', 'Query failed');
		running = false;
		if (!ok) result = undefined;
	}
</script>

<div class="space-y-3" data-testid="sql-query">
	<p class="text-xs text-gray-500">Read-only: a single SELECT statement, at most 500 rows, no comments. Requires a running SQL engine (container runtime).</p>
	<form class="space-y-3" onsubmit={run}>
		<label class="block text-sm">Database
			<select class="{GCP_INPUT} !w-64" value={dbName} onchange={(e) => (database = e.currentTarget.value)} data-testid="sql-query-db">
				<option value="master">master</option>
				{#each databases.filter((d) => d !== 'master') as d (d)}<option value={d}>{d}</option>{/each}
			</select>
		</label>
		<textarea class="{GCP_INPUT} font-mono" rows="5" bind:value={query} aria-label="SQL query" data-testid="sql-query-text"></textarea>
		<div class="flex items-center gap-3">
			<button type="submit" class={GCP_PRIMARY} disabled={running || Boolean(invalid)} data-testid="sql-query-run">{running ? 'Running...' : 'Run'}</button>
			{#if invalid}<span class="text-sm text-gray-500" data-testid="sql-query-hint">{invalid}</span>{/if}
		</div>
	</form>

	{#if result}
		<p class="text-xs text-gray-500" data-testid="sql-query-meta">{result.rows.length} rows in {result.elapsedMs} ms{result.truncated ? ` (truncated at ${result.maxRows} rows)` : ''}</p>
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="sql-query-result">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr>{#each result.columns as c, i (i)}<th class="px-4 py-2">{c}</th>{/each}</tr></thead>
				<tbody>
					{#each result.rows as row, r (r)}
						<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="sql-query-row">{#each row as cell, i (i)}<td class="px-4 py-2 font-mono text-xs">{cell}</td>{/each}</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
