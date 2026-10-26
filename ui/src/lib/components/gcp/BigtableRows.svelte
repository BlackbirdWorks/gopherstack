<script lang="ts">
	import { Search } from 'lucide-svelte';
	import { GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { DEFAULT_ROW_LIMIT, readRows, type RowInfo } from '#lib/gcpbigtable.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { table: string };
	let { table }: Props = $props();

	const LIMITS = [25, 100, 250, 500];
	let prefix = $state('');
	let limit = $state(DEFAULT_ROW_LIMIT);
	let rows = $state<RowInfo[]>([]);
	let loaded = $state(false);
	let busy = $state(false);
	let error = $state('');

	async function run(event?: Event) {
		event?.preventDefault();
		busy = true;
		try {
			rows = await readRows(table, prefix, limit);
			error = '';
		} catch (e) {
			rows = [];
			error = describeGcpError(e);
		} finally {
			loaded = true;
			busy = false;
		}
	}

	let lastTable = '';
	$effect(() => {
		if (table === lastTable) return;
		lastTable = table;
		rows = [];
		loaded = false;
		error = '';
		void run();
	});

	const when = (d: Date | null, micros: string) => (d ? `${d.toLocaleString()} (${micros})` : micros);
</script>

<div class="space-y-3" data-testid="bigtable-rows">
	<form class="flex flex-wrap items-end gap-2" onsubmit={run}>
		<label class="space-y-1 text-xs text-gray-500 dark:text-gray-400">
			<span class="block">Row key prefix</span>
			<input class="{GCP_INPUT} font-mono w-64" bind:value={prefix} placeholder="(all rows)" autocomplete="off" data-testid="bigtable-prefix" aria-label="Row key prefix" />
		</label>
		<label class="space-y-1 text-xs text-gray-500 dark:text-gray-400">
			<span class="block">Limit</span>
			<select class={GCP_INPUT} bind:value={limit} data-testid="bigtable-limit" aria-label="Row limit">
				{#each LIMITS as l (l)}<option value={l}>{l}</option>{/each}
			</select>
		</label>
		<button type="submit" class="{GCP_PRIMARY} flex items-center gap-2" disabled={busy} data-testid="bigtable-read"><Search class="h-4 w-4" /> Read rows</button>
	</form>

	{#if error}
		<p class="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="bigtable-rows-error">{error}</p>
	{:else if loaded && rows.length === 0}
		<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="bigtable-rows-empty">No rows{prefix ? ` with prefix "${prefix}"` : ''}</p>
	{:else}
		<div class="space-y-3">
			{#each rows as row (row.key)}
				<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="bigtable-row">
					<p class="bg-gray-50 dark:bg-slate-800/60 px-4 py-2 font-mono text-xs break-all"><span class="text-gray-500 dark:text-gray-400">Row key</span> <span data-testid="bigtable-row-key">{row.key}</span> <span class="text-gray-500 dark:text-gray-400">· {row.cellCount} cells</span></p>
					<table class="w-full text-xs">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Family</th><th class="px-4 py-2">Qualifier</th><th class="px-4 py-2">Timestamp (µs)</th><th class="px-4 py-2">Value</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each row.families as fam (fam.name)}
								{#each fam.columns as col (col.qualifier)}
									{#each col.cells as c (c.micros)}
										<tr data-testid="bigtable-cell">
											<td class="px-4 py-2 font-mono">{fam.name}</td>
											<td class="px-4 py-2 font-mono">{col.qualifier}</td>
											<td class="px-4 py-2 whitespace-nowrap">{when(c.timestamp, c.micros)}</td>
											<td class="px-4 py-2 font-mono break-all">{c.value}{#if c.binary}<span class="ml-1 text-gray-500 dark:text-gray-400">(binary)</span>{/if}</td>
										</tr>
									{/each}
								{/each}
							{/each}
						</tbody>
					</table>
				</div>
			{/each}
		</div>
	{/if}
</div>
