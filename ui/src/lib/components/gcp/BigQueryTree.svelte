<script lang="ts">
	import { ChevronDown, ChevronRight, Database, Plus, Table2, Trash2 } from 'lucide-svelte';
	import type { DatasetInfo, TableInfo } from '#lib/gcpbigquery.ts';

	type Selected = { dataset: string; table: string } | null;

	let {
		project,
		datasets,
		tables,
		expanded,
		selected,
		ontoggle,
		onselect,
		onnewtable,
		ondeletedataset,
		ondeletetable
	}: {
		project: string;
		datasets: DatasetInfo[];
		tables: Record<string, TableInfo[]>;
		expanded: Set<string>;
		selected: Selected;
		ontoggle: (dataset: string) => void;
		onselect: (dataset: string, table: string) => void;
		onnewtable: (dataset: string) => void;
		ondeletedataset: (dataset: string) => void;
		ondeletetable: (dataset: string, table: string) => void;
	} = $props();

	const icon = 'rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800';
</script>

<div class="rounded-lg border border-gray-200 dark:border-gray-700 text-sm" data-testid="bq-tree">
	<p class="border-b border-gray-100 dark:border-gray-800 px-3 py-2 font-mono text-xs text-gray-500 dark:text-gray-400 truncate">{project}</p>
	{#if datasets.length === 0}
		<p class="px-3 py-6 text-center text-gray-500 dark:text-gray-400" data-testid="bq-no-datasets">No datasets</p>
	{/if}
	<ul>
		{#each datasets as d (d.id)}
			<li data-testid="bq-dataset-row">
				<div class="group flex items-center gap-1 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800/50">
					<button class="flex min-w-0 flex-1 items-center gap-1 text-left" onclick={() => ontoggle(d.id)} aria-expanded={expanded.has(d.id)} data-testid="bq-dataset-{d.id}">
						{#if expanded.has(d.id)}<ChevronDown class="h-4 w-4 shrink-0" />{:else}<ChevronRight class="h-4 w-4 shrink-0" />{/if}
						<Database class="h-4 w-4 shrink-0 text-blue-600 dark:text-blue-400" />
						<span class="truncate font-medium text-gray-900 dark:text-gray-100">{d.id}</span>
					</button>
					<button class={icon} aria-label="New table in {d.id}" onclick={() => onnewtable(d.id)}><Plus class="h-3.5 w-3.5" /></button>
					<button class="{icon} text-red-500" aria-label="Delete dataset {d.id}" onclick={() => ondeletedataset(d.id)}><Trash2 class="h-3.5 w-3.5" /></button>
				</div>
				{#if expanded.has(d.id)}
					<ul class="ml-5 border-l border-gray-200 dark:border-gray-700">
						{#each tables[d.id] ?? [] as t (t.id)}
							<li class="flex items-center gap-1 pr-2 {selected?.dataset === d.id && selected?.table === t.id ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="bq-table-row">
								<button class="flex min-w-0 flex-1 items-center gap-1 px-2 py-1 text-left hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => onselect(d.id, t.id)}>
									<Table2 class="h-3.5 w-3.5 shrink-0 text-gray-400" />
									<span class="truncate text-gray-800 dark:text-gray-200">{t.id}</span>
								</button>
								<button class="{icon} text-red-500" aria-label="Delete table {t.id}" onclick={() => ondeletetable(d.id, t.id)}><Trash2 class="h-3.5 w-3.5" /></button>
							</li>
						{:else}
							<li class="px-3 py-1 text-xs text-gray-500 dark:text-gray-400">No tables</li>
						{/each}
					</ul>
				{/if}
			</li>
		{/each}
	</ul>
</div>
