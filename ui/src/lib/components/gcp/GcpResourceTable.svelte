<script lang="ts" module>
	export type ResourceRow = { id: string; cells: string[]; raw: unknown };
</script>

<script lang="ts">
	import type { Snippet } from 'svelte';
	import { RefreshCw, Trash2 } from 'lucide-svelte';
	import { GCP_MINI } from '#lib/gcp-action.ts';

	type Props = {
		testId: string;
		noun: string;
		columns: string[];
		rows: ResourceRow[];
		loading: boolean;
		error: string;
		selected?: string;
		onRetry: () => void;
		onSelect?: (row: ResourceRow) => void;
		onDelete?: (row: ResourceRow) => void;
		detail?: Snippet<[ResourceRow]>;
	};
	let { testId, noun, columns, rows, loading, error, selected = '', onRetry, onSelect, onDelete, detail }: Props = $props();

	let expanded = $state<string | null>(null);
	const cell = 'px-4 py-3';
</script>

{#if error && rows.length === 0}
	<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="{testId}-error">
		<p class="font-medium">Could not load {noun}</p>
		<p>{error}</p>
		<button class="mt-2 underline" onclick={onRetry}>Retry</button>
	</div>
{:else if loading}
	<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
{:else if rows.length === 0}
	<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="{testId}-empty">No {noun}</p>
{:else}
	<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="{testId}-list">
		<table class="w-full text-sm">
			<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
				<tr>
					{#each columns as c (c)}<th class={cell}>{c}</th>{/each}
					<th class="{cell} text-right">Actions</th>
				</tr>
			</thead>
			<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
				{#each rows as row (row.id)}
					<tr class={selected === row.id ? 'bg-blue-50/60 dark:bg-blue-950/20' : ''} data-testid="{testId}-row">
						{#each row.cells as c, i (i)}
							<td class="{cell} text-xs break-all {i === 0 ? 'font-mono' : ''}">
								{#if i === 0 && onSelect}
									<button class="text-left underline decoration-dotted" onclick={() => onSelect(row)} data-testid="{testId}-select">{c}</button>
								{:else}{c || '—'}{/if}
							</td>
						{/each}
						<td class="{cell} text-right whitespace-nowrap space-x-1">
							<button class={GCP_MINI} onclick={() => (expanded = expanded === row.id ? null : row.id)} aria-expanded={expanded === row.id} data-testid="{testId}-details">Details</button>
							{#if onDelete}
								<button class="{GCP_MINI} text-red-600" onclick={() => onDelete(row)} aria-label="Delete {row.cells[0]}" data-testid="{testId}-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
							{/if}
						</td>
					</tr>
					{#if expanded === row.id}
						<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="{testId}-detail">
							<td colspan={columns.length + 1} class="px-4 py-3 text-xs space-y-2">
								{#if detail}{@render detail(row)}{/if}
								<pre class="max-h-80 overflow-auto whitespace-pre-wrap break-all rounded-md bg-white dark:bg-slate-900 p-2 font-mono">{JSON.stringify(row.raw, null, 2)}</pre>
							</td>
						</tr>
					{/if}
				{/each}
			</tbody>
		</table>
	</div>
{/if}
