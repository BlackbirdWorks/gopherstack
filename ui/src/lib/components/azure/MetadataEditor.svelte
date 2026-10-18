<script lang="ts">
	import { Plus, X } from 'lucide-svelte';
	import { validateRows, type MetaRow } from '#lib/azureblob.ts';

	let {
		rows = $bindable<MetaRow[]>([]),
		label,
		testId,
		saving = false,
		onsave
	}: {
		rows: MetaRow[];
		label: string;
		testId: string;
		saving?: boolean;
		onsave: () => void;
	} = $props();

	const problem = $derived(validateRows(rows));
	const field =
		'rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500';
</script>

<div class="space-y-2" data-testid={testId}>
	<div class="flex items-center justify-between">
		<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">{label}</h4>
		<button
			type="button"
			class="flex items-center gap-1 rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800"
			onclick={() => (rows = [...rows, { key: '', value: '' }])}
		>
			<Plus class="h-3.5 w-3.5" /> Add
		</button>
	</div>
	{#if rows.length === 0}
		<p class="text-xs text-gray-500 dark:text-gray-400">No metadata</p>
	{/if}
	{#each rows as row, i (i)}
		<div class="flex items-center gap-2" data-testid="{testId}-row">
			<input class="{field} w-40" placeholder="key" aria-label="Metadata key" bind:value={row.key} />
			<input class="{field} flex-1 min-w-0" placeholder="value" aria-label="Metadata value" bind:value={row.value} />
			<button type="button" class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove metadata row" onclick={() => (rows = rows.filter((_, j) => j !== i))}>
				<X class="h-4 w-4" />
			</button>
		</div>
	{/each}
	{#if problem}<p class="text-xs text-red-600 dark:text-red-400" data-testid="{testId}-error">{problem}</p>{/if}
	<button
		type="button"
		class="rounded-md bg-blue-600 px-3 py-1 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
		disabled={saving || Boolean(problem)}
		onclick={onsave}
	>
		{saving ? 'Saving…' : `Save ${label.toLowerCase()}`}
	</button>
</div>
