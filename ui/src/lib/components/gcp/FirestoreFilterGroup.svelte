<script lang="ts">
	import { Plus, Trash2 } from 'lucide-svelte';
	import {
		FILTER_VALUE_TYPES,
		FS_OPS,
		isListOp,
		isUnaryOp,
		newFilter,
		newGroup,
		type FsGroup
	} from '#lib/gcpfirestore.ts';

	const MAX_DEPTH = 3;

	let { tree = $bindable() }: { tree: FsGroup } = $props();

	const field =
		'rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const small = 'flex items-center gap-1 rounded-md border border-dashed border-gray-300 dark:border-gray-600 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800';
</script>

{@render groupView(tree, 0)}

{#snippet groupView(g: FsGroup, depth: number, onremove?: () => void)}
<div class="space-y-2 rounded-lg border border-gray-200 dark:border-gray-700 p-3 {depth > 0 ? 'bg-gray-50/60 dark:bg-slate-800/30' : ''}" data-testid="fs-filter-group">
	<div class="flex items-center gap-2 text-sm">
		<span class="text-gray-600 dark:text-gray-400">Match</span>
		<select class="{field} w-40" bind:value={g.op} aria-label="Combinator" data-testid="fs-group-op">
			<option value="AND">all (AND)</option>
			<option value="OR">any (OR)</option>
		</select>
		<span class="text-gray-600 dark:text-gray-400">of:</span>
		{#if onremove}
			<button type="button" class="ml-auto rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove group" onclick={onremove}><Trash2 class="h-4 w-4" /></button>
		{/if}
	</div>

	{#each g.items as item, i (item.id)}
		{#if item.kind === 'group'}
			{@render groupView(item, depth + 1, () => g.items.splice(i, 1))}
		{:else}
			<div class="flex flex-wrap items-center gap-2" data-testid="fs-filter">
				<input class="{field} w-36 font-mono" bind:value={item.field} placeholder="field" aria-label="Filter field" data-testid="fs-filter-field" />
				<select class="{field} w-64" bind:value={item.op} aria-label="Operator" data-testid="fs-filter-op">
					{#each FS_OPS as op (op)}<option value={op}>{op}</option>{/each}
				</select>
				{#if !isUnaryOp(item.op)}
					<select class="{field} w-28" bind:value={item.type} aria-label="Value type" data-testid="fs-filter-type">
						{#each FILTER_VALUE_TYPES as t (t)}<option value={t}>{t}</option>{/each}
					</select>
					{#if item.type === 'boolean' && !isListOp(item.op)}
						<select class="{field} w-24" bind:value={item.value} aria-label="Filter value" data-testid="fs-filter-value"><option value="">choose</option><option value="true">true</option><option value="false">false</option></select>
					{:else if item.type !== 'null' || isListOp(item.op)}
						<input class="{field} min-w-0 flex-1 font-mono" bind:value={item.value} placeholder={isListOp(item.op) ? 'a, b, c' : 'value'} aria-label="Filter value" data-testid="fs-filter-value" />
					{/if}
				{/if}
				<button type="button" class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove filter" onclick={() => g.items.splice(i, 1)}><Trash2 class="h-4 w-4" /></button>
			</div>
		{/if}
	{/each}

	<div class="flex gap-2">
		<button type="button" class={small} onclick={() => g.items.push(newFilter())} data-testid="fs-add-filter"><Plus class="h-3 w-3" /> Filter</button>
		{#if depth + 1 < MAX_DEPTH}
			<button type="button" class={small} onclick={() => g.items.push(newGroup(g.op === 'AND' ? 'OR' : 'AND'))} data-testid="fs-add-group"><Plus class="h-3 w-3" /> Group</button>
		{/if}
	</div>
</div>
{/snippet}
