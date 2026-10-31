<script lang="ts">
	import { Building2, Folder, Box } from 'lucide-svelte';
	import { isDeleted, statePill, type RmNode } from '#lib/gcpresourcemanager.ts';

	let { node, selected, onSelect }: { node: RmNode; selected: string; onSelect: (n: RmNode) => void } = $props();

	const iconOf = (n: RmNode) => (n.kind === 'organization' ? Building2 : n.kind === 'folder' ? Folder : Box);
</script>

{#snippet item(n: RmNode)}
<li data-testid="rm-node" data-kind={n.kind} data-name={n.name}>
	{#if n.name}
		{@const Icon = iconOf(n)}
		<button
			class="flex w-full items-center gap-2 rounded px-2 py-1 text-left text-sm hover:bg-gray-100 dark:hover:bg-slate-800 {selected === n.name ? 'bg-blue-50 dark:bg-blue-950/30' : ''} {isDeleted(n) ? 'opacity-70' : ''}"
			onclick={() => onSelect(n)}
			data-testid="rm-node-open"
		>
			<Icon class="h-4 w-4 shrink-0 text-gray-500" />
			<span class="truncate {isDeleted(n) ? 'line-through' : ''}">{n.label}</span>
			{#if n.kind === 'project'}<span class="font-mono text-xs text-gray-400">{n.id}</span>{/if}
			{#if isDeleted(n)}<span class="ml-auto rounded-full px-2 py-0.5 text-xs {statePill(n.state)}" data-testid="rm-node-state">{n.state}</span>{/if}
		</button>
	{:else}
		<p class="px-2 py-1 text-sm font-medium text-gray-500 dark:text-gray-400">{n.label}</p>
	{/if}
	{#if n.children.length}
		<ul class="ml-4 border-l border-gray-200 pl-2 dark:border-gray-700">
			{#each n.children as c (c.name)}
				{@render item(c)}
			{/each}
		</ul>
	{/if}
</li>
{/snippet}

{@render item(node)}
