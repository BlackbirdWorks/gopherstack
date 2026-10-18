<script lang="ts">
	import { Plus, Trash2 } from 'lucide-svelte';
	import { FS_TYPES, newNode, validateNode, type FsCtx, type FsNode, type FsType } from '#lib/gcpfirestore.ts';

	let {
		node = $bindable(),
		ctx,
		showKey = true,
		onremove
	}: {
		node: FsNode;
		ctx: FsCtx;
		showKey?: boolean;
		onremove: () => void;
	} = $props();

	const field =
		'rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';

	const isContainer = (n: FsNode) => n.type === 'array' || n.type === 'map';

	function changeType(n: FsNode, next: FsType) {
		n.type = next;
		n.value = next === 'boolean' ? 'true' : '';
		n.lat = '';
		n.lng = '';
		if (next !== 'array' && next !== 'map') n.children = [];
	}
</script>

{@render row(node, showKey, onremove)}

{#snippet row(n: FsNode, withKey: boolean, remove: () => void)}
<div class="space-y-1" data-testid="fs-field">
	<div class="flex flex-wrap items-start gap-2">
		{#if withKey}
			<input class="{field} w-40 font-mono" bind:value={n.key} placeholder="field name" aria-label="Field name" data-testid="fs-field-key" />
		{/if}
		<select class="{field} w-32" value={n.type} onchange={(e) => changeType(n, e.currentTarget.value as FsType)} aria-label="Field type" data-testid="fs-field-type">
			{#each FS_TYPES as t (t)}<option value={t}>{t}</option>{/each}
		</select>
		<div class="min-w-0 flex-1">
			{#if n.type === 'string'}
				<textarea class="{field} w-full font-mono" rows="1" bind:value={n.value} placeholder="text" aria-label="Value" data-testid="fs-field-value"></textarea>
			{:else if n.type === 'boolean'}
				<select class="{field} w-28" bind:value={n.value} aria-label="Value" data-testid="fs-field-value"><option value="true">true</option><option value="false">false</option></select>
			{:else if n.type === 'geopoint'}
				<div class="flex gap-2">
					<input class="{field} w-32 font-mono" bind:value={n.lat} placeholder="latitude" aria-label="Latitude" data-testid="fs-field-lat" />
					<input class="{field} w-32 font-mono" bind:value={n.lng} placeholder="longitude" aria-label="Longitude" data-testid="fs-field-lng" />
				</div>
			{:else if n.type === 'null'}
				<span class="inline-block py-1 text-sm italic text-gray-500 dark:text-gray-400">null</span>
			{:else if n.type === 'array' || n.type === 'map'}
				<span class="inline-block py-1 text-xs text-gray-500 dark:text-gray-400">{n.children.length} {n.type === 'array' ? 'items' : 'fields'}</span>
			{:else}
				<input
					class="{field} w-full font-mono"
					bind:value={n.value}
					aria-label="Value"
					data-testid="fs-field-value"
					placeholder={n.type === 'timestamp' ? '2024-01-02T03:04:05Z' : n.type === 'reference' ? 'collection/doc' : n.type === 'bytes' ? 'base64' : n.type === 'double' ? '1.5' : '0'}
				/>
			{/if}
		</div>
		{#if n.type === 'timestamp'}
			<button type="button" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => (n.value = new Date().toISOString())}>Now</button>
		{/if}
		<button type="button" class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove field" onclick={remove} data-testid="fs-field-remove"><Trash2 class="h-4 w-4" /></button>
	</div>
	{#if !isContainer(n) && validateNode(n, ctx)}<p class="text-xs text-red-600 dark:text-red-400" data-testid="fs-field-problem">{validateNode(n, ctx)}</p>{/if}
	{#if isContainer(n)}
		<div class="ml-3 space-y-2 border-l-2 border-gray-200 pl-3 dark:border-gray-700">
			{#each n.children as child, i (child.id)}
				{@render row(child, n.type === 'map', () => n.children.splice(i, 1))}
			{/each}
			<button type="button" class="flex items-center gap-1 rounded-md border border-dashed border-gray-300 dark:border-gray-600 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => n.children.push(newNode())} data-testid="fs-add-child">
				<Plus class="h-3 w-3" /> {n.type === 'map' ? 'Add field' : 'Add item'}
			</button>
		</div>
	{/if}
</div>
{/snippet}
