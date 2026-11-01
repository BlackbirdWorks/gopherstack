<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { bindingParent, listTagBindings, listTagKeys, type RmNode, type TagBinding, type TagKey } from '#lib/gcpresourcemanager.ts';

	let { node, org }: { node: RmNode; org: string } = $props();

	let keys = $state<TagKey[]>([]);
	let bindings = $state<TagBinding[]>([]);
	let loading = $state(true);
	let error = $state('');

	async function load() {
		try {
			const parents = node.kind === 'organization' ? [node.name] : [org, node.kind === 'project' ? `projects/${node.id}` : ''];
			const ids = node.kind === 'project' ? [node.id, node.number] : [node.id];
			const kind = node.kind === 'organization' ? 'organizations' : node.kind === 'folder' ? 'folders' : 'projects';
			const [k, b] = await Promise.all([
				Promise.all([...new Set(parents.filter(Boolean))].map((p) => listTagKeys(p))),
				listTagBindings(ids.map((i) => bindingParent(kind, i)))
			]);
			keys = k.flat();
			bindings = b;
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void node.name;
		loading = true;
		void load();
	});
</script>

<div class="space-y-4 text-sm" data-testid="rm-tags">
	{#if error}
		<p class="text-red-600" role="alert" data-testid="rm-tags-error">{error}</p>
	{:else if loading}
		<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
	{:else}
		<section>
			<h3 class="mb-1 font-semibold">Tag bindings on this {node.kind}</h3>
			{#if bindings.length === 0}
				<p class="text-gray-500" data-testid="rm-tag-bindings-empty">No tag bindings.</p>
			{:else}
				<ul class="space-y-1" data-testid="rm-tag-bindings">
					{#each bindings as b (b.name)}<li class="font-mono text-xs" data-testid="rm-tag-binding">{b.namespaced || b.tagValue} <span class="text-gray-400">({b.tagValue})</span></li>{/each}
				</ul>
			{/if}
		</section>
		<section>
			<h3 class="mb-1 font-semibold">Tag keys and values</h3>
			{#if keys.length === 0}
				<p class="text-gray-500" data-testid="rm-tag-keys-empty">No tag keys are defined for this scope.</p>
			{:else}
				<ul class="space-y-2" data-testid="rm-tag-keys">
					{#each keys as k (k.name)}
						<li data-testid="rm-tag-key">
							<span class="font-mono text-xs">{k.namespacedName || k.shortName}</span> <span class="text-xs text-gray-500">{k.description}</span>
							<ul class="ml-4 list-disc text-xs">
								{#each k.values as v (v.name)}<li class="font-mono" data-testid="rm-tag-value">{v.shortName}{v.description ? ` — ${v.description}` : ''}</li>{/each}
							</ul>
						</li>
					{/each}
				</ul>
			{/if}
		</section>
	{/if}
</div>
