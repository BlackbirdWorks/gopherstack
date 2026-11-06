<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';
	import JsonView from './JsonView.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listObjects, type ObjectKind, type SearchObject, type SearchTarget } from '#lib/azuresearch.ts';

	const KINDS: { id: ObjectKind; label: string }[] = [
		{ id: 'datasources', label: 'Data sources' },
		{ id: 'skillsets', label: 'Skillsets' },
		{ id: 'synonymmaps', label: 'Synonym maps' }
	];

	let { target }: { target: SearchTarget } = $props();

	let kind = $state<ObjectKind>('datasources');
	let objects = $state<SearchObject[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');

	const current = $derived(objects.find((o) => o.name === selected));

	async function load() {
		loading = true;
		selected = '';
		try {
			objects = await listObjects(target, kind);
			error = '';
		} catch (e) {
			objects = [];
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function pick(id: string) {
		kind = id as ObjectKind;
		void load();
	}

	onMount(load);

	const cell = 'px-4 py-3';
</script>

<div class="space-y-4" data-testid="search-objects">
	<p class="text-xs text-gray-500">Read only. Credentials in definitions are hidden.</p>
	<Tabs tabs={KINDS} active={kind} onSelect={pick} color="sky" />
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="search-objects-error">{error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="search-object-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Summary</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each objects as o (o.name)}
						<tr data-testid="search-object">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = selected === o.name ? '' : o.name)} aria-expanded={selected === o.name} data-testid="search-object-select">{o.name}</button></td>
							<td class="{cell} text-xs">{o.summary}</td>
						</tr>
					{:else}
						<tr><td colspan="2" class="{cell} text-center text-gray-500" data-testid="search-objects-empty">None defined.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
		{#if current}<JsonView value={current.raw} testid="search-object-json" />{/if}
	{/if}
</div>
