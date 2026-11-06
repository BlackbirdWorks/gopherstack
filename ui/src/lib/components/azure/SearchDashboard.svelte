<script lang="ts">
	import { onMount } from 'svelte';
	import { Search, RefreshCw } from 'lucide-svelte';
	import SearchServiceDetail from './SearchServiceDetail.svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { getSubscriptionId, listResourceGroups } from '#lib/azurearm.ts';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listServices, searchUnits, type SearchService } from '#lib/azuresearch.ts';

	let subscription = $state('');
	let groups = $state<string[]>([]);
	let group = $state('');
	let services = $state<SearchService[]>([]);
	let selected = $state('');
	let loading = $state(true);
	let error = $state('');

	const current = $derived(services.find((s) => s.name === selected));
	const target = $derived({ subscription, group, service: selected });

	async function loadServices() {
		if (!group) {
			services = [];
			return;
		}
		try {
			services = await listServices(subscription, group);
			error = '';
			if (!services.some((s) => s.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			groups = (await listResourceGroups(subscription)).map((g) => g.name).toSorted();
			if (!groups.includes(group)) group = groups[0] ?? '';
			await loadServices();
		} catch (e) {
			error = describeGcpError(e);
			groups = [];
		} finally {
			loading = false;
		}
	}

	function pickGroup() {
		selected = '';
		void loadServices();
	}

	const cell = 'px-4 py-3';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="search-dashboard">
	<PageHeader icon={Search} title="AI Search" description="Azure AI Search services, keys, indexes, a search explorer and indexers" color="sky" onRefresh={load} />

	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if groups.length === 0}
		{#if error}
			<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="search-error">
				<p class="font-medium">Could not load resource groups</p>
				<p>{error}</p>
				<button class="mt-2 underline" onclick={load}>Retry</button>
			</div>
		{:else}
			<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="search-empty">No resource groups. Create one in Resource Manager first.</p>
		{/if}
	{:else}
		<div class="flex flex-wrap items-center gap-2 text-xs text-gray-500">
			<span>Subscription</span><span class="font-mono" data-testid="search-subscription">{subscription}</span>
			<label for="search-group" class="ml-4">Resource group</label>
			<select id="search-group" class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={group} onchange={pickGroup} data-testid="search-group">
				{#each groups as g (g)}<option value={g}>{g}</option>{/each}
			</select>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="search-list-error">{error}</p>{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="search-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>SKU</th><th class={cell}>Replicas</th><th class={cell}>Partitions</th><th class={cell}>Units</th><th class={cell}>Status</th><th class={cell}>Endpoint</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each services as s (s.name)}
						<tr class={selected === s.name ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="search-service">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = selected === s.name ? '' : s.name)} aria-expanded={selected === s.name} data-testid="search-select">{s.name}</button></td>
							<td class="{cell} text-xs" data-testid="search-sku">{s.sku}</td>
							<td class="{cell} text-xs">{s.replicas}</td>
							<td class="{cell} text-xs">{s.partitions}</td>
							<td class="{cell} text-xs">{searchUnits(s)}</td>
							<td class="{cell} text-xs" data-testid="search-status">{s.status || s.provisioning}</td>
							<td class="{cell} font-mono text-xs break-all">{s.endpoint}</td>
						</tr>
					{:else}
						<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="search-services-empty">No search services in this resource group.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			{#key selected}<SearchServiceDetail service={current} {target} />{/key}
		{/if}
	{/if}
</div>
