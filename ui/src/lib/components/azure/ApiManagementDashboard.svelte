<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw, Waypoints } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import ApimServiceDetail from '#lib/components/azure/ApimServiceDetail.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listServices, type ApimService } from '#lib/azureapim.ts';

	let subscription = $state('');
	let services = $state<ApimService[]>([]);
	let loading = $state(true);
	let error = $state('');
	let group = $state('');
	let selected = $state('');
	let detailKey = $state(0);

	const groups = $derived([...new Set(services.map((s) => s.resourceGroup))].toSorted());
	const visible = $derived(group ? services.filter((s) => s.resourceGroup === group) : services);
	const current = $derived(services.find((s) => s.id === selected));

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			services = await listServices(subscription);
			error = '';
			if (!services.some((s) => s.id === selected)) selected = '';
			detailKey += 1;
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function select(s: ApimService) {
		selected = selected === s.id ? '' : s.id;
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'Succeeded' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'Failed' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="apim-dashboard">
	<PageHeader icon={Waypoints} title="API Management" description="Services, APIs, products, subscriptions, named values and policies" color="sky" onRefresh={load} />

	{#if error && services.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="apim-error">
			<p class="font-medium">Could not load API Management services</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if services.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="apim-empty">No API Management services. Create one through Resource Manager (Microsoft.ApiManagement service).</p>
	{:else}
		<div class="flex items-center gap-2 text-xs text-gray-500">
			<label for="apim-group">Resource group</label>
			<select id="apim-group" class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={group} data-testid="apim-group">
				<option value="">All resource groups</option>
				{#each groups as g (g)}<option value={g}>{g}</option>{/each}
			</select>
		</div>

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="apim-services">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Service</th><th class={cell}>Resource group</th><th class={cell}>SKU</th><th class={cell}>State</th><th class={cell}>Gateway URL</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each visible as s (s.id)}
						<tr class={selected === s.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="apim-service">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(s)} aria-expanded={selected === s.id} data-testid="apim-select">{s.name}</button></td>
							<td class="{cell} font-mono text-xs">{s.resourceGroup}</td>
							<td class="{cell} text-xs" data-testid="apim-sku">{s.sku || '—'}{s.capacity ? ` x${s.capacity}` : ''}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(s.state)}" data-testid="apim-state">{s.state || '—'}</span></td>
							<td class="{cell} font-mono text-xs break-all" data-testid="apim-gateway-url">{s.gatewayUrl || '—'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			{#key `${current.id}:${detailKey}`}
				<ApimServiceDetail {subscription} service={current} />
			{/key}
		{/if}
	{/if}
</div>
