<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { listPublicIps, type PublicIp } from '#lib/azurenetwork.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let { subscription, group }: { subscription: string; group: string } = $props();

	let ips = $state<PublicIp[]>([]);
	let loading = $state(true);
	let error = $state('');

	async function load() {
		loading = true;
		try {
			ips = await listPublicIps(subscription, group);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	const cell = 'px-4 py-3';

	$effect(() => {
		void group;
		void load();
	});
</script>

<div class="space-y-4" data-testid="net-ips">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="net-ips-error">Could not load public IP addresses: {error}</p>{/if}
	{#if loading && ips.length === 0}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if ips.length === 0}
		<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="net-ips-empty">No public IP addresses in this resource group.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="net-ip-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Location</th><th class={cell}>SKU</th><th class={cell}>Allocation</th><th class={cell}>Version</th><th class={cell}>IP address</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each ips as ip (ip.id)}
						<tr data-testid="net-ip">
							<td class="{cell} font-mono text-xs">{ip.name}</td>
							<td class="{cell} text-xs">{ip.location}</td>
							<td class="{cell} text-xs" data-testid="net-ip-sku">{ip.sku || '—'}{ip.tier ? ` (${ip.tier})` : ''}</td>
							<td class="{cell} text-xs" data-testid="net-ip-allocation">{ip.allocation || '—'}</td>
							<td class="{cell} text-xs">{ip.version || '—'}</td>
							<td class="{cell} font-mono text-xs" data-testid="net-ip-address">{ip.address || 'Not assigned'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
