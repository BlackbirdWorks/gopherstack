<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { deleteLoadBalancer, listLoadBalancers, type LoadBalancer } from '#lib/azurenetwork.ts';
	import { confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let { subscription, group }: { subscription: string; group: string } = $props();

	let lbs = $state<LoadBalancer[]>([]);
	let loading = $state(true);
	let error = $state('');

	async function load() {
		loading = true;
		try {
			lbs = await listLoadBalancers(subscription, group);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function remove(lb: LoadBalancer) {
		const ok = await confirmedAttempt({ title: 'Delete load balancer', message: `Delete load balancer "${lb.name}"?`, confirmLabel: 'Delete' }, () => deleteLoadBalancer(subscription, group, lb.name), `Load balancer ${lb.name} deleted`, `Failed to delete ${lb.name}`);
		if (ok) await load();
	}

	const head = 'text-sm font-semibold text-gray-700 dark:text-gray-300';
	const dash = (v: string | number) => (v === '' || v === 0 ? '—' : v);

	$effect(() => {
		void group;
		void load();
	});
</script>

<div class="space-y-4" data-testid="net-lbs">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="net-lbs-error">Could not load load balancers: {error}</p>{/if}
	{#if loading && lbs.length === 0}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if lbs.length === 0}
		<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="net-lbs-empty">No load balancers in this resource group.</p>
	{/if}
	{#each lbs as lb (lb.id)}
		<div class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="net-lb">
			<div class="flex items-center justify-between">
				<h3 class="text-base font-semibold text-gray-900 dark:text-white">{lb.name} <span class="text-xs font-normal text-gray-500">{lb.sku} · {lb.location}</span></h3>
				<button class={GCP_MINI} onclick={() => remove(lb)} data-testid="net-lb-delete">Delete</button>
			</div>
			<div class="grid gap-3 text-xs md:grid-cols-2">
				<div><h4 class={head}>Frontends</h4>
					{#each lb.frontends as f (f.name)}<p data-testid="net-lb-frontend">{f.name}: {f.publicIp ? `public ${f.publicIp}` : f.privateIp || '—'}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
				<div><h4 class={head}>Backend pools</h4>
					{#each lb.pools as p (p.name)}<p data-testid="net-lb-pool">{p.name}: {p.members} member(s)</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
				<div><h4 class={head}>Health probes</h4>
					{#each lb.probes as p (p.name)}<p data-testid="net-lb-probe">{p.name}: {p.protocol} :{p.port}{p.path}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
				<div><h4 class={head}>Load balancing rules</h4>
					{#each lb.rules as r (r.name)}<p data-testid="net-lb-rule">{r.name}: {r.protocol} {dash(r.frontendPort)} → {dash(r.backendPort)}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
			</div>
		</div>
	{/each}
</div>
