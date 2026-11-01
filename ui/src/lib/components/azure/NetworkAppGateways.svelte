<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import {
		gatewayAction,
		gatewayRunning,
		getBackendHealth,
		listAppGateways,
		type AppGateway,
		type BackendHealthPool,
		type GatewayAction
	} from '#lib/azurenetwork.ts';
	import { attempt, confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let { subscription, group }: { subscription: string; group: string } = $props();

	let gateways = $state<AppGateway[]>([]);
	let loading = $state(true);
	let error = $state('');
	let health = $state<Record<string, BackendHealthPool[]>>({});
	let healthError = $state<Record<string, string>>({});

	async function load() {
		loading = true;
		try {
			gateways = await listAppGateways(subscription, group);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function act(g: AppGateway, action: GatewayAction) {
		const run = () => gatewayAction(subscription, group, g.name, action);
		const ok =
			action === 'stop'
				? await confirmedAttempt({ title: 'Stop application gateway', message: `Stop "${g.name}"? It stops serving traffic until started again.`, confirmLabel: 'Stop' }, run, `Stopped ${g.name}`, `Failed to stop ${g.name}`)
				: await attempt(run, `Started ${g.name}`, `Failed to start ${g.name}`);
		if (ok) await load();
	}

	async function probe(g: AppGateway) {
		healthError = { ...healthError, [g.name]: '' };
		try {
			health = { ...health, [g.name]: await getBackendHealth(subscription, group, g.name) };
		} catch (e) {
			healthError = { ...healthError, [g.name]: describeGcpError(e) };
		}
	}

	const head = 'text-sm font-semibold text-gray-700 dark:text-gray-300';
	const pill = (s: string) => (s === 'Running' || s === 'Healthy' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'Unhealthy' || s === 'Down' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300');

	$effect(() => {
		void group;
		void load();
	});
</script>

<div class="space-y-4" data-testid="net-agws">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="net-agws-error">Could not load application gateways: {error}</p>{/if}
	{#if loading && gateways.length === 0}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if gateways.length === 0}
		<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="net-agws-empty">No application gateways in this resource group.</p>
	{/if}
	{#each gateways as g (g.id)}
		<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="net-agw">
			<div class="flex flex-wrap items-center justify-between gap-2">
				<h3 class="text-base font-semibold text-gray-900 dark:text-white">{g.name}
					<span class="rounded-full px-2 py-0.5 text-xs font-normal {pill(g.operationalState)}" data-testid="net-agw-state">{g.operationalState || '—'}</span>
				</h3>
				<div class="space-x-1">
					<button class={GCP_MINI} onclick={() => act(g, 'start')} disabled={gatewayRunning(g)} data-testid="net-agw-start">Start</button>
					<button class={GCP_MINI} onclick={() => act(g, 'stop')} disabled={!gatewayRunning(g)} data-testid="net-agw-stop">Stop</button>
					<button class={GCP_MINI} onclick={() => probe(g)} disabled={!gatewayRunning(g)} data-testid="net-agw-health">Backend health</button>
				</div>
			</div>
			<p class="text-xs text-gray-600 dark:text-gray-400" data-testid="net-agw-sku">{g.sku} / {g.tier} · capacity {g.capacity || '—'} · {g.location}</p>
			<div class="grid gap-3 text-xs md:grid-cols-2">
				<div><h4 class={head}>Listeners</h4>
					{#each g.listeners as l (l.name)}<p data-testid="net-agw-listener">{l.name}: {l.protocol} :{l.port}{l.host ? ` (${l.host})` : ''}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
				<div><h4 class={head}>Backend pools</h4>
					{#each g.pools as p (p.name)}<p data-testid="net-agw-pool">{p.name}: {p.addresses.join(', ') || 'no addresses'}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
				<div><h4 class={head}>Backend settings</h4>
					{#each g.settings as s (s.name)}<p data-testid="net-agw-setting">{s.name}: {s.protocol} :{s.port}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
				<div><h4 class={head}>Routing rules</h4>
					{#each g.rules as r (r.name)}<p data-testid="net-agw-rule">{r.priority ? `${r.priority} ` : ''}{r.name}: {r.type} {r.listener} → {r.pool || '—'}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
				<div><h4 class={head}>Health probes</h4>
					{#each g.probes as p (p.name)}<p data-testid="net-agw-probe">{p.name}: {p.protocol} {p.host}{p.path}</p>{:else}<p class="text-gray-500">None</p>{/each}
				</div>
			</div>
			{#if healthError[g.name]}<p class="text-xs text-red-600" role="alert" data-testid="net-agw-health-error">{healthError[g.name]}</p>{/if}
			{#if health[g.name]}
				<div class="space-y-1" data-testid="net-agw-health-result">
					<h4 class={head}>Backend health</h4>
					{#each health[g.name] as pool (pool.pool + pool.settings)}
						{#each pool.servers as s (s.address)}
							<p class="text-xs" data-testid="net-agw-health-server">{pool.pool} / {pool.settings} · {s.address}: <span class="rounded-full px-2 py-0.5 {pill(s.health)}">{s.health}</span> <span class="text-gray-500">{s.log}</span></p>
						{/each}
					{:else}
						<p class="text-xs text-gray-500">No backend servers.</p>
					{/each}
				</div>
			{/if}
		</div>
	{/each}
</div>
