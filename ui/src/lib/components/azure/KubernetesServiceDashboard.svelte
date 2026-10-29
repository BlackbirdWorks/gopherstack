<script lang="ts">
	import { onMount } from 'svelte';
	import { Boxes, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import {
		credentialsCommand,
		deleteCluster,
		getUpgradeProfile,
		listClusters,
		powerAction,
		scalePool,
		totalNodes,
		validPoolCount,
		type AgentPool,
		type AksCluster,
		type PowerAction,
		type UpgradeProfile
	} from '#lib/azureaks.ts';
	import { attempt, confirmedAttempt, copyText, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let subscription = $state('');
	let clusters = $state<AksCluster[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let upgrades = $state<UpgradeProfile | null>(null);
	let draft = $state<Record<string, string>>({});
	let detailError = $state('');

	const current = $derived(clusters.find((c) => c.id === selected));

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			clusters = await listClusters(subscription);
			error = '';
			if (!clusters.some((c) => c.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadDetail();
	}

	async function loadDetail() {
		const c = current;
		if (!c) return;
		draft = Object.fromEntries(c.pools.map((p) => [p.name, String(p.count)]));
		detailError = '';
		try {
			upgrades = await getUpgradeProfile(subscription, c);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function select(c: AksCluster) {
		selected = selected === c.id ? '' : c.id;
		upgrades = null;
		if (selected) void loadDetail();
	}

	async function toggle(c: AksCluster, action: PowerAction) {
		const run = () => powerAction(subscription, c, action);
		const ok =
			action === 'stop'
				? await confirmedAttempt({ title: 'Stop cluster', message: `Stop "${c.name}"? Its control plane and nodes stay stopped until it is started again.`, confirmLabel: 'Stop' }, run, `Stop requested for ${c.name}`, `Failed to stop ${c.name}`)
				: await attempt(run, `Start requested for ${c.name}`, `Failed to start ${c.name}`);
		if (ok) await load();
	}

	async function remove(c: AksCluster) {
		const ok = await confirmedAttempt({ title: 'Delete cluster', message: `Delete cluster "${c.name}" and its agent pools?`, confirmLabel: 'Delete' }, () => deleteCluster(subscription, c), `Deletion started for ${c.name}`, `Failed to delete ${c.name}`);
		if (ok) await load();
	}

	async function scale(c: AksCluster, p: AgentPool) {
		const value = draft[p.name] ?? '';
		if (!validPoolCount(value)) return;
		const ok = await attempt(() => scalePool(subscription, c, p.name, Number(value)), `Scale requested for ${p.name}`, `Failed to scale ${p.name}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'Running' || s === 'Succeeded' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'Failed' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300';
	const scaling = (p: AgentPool) => (p.autoscale ? `autoscaling ${p.minCount}–${p.maxCount}` : 'fixed');

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="aks-dashboard">
	<PageHeader icon={Boxes} title="Kubernetes Service" description="Azure Kubernetes Service clusters, agent pools and upgrade availability" color="sky" onRefresh={load} />

	{#if error && clusters.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="aks-error">
			<p class="font-medium">Could not load clusters</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if clusters.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="aks-empty">No managed clusters. Create one through Resource Manager (Microsoft.ContainerService/managedClusters).</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="aks-clusters">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Cluster</th><th class={cell}>Resource group</th><th class={cell}>Power</th><th class={cell}>Version</th><th class={cell}>FQDN</th><th class={cell}>Node resource group</th><th class={cell}>Nodes</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each clusters as c (c.id)}
						<tr class={selected === c.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="aks-cluster">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(c)} aria-expanded={selected === c.id} data-testid="aks-select">{c.name}</button></td>
							<td class="{cell} font-mono text-xs">{c.resourceGroup}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(c.power)}" data-testid="aks-power">{c.power || '—'}</span></td>
							<td class="{cell} font-mono text-xs" data-testid="aks-version">{c.currentVersion || c.version || '—'}</td>
							<td class="{cell} font-mono text-xs break-all" data-testid="aks-fqdn">{c.fqdn || '—'}{c.private ? ' (private)' : ''}</td>
							<td class="{cell} font-mono text-xs">{c.nodeResourceGroup || '—'}</td>
							<td class="{cell} text-xs" data-testid="aks-nodes">{totalNodes(c)}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								<button class={GCP_MINI} onclick={() => toggle(c, 'start')} disabled={c.power === 'Running'} data-testid="aks-start">Start</button>
								<button class={GCP_MINI} onclick={() => toggle(c, 'stop')} disabled={c.power === 'Stopped'} data-testid="aks-stop">Stop</button>
								<button class={GCP_MINI} onclick={() => remove(c)} data-testid="aks-delete">Delete</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="aks-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name}</h2>
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="aks-detail-error">{detailError}</p>{/if}

				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs" data-testid="aks-network">
					<dt class="text-gray-500">Location</dt><dd>{current.location}</dd>
					<dt class="text-gray-500">Tier</dt><dd>{current.tier || '—'}</dd>
					<dt class="text-gray-500">Network plugin</dt><dd>{current.network.plugin || '—'}{current.network.pluginMode ? ` (${current.network.pluginMode})` : ''}</dd>
					<dt class="text-gray-500">Network policy</dt><dd>{current.network.policy || '—'}</dd>
					<dt class="text-gray-500">Service CIDR</dt><dd class="font-mono">{current.network.serviceCidr || '—'}</dd>
					<dt class="text-gray-500">DNS service IP</dt><dd class="font-mono">{current.network.dnsServiceIp || '—'}</dd>
					<dt class="text-gray-500">Pod CIDR</dt><dd class="font-mono">{current.network.podCidr || '—'}</dd>
					<dt class="text-gray-500">Load balancer</dt><dd>{current.network.loadBalancerSku || '—'}</dd>
					<dt class="text-gray-500">Outbound type</dt><dd>{current.network.outboundType || '—'}</dd>
				</dl>

				<div class="space-y-1">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Credentials</h3>
					<p class="text-xs text-gray-500">The dashboard does not show cluster credentials. Fetch a kubeconfig with the CLI:</p>
					<div class="flex items-center gap-2">
						<code class="rounded bg-gray-100 px-2 py-1 text-xs dark:bg-slate-800" data-testid="aks-credentials-command">{credentialsCommand(current)}</code>
						<button class={GCP_MINI} onclick={() => copyText(credentialsCommand(current))} data-testid="aks-copy-command">Copy command</button>
					</div>
				</div>

				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Agent pools</h3>
				{#if current.pools.length === 0}
					<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="aks-pools-empty">This cluster has no agent pools</p>
				{:else}
					<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
						<table class="w-full text-sm" data-testid="aks-pools">
							<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Pool</th><th class={cell}>Mode</th><th class={cell}>VM size</th><th class={cell}>Version</th><th class={cell}>Zones</th><th class={cell}>Scaling</th><th class={cell}>Count</th><th class="{cell} text-right">Scale</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each current.pools as p (p.name)}
									<tr data-testid="aks-pool">
										<td class="{cell} font-mono text-xs">{p.name}</td>
										<td class="{cell} text-xs" data-testid="aks-pool-mode">{p.mode || '—'}</td>
										<td class="{cell} font-mono text-xs" data-testid="aks-pool-vmsize">{p.vmSize || '—'}</td>
										<td class="{cell} font-mono text-xs">{p.version || '—'}</td>
										<td class="{cell} text-xs" data-testid="aks-pool-zones">{p.zones.length ? p.zones.join(', ') : '—'}</td>
										<td class="{cell} text-xs" data-testid="aks-pool-scaling">{scaling(p)}</td>
										<td class="{cell} text-xs" data-testid="aks-pool-count">{p.count}</td>
										<td class="{cell} text-right whitespace-nowrap">
											<input class="{GCP_INPUT} !w-20 !py-1 text-xs" type="text" inputmode="numeric" bind:value={draft[p.name]} disabled={p.autoscale || current.power === 'Stopped'} aria-label="Node count for {p.name}" data-testid="aks-pool-input" />
											<button class={GCP_MINI} onclick={() => scale(current, p)} disabled={p.autoscale || current.power === 'Stopped' || !validPoolCount(draft[p.name] ?? '')} title={p.autoscale ? 'Disable autoscaling to scale manually' : current.power === 'Stopped' ? 'Start the cluster to scale' : ''} data-testid="aks-pool-scale">Scale</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}

				{#if upgrades}
					<div class="space-y-1" data-testid="aks-upgrades">
						<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Upgrade profile</h3>
						<p class="text-xs" data-testid="aks-upgrades-control">Control plane {upgrades.controlPlane.version || '—'}: {upgrades.controlPlane.upgrades.length ? upgrades.controlPlane.upgrades.map((u) => u.version + (u.preview ? ' (preview)' : '')).join(', ') : 'no upgrades available'}</p>
						{#each upgrades.pools as up (up.name)}
							<p class="text-xs" data-testid="aks-upgrades-pool">Pool {up.name} {up.version || '—'}: {up.upgrades.length ? up.upgrades.map((u) => u.version + (u.preview ? ' (preview)' : '')).join(', ') : 'no upgrades available'}</p>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	{/if}
</div>
