<script lang="ts">
	import { onMount } from 'svelte';
	import { Boxes, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		deleteCluster,
		getPoolSizes,
		listClusters,
		listOperations,
		readStoredGkeProject,
		releaseChannelLabel,
		resizeNodePool,
		validNodeCount,
		writeStoredGkeProject,
		type Cluster,
		type GkeOperation,
		type NodePool
	} from '#lib/gcpcontainer.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'clusters', label: 'Clusters' },
		{ id: 'operations', label: 'Operations' }
	];

	let project = $state(readStoredGkeProject());
	let tab = $state('clusters');
	let clusters = $state<Cluster[]>([]);
	let operations = $state<GkeOperation[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let sizes = $state<Record<string, number>>({});
	let draft = $state<Record<string, string>>({});
	let detailError = $state('');

	const current = $derived(clusters.find((c) => c.path === selected));

	async function load() {
		try {
			[clusters, operations] = await Promise.all([listClusters(project), listOperations(project)]);
			error = '';
			if (!clusters.some((c) => c.path === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		if (selected) await loadSizes();
	}

	async function loadSizes() {
		const c = current;
		if (!c) return;
		detailError = '';
		try {
			sizes = await getPoolSizes(c.path);
			draft = Object.fromEntries(c.pools.map((p) => [p.name, String(sizes[p.name] ?? 0)]));
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function applyProject(p: string) {
		project = p;
		writeStoredGkeProject(p);
		selected = '';
		loading = true;
		void load();
	}

	function select(c: Cluster) {
		selected = selected === c.path ? '' : c.path;
		sizes = {};
		if (selected) void loadSizes();
	}

	async function resize(c: Cluster, p: NodePool) {
		const value = draft[p.name] ?? '';
		if (!validNodeCount(value)) return;
		const ok = await attempt(() => resizeNodePool(c.path, p.name, Number(value)), `Resize requested for ${p.name}`, `Failed to resize ${p.name}`);
		if (ok) await load();
	}

	async function remove(c: Cluster) {
		const ok = await confirmedAttempt({ title: 'Delete cluster', message: `Delete cluster "${c.name}" and its node pools?`, confirmLabel: 'Delete' }, () => deleteCluster(c.path), `Deletion started for ${c.name}`, `Failed to delete ${c.name}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'RUNNING' || s === 'DONE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'ERROR' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';
	const scaling = (p: NodePool) => (p.autoscaling.enabled ? `autoscaling ${p.autoscaling.min}–${p.autoscaling.max}` : 'fixed');

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="gke-dashboard">
	<PageHeader icon={Boxes} title="Kubernetes Engine" description="GKE clusters, node pools and operations" color="blue" onRefresh={load} />

	<GcpScopeBar prefix="gke" {project} hint="Clusters of every location in the project are listed." onApply={applyProject} />
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	{#if error && clusters.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="gke-error">
			<p class="font-medium">Could not load clusters</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if tab === 'clusters'}
		{#if clusters.length === 0}
			<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="gke-empty">No clusters in this project. Create one with the Kubernetes Engine API or gcloud.</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="gke-clusters">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
						<tr><th class={cell}>Name</th><th class={cell}>Location</th><th class={cell}>Status</th><th class={cell}>Version</th><th class={cell}>Endpoint</th><th class={cell}>Release channel</th><th class={cell}>Network</th><th class={cell}>Nodes</th><th class="{cell} text-right">Actions</th></tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each clusters as c (c.path)}
							<tr class={selected === c.path ? 'bg-blue-50/60 dark:bg-blue-950/20' : ''} data-testid="gke-cluster">
								<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(c)} aria-expanded={selected === c.path} data-testid="gke-select">{c.name}</button></td>
								<td class="{cell} font-mono text-xs">{c.location}</td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(c.status)}" data-testid="gke-status" title={c.statusMessage}>{c.status || '—'}</span></td>
								<td class="{cell} font-mono text-xs" data-testid="gke-version">{c.masterVersion || '—'}</td>
								<td class="{cell} font-mono text-xs" data-testid="gke-endpoint">{c.endpoint || '—'}</td>
								<td class="{cell} text-xs" data-testid="gke-channel">{releaseChannelLabel(c.releaseChannel)}</td>
								<td class="{cell} font-mono text-xs">{c.network || '—'}{c.subnetwork ? ` / ${c.subnetwork}` : ''}</td>
								<td class="{cell} text-xs">{c.currentNodeCount}</td>
								<td class="{cell} text-right whitespace-nowrap"><button class={GCP_MINI} onclick={() => remove(c)} data-testid="gke-delete">Delete</button></td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if current}
				<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="gke-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name} node pools</h2>
					{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="gke-detail-error">{detailError}</p>{/if}
					{#if current.pools.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="gke-pools-empty">This cluster has no node pools</p>
					{:else}
						<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
							<table class="w-full text-sm" data-testid="gke-pools">
								<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Pool</th><th class={cell}>Status</th><th class={cell}>Version</th><th class={cell}>Machine type</th><th class={cell}>Autoscaling</th><th class={cell}>Nodes per zone</th><th class="{cell} text-right">Resize</th></tr></thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each current.pools as p (p.name)}
										<tr data-testid="gke-pool">
											<td class="{cell} font-mono text-xs">{p.name}</td>
											<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(p.status)}" data-testid="gke-pool-status">{p.status || '—'}</span></td>
											<td class="{cell} font-mono text-xs">{p.version || '—'}</td>
											<td class="{cell} font-mono text-xs" data-testid="gke-pool-machine">{p.machineType || '—'}</td>
											<td class="{cell} text-xs" data-testid="gke-pool-scaling">{scaling(p)}</td>
											<td class="{cell} text-xs" data-testid="gke-pool-size">{sizes[p.name] ?? '—'}</td>
											<td class="{cell} text-right whitespace-nowrap">
												<input class="{GCP_INPUT} !w-20 !py-1 text-xs" type="text" inputmode="numeric" bind:value={draft[p.name]} disabled={p.autoscaling.enabled} aria-label="Node count for {p.name}" data-testid="gke-pool-count" />
												<button class={GCP_MINI} onclick={() => resize(current, p)} disabled={p.autoscaling.enabled || !validNodeCount(draft[p.name] ?? '')} title={p.autoscaling.enabled ? 'Disable autoscaling to resize' : ''} data-testid="gke-pool-resize">Resize</button>
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{:else if operations.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="gke-operations-empty">No operations yet</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="gke-operations">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Operation</th><th class={cell}>Type</th><th class={cell}>Status</th><th class={cell}>Target</th><th class={cell}>Location</th><th class={cell}>Started</th><th class={cell}>Finished</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each operations as o (o.name)}
						<tr data-testid="gke-operation">
							<td class="{cell} font-mono text-xs">{o.name}</td>
							<td class="{cell} text-xs">{o.type}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(o.status)}" title={o.detail}>{o.status}</span></td>
							<td class="{cell} font-mono text-xs">{o.target || '—'}</td>
							<td class="{cell} font-mono text-xs">{o.location}</td>
							<td class="{cell} text-xs">{o.start || '—'}</td>
							<td class="{cell} text-xs">{o.end || '—'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
