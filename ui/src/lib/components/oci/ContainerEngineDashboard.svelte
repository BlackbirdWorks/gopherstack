<script lang="ts">
	import { onMount } from 'svelte';
	import { Boxes, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { ociServiceById } from '#lib/cloud.ts';
	import { attempt, confirmedAttempt, copyText, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		deleteCluster,
		kubeconfigCommand,
		listClusters,
		listCompartments,
		listNodePools,
		listWorkRequests,
		readStoredOkeCompartment,
		scaleNodePool,
		validPoolSize,
		writeStoredOkeCompartment,
		type OkeCluster,
		type OkeNodePool,
		type OkeWorkRequest
	} from '#lib/ocicontainerengine.ts';
	import { shortOcid } from '#lib/ocirest.ts';

	const TABS = [
		{ id: 'pools', label: 'Node pools' },
		{ id: 'requests', label: 'Work requests' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredOkeCompartment());
	let clusters = $state<OkeCluster[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let tab = $state('pools');
	let pools = $state<OkeNodePool[]>([]);
	let requests = $state<OkeWorkRequest[]>([]);
	let draft = $state<Record<string, string>>({});
	let detailError = $state('');

	const current = $derived(clusters.find((c) => c.id === selected));
	const endpoint = `http://${typeof location === 'undefined' ? 'localhost' : location.hostname}:${ociServiceById('containerengine')?.defaultPort ?? 8158}`;

	async function loadClusters() {
		if (!compartment) {
			clusters = [];
			return;
		}
		try {
			clusters = await listClusters(compartment);
			error = '';
			if (!clusters.some((c) => c.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadDetail();
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadClusters();
		loading = false;
	}

	async function loadDetail() {
		const c = current;
		if (!c) return;
		detailError = '';
		try {
			[pools, requests] = await Promise.all([listNodePools(compartment, c.id), listWorkRequests(compartment, c.id)]);
			draft = Object.fromEntries(pools.map((p) => [p.id, String(p.size)]));
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredOkeCompartment(compartment);
		selected = '';
		loading = true;
		void loadClusters();
	}

	function select(c: OkeCluster) {
		selected = selected === c.id ? '' : c.id;
		pools = [];
		requests = [];
		if (selected) void loadDetail();
	}

	async function scale(p: OkeNodePool) {
		const value = draft[p.id] ?? '';
		if (!validPoolSize(value)) return;
		const ok = await attempt(() => scaleNodePool(p, Number(value)), `Scale requested for ${p.name}`, `Failed to scale ${p.name}`);
		if (ok) await load();
	}

	async function remove(c: OkeCluster) {
		const ok = await confirmedAttempt({ title: 'Delete cluster', message: `Delete cluster "${c.name}" and its node pools?`, confirmLabel: 'Delete' }, () => deleteCluster(c.id), `Deletion started for ${c.name}`, `Failed to delete ${c.name}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' || s === 'SUCCEEDED' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="oke-dashboard">
	<PageHeader icon={Boxes} title="Container Engine" description="Oracle Container Engine for Kubernetes: clusters, node pools and work requests" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="oke-error">
			<p class="font-medium">Could not load clusters</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="oke-empty">No clusters yet. Create one with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="oke-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="oke-list-error">{error}</p>{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="oke-clusters">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Cluster</th><th class={cell}>State</th><th class={cell}>Version</th><th class={cell}>Type</th><th class={cell}>Endpoint</th><th class={cell}>VCN</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each clusters as c (c.id)}
						<tr class={selected === c.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="oke-cluster">
							<td class="{cell} font-mono text-xs" title={c.id}><button class="underline decoration-dotted" onclick={() => select(c)} aria-expanded={selected === c.id} data-testid="oke-select">{c.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(c.state)}" data-testid="oke-state" title={c.details}>{c.state || '—'}</span></td>
							<td class="{cell} font-mono text-xs" data-testid="oke-version">{c.version || '—'}</td>
							<td class="{cell} text-xs">{c.type || '—'}</td>
							<td class="{cell} font-mono text-xs" data-testid="oke-endpoint">{c.publicEndpoint || c.privateEndpoint || c.kubernetesEndpoint || '—'}</td>
							<td class="{cell} font-mono text-xs" title={c.vcnId} data-testid="oke-vcn">{c.vcnId ? shortOcid(c.vcnId) : '—'}</td>
							<td class="{cell} text-right whitespace-nowrap"><button class={GCP_MINI} onclick={() => remove(c)} disabled={c.state === 'DELETING' || c.state === 'DELETED'} data-testid="oke-delete">Delete</button></td>
						</tr>
					{:else}
						<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="oke-none">No clusters in this compartment</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="oke-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name}</h2>
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="oke-detail-error">{detailError}</p>{/if}
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs" data-testid="oke-config">
					<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{current.id}</dd>
					<dt class="text-gray-500">VCN</dt><dd class="font-mono break-all">{current.vcnId || '—'}</dd>
					<dt class="text-gray-500">Kubernetes API</dt><dd class="font-mono">{current.kubernetesEndpoint || '—'}</dd>
					<dt class="text-gray-500">Available upgrades</dt><dd data-testid="oke-upgrades">{current.upgrades.length ? current.upgrades.join(', ') : 'none'}</dd>
				</dl>
				<div class="space-y-1">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Credentials</h3>
					<p class="text-xs text-gray-500">The dashboard does not show cluster credentials. Fetch a kubeconfig with the CLI:</p>
					<div class="flex items-center gap-2">
						<code class="rounded bg-gray-100 px-2 py-1 text-xs break-all dark:bg-slate-800" data-testid="oke-credentials-command">{kubeconfigCommand(current, endpoint)}</code>
						<button class={GCP_MINI} onclick={() => copyText(kubeconfigCommand(current, endpoint))} data-testid="oke-copy-command">Copy command</button>
					</div>
				</div>

				<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

				{#if tab === 'pools'}
					{#if pools.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="oke-pools-empty">This cluster has no node pools</p>
					{:else}
						<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
							<table class="w-full text-sm" data-testid="oke-pools">
								<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Pool</th><th class={cell}>State</th><th class={cell}>Shape</th><th class={cell}>Placements</th><th class={cell}>Size</th><th class={cell}>Nodes</th><th class="{cell} text-right">Scale</th></tr></thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each pools as p (p.id)}
										<tr data-testid="oke-pool">
											<td class="{cell} font-mono text-xs" title={p.id}>{p.name}</td>
											<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(p.state)}" data-testid="oke-pool-state">{p.state || '—'}</span></td>
											<td class="{cell} font-mono text-xs" data-testid="oke-pool-shape">{p.shape}{p.ocpus ? ` (${p.ocpus} OCPU, ${p.memoryGbs} GB)` : ''}</td>
											<td class="{cell} text-xs" data-testid="oke-pool-placements">{p.placements.length ? p.placements.map((x) => x.availabilityDomain).join(', ') : '—'}</td>
											<td class="{cell} text-xs" data-testid="oke-pool-size">{p.size}{p.legacySize ? ' per subnet' : ''}</td>
											<td class="{cell} text-xs" data-testid="oke-pool-nodes">{p.nodes.filter((n) => n.state !== 'DELETED').length}</td>
											<td class="{cell} text-right whitespace-nowrap">
												<input class="{GCP_INPUT} !w-20 !py-1 text-xs" type="text" inputmode="numeric" bind:value={draft[p.id]} aria-label="Size for {p.name}" data-testid="oke-pool-input" />
												<button class={GCP_MINI} onclick={() => scale(p)} disabled={!validPoolSize(draft[p.id] ?? '')} data-testid="oke-pool-scale">Scale</button>
											</td>
										</tr>
										{#each p.nodes as n (n.id)}
											<tr class="text-xs text-gray-500" data-testid="oke-node">
												<td class="{cell} pl-8 font-mono" title={n.id}>{n.name}</td>
												<td class={cell}>{n.state}</td>
												<td class="{cell} font-mono">{n.privateIp || '—'}</td>
												<td class={cell} colspan="4">{n.availabilityDomain}</td>
											</tr>
										{/each}
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				{:else if requests.length === 0}
					<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="oke-requests-empty">No work requests</p>
				{:else}
					<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
						<table class="w-full text-sm" data-testid="oke-requests">
							<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Operation</th><th class={cell}>Status</th><th class={cell}>Resource</th><th class={cell}>Accepted</th><th class={cell}>Finished</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each requests as r (r.id)}
									<tr data-testid="oke-request">
										<td class="{cell} text-xs" data-testid="oke-request-op">{r.operation}</td>
										<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(r.status)}">{r.status}</span></td>
										<td class="{cell} font-mono text-xs" title={r.resource}>{shortOcid(r.resource)}</td>
										<td class="{cell} text-xs">{r.accepted}</td>
										<td class="{cell} text-xs">{r.finished || '—'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		{/if}
	{/if}
</div>
