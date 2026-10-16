<script lang="ts">
	import { onRegionChange, regionalClient } from '#lib/region-effect.svelte.ts';
	import { getRoute53RecoveryControlConfigClient } from '#lib/aws-client.ts';
	import { currentRegion } from '#lib/region.svelte.ts';
	import { describeError, endpointPath, statusBadgeClass, withClusterEndpoints } from '#lib/arc.ts';
	import { ListClustersCommand, type Cluster } from '@aws-sdk/client-route53-recovery-control-config';
	import {
		ListRoutingControlsCommand,
		UpdateRoutingControlStateCommand,
		type RoutingControl
	} from '@aws-sdk/client-route53-recovery-cluster';
	import { toast } from 'svelte-sonner';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import SearchInput from '#lib/components/SearchInput.svelte';
	import { Network } from 'lucide-svelte';

	const configClient = regionalClient(getRoute53RecoveryControlConfigClient);

	let clusters = $state<Cluster[]>([]);
	let selectedCluster = $state('');
	let endpointRegion = $state('');
	let controls = $state<RoutingControl[]>([]);
	let loading = $state(false);
	let loadError = $state<string | null>(null);
	let flipError = $state<string | null>(null);
	let flipping = $state<string | null>(null);
	let searchQuery = $state('');

	const cluster = $derived(clusters.find((c) => c.ClusterArn === selectedCluster));
	const endpoints = $derived(cluster?.ClusterEndpoints ?? []);
	const filtered = $derived(
		controls.filter((c) =>
			(c.RoutingControlName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadControls(): Promise<void> {
		controls = [];
		loadError = null;
		if (!cluster) {
			loading = false;
			return;
		}
		loading = true;
		try {
			const out: RoutingControl[] = [];
			let token: string | undefined;
			do {
				const NextToken = token;
				const resp = await withClusterEndpoints(endpoints, endpointRegion, (c) =>
					c.send(new ListRoutingControlsCommand({ NextToken }))
				);
				out.push(...(resp.RoutingControls ?? []));
				token = resp.NextToken;
			} while (token);
			controls = out;
		} catch (e) {
			loadError = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadClusters(): Promise<void> {
		loading = true;
		loadError = null;
		try {
			const all: Cluster[] = [];
			let token: string | undefined;
			do {
				const resp = await configClient().send(new ListClustersCommand({ NextToken: token }));
				all.push(...(resp.Clusters ?? []));
				token = resp.NextToken;
			} while (token);
			clusters = all;
			if (!clusters.some((c) => c.ClusterArn === selectedCluster)) {
				selectedCluster = clusters[0]?.ClusterArn ?? '';
			}
			endpointRegion = endpoints.find((e) => e.Region === currentRegion())?.Region ?? endpoints[0]?.Region ?? '';
		} catch (e) {
			loadError = describeError(e);
			loading = false;
			return;
		}
		await loadControls();
	}

	onRegionChange(() => {
		void loadClusters();
	});

	async function selectCluster(arn: string): Promise<void> {
		selectedCluster = arn;
		endpointRegion = endpoints[0]?.Region ?? '';
		flipError = null;
		await loadControls();
	}

	async function selectEndpoint(region: string): Promise<void> {
		endpointRegion = region;
		await loadControls();
	}

	async function flip(rc: RoutingControl): Promise<void> {
		if (!rc.RoutingControlArn) return;
		const next = rc.RoutingControlState === 'On' ? 'Off' : 'On';
		flipping = rc.RoutingControlArn;
		flipError = null;
		try {
			await withClusterEndpoints(endpoints, endpointRegion, (c) =>
				c.send(
					new UpdateRoutingControlStateCommand({
						RoutingControlArn: rc.RoutingControlArn,
						RoutingControlState: next
					})
				)
			);
			toast.success(`${rc.RoutingControlName} set to ${next}`);
			await loadControls();
		} catch (e) {
			flipError = describeError(e);
			toast.error(flipError);
		} finally {
			flipping = null;
		}
	}

	const th = 'px-4 py-2 font-medium';
	const td = 'px-4 py-3 text-gray-700 dark:text-gray-300';
	const select =
		'px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white';
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Network}
		title="Route 53 Recovery Cluster"
		description="Read and flip routing control state through the cluster data plane"
		onRefresh={loadClusters}
		color="emerald"
	/>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-wrap gap-3 items-center justify-between">
			<div class="flex flex-wrap gap-3 items-center">
				<label for="cluster-select" class="text-sm text-gray-500 dark:text-gray-400">Cluster</label>
				<select
					id="cluster-select"
					class={select}
					value={selectedCluster}
					onchange={(e) => selectCluster((e.target as HTMLSelectElement).value)}
				>
					{#if clusters.length === 0}
						<option value="">No clusters</option>
					{/if}
					{#each clusters as c (c.ClusterArn)}
						<option value={c.ClusterArn}>{c.Name}</option>
					{/each}
				</select>
				<label for="endpoint-select" class="text-sm text-gray-500 dark:text-gray-400">Endpoint</label>
				<select
					id="endpoint-select"
					class={select}
					value={endpointRegion}
					onchange={(e) => selectEndpoint((e.target as HTMLSelectElement).value)}
				>
					{#each endpoints as ep (ep.Region)}
						<option value={ep.Region}>{ep.Region}</option>
					{/each}
				</select>
				{#if cluster}
					<span class="text-xs px-2 py-1 rounded-full {statusBadgeClass(cluster.Status)}">{cluster.Status}</span>
				{/if}
			</div>
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if cluster}
				<p class="text-xs font-mono text-gray-500 dark:text-gray-400 break-all">
					{endpointPath(endpoints.find((e) => e.Region === endpointRegion)?.Endpoint)}
				</p>
			{/if}
			{#if loadError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{loadError}</p>
				</div>
			{/if}
			{#if flipError}
				<div role="alert" data-testid="flip-error" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Routing control state change rejected</p>
					<p>{flipError}</p>
				</div>
			{/if}
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if filtered.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No routing controls found</div>
			{:else}
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
								<th class={th}>Routing control</th><th class={th}>Control panel</th>
								<th class={th}>State</th><th class={th}>Change state</th>
							</tr>
						</thead>
						<tbody>
							{#each filtered as rc (rc.RoutingControlArn)}
								<tr class="border-b border-gray-100 dark:border-gray-800">
									<td class={td}>
										<div class="font-medium">{rc.RoutingControlName}</div>
										<div class="text-xs text-gray-400 break-all">{rc.RoutingControlArn}</div>
									</td>
									<td class={td}>{rc.ControlPanelName}</td>
									<td class={td}>
										<span data-testid="state-{rc.RoutingControlName}" class="text-xs px-2 py-1 rounded-full {statusBadgeClass(rc.RoutingControlState)}">{rc.RoutingControlState ?? 'Unknown'}</span>
									</td>
									<td class={td}>
										<button
											onclick={() => flip(rc)}
											disabled={flipping === rc.RoutingControlArn}
											aria-label="Turn {rc.RoutingControlName} {rc.RoutingControlState === 'On' ? 'Off' : 'On'}"
											class="px-3 py-1 rounded-lg border border-emerald-600 text-emerald-700 dark:text-emerald-400 hover:bg-emerald-50 dark:hover:bg-emerald-900/20 text-xs disabled:opacity-50"
										>
											Turn {rc.RoutingControlState === 'On' ? 'Off' : 'On'}
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	</div>
</div>
