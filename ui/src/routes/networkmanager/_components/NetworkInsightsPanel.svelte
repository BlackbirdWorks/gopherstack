<script lang="ts">
	// Network introspection (services/networkmanager/PARITY.md family T, 5
	// ops). GetNetworkResources/GetNetworkResourceCounts/
	// GetNetworkResourceRelationships ARE real rollups over modeled state --
	// genuine features, not gaps. GetNetworkRoutes is honestly empty (no
	// route-propagation engine in this pass). GetNetworkTelemetry reports UP
	// only for resources already AVAILABLE -- deterministic, not real
	// polling -- labeled as such rather than presented as live health data.
	import {
		GetNetworkResourcesCommand,
		GetNetworkResourceCountsCommand,
		GetNetworkResourceRelationshipsCommand,
		GetNetworkRoutesCommand,
		GetNetworkTelemetryCommand,
		type NetworkResource,
		type NetworkResourceCount,
		type Relationship,
		type NetworkRoute,
		type NetworkTelemetry,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab } from '$lib/components/Tabs.svelte';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import GlobalNetworkSelect from './GlobalNetworkSelect.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	type SubTab = 'resources' | 'counts' | 'relationships' | 'routes' | 'telemetry';
	const subTabs: Tab[] = [
		{ id: 'resources', label: 'Resources' },
		{ id: 'counts', label: 'Counts' },
		{ id: 'relationships', label: 'Relationships' },
		{ id: 'routes', label: 'Routes' },
		{ id: 'telemetry', label: 'Telemetry' }
	];

	let globalNetworkId = $state('');
	let subTab = $state<SubTab>('resources');
	let loading = $state(false);
	let error = $state<string | null>(null);

	let resources = $state<NetworkResource[]>([]);
	let resourcesNextToken = $state<string | undefined>();
	let loadingMoreResources = $state(false);

	let counts = $state<NetworkResourceCount[]>([]);
	let relationships = $state<Relationship[]>([]);
	let telemetry = $state<NetworkTelemetry[]>([]);

	let routeTableArn = $state('');
	let routes = $state<NetworkRoute[] | null>(null);
	let routesBusy = $state(false);
	let routesError = $state<string | null>(null);

	async function loadResources(reset: boolean): Promise<void> {
		const resp = await client().send(
			new GetNetworkResourcesCommand({
				GlobalNetworkId: globalNetworkId,
				MaxResults: 50,
				NextToken: reset ? undefined : resourcesNextToken
			})
		);
		resources = reset ? (resp.NetworkResources ?? []) : [...resources, ...(resp.NetworkResources ?? [])];
		resourcesNextToken = resp.NextToken;
	}

	async function loadMoreResources(): Promise<void> {
		loadingMoreResources = true;
		try {
			await loadResources(false);
		} catch (e) {
			error = describeError(e);
		} finally {
			loadingMoreResources = false;
		}
	}

	async function load(): Promise<void> {
		if (!globalNetworkId) return;
		loading = true;
		error = null;
		try {
			if (subTab === 'resources') {
				await loadResources(true);
			} else if (subTab === 'counts') {
				const resp = await client().send(new GetNetworkResourceCountsCommand({ GlobalNetworkId: globalNetworkId }));
				counts = resp.NetworkResourceCounts ?? [];
			} else if (subTab === 'relationships') {
				const resp = await client().send(
					new GetNetworkResourceRelationshipsCommand({ GlobalNetworkId: globalNetworkId })
				);
				relationships = resp.Relationships ?? [];
			} else if (subTab === 'telemetry') {
				const resp = await client().send(new GetNetworkTelemetryCommand({ GlobalNetworkId: globalNetworkId }));
				telemetry = resp.NetworkTelemetry ?? [];
			}
			// 'routes' is fetched on demand via runRoutesQuery, not on tab load
			// -- it requires a route table identifier the user supplies.
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	export async function refresh(): Promise<void> {
		await load();
	}

	$effect(() => {
		void load();
	});

	function switchSubTab(id: string): void {
		subTab = id as SubTab;
	}

	async function runRoutesQuery(): Promise<void> {
		if (!globalNetworkId || !routeTableArn.trim()) return;
		routesBusy = true;
		routesError = null;
		try {
			const resp = await client().send(
				new GetNetworkRoutesCommand({
					GlobalNetworkId: globalNetworkId,
					RouteTableIdentifier: { TransitGatewayRouteTableArn: routeTableArn.trim() }
				})
			);
			routes = resp.NetworkRoutes ?? [];
		} catch (e) {
			routesError = describeError(e);
		} finally {
			routesBusy = false;
		}
	}

	const filteredResources = $derived(
		resources.filter((r) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(r.ResourceArn ?? '').toLowerCase().includes(q) ||
				(r.ResourceType ?? '').toLowerCase().includes(q) ||
				(r.ResourceId ?? '').toLowerCase().includes(q)
			);
		})
	);

	// Group relationships by their `From` ARN so the tab reads as a
	// grouped graph (one source resource -> its outbound edges) rather than
	// a flat, hard-to-scan list of ARN pairs.
	const groupedRelationships = $derived.by(() => {
		const groups = new Map<string, string[]>();
		for (const r of relationships) {
			if (!r.From) continue;
			const q = searchQuery.toLowerCase();
			if (q && !r.From.toLowerCase().includes(q) && !(r.To ?? '').toLowerCase().includes(q)) continue;
			const existing = groups.get(r.From) ?? [];
			existing.push(r.To ?? '?');
			groups.set(r.From, existing);
		}
		return Array.from(groups.entries());
	});
</script>

<div class="space-y-4">
	<GlobalNetworkSelect {client} bind:value={globalNetworkId} id="nm-insights-gn" />
	<Tabs tabs={subTabs} active={subTab} onSelect={switchSubTab} color="cyan" />

	{#if error}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load data</p>
			<p>{error}</p>
		</div>
	{:else if loading}
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
	{:else if subTab === 'resources'}
		{#if filteredResources.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No network resources found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-2 py-1 font-medium">Resource</th>
						<th class="px-2 py-1 font-medium">Type</th>
						<th class="px-2 py-1 font-medium">Region</th>
						<th class="px-2 py-1 font-medium">Resource ID</th>
					</tr>
				</thead>
				<tbody>
					{#each filteredResources as r (r.ResourceArn)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-2 py-1 break-all">{r.ResourceArn}</td>
							<td class="px-2 py-1">{r.ResourceType ?? '—'}</td>
							<td class="px-2 py-1">{r.AwsRegion ?? '—'}</td>
							<td class="px-2 py-1">{r.ResourceId ?? '—'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
		<LoadMore hasMore={!!resourcesNextToken} loading={loadingMoreResources} onLoadMore={loadMoreResources} />
	{:else if subTab === 'counts'}
		{#if counts.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resource counts found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-2 py-1 font-medium">Resource type</th>
						<th class="px-2 py-1 font-medium">Count</th>
					</tr>
				</thead>
				<tbody>
					{#each counts as c (c.ResourceType)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-2 py-1">{c.ResourceType}</td>
							<td class="px-2 py-1">{c.Count}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if subTab === 'relationships'}
		{#if groupedRelationships.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resource relationships found</div>
		{:else}
			<ul class="space-y-3">
				{#each groupedRelationships as [from, tos] (from)}
					<li class="rounded-lg border border-slate-200 dark:border-slate-700 p-3">
						<p class="text-sm font-medium break-all">{from}</p>
						<ul class="mt-1 space-y-1 pl-4 border-l-2 border-slate-200 dark:border-slate-700">
							{#each tos as to (to)}
								<li class="text-sm text-slate-600 dark:text-slate-300 break-all">→ {to}</li>
							{/each}
						</ul>
					</li>
				{/each}
			</ul>
		{/if}
	{:else if subTab === 'routes'}
		<div class="rounded-lg border border-amber-200 dark:border-amber-900 bg-amber-50 dark:bg-amber-950/30 px-4 py-3 text-sm text-amber-800 dark:text-amber-300">
			This emulator has no route-propagation engine, so a real route table always returns an empty
			route list here -- an empty result is the honest answer, not a bug.
		</div>
		<div class="flex gap-2">
			<input bind:value={routeTableArn} placeholder="Transit gateway route table ARN" class="flex-1 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			<button onclick={runRoutesQuery} disabled={routesBusy || !routeTableArn.trim()} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Look up routes</button>
		</div>
		{#if routesError}<p class="text-sm text-red-600 dark:text-red-400">{routesError}</p>{/if}
		{#if routes !== null}
			{#if routes.length === 0}
				<p class="text-sm text-slate-500 dark:text-slate-400">No routes found for this route table.</p>
			{:else}
				<table class="w-full text-sm">
					<thead>
						<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
							<th class="px-2 py-1 font-medium">Destination CIDR</th>
							<th class="px-2 py-1 font-medium">Type</th>
							<th class="px-2 py-1 font-medium">State</th>
						</tr>
					</thead>
					<tbody>
						{#each routes as r (r.DestinationCidrBlock)}
							<tr class="border-b border-gray-100 dark:border-gray-800">
								<td class="px-2 py-1">{r.DestinationCidrBlock}</td>
								<td class="px-2 py-1">{r.Type}</td>
								<td class="px-2 py-1">{r.State}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		{/if}
	{:else}
		<div class="rounded-lg border border-amber-200 dark:border-amber-900 bg-amber-50 dark:bg-amber-950/30 px-4 py-3 text-sm text-amber-800 dark:text-amber-300">
			Health status is deterministic (<strong>UP</strong> once the underlying resource reaches
			<strong>AVAILABLE</strong>), not real device/BGP/IPsec polling -- there is no real network
			hardware behind this emulator.
		</div>
		{#if telemetry.length === 0}
			<div class="text-center py-8 text-gray-500 dark:text-gray-400">No telemetry entries found</div>
		{:else}
			<table class="w-full text-sm">
				<thead>
					<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
						<th class="px-2 py-1 font-medium">Resource</th>
						<th class="px-2 py-1 font-medium">Type</th>
						<th class="px-2 py-1 font-medium">Health</th>
					</tr>
				</thead>
				<tbody>
					{#each telemetry as t (t.ResourceArn ?? t.ResourceId)}
						<tr class="border-b border-gray-100 dark:border-gray-800">
							<td class="px-2 py-1 break-all">{t.ResourceArn ?? t.ResourceId}</td>
							<td class="px-2 py-1">{t.ResourceType ?? '—'}</td>
							<td class="px-2 py-1">
								<span class="text-xs px-2 py-0.5 rounded-full {t.Health?.Status === 'UP' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'}">
									{t.Health?.Status ?? '—'}
								</span>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{/if}
</div>
