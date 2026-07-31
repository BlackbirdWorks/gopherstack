<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDirectConnectClient } from '$lib/aws-client';
	import {
		DescribeConnectionsCommand,
		DescribeVirtualInterfacesCommand,
		DescribeDirectConnectGatewaysCommand,
		type Connection,
		type VirtualInterface,
		type DirectConnectGateway
	} from '@aws-sdk/client-direct-connect';
	import { toast } from 'svelte-sonner';
	import { Cable, RefreshCw, Search, Router, Network, CheckCircle } from 'lucide-svelte';

	const dc = regionalClient(getDirectConnectClient);

	let loading = $state(false);
	let activeTab = $state<'connections' | 'vifs' | 'gateways'>('connections');
	let searchQuery = $state('');
	let connections = $state<Connection[]>([]);
	let vifs = $state<VirtualInterface[]>([]);
	let gateways = $state<DirectConnectGateway[]>([]);

	const filteredConnections = $derived(connections.filter((c) => (c.connectionName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredVifs = $derived(vifs.filter((v) => (v.virtualInterfaceName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredGateways = $derived(gateways.filter((g) => (g.directConnectGatewayName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const availableConnections = $derived(connections.filter((c) => c.connectionState === 'available').length);

	async function loadData() {
		loading = true;
		try {
			const [connResp, vifResp, gwResp] = await Promise.all([
				dc().send(new DescribeConnectionsCommand({})),
				dc().send(new DescribeVirtualInterfacesCommand({})),
				dc().send(new DescribeDirectConnectGatewaysCommand({}))
			]);
			connections = connResp.connections ?? [];
			vifs = vifResp.virtualInterfaces ?? [];
			gateways = gwResp.directConnectGateways ?? [];
		} catch (e) {
			toast.error('Failed to load Direct Connect data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Cable class="w-7 h-7 text-teal-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Direct Connect</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Dedicated network connections to AWS</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg"><Cable class="w-5 h-5 text-teal-600 dark:text-teal-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{connections.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Connections</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Network class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{vifs.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Virtual Interfaces</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Router class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{gateways.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Direct Connect Gateways</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['connections', 'Connections'], ['vifs', 'Virtual Interfaces'], ['gateways', 'Gateways']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-teal-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'connections'}
				{#if filteredConnections.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No connections found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredConnections as conn}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Cable class="w-5 h-5 text-teal-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{conn.connectionName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{conn.bandwidth} · {conn.location}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {conn.connectionState === 'available' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{conn.connectionState}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'vifs'}
				{#if filteredVifs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No virtual interfaces found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredVifs as vif}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Network class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{vif.virtualInterfaceName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{vif.virtualInterfaceType} · VLAN {vif.vlan}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{vif.virtualInterfaceState}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'gateways'}
				{#if filteredGateways.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No Direct Connect gateways found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredGateways as gw}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Router class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{gw.directConnectGatewayName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">ASN {gw.amazonSideAsn}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{gw.directConnectGatewayState}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
