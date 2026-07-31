<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getNetworkManagerClient } from '$lib/aws-client';
	import {
		DescribeGlobalNetworksCommand,
		GetDevicesCommand,
		GetLinksCommand,
		type GlobalNetwork,
		type Device,
		type Link
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { Globe, RefreshCw, Search, Router, Link as LinkIcon, Monitor } from 'lucide-svelte';

	const nm = regionalClient(getNetworkManagerClient);

	let loading = $state(false);
	let activeTab = $state<'networks' | 'devices' | 'links'>('networks');
	let searchQuery = $state('');
	let globalNetworks = $state<GlobalNetwork[]>([]);
	let devices = $state<Device[]>([]);
	let links = $state<Link[]>([]);
	let selectedNetworkId = $state<string | null>(null);

	const filteredNetworks = $derived(globalNetworks.filter((n) => (n.GlobalNetworkId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredDevices = $derived(devices.filter((d) => (d.DeviceId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredLinks = $derived(links.filter((l) => (l.LinkId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const resp = await nm().send(new DescribeGlobalNetworksCommand({}));
			globalNetworks = resp.GlobalNetworks ?? [];
		} catch (e) {
			toast.error('Failed to load Network Manager data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadNetworkResources(networkId: string) {
		selectedNetworkId = networkId;
		try {
			const [devResp, linkResp] = await Promise.all([
				nm().send(new GetDevicesCommand({ GlobalNetworkId: networkId })),
				nm().send(new GetLinksCommand({ GlobalNetworkId: networkId }))
			]);
			devices = devResp.Devices ?? [];
			links = linkResp.Links ?? [];
			activeTab = 'devices';
		} catch (e) {
			toast.error('Failed to load network resources: ' + String(e));
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Globe class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Network Manager</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Centrally manage your global network across AWS and on-premises</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Globe class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{globalNetworks.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Global Networks</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Router class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{devices.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Devices</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><LinkIcon class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{links.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Links</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['networks', 'Global Networks'], ['devices', 'Devices'], ['links', 'Links']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'networks'}
				{#if filteredNetworks.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No global networks found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredNetworks as network}
							<button onclick={() => loadNetworkResources(network.GlobalNetworkId ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<Globe class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{network.GlobalNetworkId}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{network.Description ?? 'No description'}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {network.State === 'AVAILABLE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{network.State}</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'devices'}
				{#if filteredDevices.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No devices found. Select a global network to view its devices.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDevices as device}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Router class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{device.DeviceId}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{device.Description ?? device.Type ?? 'Device'}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{device.State}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'links'}
				{#if filteredLinks.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No links found. Select a global network to view its links.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredLinks as link}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<LinkIcon class="w-5 h-5 text-purple-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{link.LinkId}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{link.Type} · {link.Bandwidth?.UploadSpeed} Mbps upload</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
