<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMGNClient } from '$lib/aws-client';
	import {
		DescribeSourceServersCommand,
		ListApplicationsCommand,
		ListWavesCommand,
		type SourceServer,
		type Application,
		type Wave
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { Server, RefreshCw, Search, Layers, Waves, Box } from 'lucide-svelte';

	const mgn = regionalClient(getMGNClient);

	let loading = $state(false);
	let activeTab = $state<'servers' | 'applications' | 'waves'>('servers');
	let searchQuery = $state('');
	let servers = $state<SourceServer[]>([]);
	let applications = $state<Application[]>([]);
	let waves = $state<Wave[]>([]);

	const filteredServers = $derived(servers.filter((s) => (s.sourceServerID ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredApps = $derived(applications.filter((a) => (a.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredWaves = $derived(waves.filter((w) => (w.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const readyServers = $derived(servers.filter((s) => s.dataReplicationInfo?.dataReplicationState === 'CONTINUOUS').length);

	async function loadData() {
		loading = true;
		try {
			const [srvResp, appResp, waveResp] = await Promise.all([
				mgn().send(new DescribeSourceServersCommand({ filters: {} })),
				mgn().send(new ListApplicationsCommand({})),
				mgn().send(new ListWavesCommand({}))
			]);
			servers = srvResp.items ?? [];
			applications = appResp.items ?? [];
			waves = waveResp.items ?? [];
		} catch (e) {
			toast.error('Failed to load MGN data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Server class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Application Migration Service</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Lift-and-shift migration service to AWS</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Server class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{servers.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Source Servers</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Box class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{applications.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Waves class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{waves.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Waves</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['servers', 'Source Servers'], ['applications', 'Applications'], ['waves', 'Waves']] as [tab, label]}
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
			{:else if activeTab === 'servers'}
				{#if filteredServers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No source servers found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredServers as server}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{server.sourceServerID}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{server.dataReplicationInfo?.dataReplicationState}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{server.lifeCycle?.state}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'applications'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Box class="w-5 h-5 text-green-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{app.name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{app.applicationID}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'waves'}
				{#if filteredWaves.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No waves found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredWaves as wave}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Waves class="w-5 h-5 text-purple-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{wave.name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{wave.waveID}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
