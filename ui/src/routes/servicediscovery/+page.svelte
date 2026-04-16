<script lang="ts">
	import { onMount } from 'svelte';
	import { getServiceDiscoveryClient } from '$lib/aws-client';
	import {
		ListNamespacesCommand,
		ListServicesCommand,
		ListInstancesCommand,
		type NamespaceSummary,
		type ServiceSummary
	} from '@aws-sdk/client-servicediscovery';
	import { toast } from 'svelte-sonner';
	import { Network, RefreshCw, Search, Globe, Server, Box } from 'lucide-svelte';

	const sd = getServiceDiscoveryClient();

	let loading = $state(false);
	let activeTab = $state<'namespaces' | 'services'>('namespaces');
	let searchQuery = $state('');
	let namespaces = $state<NamespaceSummary[]>([]);
	let services = $state<ServiceSummary[]>([]);

	const filteredNamespaces = $derived(namespaces.filter((n) => (n.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredServices = $derived(services.filter((s) => (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const dnsNamespaces = $derived(namespaces.filter((n) => n.Type?.startsWith('DNS')).length);

	async function loadData() {
		loading = true;
		try {
			const [nsResp, svcResp] = await Promise.all([
				sd.send(new ListNamespacesCommand({})),
				sd.send(new ListServicesCommand({}))
			]);
			namespaces = nsResp.Namespaces ?? [];
			services = svcResp.Services ?? [];
		} catch (e) {
			toast.error('Failed to load Service Discovery data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Network class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Cloud Map</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Service discovery for cloud resources</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg"><Globe class="w-5 h-5 text-indigo-600 dark:text-indigo-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{namespaces.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Namespaces</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Server class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{services.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Services</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Network class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{dnsNamespaces}</p><p class="text-sm text-gray-500 dark:text-gray-400">DNS Namespaces</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['namespaces', 'Namespaces'], ['services', 'Services']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-indigo-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'namespaces'}
				{#if filteredNamespaces.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No namespaces found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredNamespaces as ns}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Globe class="w-5 h-5 text-indigo-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{ns.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{ns.Id} · {ns.Type}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">{ns.ServiceCount} services</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'services'}
				{#if filteredServices.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No services found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredServices as svc}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{svc.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{svc.Id}</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">{svc.InstanceCount} instances</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
