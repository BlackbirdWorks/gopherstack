<script lang="ts">
	import { onMount } from 'svelte';
	import { getCodeArtifactClient } from '$lib/aws-client';
	import {
		ListDomainsCommand,
		ListRepositoriesCommand,
		ListPackagesCommand,
		type DomainSummary,
		type RepositorySummary,
		type PackageSummary
	} from '@aws-sdk/client-codeartifact';
	import { toast } from 'svelte-sonner';
	import { Package, RefreshCw, Search, Database, Archive, ChevronRight } from 'lucide-svelte';

	const ca = getCodeArtifactClient();

	let loading = $state(false);
	let activeTab = $state<'domains' | 'repositories' | 'packages'>('domains');
	let searchQuery = $state('');
	let domains = $state<DomainSummary[]>([]);
	let repositories = $state<RepositorySummary[]>([]);
	let packages = $state<PackageSummary[]>([]);

	const filteredDomains = $derived(domains.filter((d) => (d.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRepos = $derived(repositories.filter((r) => (r.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredPackages = $derived(packages.filter((p) => (p.package ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [domainsResp, reposResp] = await Promise.all([
				ca.send(new ListDomainsCommand({})),
				ca.send(new ListRepositoriesCommand({}))
			]);
			domains = domainsResp.domains ?? [];
			repositories = reposResp.repositories ?? [];
		} catch (e) {
			toast.error('Failed to load CodeArtifact data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadPackages(domain: string, repository: string) {
		try {
			const resp = await ca.send(new ListPackagesCommand({ domain, repository }));
			packages = resp.packages ?? [];
			activeTab = 'packages';
		} catch (e) {
			toast.error('Failed to load packages: ' + String(e));
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Package class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS CodeArtifact</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fully managed artifact repository service</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg"><Database class="w-5 h-5 text-indigo-600 dark:text-indigo-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{domains.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Domains</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Archive class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{repositories.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Repositories</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Package class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{packages.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Packages</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each ['domains', 'repositories', 'packages'] as tab}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-indigo-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{tab.charAt(0).toUpperCase() + tab.slice(1)}
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
			{:else if activeTab === 'domains'}
				{#if filteredDomains.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No domains found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDomains as domain}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Database class="w-5 h-5 text-indigo-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{domain.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{domain.owner}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">{domain.status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'repositories'}
				{#if filteredRepos.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No repositories found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRepos as repo}
							<button onclick={() => loadPackages(repo.domainName ?? '', repo.name ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<Archive class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{repo.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{repo.domainName}</p>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-gray-400" />
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'packages'}
				{#if filteredPackages.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No packages found. Select a repository to browse packages.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPackages as pkg}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Package class="w-5 h-5 text-purple-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{pkg.package}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{pkg.format} · {pkg.namespace ?? ''}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
