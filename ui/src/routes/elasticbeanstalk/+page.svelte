<script lang="ts">
	import { onMount } from 'svelte';
	import { getElasticBeanstalkClient } from '$lib/aws-client';
	import {
		DescribeApplicationsCommand,
		DescribeEnvironmentsCommand,
		type ApplicationDescription,
		type EnvironmentDescription
	} from '@aws-sdk/client-elastic-beanstalk';
	import { toast } from 'svelte-sonner';
	import { Leaf, RefreshCw, Search, Server, Box, CheckCircle } from 'lucide-svelte';

	const eb = getElasticBeanstalkClient();

	let loading = $state(false);
	let activeTab = $state<'applications' | 'environments'>('applications');
	let searchQuery = $state('');
	let applications = $state<ApplicationDescription[]>([]);
	let environments = $state<EnvironmentDescription[]>([]);

	const filteredApps = $derived(applications.filter((a) => (a.ApplicationName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredEnvs = $derived(environments.filter((e) => (e.EnvironmentName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const readyEnvs = $derived(environments.filter((e) => e.Status === 'Ready').length);

	async function loadData() {
		loading = true;
		try {
			const [appsResp, envsResp] = await Promise.all([
				eb.send(new DescribeApplicationsCommand({})),
				eb.send(new DescribeEnvironmentsCommand({}))
			]);
			applications = appsResp.Applications ?? [];
			environments = envsResp.Environments ?? [];
		} catch (e) {
			toast.error('Failed to load Elastic Beanstalk data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Leaf class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Elastic Beanstalk</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Deploy and scale web applications and services</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Box class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{applications.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Server class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{environments.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Environments</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-emerald-600 dark:text-emerald-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{readyEnvs}</p><p class="text-sm text-gray-500 dark:text-gray-400">Ready</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Leaf class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{environments.filter((e) => e.Health === 'Green').length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Healthy</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['applications', 'Applications'], ['environments', 'Environments']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-green-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'applications'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Box class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{app.ApplicationName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{app.Description ?? 'No description'}</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">{(app.Versions ?? []).length} versions</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'environments'}
				{#if filteredEnvs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No environments found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEnvs as env}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{env.EnvironmentName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{env.ApplicationName} · {env.PlatformArn?.split('/').pop() ?? 'Unknown'}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span class="text-xs px-2 py-1 rounded-full {env.Health === 'Green' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : env.Health === 'Red' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400'}">{env.Health}</span>
									<span class="text-xs text-gray-500 dark:text-gray-400">{env.Status}</span>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
