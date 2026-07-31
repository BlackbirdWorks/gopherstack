<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getGrafanaClient } from '$lib/aws-client';
	import {
		ListWorkspacesCommand,
		type WorkspaceSummary
	} from '@aws-sdk/client-grafana';
	import { toast } from 'svelte-sonner';
	import { BarChart3, RefreshCw, Search, Monitor, CheckCircle } from 'lucide-svelte';

	const grafana = regionalClient(getGrafanaClient);

	let loading = $state(false);
	let searchQuery = $state('');
	let workspaces = $state<WorkspaceSummary[]>([]);

	const filteredWorkspaces = $derived(workspaces.filter((w) => (w.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const activeWorkspaces = $derived(workspaces.filter((w) => w.status === 'ACTIVE').length);

	async function loadData() {
		loading = true;
		try {
			const resp = await grafana().send(new ListWorkspacesCommand({}));
			workspaces = resp.workspaces ?? [];
		} catch (e) {
			toast.error('Failed to load Grafana data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<BarChart3 class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Managed Grafana</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fully managed Grafana workspace for operational observability</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><BarChart3 class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{workspaces.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Workspaces</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{activeWorkspaces}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Monitor class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{workspaces.filter((w) => w.authentication?.providers?.includes('SAML')).length}</p><p class="text-sm text-gray-500 dark:text-gray-400">SAML Enabled</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex items-center gap-3">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search workspaces..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if filteredWorkspaces.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No workspaces found</div>
			{:else}
				<div class="space-y-2">
					{#each filteredWorkspaces as workspace}
						<div class="flex items-center justify-between p-4 rounded-lg bg-gray-50 dark:bg-slate-700/50">
							<div class="flex items-center gap-3">
								<BarChart3 class="w-5 h-5 text-orange-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{workspace.name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{workspace.id} · {workspace.endpoint}</p>
									{#if workspace.authentication?.providers && workspace.authentication.providers.length > 0}
										<div class="flex gap-1 mt-1">
											{#each workspace.authentication.providers as provider}
												<span class="text-xs px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400">{provider}</span>
											{/each}
										</div>
									{/if}
								</div>
							</div>
							<div class="text-right">
								<span class="text-xs px-2 py-1 rounded-full {workspace.status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{workspace.status}</span>
								<p class="text-xs text-gray-400 mt-1">v{workspace.grafanaVersion}</p>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	</div>
</div>
