<script lang="ts">
	import { onMount } from 'svelte';
	import { getDataSyncClient } from '$lib/aws-client';
	import {
		ListAgentsCommand,
		ListLocationsCommand,
		ListTasksCommand,
		type AgentListEntry,
		type LocationListEntry,
		type TaskListEntry
	} from '@aws-sdk/client-datasync';
	import { toast } from 'svelte-sonner';
	import { ArrowLeftRight, RefreshCw, Search } from 'lucide-svelte';

	const client = getDataSyncClient();

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'tasks' | 'locations' | 'agents'>('tasks');
	let searchQuery = $state('');
	let tasksData = $state<TaskListEntry[]>([]);
	let locationsData = $state<LocationListEntry[]>([]);
	let agentsData = $state<AgentListEntry[]>([]);

	const filteredTasks = $derived(tasksData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredLocations = $derived(locationsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredAgents = $derived(agentsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'tasks') {
				const resp = await client.send(new ListTasksCommand({}));
				tasksData = resp.Tasks ?? [];
			}
			if (activeTab === 'locations') {
				const resp = await client.send(new ListLocationsCommand({}));
				locationsData = resp.Locations ?? [];
			}
			if (activeTab === 'agents') {
				const resp = await client.send(new ListAgentsCommand({}));
				agentsData = resp.Agents ?? [];
			}
		} catch (e) {
			toast.error('Failed to load AWS DataSync data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<ArrowLeftRight class="w-7 h-7 text-cyan-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS DataSync</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Online data transfer service</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['tasks', 'Tasks'], ['locations', 'Locations'], ['agents', 'Agents']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-cyan-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'tasks'}
				{#if filteredTasks.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No tasks found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTasks as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<ArrowLeftRight class="w-5 h-5 text-cyan-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.TaskArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.Status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.Status)}">{a.Status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'locations'}
				{#if filteredLocations.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No locations found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredLocations as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<ArrowLeftRight class="w-5 h-5 text-cyan-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.LocationUri ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.LocationArn ?? ''}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'agents'}
				{#if filteredAgents.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No agents found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAgents as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<ArrowLeftRight class="w-5 h-5 text-cyan-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.AgentArn ?? ''}`}</p>
									</div>
								</div>
								{#if a.Status}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.Status)}">{a.Status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
