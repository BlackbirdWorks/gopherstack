<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDataBrewClient } from '$lib/aws-client';
	import {
		ListDatasetsCommand,
		ListJobsCommand,
		ListProjectsCommand,
		ListRecipesCommand,
		ListSchedulesCommand,
		type Dataset,
		type Job,
		type Project,
		type Recipe,
		type Schedule
	} from '@aws-sdk/client-databrew';
	import { toast } from 'svelte-sonner';
	import { Brush, RefreshCw, Search } from 'lucide-svelte';

	const client = regionalClient(getDataBrewClient);

	let loading = $state(false);
	let activeTab = $state<'datasets' | 'jobs' | 'projects' | 'recipes' | 'schedules'>('datasets');
	let searchQuery = $state('');
	let datasetsData = $state<Dataset[]>([]);
	let jobsData = $state<Job[]>([]);
	let projectsData = $state<Project[]>([]);
	let recipesData = $state<Recipe[]>([]);
	let schedulesData = $state<Schedule[]>([]);

	const filteredDatasets = $derived(datasetsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredJobs = $derived(jobsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredProjects = $derived(projectsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRecipes = $derived(recipesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredSchedules = $derived(schedulesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			// `activeTab` is read with `untrack` so it never becomes a
			// dependency of the `onRegionChange` effect below -- switchTab()
			// already writes activeTab and calls loadData() directly, so
			// letting the effect also depend on activeTab would double-fetch
			// on every region change.
			const tab = untrack(() => activeTab);
			if (tab === 'datasets') {
				const resp = await client().send(new ListDatasetsCommand({}));
				datasetsData = resp.Datasets ?? [];
			}
			if (tab === 'jobs') {
				const resp = await client().send(new ListJobsCommand({}));
				jobsData = resp.Jobs ?? [];
			}
			if (tab === 'projects') {
				const resp = await client().send(new ListProjectsCommand({}));
				projectsData = resp.Projects ?? [];
			}
			if (tab === 'recipes') {
				const resp = await client().send(new ListRecipesCommand({}));
				recipesData = resp.Recipes ?? [];
			}
			if (tab === 'schedules') {
				const resp = await client().send(new ListSchedulesCommand({}));
				schedulesData = resp.Schedules ?? [];
			}
		} catch (e) {
			toast.error('Failed to load AWS Glue DataBrew data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<Brush class="w-7 h-7 text-amber-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Glue DataBrew</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Visual data preparation</p>
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
				{#each [['datasets', 'Datasets'], ['jobs', 'Jobs'], ['projects', 'Projects'], ['recipes', 'Recipes'], ['schedules', 'Schedules']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-amber-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'datasets'}
				{#if filteredDatasets.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No datasets found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDatasets as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Brush class="w-5 h-5 text-amber-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Format: ${a.Format ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'jobs'}
				{#if filteredJobs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No jobs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJobs as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Brush class="w-5 h-5 text-amber-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Type: ${a.Type ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'projects'}
				{#if filteredProjects.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No projects found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredProjects as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Brush class="w-5 h-5 text-amber-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Recipe: ${a.RecipeName ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'recipes'}
				{#if filteredRecipes.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No recipes found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRecipes as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Brush class="w-5 h-5 text-amber-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`v${a.RecipeVersion ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'schedules'}
				{#if filteredSchedules.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No schedules found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSchedules as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Brush class="w-5 h-5 text-amber-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`Cron: ${a.CronExpression ?? '-'}`}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
