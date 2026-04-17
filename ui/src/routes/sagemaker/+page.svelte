<script lang="ts">
	import { onMount } from 'svelte';
	import { getSageMakerClient } from '$lib/aws-client';
	import {
		ListNotebookInstancesCommand,
		ListTrainingJobsCommand,
		ListModelsCommand,
		ListEndpointsCommand,
		type NotebookInstanceSummary,
		type TrainingJobSummary,
		type ModelSummary,
		type EndpointSummary
	} from '@aws-sdk/client-sagemaker';
	import { toast } from 'svelte-sonner';
	import { Brain, RefreshCw, Search, Server, Activity, Box, BookOpen } from 'lucide-svelte';

	const sm = getSageMakerClient();

	let loading = $state(false);
	let activeTab = $state<'notebooks' | 'training' | 'models' | 'endpoints'>('notebooks');
	let searchQuery = $state('');
	let notebooks = $state<NotebookInstanceSummary[]>([]);
	let trainingJobs = $state<TrainingJobSummary[]>([]);
	let models = $state<ModelSummary[]>([]);
	let endpoints = $state<EndpointSummary[]>([]);

	const filteredNotebooks = $derived(
		notebooks.filter((n) => (n.NotebookInstanceName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredTraining = $derived(
		trainingJobs.filter((j) => (j.TrainingJobName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredModels = $derived(
		models.filter((m) => (m.ModelName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredEndpoints = $derived(
		endpoints.filter((e) => (e.EndpointName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const inServiceEndpoints = $derived(endpoints.filter((e) => e.EndpointStatus === 'InService').length);
	const completedJobs = $derived(trainingJobs.filter((j) => j.TrainingJobStatus === 'Completed').length);

	async function loadData() {
		loading = true;
		try {
			const [nb, tj, mo, ep] = await Promise.all([
				sm.send(new ListNotebookInstancesCommand({})),
				sm.send(new ListTrainingJobsCommand({})),
				sm.send(new ListModelsCommand({})),
				sm.send(new ListEndpointsCommand({}))
			]);
			notebooks = nb.NotebookInstances ?? [];
			trainingJobs = tj.TrainingJobSummaries ?? [];
			models = mo.Models ?? [];
			endpoints = ep.Endpoints ?? [];
		} catch (e) {
			toast.error('Failed to load SageMaker data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Brain class="w-7 h-7 text-teal-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon SageMaker</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Build, train, and deploy machine learning models</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg"><BookOpen class="w-5 h-5 text-teal-600 dark:text-teal-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{notebooks.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Notebooks</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{trainingJobs.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Training Jobs</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Box class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{models.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Models</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Server class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{inServiceEndpoints}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active Endpoints</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each ['notebooks', 'training', 'models', 'endpoints'] as tab}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-teal-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'notebooks'}
				{#if filteredNotebooks.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No notebook instances found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredNotebooks as nb}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<BookOpen class="w-5 h-5 text-teal-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{nb.NotebookInstanceName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{nb.InstanceType ?? 'Unknown'}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {nb.NotebookInstanceStatus === 'InService' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{nb.NotebookInstanceStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'training'}
				{#if filteredTraining.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No training jobs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTraining as job}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{job.TrainingJobName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{job.TrainingJobArn}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {job.TrainingJobStatus === 'Completed' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : job.TrainingJobStatus === 'Failed' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400'}">{job.TrainingJobStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'models'}
				{#if filteredModels.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No models found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredModels as model}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Box class="w-5 h-5 text-purple-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{model.ModelName}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{model.ModelArn}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'endpoints'}
				{#if filteredEndpoints.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No endpoints found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEndpoints as ep}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{ep.EndpointName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{ep.EndpointArn}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {ep.EndpointStatus === 'InService' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{ep.EndpointStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
