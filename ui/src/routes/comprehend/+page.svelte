<script lang="ts">
	import { onMount } from 'svelte';
	import { getComprehendClient } from '$lib/aws-client';
	import {
		ListDocumentClassifiersCommand,
		ListEntityRecognizersCommand,
		ListTopicsDetectionJobsCommand,
		type DocumentClassifierProperties,
		type EntityRecognizerProperties,
		type TopicsDetectionJobProperties
	} from '@aws-sdk/client-comprehend';
	import { toast } from 'svelte-sonner';
	import { MessageSquare, RefreshCw, Search, FileText, Tag, Activity } from 'lucide-svelte';

	const comp = getComprehendClient();

	let loading = $state(false);
	let activeTab = $state<'classifiers' | 'recognizers' | 'topics'>('classifiers');
	let searchQuery = $state('');
	let classifiers = $state<DocumentClassifierProperties[]>([]);
	let recognizers = $state<EntityRecognizerProperties[]>([]);
	let topicsJobs = $state<TopicsDetectionJobProperties[]>([]);

	const filteredClassifiers = $derived(classifiers.filter((c) => (c.DocumentClassifierArn ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRecognizers = $derived(recognizers.filter((r) => (r.EntityRecognizerArn ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredTopics = $derived(topicsJobs.filter((j) => (j.JobId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const trainedClassifiers = $derived(classifiers.filter((c) => c.Status === 'TRAINED').length);

	async function loadData() {
		loading = true;
		try {
			const [classResp, recResp, topResp] = await Promise.all([
				comp.send(new ListDocumentClassifiersCommand({})),
				comp.send(new ListEntityRecognizersCommand({})),
				comp.send(new ListTopicsDetectionJobsCommand({}))
			]);
			classifiers = classResp.DocumentClassifierPropertiesList ?? [];
			recognizers = recResp.EntityRecognizerPropertiesList ?? [];
			topicsJobs = topResp.TopicsDetectionJobPropertiesList ?? [];
		} catch (e) {
			toast.error('Failed to load Comprehend data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<MessageSquare class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Comprehend</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Natural language processing to find insights in text</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><FileText class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{classifiers.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Document Classifiers</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Tag class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{recognizers.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Entity Recognizers</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Activity class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{topicsJobs.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Topics Jobs</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['classifiers', 'Classifiers'], ['recognizers', 'Entity Recognizers'], ['topics', 'Topics Jobs']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-orange-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'classifiers'}
				{#if filteredClassifiers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No document classifiers found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredClassifiers as clf}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<FileText class="w-5 h-5 text-orange-500" />
									<p class="text-sm font-medium text-gray-900 dark:text-white truncate max-w-sm">{clf.DocumentClassifierArn}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {clf.Status === 'TRAINED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{clf.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'recognizers'}
				{#if filteredRecognizers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No entity recognizers found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRecognizers as rec}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Tag class="w-5 h-5 text-blue-500" />
									<p class="text-sm font-medium text-gray-900 dark:text-white truncate max-w-sm">{rec.EntityRecognizerArn}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {rec.Status === 'TRAINED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{rec.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'topics'}
				{#if filteredTopics.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No topics detection jobs found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTopics as job}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-green-500" />
									<p class="font-medium text-gray-900 dark:text-white">{job.JobId}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{job.JobStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
