<script lang="ts">
	import { onMount } from 'svelte';
	import { getCloudControlClient } from '$lib/aws-client';
	import {
		ListResourcesCommand,
		ListResourceRequestsCommand,
		type ResourceDescription,
		type ProgressEvent
	} from '@aws-sdk/client-cloudcontrol';
	import { toast } from 'svelte-sonner';
	import { Settings, RefreshCw, Search, Box, Activity } from 'lucide-svelte';

	const cc = getCloudControlClient();

	let loading = $state(false);
	let activeTab = $state<'resources' | 'requests'>('resources');
	let searchQuery = $state('');
	let resourceType = $state('AWS::S3::Bucket');
	let resources = $state<ResourceDescription[]>([]);
	let requests = $state<ProgressEvent[]>([]);

	const filteredResources = $derived(resources.filter((r) => (r.Identifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRequests = $derived(requests.filter((r) => (r.ResourceModel ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const resourceTypeOptions = [
		'AWS::S3::Bucket',
		'AWS::DynamoDB::Table',
		'AWS::Lambda::Function',
		'AWS::EC2::Instance',
		'AWS::IAM::Role',
		'AWS::SNS::Topic',
		'AWS::SQS::Queue'
	];

	async function loadData() {
		loading = true;
		try {
			const [resResp, reqResp] = await Promise.all([
				cc.send(new ListResourcesCommand({ TypeName: resourceType })),
				cc.send(new ListResourceRequestsCommand({}))
			]);
			resources = resResp.ResourceDescriptions ?? [];
			requests = reqResp.ResourceRequestStatusSummaries ?? [];
		} catch (e) {
			toast.error('Failed to load Cloud Control data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Settings class="w-7 h-7 text-gray-600 dark:text-gray-400" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Cloud Control API</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Manage AWS and third-party resources using a common API</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-2 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-gray-100 dark:bg-gray-700 rounded-lg"><Box class="w-5 h-5 text-gray-600 dark:text-gray-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{resources.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resources</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{requests.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resource Requests</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center">
			<div class="flex gap-2">
				{#each [['resources', 'Resources'], ['requests', 'Resource Requests']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-gray-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="flex gap-2 w-full sm:w-auto">
				{#if activeTab === 'resources'}
					<select bind:value={resourceType} onchange={loadData} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						{#each resourceTypeOptions as rt}
							<option value={rt}>{rt}</option>
						{/each}
					</select>
				{/if}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48" />
				</div>
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'resources'}
				{#if filteredResources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resources found for {resourceType}</div>
				{:else}
					<div class="space-y-2">
						{#each filteredResources as resource}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Box class="w-5 h-5 text-gray-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{resource.Identifier}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400 font-mono truncate max-w-sm">{resource.Properties?.substring(0, 100)}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'requests'}
				{#if filteredRequests.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resource requests found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRequests as req}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{req.TypeName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{req.Identifier}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{req.OperationStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
