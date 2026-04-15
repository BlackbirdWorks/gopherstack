<script lang="ts">
	import { onMount } from 'svelte';
	import { getCloudFormationClient } from '$lib/aws-client';
	import {
		DescribeStacksCommand,
		CreateStackCommand,
		DeleteStackCommand,
		UpdateStackCommand,
		ListStackResourcesCommand,
		DescribeStackEventsCommand,
		GetTemplateCommand,
		ValidateTemplateCommand,
		type Stack,
		type StackResourceSummary,
		type StackEvent,
		StackStatus
	} from '@aws-sdk/client-cloudformation';
	import { toast } from 'svelte-sonner';
	import { Layers, Search, RefreshCw, Plus, Trash2, ChevronRight, FileCode, Activity, AlertCircle } from 'lucide-svelte';

	const cfn = getCloudFormationClient();

	let loading = $state(false);
	let stacks = $state<Stack[]>([]);
	let selectedStack = $state<Stack | null>(null);
	let activeTab = $state<'overview' | 'resources' | 'events' | 'template'>('overview');
	let searchQuery = $state('');

	// Resources & Events
	let resources = $state<StackResourceSummary[]>([]);
	let loadingResources = $state(false);
	let events = $state<StackEvent[]>([]);
	let loadingEvents = $state(false);
	let template = $state('');
	let loadingTemplate = $state(false);

	// Create Stack
	let showCreateStack = $state(false);
	let creatingStack = $state(false);
	let newStackName = $state('');
	let newTemplateBody = $state('');
	let newParameters = $state('');

	// Update Stack
	let showUpdateStack = $state(false);
	let updatingStack = $state(false);
	let updateTemplateBody = $state('');

	// Validate
	let validatingTemplate = $state(false);
	let validationResult = $state('');

	// Delete
	let deletingStack = $state<string | null>(null);

	const filteredStacks = $derived(
		stacks.filter((s) => !searchQuery || (s.StackName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const statusColor = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status.includes('COMPLETE') && !status.includes('DELETE')) return 'green';
		if (status.includes('FAILED') || status.includes('ROLLBACK')) return 'red';
		if (status.includes('IN_PROGRESS')) return 'yellow';
		if (status.includes('DELETE')) return 'gray';
		return 'blue';
	};

	async function loadStacks() {
		loading = true;
		try {
			const resp = await cfn.send(new DescribeStacksCommand({}));
			stacks = resp.Stacks ?? [];
		} catch (e) {
			toast.error('Failed to load stacks: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectStack(stack: Stack) {
		selectedStack = stack;
		activeTab = 'overview';
		await loadResources(stack.StackName ?? '');
	}

	async function loadResources(stackName: string) {
		loadingResources = true;
		try {
			const resp = await cfn.send(new ListStackResourcesCommand({ StackName: stackName }));
			resources = resp.StackResourceSummaries ?? [];
		} catch (e) {
			toast.error('Failed to load resources: ' + String(e));
		} finally {
			loadingResources = false;
		}
	}

	async function loadEvents(stackName: string) {
		loadingEvents = true;
		try {
			const resp = await cfn.send(new DescribeStackEventsCommand({ StackName: stackName }));
			events = resp.StackEvents ?? [];
		} catch (e) {
			toast.error('Failed to load events: ' + String(e));
		} finally {
			loadingEvents = false;
		}
	}

	async function loadTemplate(stackName: string) {
		loadingTemplate = true;
		try {
			const resp = await cfn.send(new GetTemplateCommand({ StackName: stackName }));
			template = resp.TemplateBody ?? '';
		} catch (e) {
			toast.error('Failed to load template: ' + String(e));
		} finally {
			loadingTemplate = false;
		}
	}

	async function handleTabChange(tab: 'overview' | 'resources' | 'events' | 'template') {
		activeTab = tab;
		if (!selectedStack) return;
		const name = selectedStack.StackName ?? '';
		if (tab === 'events' && events.length === 0) await loadEvents(name);
		if (tab === 'template' && !template) await loadTemplate(name);
	}

	async function createStack() {
		if (!newStackName.trim() || !newTemplateBody.trim()) return;
		creatingStack = true;
		try {
			const params: Parameters<typeof cfn.send>[0] extends CreateStackCommand ? never : { StackName: string; TemplateBody: string; Parameters?: { ParameterKey: string; ParameterValue: string }[] } = {
				StackName: newStackName.trim(),
				TemplateBody: newTemplateBody.trim()
			};
			if (newParameters.trim()) {
				try {
					params.Parameters = JSON.parse(newParameters);
				} catch { /* ignore */ }
			}
			await cfn.send(new CreateStackCommand(params));
			toast.success(`Stack "${newStackName}" creation initiated`);
			showCreateStack = false;
			newStackName = '';
			newTemplateBody = '';
			newParameters = '';
			await loadStacks();
		} catch (e) {
			toast.error('Failed to create stack: ' + String(e));
		} finally {
			creatingStack = false;
		}
	}

	async function updateStack() {
		if (!selectedStack || !updateTemplateBody.trim()) return;
		updatingStack = true;
		try {
			await cfn.send(new UpdateStackCommand({
				StackName: selectedStack.StackName ?? '',
				TemplateBody: updateTemplateBody.trim()
			}));
			toast.success(`Stack "${selectedStack.StackName}" update initiated`);
			showUpdateStack = false;
			updateTemplateBody = '';
			await loadStacks();
		} catch (e) {
			toast.error('Failed to update stack: ' + String(e));
		} finally {
			updatingStack = false;
		}
	}

	async function deleteStack(name: string) {
		if (!confirm(`Delete stack "${name}"? This cannot be undone.`)) return;
		deletingStack = name;
		try {
			await cfn.send(new DeleteStackCommand({ StackName: name }));
			toast.success(`Stack "${name}" deletion initiated`);
			if (selectedStack?.StackName === name) selectedStack = null;
			await loadStacks();
		} catch (e) {
			toast.error('Failed to delete stack: ' + String(e));
		} finally {
			deletingStack = null;
		}
	}

	async function validateTemplate() {
		if (!newTemplateBody.trim()) return;
		validatingTemplate = true;
		validationResult = '';
		try {
			const resp = await cfn.send(new ValidateTemplateCommand({ TemplateBody: newTemplateBody.trim() }));
			validationResult = `Valid! Parameters: ${(resp.Parameters ?? []).map((p) => p.ParameterKey).join(', ') || 'none'}`;
		} catch (e) {
			validationResult = 'Invalid: ' + String(e);
		} finally {
			validatingTemplate = false;
		}
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	onMount(loadStacks);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Layers class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">CloudFormation</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Infrastructure as Code</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={loadStacks} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateStack = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Stack
			</button>
		</div>
	</div>

	{#if selectedStack}
		<!-- Stack Detail -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedStack = null; resources = []; events = []; template = ''; }} class="text-orange-600 hover:underline">Stacks</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedStack.StackName}</span>
			<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedStack.StackStatus)}-100 dark:bg-${statusColor(selectedStack.StackStatus)}-900/30 text-${statusColor(selectedStack.StackStatus)}-700 dark:text-${statusColor(selectedStack.StackStatus)}-400`}>
				{selectedStack.StackStatus}
			</span>
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each ['overview', 'resources', 'events', 'template'] as tab}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'resources' | 'events' | 'template')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-orange-500 text-orange-600 dark:text-orange-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}`}
				>
					{tab.charAt(0).toUpperCase() + tab.slice(1)}
				</button>
			{/each}
			<button onclick={() => { showUpdateStack = true; updateTemplateBody = template; }} class="ml-auto px-4 py-2 text-sm text-orange-600 hover:underline">Update Stack</button>
			<button onclick={() => deleteStack(selectedStack?.StackName ?? '')} class="px-4 py-2 text-sm text-red-500 hover:underline">Delete</button>
		</div>

		{#if activeTab === 'overview'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'Stack ID', value: selectedStack.StackId },
					{ label: 'Description', value: selectedStack.Description || '-' },
					{ label: 'Status', value: selectedStack.StackStatus },
					{ label: 'Status Reason', value: selectedStack.StackStatusReason || '-' },
					{ label: 'Created', value: formatDate(selectedStack.CreationTime) },
					{ label: 'Last Updated', value: formatDate(selectedStack.LastUpdatedTime) },
					{ label: 'Capabilities', value: (selectedStack.Capabilities ?? []).join(', ') || 'none' },
					{ label: 'Outputs', value: (selectedStack.Outputs ?? []).length + ' outputs' }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
						<div class="text-xs text-gray-500 dark:text-gray-400 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1 truncate font-mono">{row.value}</div>
					</div>
				{/each}
			</div>

			{#if (selectedStack.Outputs ?? []).length > 0}
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Outputs</h3>
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Key</th>
									<th class="px-4 py-3 text-left">Value</th>
									<th class="px-4 py-3 text-left">Description</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each selectedStack.Outputs ?? [] as output}
									<tr>
										<td class="px-4 py-3 font-mono text-xs">{output.OutputKey}</td>
										<td class="px-4 py-3 text-gray-700 dark:text-gray-300">{output.OutputValue}</td>
										<td class="px-4 py-3 text-gray-500">{output.Description ?? '-'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'resources'}
			{#if loadingResources}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Logical ID</th>
								<th class="px-4 py-3 text-left">Physical ID</th>
								<th class="px-4 py-3 text-left">Type</th>
								<th class="px-4 py-3 text-left">Status</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each resources as r}
								<tr>
									<td class="px-4 py-3 font-medium">{r.LogicalResourceId}</td>
									<td class="px-4 py-3 font-mono text-xs text-gray-500 truncate max-w-xs">{r.PhysicalResourceId ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400 text-xs">{r.ResourceType}</td>
									<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(r.ResourceStatus)}-100 text-${statusColor(r.ResourceStatus)}-700`}>{r.ResourceStatus}</span></td>
								</tr>
							{/each}
						</tbody>
					</table>
					{#if resources.length === 0}<div class="text-center py-8 text-gray-500">No resources</div>{/if}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'events'}
			{#if loadingEvents}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden max-h-96 overflow-y-auto">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase sticky top-0">
							<tr>
								<th class="px-4 py-3 text-left">Time</th>
								<th class="px-4 py-3 text-left">Resource</th>
								<th class="px-4 py-3 text-left">Type</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Reason</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each events as ev}
								<tr>
									<td class="px-4 py-3 text-xs text-gray-500 whitespace-nowrap">{formatDate(ev.Timestamp)}</td>
									<td class="px-4 py-3 text-xs font-medium">{ev.LogicalResourceId}</td>
									<td class="px-4 py-3 text-xs text-gray-500">{ev.ResourceType}</td>
									<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(ev.ResourceStatus)}-100 text-${statusColor(ev.ResourceStatus)}-700`}>{ev.ResourceStatus}</span></td>
									<td class="px-4 py-3 text-xs text-gray-500 truncate max-w-xs">{ev.ResourceStatusReason ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
					{#if events.length === 0}<div class="text-center py-8 text-gray-500">No events</div>{/if}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'template'}
			{#if loadingTemplate}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<pre class="bg-gray-900 text-green-400 rounded-xl p-4 text-xs overflow-auto max-h-96">{template || 'No template available'}</pre>
			{/if}
		{/if}

	{:else}
		<!-- Stack List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search stacks..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredStacks.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Layers class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No stacks found</p>
				<p class="text-sm mt-1">Create a CloudFormation stack to get started</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Stack Name</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Description</th>
							<th class="px-4 py-3 text-left">Created</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredStacks as stack}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectStack(stack)} class="text-orange-600 dark:text-orange-400 hover:underline font-medium">{stack.StackName}</button>
								</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(stack.StackStatus)}-100 dark:bg-${statusColor(stack.StackStatus)}-900/30 text-${statusColor(stack.StackStatus)}-700 dark:text-${statusColor(stack.StackStatus)}-400`}>
										{stack.StackStatus}
									</span>
								</td>
								<td class="px-4 py-3 text-gray-500 text-xs truncate max-w-xs">{stack.Description ?? '-'}</td>
								<td class="px-4 py-3 text-gray-500 text-xs">{formatDate(stack.CreationTime)}</td>
								<td class="px-4 py-3">
									<button onclick={() => deleteStack(stack.StackName ?? '')} disabled={deletingStack === stack.StackName} class="text-red-500 hover:text-red-700 p-1">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Stack Modal -->
{#if showCreateStack}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-2xl p-6 space-y-4 max-h-[90vh] overflow-y-auto">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Stack</h2>
			<div>
				<label for="stack-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Stack Name</label>
				<input id="stack-name" bind:value={newStackName} type="text" placeholder="my-stack" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="template-body" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Template Body (JSON/YAML)</label>
				<textarea id="template-body" bind:value={newTemplateBody} rows={10} placeholder="Paste your CloudFormation template here (JSON or YAML)" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			{#if validationResult}
				<div class={`text-sm p-3 rounded-lg ${validationResult.startsWith('Valid') ? 'bg-green-50 text-green-700 dark:bg-green-900/20 dark:text-green-400' : 'bg-red-50 text-red-700 dark:bg-red-900/20 dark:text-red-400'}`}>
					{validationResult}
				</div>
			{/if}
			<div>
				<label for="parameters" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Parameters (JSON array, optional)</label>
				<textarea id="parameters" bind:value={newParameters} rows={3} placeholder="JSON array of parameters, e.g. ParameterKey/ParameterValue pairs" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={validateTemplate} disabled={validatingTemplate || !newTemplateBody.trim()} class="px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50">
					{validatingTemplate ? 'Validating...' : 'Validate'}
				</button>
				<div class="flex-1"></div>
				<button onclick={() => { showCreateStack = false; validationResult = ''; }} class="px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createStack} disabled={creatingStack || !newStackName.trim() || !newTemplateBody.trim()} class="px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700 disabled:opacity-50">
					{creatingStack ? 'Creating...' : 'Create Stack'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Update Stack Modal -->
{#if showUpdateStack && selectedStack}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-2xl p-6 space-y-4 max-h-[90vh] overflow-y-auto">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Update Stack: {selectedStack.StackName}</h2>
			<div>
				<label for="update-template-body" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">New Template Body</label>
				<textarea id="update-template-body" bind:value={updateTemplateBody} rows={12} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showUpdateStack = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={updateStack} disabled={updatingStack || !updateTemplateBody.trim()} class="flex-1 px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700 disabled:opacity-50">
					{updatingStack ? 'Updating...' : 'Update Stack'}
				</button>
			</div>
		</div>
	</div>
{/if}
