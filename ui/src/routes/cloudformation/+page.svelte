<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
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
		CreateChangeSetCommand,
		DescribeChangeSetCommand,
		ListChangeSetsCommand,
		ExecuteChangeSetCommand,
		DeleteChangeSetCommand,
		DetectStackDriftCommand,
		DescribeStackDriftDetectionStatusCommand,
		DescribeStackResourceDriftsCommand,
		CreateStackSetCommand,
		DeleteStackSetCommand,
		ListStackSetsCommand,
		GetStackPolicyCommand,
		SetStackPolicyCommand,
		type Stack,
		type StackResourceSummary,
		type StackEvent,
		type Change,
		type StackResourceDrift,
		type StackSetSummary,
		StackStatus
	} from '@aws-sdk/client-cloudformation';
	import { toast } from 'svelte-sonner';
	import {
		Layers,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		ChevronRight,
		FileCode,
		Activity,
		AlertCircle,
		GitCompare,
		ScanSearch,
		Server
	} from 'lucide-svelte';

	const cfn = regionalClient(getCloudFormationClient);

	// Every row carries the region its Describe/List call was made against.
	// Detail/action calls must build a client for THAT region -- in All mode
	// the same stack/StackSet name can legitimately exist in two different
	// regions.
	type Regioned<T> = T & { region: string };

	// Main view: 'stacks' | 'stacksets'
	let mainView = $state<'stacks' | 'stacksets'>('stacks');

	let loading = $state(false);
	let stacks = $state<Regioned<Stack>[]>([]);
	let selectedStack = $state<Regioned<Stack> | null>(null);
	let activeTab = $state<'overview' | 'resources' | 'events' | 'template' | 'changesets' | 'drift' | 'policy'>(
		'overview'
	);
	let searchQuery = $state('');

	// Stack policy editor
	let stackPolicy = $state('');
	let loadingPolicy = $state(false);
	let savingPolicy = $state(false);

	// Resources & Events
	let resources = $state<StackResourceSummary[]>([]);
	let loadingResources = $state(false);
	let events = $state<StackEvent[]>([]);
	let loadingEvents = $state(false);
	let template = $state('');
	let loadingTemplate = $state(false);

	// Change sets
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let changeSets = $state<any[]>([]);
	let loadingChangeSets = $state(false);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let selectedChangeSet = $state<any | null>(null);
	let changeSetChanges = $state<Change[]>([]);
	let showCreateChangeSet = $state(false);
	let newChangeSetName = $state('');
	let newChangeSetTemplate = $state('');
	let creatingChangeSet = $state(false);

	// Drift
	let driftDetectionID = $state('');
	let driftStatus = $state('');
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let driftResourceDrifts = $state<StackResourceDrift[]>([]);
	let detectingDrift = $state(false);
	let loadingDrift = $state(false);

	// StackSets
	let stackSets = $state<Regioned<StackSetSummary>[]>([]);
	let loadingStackSets = $state(false);
	let showCreateStackSet = $state(false);
	let newStackSetName = $state('');
	let newStackSetTemplate = $state('');
	let creatingStackSet = $state(false);

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
		stacks.filter(
			(s) => !searchQuery || (s.StackName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const statusColor = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status.includes('COMPLETE') && !status.includes('DELETE')) return 'green';
		if (status.includes('FAILED') || status.includes('ROLLBACK')) return 'red';
		if (status.includes('IN_PROGRESS')) return 'yellow';
		if (status.includes('DELETE')) return 'gray';
		return 'blue';
	};

	const changeActionColor = (action: string | undefined) => {
		if (action === 'Add') return 'green';
		if (action === 'Remove') return 'red';
		if (action === 'Modify') return 'yellow';
		return 'blue';
	};

	const driftStatusColor = (status: string | undefined) => {
		if (status === 'IN_SYNC') return 'green';
		if (status === 'MODIFIED' || status === 'DELETED') return 'red';
		if (status === 'NOT_CHECKED') return 'gray';
		return 'yellow';
	};

	async function loadStacks() {
		loading = true;
		try {
			const result = await multiRegionList(
				(region) => getCloudFormationClient(region).send(new DescribeStacksCommand({})),
				(r) => r.Stacks ?? []
			);
			stacks = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load stacks from ${result.errors.length} region(s)`);
		} catch (e) {
			toast.error('Failed to load stacks: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadStackSets() {
		loadingStackSets = true;
		try {
			const result = await multiRegionList(
				(region) => getCloudFormationClient(region).send(new ListStackSetsCommand({})),
				(r) => r.Summaries ?? []
			);
			stackSets = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load stack sets from ${result.errors.length} region(s)`);
		} catch (e) {
			toast.error('Failed to load stack sets: ' + String(e));
		} finally {
			loadingStackSets = false;
		}
	}

	async function selectStack(stack: Regioned<Stack>) {
		selectedStack = stack;
		activeTab = 'overview';
		await loadResources(stack.StackName ?? '', stack.region);
	}

	async function loadResources(stackName: string, region: string) {
		loadingResources = true;
		try {
			const resp = await getCloudFormationClient(region).send(new ListStackResourcesCommand({ StackName: stackName }));
			resources = resp.StackResourceSummaries ?? [];
		} catch (e) {
			toast.error('Failed to load resources: ' + String(e));
		} finally {
			loadingResources = false;
		}
	}

	async function loadEvents(stackName: string, region: string) {
		loadingEvents = true;
		try {
			const resp = await getCloudFormationClient(region).send(new DescribeStackEventsCommand({ StackName: stackName }));
			events = resp.StackEvents ?? [];
		} catch (e) {
			toast.error('Failed to load events: ' + String(e));
		} finally {
			loadingEvents = false;
		}
	}

	async function loadTemplate(stackName: string, region: string) {
		loadingTemplate = true;
		try {
			const resp = await getCloudFormationClient(region).send(new GetTemplateCommand({ StackName: stackName }));
			template = resp.TemplateBody ?? '';
		} catch (e) {
			toast.error('Failed to load template: ' + String(e));
		} finally {
			loadingTemplate = false;
		}
	}

	async function loadChangeSets(stackName: string, region: string) {
		loadingChangeSets = true;
		try {
			const resp = await getCloudFormationClient(region).send(new ListChangeSetsCommand({ StackName: stackName }));
			changeSets = resp.Summaries ?? [];
		} catch (e) {
			toast.error('Failed to load change sets: ' + String(e));
		} finally {
			loadingChangeSets = false;
		}
	}

	async function selectChangeSet(changeSetName: string) {
		if (!selectedStack) return;
		try {
			const resp = await getCloudFormationClient(selectedStack.region).send(
				new DescribeChangeSetCommand({
					StackName: selectedStack.StackName ?? '',
					ChangeSetName: changeSetName
				})
			);
			selectedChangeSet = resp;
			changeSetChanges = resp.Changes ?? [];
		} catch (e) {
			toast.error('Failed to load change set: ' + String(e));
		}
	}

	async function executeChangeSet(changeSetName: string) {
		if (!selectedStack) return;
		if (
			!await confirmDestructive({
				title: 'Execute Change Set',
				message: `Execute change set "${changeSetName}"? This will modify the stack.`
			})
		)
			return;
		try {
			await getCloudFormationClient(selectedStack.region).send(
				new ExecuteChangeSetCommand({
					StackName: selectedStack.StackName ?? '',
					ChangeSetName: changeSetName
				})
			);
			toast.success('Change set execution initiated');
			selectedChangeSet = null;
			await loadChangeSets(selectedStack.StackName ?? '', selectedStack.region);
			await loadStacks();
		} catch (e) {
			toast.error('Failed to execute change set: ' + String(e));
		}
	}

	async function deleteChangeSet(changeSetName: string) {
		if (!selectedStack) return;
		try {
			await getCloudFormationClient(selectedStack.region).send(
				new DeleteChangeSetCommand({
					StackName: selectedStack.StackName ?? '',
					ChangeSetName: changeSetName
				})
			);
			toast.success(`Change set "${changeSetName}" deleted`);
			if (selectedChangeSet?.ChangeSetName === changeSetName) selectedChangeSet = null;
			await loadChangeSets(selectedStack.StackName ?? '', selectedStack.region);
		} catch (e) {
			toast.error('Failed to delete change set: ' + String(e));
		}
	}

	async function createChangeSet() {
		if (!selectedStack || !newChangeSetName.trim() || !newChangeSetTemplate.trim()) return;
		creatingChangeSet = true;
		try {
			await getCloudFormationClient(selectedStack.region).send(
				new CreateChangeSetCommand({
					StackName: selectedStack.StackName ?? '',
					ChangeSetName: newChangeSetName.trim(),
					TemplateBody: newChangeSetTemplate.trim()
				})
			);
			toast.success(`Change set "${newChangeSetName}" created`);
			showCreateChangeSet = false;
			newChangeSetName = '';
			newChangeSetTemplate = '';
			await loadChangeSets(selectedStack.StackName ?? '', selectedStack.region);
		} catch (e) {
			toast.error('Failed to create change set: ' + String(e));
		} finally {
			creatingChangeSet = false;
		}
	}

	async function detectDrift() {
		if (!selectedStack) return;
		detectingDrift = true;
		driftStatus = '';
		driftResourceDrifts = [];
		try {
			const client = getCloudFormationClient(selectedStack.region);
			const resp = await client.send(
				new DetectStackDriftCommand({ StackName: selectedStack.StackName ?? '' })
			);
			driftDetectionID = resp.StackDriftDetectionId ?? '';
			// Poll for completion
			let attempts = 0;
			const poll = async () => {
				attempts++;
				const statusResp = await client.send(
					new DescribeStackDriftDetectionStatusCommand({
						StackDriftDetectionId: driftDetectionID
					})
				);
				driftStatus = statusResp.DetectionStatus ?? '';
				if (
					statusResp.DetectionStatus === 'DETECTION_COMPLETE' ||
					statusResp.DetectionStatus === 'DETECTION_FAILED' ||
					attempts > 10
				) {
					detectingDrift = false;
					await loadDriftResults();
				} else {
					setTimeout(poll, 1000);
				}
			};
			await poll();
		} catch (e) {
			toast.error('Failed to detect drift: ' + String(e));
			detectingDrift = false;
		}
	}

	async function loadDriftResults() {
		if (!selectedStack) return;
		loadingDrift = true;
		try {
			const resp = await getCloudFormationClient(selectedStack.region).send(
				new DescribeStackResourceDriftsCommand({ StackName: selectedStack.StackName ?? '' })
			);
			driftResourceDrifts = resp.StackResourceDrifts ?? [];
		} catch (e) {
			toast.error('Failed to load drift results: ' + String(e));
		} finally {
			loadingDrift = false;
		}
	}

	async function handleTabChange(
		tab: 'overview' | 'resources' | 'events' | 'template' | 'changesets' | 'drift' | 'policy'
	) {
		activeTab = tab;
		if (!selectedStack) return;
		const name = selectedStack.StackName ?? '';
		const region = selectedStack.region;
		if (tab === 'events' && events.length === 0) await loadEvents(name, region);
		if (tab === 'template' && !template) await loadTemplate(name, region);
		if (tab === 'changesets') await loadChangeSets(name, region);
		if (tab === 'drift') await loadDriftResults();
		if (tab === 'policy') await loadStackPolicy(name, region);
	}

	async function loadStackPolicy(stackName: string, region: string) {
		loadingPolicy = true;
		stackPolicy = '';
		try {
			const resp = await getCloudFormationClient(region).send(new GetStackPolicyCommand({ StackName: stackName }));
			if (resp.StackPolicyBody) {
				try {
					stackPolicy = JSON.stringify(JSON.parse(resp.StackPolicyBody), null, 2);
				} catch {
					stackPolicy = resp.StackPolicyBody;
				}
			}
		} catch (e) {
			toast.error('Failed to load stack policy: ' + String(e));
		} finally {
			loadingPolicy = false;
		}
	}

	async function saveStackPolicy() {
		if (!selectedStack) return;
		let body = stackPolicy;
		try {
			body = JSON.stringify(JSON.parse(stackPolicy));
		} catch {
			toast.error('Stack policy is not valid JSON');
			return;
		}
		savingPolicy = true;
		try {
			await getCloudFormationClient(selectedStack.region).send(new SetStackPolicyCommand({ StackName: selectedStack.StackName ?? '', StackPolicyBody: body }));
			toast.success('Stack policy saved');
		} catch (e) {
			toast.error('Failed to save stack policy: ' + String(e));
		} finally {
			savingPolicy = false;
		}
	}

	async function createStack() {
		if (!newStackName.trim() || !newTemplateBody.trim()) return;
		creatingStack = true;
		try {
			const params: { StackName: string; TemplateBody: string; Parameters?: { ParameterKey: string; ParameterValue: string }[] } = {
				StackName: newStackName.trim(),
				TemplateBody: newTemplateBody.trim()
			};
			if (newParameters.trim()) {
				try {
					params.Parameters = JSON.parse(newParameters);
				} catch {
					// Ignore invalid JSON and submit without parameters.
				}
			}
			await cfn().send(new CreateStackCommand(params));
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
			await getCloudFormationClient(selectedStack.region).send(
				new UpdateStackCommand({
					StackName: selectedStack.StackName ?? '',
					TemplateBody: updateTemplateBody.trim()
				})
			);
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

	async function deleteStack(stack: Regioned<Stack> | null) {
		const name = stack?.StackName;
		if (
			!name ||
			!await confirmDestructive({
				title: 'Delete Stack',
				message: `Delete stack "${name}"? All provisioned resources will be destroyed.`
			})
		)
			return;
		deletingStack = name;
		try {
			await getCloudFormationClient(stack.region).send(new DeleteStackCommand({ StackName: name }));
			toast.success(`Stack "${name}" deletion initiated`);
			if (selectedStack && selectedStack.StackName === name && selectedStack.region === stack.region) selectedStack = null;
			await loadStacks();
		} catch (e) {
			toast.error('Failed to delete stack: ' + String(e));
		} finally {
			deletingStack = null;
		}
	}

	async function createStackSet() {
		if (!newStackSetName.trim() || !newStackSetTemplate.trim()) return;
		creatingStackSet = true;
		try {
			await cfn().send(
				new CreateStackSetCommand({
					StackSetName: newStackSetName.trim(),
					TemplateBody: newStackSetTemplate.trim()
				})
			);
			toast.success(`StackSet "${newStackSetName}" created`);
			showCreateStackSet = false;
			newStackSetName = '';
			newStackSetTemplate = '';
			await loadStackSets();
		} catch (e) {
			toast.error('Failed to create stack set: ' + String(e));
		} finally {
			creatingStackSet = false;
		}
	}

	async function deleteStackSet(stackSet: Regioned<StackSetSummary>) {
		const name = stackSet.StackSetName;
		if (
			!await confirmDestructive({
				title: 'Delete StackSet',
				message: `Delete StackSet "${name}"?`
			})
		)
			return;
		try {
			await getCloudFormationClient(stackSet.region).send(new DeleteStackSetCommand({ StackSetName: name }));
			toast.success(`StackSet "${name}" deleted`);
			await loadStackSets();
		} catch (e) {
			toast.error('Failed to delete stack set: ' + String(e));
		}
	}

	async function validateTemplate() {
		if (!newTemplateBody.trim()) return;
		validatingTemplate = true;
		validationResult = '';
		try {
			const resp = await cfn().send(
				new ValidateTemplateCommand({ TemplateBody: newTemplateBody.trim() })
			);
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

	// selectedStack and all of its per-tab data (resources, events, template,
	// change sets, drift, policy) reference a stack name that is only
	// unique within the region it was fetched from — clear the whole
	// selection chain and fall back to the list. `mainView` is read via
	// untrack() because the view-toggle buttons also write it — without
	// untrack, switching views would re-trigger this region effect and
	// double-fetch.
	onRegionChange(() => {
		selectedStack = null;
		activeTab = 'overview';
		resources = [];
		events = [];
		template = '';
		changeSets = [];
		selectedChangeSet = null;
		changeSetChanges = [];
		driftDetectionID = '';
		driftStatus = '';
		driftResourceDrifts = [];
		stackPolicy = '';
		stackSets = [];
		loadStacks();
		if (untrack(() => mainView) === 'stacksets') loadStackSets();
	});
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
			<!-- View toggle -->
			<div class="flex rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden text-sm">
				<button
					onclick={() => { mainView = 'stacks'; selectedStack = null; }}
					class={`px-3 py-2 flex items-center gap-1 ${mainView === 'stacks' ? 'bg-orange-600 text-white' : 'text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
				>
					<Layers class="w-4 h-4" /> Stacks
				</button>
				<button
					onclick={() => { mainView = 'stacksets'; loadStackSets(); }}
					class={`px-3 py-2 flex items-center gap-1 ${mainView === 'stacksets' ? 'bg-orange-600 text-white' : 'text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
				>
					<Server class="w-4 h-4" /> StackSets
				</button>
			</div>
			{#if mainView === 'stacks'}
				<button onclick={loadStacks} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
					<RefreshCw class="w-4 h-4" /> Refresh
				</button>
				<button onclick={() => (showCreateStack = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Create Stack
				</button>
			{:else}
				<button onclick={loadStackSets} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
					<RefreshCw class="w-4 h-4" /> Refresh
				</button>
				<button onclick={() => (showCreateStackSet = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Create StackSet
				</button>
			{/if}
		</div>
	</div>

	<!-- StackSets view -->
	{#if mainView === 'stacksets'}
		{#if loadingStackSets}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
		{:else if stackSets.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Server class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No StackSets found</p>
				<p class="text-sm mt-1">Create a StackSet to manage stacks across accounts and regions</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">StackSet Name</th>
							<th class="px-4 py-3 text-left">Region</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Description</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each stackSets as ss}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3 font-medium">{ss.StackSetName}</td>
								<td class="px-4 py-3"><RegionChip region={ss.region} /></td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(ss.Status)}-100 text-${statusColor(ss.Status)}-700`}>
										{ss.Status ?? '-'}
									</span>
								</td>
								<td class="px-4 py-3 text-gray-500 text-xs">{ss.Description ?? '-'}</td>
								<td class="px-4 py-3">
									<button onclick={() => deleteStackSet(ss)} class="text-red-500 hover:text-red-700 p-1">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

	<!-- Stacks view -->
	{:else if selectedStack}
		<!-- Stack Detail -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedStack = null; resources = []; events = []; template = ''; changeSets = []; driftResourceDrifts = []; }} class="text-orange-600 hover:underline">Stacks</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedStack.StackName}</span>
			<RegionChip region={selectedStack.region} />
			<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedStack.StackStatus)}-100 dark:bg-${statusColor(selectedStack.StackStatus)}-900/30 text-${statusColor(selectedStack.StackStatus)}-700 dark:text-${statusColor(selectedStack.StackStatus)}-400`}>
				{selectedStack.StackStatus}
			</span>
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each ['overview', 'resources', 'events', 'template', 'changesets', 'drift', 'policy'] as tab}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'resources' | 'events' | 'template' | 'changesets' | 'drift' | 'policy')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors flex items-center gap-1 ${activeTab === tab ? 'border-orange-500 text-orange-600 dark:text-orange-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}`}
				>
					{#if tab === 'changesets'}<GitCompare class="w-3.5 h-3.5" />{/if}
					{#if tab === 'drift'}<ScanSearch class="w-3.5 h-3.5" />{/if}
					{tab === 'changesets' ? 'Change Sets' : tab === 'drift' ? 'Drift' : tab === 'policy' ? 'Stack Policy' : tab.charAt(0).toUpperCase() + tab.slice(1)}
				</button>
			{/each}
			<button onclick={() => { showUpdateStack = true; updateTemplateBody = template; }} class="ml-auto px-4 py-2 text-sm text-orange-600 hover:underline">Update Stack</button>
			<button onclick={() => deleteStack(selectedStack)} class="px-4 py-2 text-sm text-red-500 hover:underline">Delete</button>
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

		<!-- Change Sets Tab -->
		{#if activeTab === 'changesets'}
			<div class="flex items-center justify-between mb-2">
				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Change Sets</h3>
				<button onclick={() => (showCreateChangeSet = true)} class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-orange-600 text-white text-xs font-medium hover:bg-orange-700">
					<Plus class="w-3.5 h-3.5" /> New Change Set
				</button>
			</div>
			{#if loadingChangeSets}
				<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
					<!-- Change set list -->
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-2 text-left">Name</th>
									<th class="px-4 py-2 text-left">Status</th>
									<th class="px-4 py-2 text-left">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each changeSets as cs}
									<tr class={`hover:bg-gray-50 dark:hover:bg-gray-800/50 cursor-pointer ${selectedChangeSet?.ChangeSetName === cs.ChangeSetName ? 'bg-orange-50 dark:bg-orange-900/10' : ''}`}>
										<td class="px-4 py-2">
											<button onclick={() => selectChangeSet(cs.ChangeSetName ?? '')} class="text-orange-600 hover:underline text-xs font-medium">{cs.ChangeSetName}</button>
										</td>
										<td class="px-4 py-2">
											<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(cs.Status)}-100 text-${statusColor(cs.Status)}-700`}>{cs.Status}</span>
										</td>
										<td class="px-4 py-2 flex gap-1">
											{#if cs.Status === 'CREATE_COMPLETE'}
												<button onclick={() => executeChangeSet(cs.ChangeSetName ?? '')} class="px-2 py-0.5 bg-green-600 text-white rounded text-xs hover:bg-green-700">Execute</button>
											{/if}
											<button onclick={() => deleteChangeSet(cs.ChangeSetName ?? '')} class="text-red-500 hover:text-red-700 p-0.5"><Trash2 class="w-3.5 h-3.5" /></button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
						{#if changeSets.length === 0}<div class="text-center py-6 text-gray-500 text-sm">No change sets</div>{/if}
					</div>

					<!-- Change set diff viewer -->
					{#if selectedChangeSet}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-3">
							<div class="flex items-center justify-between">
								<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300 flex items-center gap-1">
									<GitCompare class="w-4 h-4 text-orange-500" />
									{selectedChangeSet.ChangeSetName}
								</h4>
								<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedChangeSet.Status)}-100 text-${statusColor(selectedChangeSet.Status)}-700`}>{selectedChangeSet.Status}</span>
							</div>
							{#if selectedChangeSet.Description}
								<p class="text-xs text-gray-500">{selectedChangeSet.Description}</p>
							{/if}
							<div class="space-y-2">
								<p class="text-xs font-medium text-gray-600 dark:text-gray-400">{changeSetChanges.length} change{changeSetChanges.length !== 1 ? 's' : ''}</p>
								{#each changeSetChanges as change}
									<div class="flex items-center gap-3 p-2 rounded-lg border border-gray-100 dark:border-gray-800 bg-gray-50 dark:bg-gray-800/50">
										<span class={`px-2 py-0.5 rounded text-xs font-bold bg-${changeActionColor(change.ResourceChange?.Action)}-100 text-${changeActionColor(change.ResourceChange?.Action)}-700 min-w-[56px] text-center`}>
											{change.ResourceChange?.Action ?? '-'}
										</span>
										<div class="flex-1 min-w-0">
											<p class="text-xs font-medium text-gray-900 dark:text-white truncate">{change.ResourceChange?.LogicalResourceId}</p>
											<p class="text-xs text-gray-500 truncate">{change.ResourceChange?.ResourceType}</p>
										</div>
									</div>
								{/each}
								{#if changeSetChanges.length === 0}
									<p class="text-xs text-gray-500 text-center py-4">No changes</p>
								{/if}
							</div>
						</div>
					{/if}
				</div>
			{/if}
		{/if}

		<!-- Drift Tab -->
		{#if activeTab === 'drift'}
			<div class="space-y-4">
				<div class="flex items-center justify-between">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 flex items-center gap-2">
						<ScanSearch class="w-4 h-4 text-orange-500" /> Drift Detection
					</h3>
					<button
						onclick={detectDrift}
						disabled={detectingDrift}
						class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-orange-600 text-white text-xs font-medium hover:bg-orange-700 disabled:opacity-50"
					>
						{#if detectingDrift}
							<div class="animate-spin w-3 h-3 border-2 border-white border-t-transparent rounded-full"></div>
							Detecting...
						{:else}
							<ScanSearch class="w-3.5 h-3.5" /> Detect Drift
						{/if}
					</button>
				</div>

				{#if driftStatus}
					<div class="flex items-center gap-2 text-sm">
						<span class="text-gray-500">Detection status:</span>
						<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(driftStatus)}-100 text-${statusColor(driftStatus)}-700`}>{driftStatus}</span>
					</div>
				{/if}

				{#if loadingDrift}
					<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
				{:else if driftResourceDrifts.length > 0}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Logical ID</th>
									<th class="px-4 py-3 text-left">Type</th>
									<th class="px-4 py-3 text-left">Drift Status</th>
									<th class="px-4 py-3 text-left">Physical ID</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each driftResourceDrifts as drift}
									<tr>
										<td class="px-4 py-3 font-medium text-xs">{drift.LogicalResourceId}</td>
										<td class="px-4 py-3 text-xs text-gray-500">{drift.ResourceType}</td>
										<td class="px-4 py-3">
											<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${driftStatusColor(drift.StackResourceDriftStatus)}-100 text-${driftStatusColor(drift.StackResourceDriftStatus)}-700`}>
												{drift.StackResourceDriftStatus}
											</span>
										</td>
										<td class="px-4 py-3 text-xs font-mono text-gray-500 truncate max-w-xs">{drift.PhysicalResourceId ?? '-'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{:else}
					<div class="text-center py-12 text-gray-500 dark:text-gray-400">
						<ScanSearch class="w-10 h-10 mx-auto mb-3 opacity-40" />
						<p class="text-sm">Run drift detection to compare stack resources against their expected configuration</p>
					</div>
				{/if}
			</div>
		{/if}

		{#if activeTab === 'policy'}
			<div class="space-y-3">
				<div class="flex items-center justify-between">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Stack Policy (JSON)</h3>
					<button onclick={saveStackPolicy} disabled={savingPolicy || loadingPolicy} class="px-3 py-1.5 rounded-lg bg-orange-600 text-white text-xs font-medium hover:bg-orange-700 disabled:opacity-50">{savingPolicy ? 'Saving...' : 'Save Policy'}</button>
				</div>
				{#if loadingPolicy}
					<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-4 border-orange-600 border-t-transparent rounded-full"></div></div>
				{:else}
					<p class="text-xs text-gray-500 dark:text-gray-400">A stack policy protects resources from unintended updates. Define Allow/Deny statements for Update:* actions.</p>
					<textarea bind:value={stackPolicy} rows="16" placeholder={'{\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": "Update:*",\n      "Principal": "*",\n      "Resource": "*"\n    }\n  ]\n}'} class="w-full px-3 py-2 font-mono text-xs rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-gray-900 dark:text-white"></textarea>
				{/if}
			</div>
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
							<th class="px-4 py-3 text-left">Region</th>
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
								<td class="px-4 py-3"><RegionChip region={stack.region} /></td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(stack.StackStatus)}-100 dark:bg-${statusColor(stack.StackStatus)}-900/30 text-${statusColor(stack.StackStatus)}-700 dark:text-${statusColor(stack.StackStatus)}-400`}>
										{stack.StackStatus}
									</span>
								</td>
								<td class="px-4 py-3 text-gray-500 text-xs truncate max-w-xs">{stack.Description ?? '-'}</td>
								<td class="px-4 py-3 text-gray-500 text-xs">{formatDate(stack.CreationTime)}</td>
								<td class="px-4 py-3">
									<button onclick={() => deleteStack(stack)} disabled={deletingStack === stack.StackName} class="text-red-500 hover:text-red-700 p-1">
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
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Stack</h2>
				<WriteRegionHint />
			</div>
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

<!-- Create Change Set Modal -->
{#if showCreateChangeSet && selectedStack}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-2xl p-6 space-y-4 max-h-[90vh] overflow-y-auto">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2">
				<GitCompare class="w-5 h-5 text-orange-500" /> Create Change Set
			</h2>
			<div>
				<label for="changeset-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Change Set Name</label>
				<input id="changeset-name" bind:value={newChangeSetName} type="text" placeholder="my-changeset" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="changeset-template" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">New Template Body</label>
				<textarea id="changeset-template" bind:value={newChangeSetTemplate} rows={10} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateChangeSet = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createChangeSet} disabled={creatingChangeSet || !newChangeSetName.trim() || !newChangeSetTemplate.trim()} class="flex-1 px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700 disabled:opacity-50">
					{creatingChangeSet ? 'Creating...' : 'Create Change Set'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create StackSet Modal -->
{#if showCreateStackSet}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-2xl p-6 space-y-4 max-h-[90vh] overflow-y-auto">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2">
					<Server class="w-5 h-5 text-orange-500" /> Create StackSet
				</h2>
				<WriteRegionHint />
			</div>
			<div>
				<label for="stackset-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">StackSet Name</label>
				<input id="stackset-name" bind:value={newStackSetName} type="text" placeholder="my-stackset" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="stackset-template" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Template Body</label>
				<textarea id="stackset-template" bind:value={newStackSetTemplate} rows={10} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateStackSet = false)} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createStackSet} disabled={creatingStackSet || !newStackSetName.trim() || !newStackSetTemplate.trim()} class="flex-1 px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700 disabled:opacity-50">
					{creatingStackSet ? 'Creating...' : 'Create StackSet'}
				</button>
			</div>
		</div>
	</div>
{/if}
