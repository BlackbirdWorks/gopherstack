<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getAutoScalingClient } from '$lib/aws-client';
	import {
		DescribeAutoScalingGroupsCommand,
		CreateAutoScalingGroupCommand,
		DeleteAutoScalingGroupCommand,
		UpdateAutoScalingGroupCommand,
		SetDesiredCapacityCommand,
		DescribePoliciesCommand,
		PutScalingPolicyCommand,
		DeletePolicyCommand,
		DescribeScalingActivitiesCommand,
		type AutoScalingGroup,
		type ScalingPolicy,
		type Activity
	} from '@aws-sdk/client-auto-scaling';
	import { toast } from 'svelte-sonner';
	import { ZoomIn, Search, RefreshCw, Plus, Trash2, ChevronRight, TrendingUp, Activity as ActivityIcon, Settings } from 'lucide-svelte';

	const asg = getAutoScalingClient();

	let loading = $state(false);
	let groups = $state<AutoScalingGroup[]>([]);
	let selectedGroup = $state<AutoScalingGroup | null>(null);
	let activeTab = $state<'overview' | 'policies' | 'activity'>('overview');
	let searchQuery = $state('');

	// Policies & Activities
	let policies = $state<ScalingPolicy[]>([]);
	let loadingPolicies = $state(false);
	let activities = $state<Activity[]>([]);
	let loadingActivities = $state(false);

	// Create ASG
	let showCreateGroup = $state(false);
	let creatingGroup = $state(false);
	let newGroupName = $state('');
	let newLaunchTemplate = $state('');
	let newLaunchTemplateVersion = $state('$Latest');
	let newMinSize = $state(1);
	let newMaxSize = $state(3);
	let newDesiredCapacity = $state(1);
	let newAZs = $state('us-east-1a');

	// Set Desired Capacity
	let showSetCapacity = $state(false);
	let settingCapacity = $state(false);
	let newDesired = $state(1);

	// Create Policy
	let showCreatePolicy = $state(false);
	let creatingPolicy = $state(false);
	let newPolicyName = $state('');
	let newPolicyType = $state<'SimpleScaling' | 'TargetTrackingScaling'>('TargetTrackingScaling');
	let newTargetValue = $state(50);
	let newMetricType = $state('ASGAverageCPUUtilization');

	const filteredGroups = $derived(
		groups.filter((g) => !searchQuery || (g.AutoScalingGroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const statusColor = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status === 'InService') return 'green';
		if (status.includes('Pend') || status.includes('Warm')) return 'yellow';
		if (status.includes('Term')) return 'red';
		return 'blue';
	};

	async function loadGroups() {
		loading = true;
		try {
			const resp = await asg.send(new DescribeAutoScalingGroupsCommand({ MaxRecords: 50 }));
			groups = resp.AutoScalingGroups ?? [];
		} catch (e) {
			toast.error('Failed to load groups: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function selectGroup(group: AutoScalingGroup) {
		selectedGroup = group;
		activeTab = 'overview';
		newDesired = group.DesiredCapacity ?? 1;
	}

	async function handleTabChange(tab: 'overview' | 'policies' | 'activity') {
		activeTab = tab;
		if (!selectedGroup) return;
		const name = selectedGroup.AutoScalingGroupName ?? '';
		if (tab === 'policies' && policies.length === 0) await loadPolicies(name);
		if (tab === 'activity' && activities.length === 0) await loadActivities(name);
	}

	async function loadPolicies(groupName: string) {
		loadingPolicies = true;
		try {
			const resp = await asg.send(new DescribePoliciesCommand({ AutoScalingGroupName: groupName }));
			policies = resp.ScalingPolicies ?? [];
		} catch (e) {
			toast.error('Failed to load policies: ' + String(e));
		} finally {
			loadingPolicies = false;
		}
	}

	async function loadActivities(groupName: string) {
		loadingActivities = true;
		try {
			const resp = await asg.send(new DescribeScalingActivitiesCommand({ AutoScalingGroupName: groupName }));
			activities = resp.Activities ?? [];
		} catch (e) {
			toast.error('Failed to load activities: ' + String(e));
		} finally {
			loadingActivities = false;
		}
	}

	async function createGroup() {
		if (!newGroupName.trim()) return;
		creatingGroup = true;
		try {
			await asg.send(new CreateAutoScalingGroupCommand({
				AutoScalingGroupName: newGroupName.trim(),
				LaunchTemplate: { LaunchTemplateName: newLaunchTemplate.trim(), Version: newLaunchTemplateVersion },
				MinSize: newMinSize,
				MaxSize: newMaxSize,
				DesiredCapacity: newDesiredCapacity,
				AvailabilityZones: newAZs.split(',').map((s) => s.trim()).filter(Boolean)
			}));
			toast.success(`Auto Scaling Group "${newGroupName}" created`);
			showCreateGroup = false;
			resetCreateForm();
			await loadGroups();
		} catch (e) {
			toast.error('Failed to create group: ' + String(e));
		} finally {
			creatingGroup = false;
		}
	}

	function resetCreateForm() {
		newGroupName = '';
		newLaunchTemplate = '';
		newLaunchTemplateVersion = '$Latest';
		newMinSize = 1;
		newMaxSize = 3;
		newDesiredCapacity = 1;
		newAZs = 'us-east-1a';
	}

	async function deleteGroup(name: string) {
		if (!await confirmDestructive(`Delete Auto Scaling Group "${name}"?`)) return;
		try {
			await asg.send(new DeleteAutoScalingGroupCommand({ AutoScalingGroupName: name, ForceDelete: true }));
			toast.success(`Group "${name}" deleted`);
			if (selectedGroup?.AutoScalingGroupName === name) selectedGroup = null;
			await loadGroups();
		} catch (e) {
			toast.error('Failed to delete group: ' + String(e));
		}
	}

	async function setDesiredCapacity() {
		if (!selectedGroup) return;
		settingCapacity = true;
		try {
			await asg.send(new SetDesiredCapacityCommand({
				AutoScalingGroupName: selectedGroup.AutoScalingGroupName ?? '',
				DesiredCapacity: newDesired
			}));
			toast.success(`Desired capacity set to ${newDesired}`);
			showSetCapacity = false;
			await loadGroups();
			// Refresh selected group data
			const updated = groups.find((g) => g.AutoScalingGroupName === selectedGroup?.AutoScalingGroupName);
			if (updated) selectedGroup = updated;
		} catch (e) {
			toast.error('Failed to set capacity: ' + String(e));
		} finally {
			settingCapacity = false;
		}
	}

	async function createPolicy() {
		if (!selectedGroup || !newPolicyName.trim()) return;
		creatingPolicy = true;
		try {
			await asg.send(new PutScalingPolicyCommand({
				AutoScalingGroupName: selectedGroup.AutoScalingGroupName ?? '',
				PolicyName: newPolicyName.trim(),
				PolicyType: newPolicyType,
				TargetTrackingConfiguration: newPolicyType === 'TargetTrackingScaling' ? {
					PredefinedMetricSpecification: { PredefinedMetricType: newMetricType as 'ASGAverageCPUUtilization' },
					TargetValue: newTargetValue
				} : undefined
			}));
			toast.success(`Policy "${newPolicyName}" created`);
			showCreatePolicy = false;
			newPolicyName = '';
			policies = [];
			await loadPolicies(selectedGroup.AutoScalingGroupName ?? '');
		} catch (e) {
			toast.error('Failed to create policy: ' + String(e));
		} finally {
			creatingPolicy = false;
		}
	}

	async function deletePolicy(groupName: string, policyName: string) {
		try {
			await asg.send(new DeletePolicyCommand({ AutoScalingGroupName: groupName, PolicyName: policyName }));
			toast.success(`Policy "${policyName}" deleted`);
			policies = policies.filter((p) => p.PolicyName !== policyName);
		} catch (e) {
			toast.error('Failed to delete policy: ' + String(e));
		}
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	onMount(loadGroups);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ZoomIn class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Auto Scaling Groups</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Manage application scaling</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={loadGroups} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateGroup = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Group
			</button>
		</div>
	</div>

	{#if selectedGroup}
		<!-- Group Detail -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedGroup = null; policies = []; activities = []; }} class="text-green-600 hover:underline">Auto Scaling Groups</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedGroup.AutoScalingGroupName}</span>
		</div>

		<!-- Stats row -->
		<div class="grid grid-cols-4 gap-4">
			{#each [
				{ label: 'Desired', value: selectedGroup.DesiredCapacity ?? 0, color: 'blue' },
				{ label: 'Min', value: selectedGroup.MinSize ?? 0, color: 'gray' },
				{ label: 'Max', value: selectedGroup.MaxSize ?? 0, color: 'gray' },
				{ label: 'Instances', value: (selectedGroup.Instances ?? []).length, color: 'green' }
			] as stat}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4 text-center">
					<div class={`text-3xl font-bold text-${stat.color}-600 dark:text-${stat.color}-400`}>{stat.value}</div>
					<div class="text-xs text-gray-500 dark:text-gray-400 mt-1">{stat.label}</div>
				</div>
			{/each}
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each ['overview', 'policies', 'activity'] as tab}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'policies' | 'activity')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-green-500 text-green-600 dark:text-green-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
				>
					{tab.charAt(0).toUpperCase() + tab.slice(1)}
				</button>
			{/each}
			<button onclick={() => { showSetCapacity = true; }} class="ml-auto px-4 py-2 text-sm text-green-600 hover:underline">Set Capacity</button>
			<button onclick={() => deleteGroup(selectedGroup?.AutoScalingGroupName ?? '')} class="px-4 py-2 text-sm text-red-500 hover:underline">Delete</button>
		</div>

		{#if activeTab === 'overview'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'ARN', value: selectedGroup.AutoScalingGroupARN },
					{ label: 'Launch Template', value: selectedGroup.LaunchTemplate?.LaunchTemplateName ?? (selectedGroup.LaunchConfigurationName ?? '-') },
					{ label: 'Availability Zones', value: (selectedGroup.AvailabilityZones ?? []).join(', ') },
					{ label: 'Health Check Type', value: selectedGroup.HealthCheckType },
					{ label: 'Default Cooldown', value: `${selectedGroup.DefaultCooldown}s` },
					{ label: 'Created', value: formatDate(selectedGroup.CreatedTime) }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
						<div class="text-xs text-gray-500 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1 truncate font-mono">{row.value}</div>
					</div>
				{/each}
			</div>

			{#if (selectedGroup.Instances ?? []).length > 0}
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Instances</h3>
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Instance ID</th>
									<th class="px-4 py-3 text-left">AZ</th>
									<th class="px-4 py-3 text-left">Lifecycle State</th>
									<th class="px-4 py-3 text-left">Health</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each selectedGroup.Instances ?? [] as inst}
									<tr>
										<td class="px-4 py-3 font-mono text-xs">{inst.InstanceId}</td>
										<td class="px-4 py-3 text-xs text-gray-500">{inst.AvailabilityZone}</td>
										<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(inst.LifecycleState)}-100 text-${statusColor(inst.LifecycleState)}-700`}>{inst.LifecycleState}</span></td>
										<td class="px-4 py-3 text-xs">{inst.HealthStatus}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'policies'}
			<div class="flex justify-end mb-2">
				<button onclick={() => (showCreatePolicy = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm font-medium">
					<Plus class="w-4 h-4" /> Add Policy
				</button>
			</div>
			{#if loadingPolicies}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
			{:else if policies.length === 0}
				<div class="text-center py-12 text-gray-500"><TrendingUp class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No scaling policies</p></div>
			{:else}
				<div class="space-y-3">
					{#each policies as policy}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
							<div class="flex items-center justify-between">
								<div>
									<div class="font-medium">{policy.PolicyName}</div>
									<div class="text-xs text-gray-500 mt-1">{policy.PolicyType}</div>
								</div>
								<button onclick={() => deletePolicy(selectedGroup?.AutoScalingGroupName ?? '', policy.PolicyName ?? '')} class="text-red-500 hover:text-red-700 p-1">
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
							{#if policy.TargetTrackingConfiguration}
								<div class="mt-2 text-sm text-gray-600 dark:text-gray-400">
									Target: {policy.TargetTrackingConfiguration.TargetValue} | Metric: {policy.TargetTrackingConfiguration.PredefinedMetricSpecification?.PredefinedMetricType}
								</div>
							{/if}
						</div>
					{/each}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'activity'}
			{#if loadingActivities}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
			{:else if activities.length === 0}
				<div class="text-center py-12 text-gray-500"><ActivityIcon class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No recent activity</p></div>
			{:else}
				<div class="space-y-2">
					{#each activities as act}
						<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
							<div class="flex items-center justify-between">
								<span class="text-sm font-medium">{act.Description}</span>
								<span class={`px-2 py-0.5 rounded text-xs font-medium ${act.StatusCode === 'Successful' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}>{act.StatusCode}</span>
							</div>
							<div class="text-xs text-gray-500 mt-1">{formatDate(act.StartTime)} • {act.Progress ?? 0}% complete</div>
						</div>
					{/each}
				</div>
			{/if}
		{/if}

	{:else}
		<!-- Group List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search groups..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredGroups.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<ZoomIn class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No Auto Scaling Groups found</p>
				<p class="text-sm mt-1">Create a group to start scaling your application</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Group Name</th>
							<th class="px-4 py-3 text-left">Desired / Min / Max</th>
							<th class="px-4 py-3 text-left">Instances</th>
							<th class="px-4 py-3 text-left">AZs</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredGroups as group}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectGroup(group)} class="text-green-600 dark:text-green-400 hover:underline font-medium">{group.AutoScalingGroupName}</button>
								</td>
								<td class="px-4 py-3 text-gray-700 dark:text-gray-300 font-mono text-xs">
									{group.DesiredCapacity} / {group.MinSize} / {group.MaxSize}
								</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{(group.Instances ?? []).length}</td>
								<td class="px-4 py-3 text-gray-500 text-xs">{(group.AvailabilityZones ?? []).join(', ')}</td>
								<td class="px-4 py-3">
									<button onclick={() => deleteGroup(group.AutoScalingGroupName ?? '')} class="text-red-500 hover:text-red-700 p-1">
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

<!-- Create Group Modal -->
{#if showCreateGroup}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Auto Scaling Group</h2>
			<div>
				<label for="asg-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Group Name</label>
				<input id="asg-name" bind:value={newGroupName} type="text" placeholder="my-asg" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="launch-template" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Launch Template Name</label>
				<input id="launch-template" bind:value={newLaunchTemplate} type="text" placeholder="my-launch-template" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="grid grid-cols-3 gap-3">
				<div>
					<label for="min-size" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Min</label>
					<input id="min-size" bind:value={newMinSize} type="number" min="0" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="desired-cap" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Desired</label>
					<input id="desired-cap" bind:value={newDesiredCapacity} type="number" min="0" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="max-size" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Max</label>
					<input id="max-size" bind:value={newMaxSize} type="number" min="0" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div>
				<label for="availability-zones" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Availability Zones (comma-separated)</label>
				<input id="availability-zones" bind:value={newAZs} type="text" placeholder="us-east-1a,us-east-1b" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => { showCreateGroup = false; resetCreateForm(); }} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createGroup} disabled={creatingGroup || !newGroupName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
					{creatingGroup ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Set Desired Capacity Modal -->
{#if showSetCapacity && selectedGroup}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-sm p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Set Desired Capacity</h2>
			<p class="text-sm text-gray-500">Current: {selectedGroup.DesiredCapacity} | Range: {selectedGroup.MinSize} - {selectedGroup.MaxSize}</p>
			<div>
				<label for="new-desired" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">New Desired Capacity</label>
				<input id="new-desired" bind:value={newDesired} type="number" min={selectedGroup.MinSize} max={selectedGroup.MaxSize} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showSetCapacity = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={setDesiredCapacity} disabled={settingCapacity} class="flex-1 px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
					{settingCapacity ? 'Setting...' : 'Set'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Policy Modal -->
{#if showCreatePolicy && selectedGroup}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Add Scaling Policy</h2>
			<div>
				<label for="policy-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Policy Name</label>
				<input id="policy-name" bind:value={newPolicyName} type="text" placeholder="scale-on-cpu" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="policy-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Policy Type</label>
				<select id="policy-type" bind:value={newPolicyType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="TargetTrackingScaling">Target Tracking</option>
					<option value="SimpleScaling">Simple Scaling</option>
				</select>
			</div>
			{#if newPolicyType === 'TargetTrackingScaling'}
				<div>
					<label for="metric-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Metric</label>
					<select id="metric-type" bind:value={newMetricType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="ASGAverageCPUUtilization">Average CPU Utilization</option>
						<option value="ASGAverageNetworkIn">Average Network In</option>
						<option value="ASGAverageNetworkOut">Average Network Out</option>
						<option value="ALBRequestCountPerTarget">ALB Requests per Target</option>
					</select>
				</div>
				<div>
					<label for="target-value" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Target Value (%)</label>
					<input id="target-value" bind:value={newTargetValue} type="number" min="0" max="100" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			{/if}
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreatePolicy = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createPolicy} disabled={creatingPolicy || !newPolicyName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
					{creatingPolicy ? 'Creating...' : 'Add Policy'}
				</button>
			</div>
		</div>
	</div>
{/if}
