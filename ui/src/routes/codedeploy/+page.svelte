<script lang="ts">
	import { onMount } from 'svelte';
	import { getCodeDeployClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		GetApplicationCommand,
		ListDeploymentGroupsCommand,
		GetDeploymentGroupCommand,
		ListDeploymentsCommand,
		GetDeploymentCommand,
		ListDeploymentInstancesCommand,
		BatchGetDeploymentInstancesCommand,
		CreateDeploymentCommand,
		StopDeploymentCommand,
		ListDeploymentConfigsCommand,
		GetDeploymentConfigCommand,
		CreateDeploymentConfigCommand,
		ListOnPremisesInstancesCommand,
		GetOnPremisesInstanceCommand,
		RegisterOnPremisesInstanceCommand,
		DeregisterOnPremisesInstanceCommand,
		type ApplicationInfo,
		type DeploymentInfo,
		type DeploymentGroupInfo,
		type InstanceSummary,
		type DeploymentConfigInfo,
		type InstanceInfo
	} from '@aws-sdk/client-codedeploy';
	import { toast } from 'svelte-sonner';
	import { Package, Search, RefreshCw, Plus, XCircle, ChevronRight, Play, CheckCircle, AlertCircle, Server, Settings } from 'lucide-svelte';

	const codedeploy = getCodeDeployClient();

	let loading = $state(false);
	let appNames = $state<string[]>([]);
	let selectedApp = $state<ApplicationInfo | null>(null);
	let activeTab = $state<'groups' | 'deployments'>('groups');
	let searchQuery = $state('');

	// Deployment Groups
	let deploymentGroups = $state<string[]>([]);
	let loadingGroups = $state(false);

	// Deployments
	let deployments = $state<string[]>([]);
	let deploymentDetails = $state<DeploymentInfo[]>([]);
	let loadingDeployments = $state(false);
	let selectedDeployment = $state<DeploymentInfo | null>(null);
	let deploymentInstances = $state<InstanceSummary[]>([]);
	let loadingInstances = $state(false);

	// Deployment Group detail
	let selectedGroup = $state<DeploymentGroupInfo | null>(null);
	let loadingGroupDetail = $state(false);

	// Create Deployment
	let showCreateDeployment = $state(false);
	let creatingDeployment = $state(false);
	let deployGroup = $state('');
	let deployRevisionType = $state<'S3' | 'GitHub'>('S3');
	let deployS3Bucket = $state('');
	let deployS3Key = $state('');
	let deployGitHubUser = $state('');
	let deployGitHubRepo = $state('');
	let deployGitHubCommit = $state('');

	// Main view tabs
	type MainView = 'applications' | 'configs' | 'onprem';
	let mainView = $state<MainView>('applications');

	// Deployment Configs
	let deployConfigs = $state<DeploymentConfigInfo[]>([]);
	let loadingConfigs = $state(false);
	let showCreateConfig = $state(false);
	let newConfigName = $state('');
	let newConfigPlatform = $state('Server');
	let creatingConfig = $state(false);

	// On-prem Instances
	let onPremInstances = $state<InstanceInfo[]>([]);
	let loadingOnPrem = $state(false);
	let showRegisterInstance = $state(false);
	let newInstanceName = $state('');
	let newInstanceIamArn = $state('');
	let registeringInstance = $state(false);

	const filteredApps = $derived(
		appNames.filter((n) => !searchQuery || n.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const statusColor = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status === 'Succeeded') return 'green';
		if (status === 'Failed' || status === 'Stopped') return 'red';
		if (status === 'InProgress') return 'blue';
		if (status === 'Created' || status === 'Queued') return 'yellow';
		if (status === 'Ready') return 'green';
		return 'gray';
	};

	async function loadApps() {
		loading = true;
		try {
			const resp = await codedeploy.send(new ListApplicationsCommand({}));
			appNames = resp.applications ?? [];
		} catch (e) {
			toast.error('Failed to load applications: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectApp(name: string) {
		loadingGroups = true;
		activeTab = 'groups';
		try {
			const [appResp, groupsResp] = await Promise.all([
				codedeploy.send(new GetApplicationCommand({ applicationName: name })),
				codedeploy.send(new ListDeploymentGroupsCommand({ applicationName: name }))
			]);
			selectedApp = appResp.application ?? null;
			deploymentGroups = groupsResp.deploymentGroups ?? [];
		} catch (e) {
			toast.error('Failed to load application details: ' + String(e));
		} finally {
			loadingGroups = false;
		}
	}

	async function loadDeployments() {
		if (!selectedApp) return;
		loadingDeployments = true;
		try {
			const resp = await codedeploy.send(new ListDeploymentsCommand({
				applicationName: selectedApp.applicationName,
				includeOnlyStatuses: ['Created', 'Queued', 'InProgress', 'Succeeded', 'Failed', 'Stopped', 'Ready']
			}));
			deployments = resp.deployments ?? [];
			const details = await Promise.allSettled(
				deployments.slice(0, 10).map((id) =>
					codedeploy.send(new GetDeploymentCommand({ deploymentId: id })).then((r) => r.deploymentInfo)
				)
			);
			deploymentDetails = details
				.filter((r) => r.status === 'fulfilled')
				.map((r) => (r as PromiseFulfilledResult<DeploymentInfo | undefined>).value!)
				.filter(Boolean);
		} catch (e) {
			toast.error('Failed to load deployments: ' + String(e));
		} finally {
			loadingDeployments = false;
		}
	}

	async function handleTabChange(tab: 'groups' | 'deployments') {
		activeTab = tab;
		selectedDeployment = null;
		selectedGroup = null;
		if (tab === 'deployments' && deploymentDetails.length === 0) await loadDeployments();
	}

	async function selectDeployment(dep: DeploymentInfo) {
		selectedDeployment = dep;
		deploymentInstances = [];
		if (!dep.deploymentId) return;
		loadingInstances = true;
		try {
			const listRes = await codedeploy.send(new ListDeploymentInstancesCommand({ deploymentId: dep.deploymentId }));
			const ids = (listRes.instancesList ?? []).slice(0, 20);
			if (ids.length > 0) {
				const batchRes = await codedeploy.send(new BatchGetDeploymentInstancesCommand({ deploymentId: dep.deploymentId, instanceIds: ids }));
				deploymentInstances = batchRes.instancesSummary ?? [];
			}
		} catch (e) {
			toast.error('Failed to load instances: ' + String(e));
		} finally {
			loadingInstances = false;
		}
	}

	async function selectGroup(grpName: string) {
		if (!selectedApp?.applicationName) return;
		loadingGroupDetail = true;
		selectedGroup = null;
		try {
			const res = await codedeploy.send(new GetDeploymentGroupCommand({ applicationName: selectedApp.applicationName, deploymentGroupName: grpName }));
			selectedGroup = res.deploymentGroupInfo ?? null;
		} catch (e) {
			toast.error('Failed to load group detail: ' + String(e));
		} finally {
			loadingGroupDetail = false;
		}
	}

	async function createDeployment() {
		if (!selectedApp || !deployGroup.trim()) return;
		creatingDeployment = true;
		try {
			const revision =
				deployRevisionType === 'S3'
					? {
							revisionType: 'S3' as const,
							s3Location: {
								bucket: deployS3Bucket.trim(),
								key: deployS3Key.trim(),
								bundleType: 'zip' as const
							}
						}
					: {
							revisionType: 'GitHub' as const,
							gitHubLocation: {
								repository: `${deployGitHubUser.trim()}/${deployGitHubRepo.trim()}`,
								commitId: deployGitHubCommit.trim()
							}
						};
			const resp = await codedeploy.send(new CreateDeploymentCommand({
				applicationName: selectedApp.applicationName,
				deploymentGroupName: deployGroup.trim(),
				revision
			}));
			toast.success(`Deployment "${resp.deploymentId}" created`);
			showCreateDeployment = false;
			resetDeployForm();
			await loadDeployments();
			activeTab = 'deployments';
		} catch (e) {
			toast.error('Failed to create deployment: ' + String(e));
		} finally {
			creatingDeployment = false;
		}
	}

	function resetDeployForm() {
		deployGroup = '';
		deployRevisionType = 'S3';
		deployS3Bucket = '';
		deployS3Key = '';
		deployGitHubUser = '';
		deployGitHubRepo = '';
		deployGitHubCommit = '';
	}

	async function stopDeployment(id: string) {
		try {
			await codedeploy.send(new StopDeploymentCommand({ deploymentId: id, autoRollbackEnabled: false }));
			toast.success('Deployment stop requested');
			await loadDeployments();
		} catch (e) {
			toast.error('Failed to stop deployment: ' + String(e));
		}
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	async function loadConfigs() {
		loadingConfigs = true;
		try {
			const listResp = await codedeploy.send(new ListDeploymentConfigsCommand({}));
			const names = listResp.deploymentConfigsList ?? [];
			const details = await Promise.allSettled(
				names.map((n) => codedeploy.send(new GetDeploymentConfigCommand({ deploymentConfigName: n })).then((r) => r.deploymentConfigInfo))
			);
			deployConfigs = details
				.filter((r) => r.status === 'fulfilled')
				.map((r) => (r as PromiseFulfilledResult<DeploymentConfigInfo | undefined>).value!)
				.filter(Boolean);
		} catch (e) {
			toast.error('Failed to load deployment configs: ' + String(e));
		} finally {
			loadingConfigs = false;
		}
	}

	async function createConfig() {
		if (!newConfigName.trim()) return;
		creatingConfig = true;
		try {
			await codedeploy.send(new CreateDeploymentConfigCommand({
				deploymentConfigName: newConfigName.trim(),
				computePlatform: newConfigPlatform as 'Server' | 'Lambda' | 'ECS'
			}));
			toast.success(`Config "${newConfigName}" created`);
			showCreateConfig = false;
			newConfigName = '';
			await loadConfigs();
		} catch (e) {
			toast.error('Failed to create config: ' + String(e));
		} finally {
			creatingConfig = false;
		}
	}

	async function loadOnPremInstances() {
		loadingOnPrem = true;
		try {
			const listResp = await codedeploy.send(new ListOnPremisesInstancesCommand({}));
			const names = listResp.instanceNames ?? [];
			const details = await Promise.allSettled(
				names.map((n) => codedeploy.send(new GetOnPremisesInstanceCommand({ instanceName: n })).then((r) => r.instanceInfo))
			);
			onPremInstances = details
				.filter((r) => r.status === 'fulfilled')
				.map((r) => (r as PromiseFulfilledResult<InstanceInfo | undefined>).value!)
				.filter(Boolean);
		} catch (e) {
			toast.error('Failed to load on-prem instances: ' + String(e));
		} finally {
			loadingOnPrem = false;
		}
	}

	async function registerInstance() {
		if (!newInstanceName.trim()) return;
		registeringInstance = true;
		try {
			await codedeploy.send(new RegisterOnPremisesInstanceCommand({
				instanceName: newInstanceName.trim(),
				iamUserArn: newInstanceIamArn.trim() || undefined
			}));
			toast.success(`Instance "${newInstanceName}" registered`);
			showRegisterInstance = false;
			newInstanceName = '';
			newInstanceIamArn = '';
			await loadOnPremInstances();
		} catch (e) {
			toast.error('Failed to register instance: ' + String(e));
		} finally {
			registeringInstance = false;
		}
	}

	async function deregisterInstance(name: string) {
		try {
			await codedeploy.send(new DeregisterOnPremisesInstanceCommand({ instanceName: name }));
			toast.success(`Instance "${name}" deregistered`);
			await loadOnPremInstances();
		} catch (e) {
			toast.error('Failed to deregister: ' + String(e));
		}
	}

	function switchMainView(v: MainView) {
		mainView = v;
		if (v === 'configs' && deployConfigs.length === 0) loadConfigs();
		if (v === 'onprem' && onPremInstances.length === 0) loadOnPremInstances();
	}

	onMount(loadApps);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Package class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS CodeDeploy</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Automated application deployments</p>
			</div>
		</div>
		<button onclick={loadApps} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- View tabs (only shown when not in app detail) -->
	{#if !selectedApp}
		<div class="flex gap-2">
			{#each ([['applications', 'Applications', Package], ['configs', 'Deployment Configs', Settings], ['onprem', 'On-Prem Instances', Server]] as const) as [v, label, Icon]}
				<button
					onclick={() => switchMainView(v)}
					class="flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all {mainView === v ? 'bg-green-600 text-white shadow' : 'bg-white dark:bg-gray-900 text-gray-600 dark:text-gray-400 border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800'}"
				>
					<Icon class="w-4 h-4" />{label}
				</button>
			{/each}
		</div>
	{/if}

	{#if mainView === 'configs' && !selectedApp}
		<!-- Deployment Configs View -->
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Deployment Configurations</h2>
				<div class="flex gap-2">
					<button onclick={loadConfigs} class="flex items-center gap-2 text-sm text-gray-600 border border-gray-200 dark:border-gray-700 px-3 py-1.5 rounded-lg hover:bg-gray-50">
						<RefreshCw class="w-3.5 h-3.5 {loadingConfigs ? 'animate-spin' : ''}" /> Refresh
					</button>
					<button onclick={() => (showCreateConfig = true)} class="flex items-center gap-2 text-sm bg-green-600 text-white px-3 py-1.5 rounded-lg hover:bg-green-700">
						<Plus class="w-3.5 h-3.5" /> New Config
					</button>
				</div>
			</div>
			{#if loadingConfigs}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
			{:else if deployConfigs.length === 0}
				<div class="text-center py-12 text-gray-500">
					<Settings class="w-10 h-10 mx-auto mb-2 opacity-40" />
					<p>No deployment configurations found</p>
				</div>
			{:else}
				<div class="overflow-hidden rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Config Name</th>
								<th class="px-4 py-3 text-left">Platform</th>
								<th class="px-4 py-3 text-left">Config ID</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each deployConfigs as cfg}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
									<td class="px-4 py-3 font-medium text-green-600 dark:text-green-400">{cfg.deploymentConfigName}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{cfg.computePlatform ?? 'Server'}</td>
									<td class="px-4 py-3 text-gray-500 font-mono text-xs">{cfg.deploymentConfigId}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{:else if mainView === 'onprem' && !selectedApp}
		<!-- On-Prem Instances View -->
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">On-Premises Instances</h2>
				<div class="flex gap-2">
					<button onclick={loadOnPremInstances} class="flex items-center gap-2 text-sm text-gray-600 border border-gray-200 dark:border-gray-700 px-3 py-1.5 rounded-lg hover:bg-gray-50">
						<RefreshCw class="w-3.5 h-3.5 {loadingOnPrem ? 'animate-spin' : ''}" /> Refresh
					</button>
					<button onclick={() => (showRegisterInstance = true)} class="flex items-center gap-2 text-sm bg-green-600 text-white px-3 py-1.5 rounded-lg hover:bg-green-700">
						<Plus class="w-3.5 h-3.5" /> Register
					</button>
				</div>
			</div>
			{#if loadingOnPrem}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
			{:else if onPremInstances.length === 0}
				<div class="text-center py-12 text-gray-500">
					<Server class="w-10 h-10 mx-auto mb-2 opacity-40" />
					<p>No on-premises instances registered</p>
				</div>
			{:else}
				<div class="overflow-hidden rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Instance Name</th>
								<th class="px-4 py-3 text-left">IAM ARN</th>
								<th class="px-4 py-3 text-left">Registered</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each onPremInstances as inst}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{inst.instanceName}</td>
									<td class="px-4 py-3 text-gray-500 text-xs font-mono truncate max-w-xs">{inst.iamUserArn ?? inst.iamSessionArn ?? '-'}</td>
									<td class="px-4 py-3 text-gray-500 text-xs">{inst.registerTime ? new Date(inst.registerTime).toLocaleDateString() : '-'}</td>
									<td class="px-4 py-3">
										<span class="px-2 py-0.5 rounded text-xs {inst.deregisterTime ? 'bg-gray-100 text-gray-600' : 'bg-green-100 text-green-700'}">
											{inst.deregisterTime ? 'Deregistered' : 'Registered'}
										</span>
									</td>
									<td class="px-4 py-3">
										{#if !inst.deregisterTime}
											<button onclick={() => deregisterInstance(inst.instanceName ?? '')} class="text-xs text-red-500 hover:text-red-700">Deregister</button>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{:else if selectedApp}
		<!-- App Detail -->
		<div class="flex items-center justify-between">
			<div class="flex items-center gap-2 text-sm">
				<button onclick={() => { selectedApp = null; deploymentGroups = []; deploymentDetails = []; }} class="text-green-600 hover:underline">Applications</button>
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedApp.applicationName}</span>
			</div>
			<button onclick={() => (showCreateDeployment = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm font-medium">
				<Play class="w-4 h-4" /> Create Deployment
			</button>
		</div>

		<!-- App Info -->
		<div class="grid grid-cols-3 gap-4">
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
				<div class="text-xs text-gray-500">App ID</div>
				<div class="text-sm font-mono mt-1">{selectedApp.applicationId}</div>
			</div>
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
				<div class="text-xs text-gray-500">Compute Platform</div>
				<div class="text-sm font-semibold mt-1">{selectedApp.computePlatform ?? 'Server'}</div>
			</div>
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
				<div class="text-xs text-gray-500">Created</div>
				<div class="text-sm mt-1">{formatDate(selectedApp.createTime)}</div>
			</div>
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['groups', 'Deployment Groups'], ['deployments', 'Deployments']] as [tab, label]}
				<button
					onclick={() => handleTabChange(tab as 'groups' | 'deployments')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-green-500 text-green-600 dark:text-green-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
				>
					{label}
				</button>
			{/each}
		</div>

		{#if activeTab === 'groups'}
			{#if loadingGroups}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
			{:else if deploymentGroups.length === 0}
				<div class="text-center py-12 text-gray-500">No deployment groups found</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr><th class="px-4 py-3 text-left">Deployment Group</th></tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each deploymentGroups as grp}
								<tr
									class="hover:bg-gray-50 dark:hover:bg-gray-800/50 cursor-pointer {selectedGroup?.deploymentGroupName === grp ? 'bg-green-50 dark:bg-green-900/10' : ''}"
									onclick={() => selectGroup(grp)}
								>
									<td class="px-4 py-3 font-medium text-green-600 dark:text-green-400">{grp}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>

				{#if loadingGroupDetail}
					<div class="flex justify-center py-6"><div class="animate-spin w-6 h-6 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
				{:else if selectedGroup}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 space-y-4">
						<div class="flex items-center justify-between">
							<h3 class="font-semibold text-gray-900 dark:text-white">{selectedGroup.deploymentGroupName}</h3>
							<button onclick={() => (selectedGroup = null)} class="text-xs text-gray-500 hover:text-gray-700">Close</button>
						</div>
						<div class="grid grid-cols-2 gap-3 text-sm sm:grid-cols-3">
							<div>
								<p class="text-xs text-gray-500">Deployment Config</p>
								<p class="font-medium mt-0.5">{selectedGroup.deploymentConfigName ?? '—'}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Compute Platform</p>
								<p class="font-medium mt-0.5">{selectedGroup.computePlatform ?? 'Server'}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Deployment Style</p>
								<p class="font-medium mt-0.5">{selectedGroup.deploymentStyle?.deploymentType ?? '—'}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Deployment Option</p>
								<p class="font-medium mt-0.5">{selectedGroup.deploymentStyle?.deploymentOption ?? '—'}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Auto-Rollback</p>
								<p class="font-medium mt-0.5">{selectedGroup.autoRollbackConfiguration?.enabled ? 'Enabled' : 'Disabled'}</p>
							</div>
							{#if selectedGroup.autoRollbackConfiguration?.enabled && (selectedGroup.autoRollbackConfiguration.events?.length ?? 0) > 0}
								<div>
									<p class="text-xs text-gray-500">Rollback Events</p>
									<div class="flex flex-wrap gap-1 mt-1">
										{#each selectedGroup.autoRollbackConfiguration.events ?? [] as evt}
											<span class="rounded bg-yellow-100 dark:bg-yellow-900/30 px-1.5 py-0.5 text-xs text-yellow-800 dark:text-yellow-300">{evt}</span>
										{/each}
									</div>
								</div>
							{/if}
						</div>
						{#if selectedGroup.serviceRoleArn}
							<div>
								<p class="text-xs text-gray-500">Service Role ARN</p>
								<p class="text-xs font-mono text-gray-700 dark:text-gray-300 break-all mt-0.5">{selectedGroup.serviceRoleArn}</p>
							</div>
						{/if}
						{#if (selectedGroup.autoScalingGroups?.length ?? 0) > 0}
							<div>
								<p class="text-xs text-gray-500 mb-1">Auto Scaling Groups</p>
								<div class="flex flex-wrap gap-1">
									{#each selectedGroup.autoScalingGroups ?? [] as asg}
										<span class="rounded bg-blue-100 dark:bg-blue-900/30 px-1.5 py-0.5 text-xs text-blue-700 dark:text-blue-300">{asg.name}</span>
									{/each}
								</div>
							</div>
						{/if}
						{#if (selectedGroup.ecsServices?.length ?? 0) > 0}
							<div>
								<p class="text-xs text-gray-500 mb-1">ECS Services</p>
								{#each selectedGroup.ecsServices ?? [] as svc}
									<p class="text-xs font-mono text-gray-700 dark:text-gray-300">{svc.clusterName} / {svc.serviceName}</p>
								{/each}
							</div>
						{/if}
					</div>
				{/if}
			{/if}
		{/if}

		{#if activeTab === 'deployments'}
			{#if loadingDeployments}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
			{:else if deploymentDetails.length === 0}
				<div class="text-center py-12 text-gray-500">No deployments found</div>
			{:else}
				<div class="space-y-3">
					{#each deploymentDetails as dep}
						<div
							class="bg-white dark:bg-gray-900 rounded-xl border p-4 cursor-pointer transition-colors {selectedDeployment?.deploymentId === dep.deploymentId ? 'border-green-500' : 'border-gray-200 dark:border-gray-700'}"
							onclick={() => selectDeployment(dep)}
							role="button"
							tabindex="0"
							onkeydown={(e) => { if (e.key === 'Enter') selectDeployment(dep); }}
						>
							<div class="flex items-start justify-between">
								<div>
									<div class="flex items-center gap-2">
										{#if dep.status === 'Succeeded'}
											<CheckCircle class="w-4 h-4 text-green-500" />
										{:else if dep.status === 'Failed' || dep.status === 'Stopped'}
											<AlertCircle class="w-4 h-4 text-red-500" />
										{:else}
											<div class="w-4 h-4 border-2 border-blue-500 border-t-transparent rounded-full animate-spin"></div>
										{/if}
										<span class="font-medium text-sm">{dep.deploymentId}</span>
										<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(dep.status)}-100 text-${statusColor(dep.status)}-700`}>{dep.status}</span>
									</div>
									<div class="text-xs text-gray-500 mt-1">
										Group: {dep.deploymentGroupName} | Config: {dep.deploymentConfigName ?? '—'} | Created: {formatDate(dep.createTime)}
									</div>
									{#if dep.description}
										<div class="text-xs text-gray-500 mt-0.5">{dep.description}</div>
									{/if}
								</div>
								{#if dep.status === 'InProgress'}
									<button
										onclick={(e) => { e.stopPropagation(); stopDeployment(dep.deploymentId ?? ''); }}
										class="flex items-center gap-1 px-3 py-1 text-xs text-red-600 border border-red-200 rounded hover:bg-red-50"
									>
										<XCircle class="w-3.5 h-3.5" /> Stop
									</button>
								{/if}
							</div>
							{#if dep.deploymentOverview}
								<div class="mt-3 grid grid-cols-5 gap-2 text-xs">
									{#each [
										{ label: 'Pending', value: dep.deploymentOverview.Pending, color: 'yellow' },
										{ label: 'In Progress', value: dep.deploymentOverview.InProgress, color: 'blue' },
										{ label: 'Succeeded', value: dep.deploymentOverview.Succeeded, color: 'green' },
										{ label: 'Failed', value: dep.deploymentOverview.Failed, color: 'red' },
										{ label: 'Skipped', value: dep.deploymentOverview.Skipped, color: 'gray' }
									] as stat}
										<div class={`text-center p-2 rounded bg-${stat.color}-50 dark:bg-${stat.color}-900/20`}>
											<div class={`font-bold text-${stat.color}-700`}>{stat.value ?? 0}</div>
											<div class="text-gray-500">{stat.label}</div>
										</div>
									{/each}
								</div>
							{/if}
						</div>
					{/each}
				</div>

				<!-- Deployment Detail Panel -->
				{#if selectedDeployment}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 space-y-4">
						<div class="flex items-center justify-between">
							<h3 class="font-semibold text-gray-900 dark:text-white">{selectedDeployment.deploymentId}</h3>
							<button onclick={() => { selectedDeployment = null; deploymentInstances = []; }} class="text-xs text-gray-500 hover:text-gray-700">Close</button>
						</div>
						<div class="grid grid-cols-2 gap-3 text-sm sm:grid-cols-3">
							<div>
								<p class="text-xs text-gray-500">Status</p>
								<span class={`inline-block mt-0.5 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedDeployment.status)}-100 text-${statusColor(selectedDeployment.status)}-700`}>{selectedDeployment.status}</span>
							</div>
							<div>
								<p class="text-xs text-gray-500">Creator</p>
								<p class="font-medium mt-0.5">{selectedDeployment.creator ?? '—'}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Deployment Config</p>
								<p class="font-medium mt-0.5">{selectedDeployment.deploymentConfigName ?? '—'}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Started</p>
								<p class="font-medium mt-0.5">{formatDate(selectedDeployment.startTime)}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Completed</p>
								<p class="font-medium mt-0.5">{formatDate(selectedDeployment.completeTime)}</p>
							</div>
							<div>
								<p class="text-xs text-gray-500">Compute Platform</p>
								<p class="font-medium mt-0.5">{selectedDeployment.computePlatform ?? 'Server'}</p>
							</div>
						</div>

						{#if selectedDeployment.revision}
							{@const rev = selectedDeployment.revision}
							<div>
								<p class="text-xs text-gray-500 mb-1">Revision</p>
								<div class="rounded-md bg-gray-50 dark:bg-gray-800 px-4 py-3 text-sm space-y-1">
									<div><span class="text-gray-500">Type:</span> {rev.revisionType ?? '—'}</div>
									{#if rev.s3Location}
										<div><span class="text-gray-500">S3:</span> <span class="font-mono">{rev.s3Location.bucket}/{rev.s3Location.key}</span></div>
										{#if rev.s3Location.version}
											<div><span class="text-gray-500">Version:</span> <span class="font-mono">{rev.s3Location.version}</span></div>
										{/if}
									{/if}
									{#if rev.gitHubLocation}
										<div><span class="text-gray-500">Repo:</span> <span class="font-mono">{rev.gitHubLocation.repository}</span></div>
										<div><span class="text-gray-500">Commit:</span> <span class="font-mono">{rev.gitHubLocation.commitId?.slice(0, 12) ?? '—'}</span></div>
									{/if}
								</div>
							</div>
						{/if}

						{#if selectedDeployment.rollbackInfo?.rollbackDeploymentId}
							<div class="rounded-md bg-yellow-50 dark:bg-yellow-900/20 px-4 py-3 text-sm">
								<p class="font-medium text-yellow-800 dark:text-yellow-200 mb-1">Rollback Info</p>
								<div class="text-yellow-700 dark:text-yellow-300 space-y-1">
									<div>Rollback Deployment: <span class="font-mono">{selectedDeployment.rollbackInfo.rollbackDeploymentId}</span></div>
									{#if selectedDeployment.rollbackInfo.rollbackMessage}
										<div>Reason: {selectedDeployment.rollbackInfo.rollbackMessage}</div>
									{/if}
								</div>
							</div>
						{/if}

						{#if selectedDeployment.errorInformation?.message}
							<div class="rounded-md bg-red-50 dark:bg-red-900/20 px-4 py-3 text-sm">
								<p class="font-medium text-red-800 dark:text-red-200 mb-1">Error: {selectedDeployment.errorInformation.code}</p>
								<p class="text-red-700 dark:text-red-300">{selectedDeployment.errorInformation.message}</p>
							</div>
						{/if}

						<!-- Instance-level status -->
						<div>
							<p class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">Instance Status</p>
							{#if loadingInstances}
								<div class="flex justify-center py-4"><div class="animate-spin w-5 h-5 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
							{:else if deploymentInstances.length === 0}
								<p class="text-xs text-gray-500">No instance data available</p>
							{:else}
								<div class="overflow-hidden rounded-lg border border-gray-200 dark:border-gray-700">
									<table class="w-full text-xs">
										<thead class="bg-gray-50 dark:bg-gray-800 text-gray-500 uppercase">
											<tr>
												<th class="px-3 py-2 text-left">Instance ID</th>
												<th class="px-3 py-2 text-left">Status</th>
												<th class="px-3 py-2 text-left">Type</th>
												<th class="px-3 py-2 text-left">Last Updated</th>
											</tr>
										</thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each deploymentInstances as inst}
												<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
													<td class="px-3 py-2 font-mono truncate max-w-[180px]">{inst.instanceId?.split('/').pop() ?? inst.instanceId}</td>
													<td class="px-3 py-2">
														<span class={`px-1.5 py-0.5 rounded text-xs bg-${statusColor(inst.status)}-100 text-${statusColor(inst.status)}-700`}>{inst.status ?? '—'}</span>
													</td>
													<td class="px-3 py-2 text-gray-500">{inst.instanceType ?? '—'}</td>
													<td class="px-3 py-2 text-gray-500">{inst.lastUpdatedAt ? new Date(inst.lastUpdatedAt).toLocaleTimeString() : '—'}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
						</div>
					</div>
				{/if}
			{/if}
		{/if}

	{:else}
		<!-- App List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search applications..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredApps.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Package class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No applications found</p>
				<p class="text-sm mt-1">Create a CodeDeploy application to start deploying</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Application Name</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredApps as app}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectApp(app)} class="text-green-600 dark:text-green-400 hover:underline font-medium">{app}</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Deployment Modal -->
{#if showCreateDeployment && selectedApp}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Deployment</h2>
			<p class="text-sm text-gray-500">Application: {selectedApp.applicationName}</p>
			<div>
				<label for="deploy-group" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Deployment Group</label>
				{#if deploymentGroups.length > 0}
					<select id="deploy-group" bind:value={deployGroup} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="">Select group...</option>
						{#each deploymentGroups as grp}<option value={grp}>{grp}</option>{/each}
					</select>
				{:else}
					<input id="deploy-group" bind:value={deployGroup} type="text" placeholder="deployment-group-name" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				{/if}
			</div>
			<div>
				<label for="rev-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Revision Type</label>
				<select id="rev-type" bind:value={deployRevisionType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="S3">Amazon S3</option>
					<option value="GitHub">GitHub</option>
				</select>
			</div>
			{#if deployRevisionType === 'S3'}
				<div>
					<label for="s3-deploy-bucket" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Bucket</label>
					<input id="s3-deploy-bucket" bind:value={deployS3Bucket} type="text" placeholder="my-deploy-bucket" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="s3-deploy-key" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Key</label>
					<input id="s3-deploy-key" bind:value={deployS3Key} type="text" placeholder="releases/app-v1.0.zip" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			{:else}
				<div>
					<label for="gh-repo" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">GitHub Repository (owner/repo)</label>
					<input id="gh-repo" bind:value={deployGitHubRepo} type="text" placeholder="my-org/my-app" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="gh-commit" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Commit ID</label>
					<input id="gh-commit" bind:value={deployGitHubCommit} type="text" placeholder="abc123def456..." class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			{/if}
			<div class="flex gap-3 pt-2">
				<button onclick={() => { showCreateDeployment = false; resetDeployForm(); }} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={createDeployment} disabled={creatingDeployment || !deployGroup.trim()} class="flex-1 px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
					{creatingDeployment ? 'Deploying...' : 'Deploy'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Config Modal -->
{#if showCreateConfig}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-sm p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Deployment Config</h2>
			<div>
				<label for="cfg-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Config Name</label>
				<input id="cfg-name" bind:value={newConfigName} type="text" placeholder="MyDeploymentConfig" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="cfg-platform" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Compute Platform</label>
				<select id="cfg-platform" bind:value={newConfigPlatform} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="Server">Server</option>
					<option value="Lambda">Lambda</option>
					<option value="ECS">ECS</option>
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateConfig = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={createConfig} disabled={creatingConfig || !newConfigName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
					{creatingConfig ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Register Instance Modal -->
{#if showRegisterInstance}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-sm p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Register On-Premises Instance</h2>
			<div>
				<label for="inst-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Name</label>
				<input id="inst-name" bind:value={newInstanceName} type="text" placeholder="my-on-prem-server" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="inst-iam" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">IAM User ARN (optional)</label>
				<input id="inst-iam" bind:value={newInstanceIamArn} type="text" placeholder="arn:aws:iam::123456789012:user/codedeploy-agent" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showRegisterInstance = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={registerInstance} disabled={registeringInstance || !newInstanceName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
					{registeringInstance ? 'Registering...' : 'Register'}
				</button>
			</div>
		</div>
	</div>
{/if}
