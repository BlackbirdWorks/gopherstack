<script lang="ts">
	import { onMount } from 'svelte';
	import { getCodeDeployClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		GetApplicationCommand,
		ListDeploymentGroupsCommand,
		ListDeploymentsCommand,
		GetDeploymentCommand,
		CreateDeploymentCommand,
		StopDeploymentCommand,
		type ApplicationInfo,
		type DeploymentInfo
	} from '@aws-sdk/client-codedeploy';
	import { toast } from 'svelte-sonner';
	import { Package, Search, RefreshCw, Plus, XCircle, ChevronRight, Play, CheckCircle, AlertCircle } from 'lucide-svelte';

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
		if (tab === 'deployments' && deploymentDetails.length === 0) await loadDeployments();
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

	{#if selectedApp}
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
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
									<td class="px-4 py-3 font-medium text-green-600 dark:text-green-400">{grp}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
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
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
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
										Group: {dep.deploymentGroupName} | Created: {formatDate(dep.createTime)}
									</div>
									{#if dep.description}
										<div class="text-xs text-gray-500 mt-0.5">{dep.description}</div>
									{/if}
								</div>
								{#if dep.status === 'InProgress'}
									<button onclick={() => stopDeployment(dep.deploymentId ?? '')} class="flex items-center gap-1 px-3 py-1 text-xs text-red-600 border border-red-200 rounded hover:bg-red-50">
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
