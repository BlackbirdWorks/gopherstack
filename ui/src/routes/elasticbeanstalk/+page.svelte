<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getElasticBeanstalkClient } from '$lib/aws-client';
	import {
		DescribeApplicationsCommand,
		DescribeEnvironmentsCommand,
		DescribeApplicationVersionsCommand,
		CreateApplicationCommand,
		DeleteApplicationCommand,
		UpdateApplicationCommand,
		CreateEnvironmentCommand,
		TerminateEnvironmentCommand,
		UpdateEnvironmentCommand,
		CreateApplicationVersionCommand,
		DeleteApplicationVersionCommand,
		ListAvailableSolutionStacksCommand,
		type ApplicationDescription,
		type EnvironmentDescription,
		type ApplicationVersionDescription
	} from '@aws-sdk/client-elastic-beanstalk';
	import { toast } from 'svelte-sonner';
	import { Leaf, RefreshCw, Search, Server, Box, CheckCircle, Plus, Trash2, Settings, Package } from 'lucide-svelte';

	const eb = getElasticBeanstalkClient();

	type TabName = 'applications' | 'environments' | 'versions';

	let loading = $state(false);
	let activeTab = $state<TabName>('applications');
	let searchQuery = $state('');
	let applications = $state<ApplicationDescription[]>([]);
	let environments = $state<EnvironmentDescription[]>([]);
	let versions = $state<ApplicationVersionDescription[]>([]);
	let solutionStacks = $state<string[]>([]);

	// Create Application
	let showCreateApp = $state(false);
	let creatingApp = $state(false);
	let newAppName = $state('');
	let newAppDesc = $state('');

	// Create Environment
	let showCreateEnv = $state(false);
	let creatingEnv = $state(false);
	let newEnvAppName = $state('');
	let newEnvName = $state('');
	let newEnvDesc = $state('');
	let newEnvSolutionStack = $state('');

	// Create Application Version
	let showCreateVersion = $state(false);
	let creatingVersion = $state(false);
	let newVerAppName = $state('');
	let newVerLabel = $state('');
	let newVerDesc = $state('');

	const filteredApps = $derived(applications.filter((a) => (a.ApplicationName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredEnvs = $derived(environments.filter((e) => (e.EnvironmentName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredVersions = $derived(versions.filter((v) => (v.VersionLabel ?? '').toLowerCase().includes(searchQuery.toLowerCase()) || (v.ApplicationName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const readyEnvs = $derived(environments.filter((e) => e.Status === 'Ready').length);

	async function loadData() {
		loading = true;
		try {
			const [appsResp, envsResp, versResp, stacksResp] = await Promise.all([
				eb.send(new DescribeApplicationsCommand({})),
				eb.send(new DescribeEnvironmentsCommand({})),
				eb.send(new DescribeApplicationVersionsCommand({})),
				eb.send(new ListAvailableSolutionStacksCommand({}))
			]);
			applications = appsResp.Applications ?? [];
			environments = envsResp.Environments ?? [];
			versions = versResp.ApplicationVersions ?? [];
			solutionStacks = stacksResp.SolutionStacks ?? [];
			if (solutionStacks.length > 0 && !newEnvSolutionStack) {
				newEnvSolutionStack = solutionStacks[0];
			}
		} catch (e) {
			toast.error('Failed to load Elastic Beanstalk data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function createApplication() {
		if (!newAppName.trim()) { toast.error('Application name is required'); return; }
		creatingApp = true;
		try {
			await eb.send(new CreateApplicationCommand({ ApplicationName: newAppName.trim(), Description: newAppDesc.trim() || undefined }));
			toast.success(`Application "${newAppName}" created`);
			newAppName = ''; newAppDesc = ''; showCreateApp = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to create application: ' + String(e));
		} finally {
			creatingApp = false;
		}
	}

	async function deleteApplication(appName: string) {
		const ok = await confirmDestructive(`Delete application "${appName}"? This will also delete all associated environments and versions.`);
		if (!ok) return;
		try {
			await eb.send(new DeleteApplicationCommand({ ApplicationName: appName }));
			toast.success(`Application "${appName}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete application: ' + String(e));
		}
	}

	async function createEnvironment() {
		if (!newEnvAppName.trim()) { toast.error('Application name is required'); return; }
		if (!newEnvName.trim()) { toast.error('Environment name is required'); return; }
		creatingEnv = true;
		try {
			await eb.send(new CreateEnvironmentCommand({
				ApplicationName: newEnvAppName.trim(),
				EnvironmentName: newEnvName.trim(),
				Description: newEnvDesc.trim() || undefined,
				SolutionStackName: newEnvSolutionStack || undefined
			}));
			toast.success(`Environment "${newEnvName}" created`);
			newEnvAppName = ''; newEnvName = ''; newEnvDesc = ''; showCreateEnv = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to create environment: ' + String(e));
		} finally {
			creatingEnv = false;
		}
	}

	async function terminateEnvironment(appName: string, envName: string) {
		const ok = await confirmDestructive(`Terminate environment "${envName}"?`);
		if (!ok) return;
		try {
			await eb.send(new TerminateEnvironmentCommand({ EnvironmentName: envName }));
			toast.success(`Environment "${envName}" terminating`);
			await loadData();
		} catch (e) {
			toast.error('Failed to terminate environment: ' + String(e));
		}
	}

	async function createVersion() {
		if (!newVerAppName.trim()) { toast.error('Application name is required'); return; }
		if (!newVerLabel.trim()) { toast.error('Version label is required'); return; }
		creatingVersion = true;
		try {
			await eb.send(new CreateApplicationVersionCommand({
				ApplicationName: newVerAppName.trim(),
				VersionLabel: newVerLabel.trim(),
				Description: newVerDesc.trim() || undefined
			}));
			toast.success(`Version "${newVerLabel}" created`);
			newVerAppName = ''; newVerLabel = ''; newVerDesc = ''; showCreateVersion = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to create version: ' + String(e));
		} finally {
			creatingVersion = false;
		}
	}

	async function deleteVersion(appName: string, versionLabel: string) {
		const ok = await confirmDestructive(`Delete version "${versionLabel}" from "${appName}"?`);
		if (!ok) return;
		try {
			await eb.send(new DeleteApplicationVersionCommand({ ApplicationName: appName, VersionLabel: versionLabel }));
			toast.success(`Version "${versionLabel}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete version: ' + String(e));
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Leaf class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Elastic Beanstalk</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Deploy and scale web applications and services</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Box class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{applications.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Server class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{environments.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Environments</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-emerald-600 dark:text-emerald-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{readyEnvs}</p><p class="text-sm text-gray-500 dark:text-gray-400">Ready</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Package class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{versions.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Versions</p></div>
		</div>
	</div>

	<!-- Create Application Modal -->
	{#if showCreateApp}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Application</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Application Name *</label>
						<input bind:value={newAppName} placeholder="my-application" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
						<input bind:value={newAppDesc} placeholder="Optional description" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button onclick={createApplication} disabled={creatingApp} class="flex-1 px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 disabled:opacity-50">
						{creatingApp ? 'Creating...' : 'Create'}
					</button>
					<button onclick={() => showCreateApp = false} class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">Cancel</button>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Environment Modal -->
	{#if showCreateEnv}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Environment</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Application Name *</label>
						<select bind:value={newEnvAppName} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							<option value="">Select application...</option>
							{#each applications as app}
								<option value={app.ApplicationName}>{app.ApplicationName}</option>
							{/each}
						</select>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Environment Name *</label>
						<input bind:value={newEnvName} placeholder="my-env" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Solution Stack</label>
						<select bind:value={newEnvSolutionStack} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							{#each solutionStacks as stack}
								<option value={stack}>{stack}</option>
							{/each}
						</select>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
						<input bind:value={newEnvDesc} placeholder="Optional description" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button onclick={createEnvironment} disabled={creatingEnv} class="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
						{creatingEnv ? 'Creating...' : 'Create'}
					</button>
					<button onclick={() => showCreateEnv = false} class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">Cancel</button>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Version Modal -->
	{#if showCreateVersion}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Application Version</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Application Name *</label>
						<select bind:value={newVerAppName} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							<option value="">Select application...</option>
							{#each applications as app}
								<option value={app.ApplicationName}>{app.ApplicationName}</option>
							{/each}
						</select>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Version Label *</label>
						<input bind:value={newVerLabel} placeholder="v1.0.0" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
						<input bind:value={newVerDesc} placeholder="Optional description" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button onclick={createVersion} disabled={creatingVersion} class="flex-1 px-4 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50">
						{creatingVersion ? 'Creating...' : 'Create'}
					</button>
					<button onclick={() => showCreateVersion = false} class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">Cancel</button>
				</div>
			</div>
		</div>
	{/if}

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['applications', 'Applications'], ['environments', 'Environments'], ['versions', 'Versions']] as [tab, label]}
					<button onclick={() => { activeTab = tab as TabName; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-green-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="flex items-center gap-2">
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48" />
				</div>
				{#if activeTab === 'applications'}
					<button onclick={() => showCreateApp = true} class="flex items-center gap-1 px-3 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700">
						<Plus class="w-4 h-4" /> New
					</button>
				{:else if activeTab === 'environments'}
					<button onclick={() => showCreateEnv = true} class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700" disabled={applications.length === 0}>
						<Plus class="w-4 h-4" /> New
					</button>
				{:else if activeTab === 'versions'}
					<button onclick={() => showCreateVersion = true} class="flex items-center gap-1 px-3 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700" disabled={applications.length === 0}>
						<Plus class="w-4 h-4" /> New
					</button>
				{/if}
			</div>
		</div>

		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'applications'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Box class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{app.ApplicationName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{app.Description ?? 'No description'}</p>
									</div>
								</div>
								<div class="flex items-center gap-3">
									<span class="text-xs text-gray-400">{(app.Versions ?? []).length} versions</span>
									<button onclick={() => deleteApplication(app.ApplicationName!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400">
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'environments'}
				{#if filteredEnvs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No environments found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEnvs as env}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{env.EnvironmentName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{env.ApplicationName} · {env.SolutionStackName ?? 'Unknown stack'}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span class="text-xs px-2 py-1 rounded-full {env.Health === 'Green' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : env.Health === 'Red' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400'}">{env.Health}</span>
									<span class="text-xs text-gray-500 dark:text-gray-400">{env.Status}</span>
									<button onclick={() => terminateEnvironment(env.ApplicationName!, env.EnvironmentName!)} title="Terminate" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400">
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'versions'}
				{#if filteredVersions.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No application versions found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredVersions as ver}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Package class="w-5 h-5 text-purple-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{ver.VersionLabel}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{ver.ApplicationName} · {ver.Description ?? 'No description'}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-slate-600 text-gray-600 dark:text-gray-300">{ver.Status}</span>
									<button onclick={() => deleteVersion(ver.ApplicationName!, ver.VersionLabel!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400">
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
