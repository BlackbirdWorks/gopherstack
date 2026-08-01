<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getElasticBeanstalkClient } from '$lib/aws-client';
	import {
		DescribeApplicationsCommand,
		DescribeEnvironmentsCommand,
		DescribeApplicationVersionsCommand,
		CreateApplicationCommand,
		DeleteApplicationCommand,
		CreateEnvironmentCommand,
		TerminateEnvironmentCommand,
		CreateApplicationVersionCommand,
		DeleteApplicationVersionCommand,
		ListAvailableSolutionStacksCommand,
		CreateConfigurationTemplateCommand,
		DeleteConfigurationTemplateCommand,
		ListPlatformVersionsCommand,
		CreatePlatformVersionCommand,
		DeletePlatformVersionCommand,
		SwapEnvironmentCNAMEsCommand,
		type ApplicationDescription,
		type EnvironmentDescription,
		type ApplicationVersionDescription,
		type PlatformSummary
	} from '@aws-sdk/client-elastic-beanstalk';
	import { toast } from 'svelte-sonner';
	import {
		Leaf,
		RefreshCw,
		Search,
		Server,
		Box,
		CheckCircle,
		Plus,
		Trash2,
		Package,
		ChevronDown,
		ChevronRight,
		ArrowLeftRight,
		Layers,
		Layout
	} from 'lucide-svelte';

	const eb = regionalClient(getElasticBeanstalkClient);

	type TabName = 'applications' | 'environments' | 'versions' | 'templates' | 'platforms';

	let loading = $state(false);
	let activeTab = $state<TabName>('applications');
	let searchQuery = $state('');
	let applications = $state<ApplicationDescription[]>([]);
	let environments = $state<EnvironmentDescription[]>([]);
	let versions = $state<ApplicationVersionDescription[]>([]);
	let solutionStacks = $state<string[]>([]);
	let platformVersions = $state<PlatformSummary[]>([]);

	// Expanded environment detail rows (improvement #19)
	let expandedEnvs = $state<Set<string>>(new Set());

	// Create Application
	let showCreateApp = $state(false);
	let creatingApp = $state(false);
	let newAppName = $state('');
	let newAppDesc = $state('');

	// Create Environment (improvements #14, #15, #16, #21, #22)
	let showCreateEnv = $state(false);
	let creatingEnv = $state(false);
	let newEnvAppName = $state('');
	let newEnvName = $state('');
	let newEnvDesc = $state('');
	let newEnvSolutionStack = $state('');
	let newEnvTierType = $state('WebServer');
	let newEnvLBType = $state('application');
	let newEnvVPCId = $state('');
	let newEnvSubnets = $state('');

	// Create Application Version (improvement #21: S3 source bundle)
	let showCreateVersion = $state(false);
	let creatingVersion = $state(false);
	let newVerAppName = $state('');
	let newVerLabel = $state('');
	let newVerDesc = $state('');
	let newVerS3Bucket = $state('');
	let newVerS3Key = $state('');

	// Config Templates tab (improvement #17)
	type ConfigTemplate = { ApplicationName: string; TemplateName: string; SolutionStackName?: string; Description?: string };
	let configTemplates = $state<ConfigTemplate[]>([]);
	let showCreateTemplate = $state(false);
	let creatingTemplate = $state(false);
	let newTmplAppName = $state('');
	let newTmplName = $state('');
	let newTmplStack = $state('');

	// Platform versions tab (improvement #18)
	let showCreatePlatform = $state(false);
	let creatingPlatform = $state(false);
	let newPlatformName = $state('');
	let newPlatformVersion = $state('');

	// CNAME swap dialog (improvement #20)
	let showCnameSwap = $state(false);
	let swappingSrc = $state('');
	let swappingDst = $state('');
	let swapping = $state(false);

	const filteredApps = $derived(
		applications.filter((a) =>
			(a.ApplicationName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredEnvs = $derived(
		environments.filter((e) =>
			(e.EnvironmentName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredVersions = $derived(
		versions.filter(
			(v) =>
				(v.VersionLabel ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(v.ApplicationName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredTemplates = $derived(
		configTemplates.filter(
			(t) =>
				(t.TemplateName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(t.ApplicationName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredPlatforms = $derived(
		platformVersions.filter((p) =>
			(p.PlatformArn ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const readyEnvs = $derived(environments.filter((e) => e.Status === 'Ready').length);

	async function loadData() {
		loading = true;
		try {
			const [appsResp, envsResp, versResp, stacksResp, pvResp] = await Promise.all([
				eb().send(new DescribeApplicationsCommand({})),
				eb().send(new DescribeEnvironmentsCommand({})),
				eb().send(new DescribeApplicationVersionsCommand({})),
				eb().send(new ListAvailableSolutionStacksCommand({})),
				eb().send(new ListPlatformVersionsCommand({}))
			]);
			applications = appsResp.Applications ?? [];
			environments = envsResp.Environments ?? [];
			versions = versResp.ApplicationVersions ?? [];
			solutionStacks = stacksResp.SolutionStacks ?? [];
			platformVersions = pvResp.PlatformSummaryList ?? [];
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
		if (!newAppName.trim()) {
			toast.error('Application name is required');
			return;
		}
		creatingApp = true;
		try {
			await eb().send(
				new CreateApplicationCommand({
					ApplicationName: newAppName.trim(),
					Description: newAppDesc.trim() || undefined
				})
			);
			toast.success(`Application "${newAppName}" created`);
			newAppName = '';
			newAppDesc = '';
			showCreateApp = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to create application: ' + String(e));
		} finally {
			creatingApp = false;
		}
	}

	async function deleteApplication(appName: string) {
		const ok = await confirmDestructive(
			`Delete application "${appName}"? This will also delete all associated environments and versions.`
		);
		if (!ok) return;
		try {
			await eb().send(new DeleteApplicationCommand({ ApplicationName: appName }));
			toast.success(`Application "${appName}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete application: ' + String(e));
		}
	}

	async function createEnvironment() {
		if (!newEnvAppName.trim()) {
			toast.error('Application name is required');
			return;
		}
		if (!newEnvName.trim()) {
			toast.error('Environment name is required');
			return;
		}
		creatingEnv = true;
		try {
			await eb().send(
				new CreateEnvironmentCommand({
					ApplicationName: newEnvAppName.trim(),
					EnvironmentName: newEnvName.trim(),
					Description: newEnvDesc.trim() || undefined,
					SolutionStackName: newEnvSolutionStack || undefined,
					Tier: {
						Name: newEnvTierType,
						Type: newEnvTierType === 'Worker' ? 'SQS/HTTP' : 'Standard'
					},
					OptionSettings: [
						...(newEnvLBType
							? [
									{
										Namespace: 'aws:elasticbeanstalk:environment',
										OptionName: 'LoadBalancerType',
										Value: newEnvLBType
									}
								]
							: []),
						...(newEnvVPCId
							? [{ Namespace: 'aws:ec2:vpc', OptionName: 'VPCId', Value: newEnvVPCId }]
							: []),
						...(newEnvSubnets
							? [{ Namespace: 'aws:ec2:vpc', OptionName: 'Subnets', Value: newEnvSubnets }]
							: [])
					]
				})
			);
			toast.success(`Environment "${newEnvName}" created`);
			newEnvAppName = '';
			newEnvName = '';
			newEnvDesc = '';
			showCreateEnv = false;
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
			await eb().send(new TerminateEnvironmentCommand({ EnvironmentName: envName }));
			toast.success(`Environment "${envName}" terminating`);
			await loadData();
		} catch (e) {
			toast.error('Failed to terminate environment: ' + String(e));
		}
	}

	function toggleEnvDetail(envName: string) {
		const next = new Set(expandedEnvs);
		if (next.has(envName)) {
			next.delete(envName);
		} else {
			next.add(envName);
		}
		expandedEnvs = next;
	}

	async function createVersion() {
		if (!newVerAppName.trim()) {
			toast.error('Application name is required');
			return;
		}
		if (!newVerLabel.trim()) {
			toast.error('Version label is required');
			return;
		}
		creatingVersion = true;
		try {
			await eb().send(
				new CreateApplicationVersionCommand({
					ApplicationName: newVerAppName.trim(),
					VersionLabel: newVerLabel.trim(),
					Description: newVerDesc.trim() || undefined,
					SourceBundle:
						newVerS3Bucket && newVerS3Key
							? { S3Bucket: newVerS3Bucket, S3Key: newVerS3Key }
							: undefined
				})
			);
			toast.success(`Version "${newVerLabel}" created`);
			newVerAppName = '';
			newVerLabel = '';
			newVerDesc = '';
			newVerS3Bucket = '';
			newVerS3Key = '';
			showCreateVersion = false;
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
			await eb().send(
				new DeleteApplicationVersionCommand({ ApplicationName: appName, VersionLabel: versionLabel })
			);
			toast.success(`Version "${versionLabel}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete version: ' + String(e));
		}
	}

	// Config templates CRUD (improvement #17)
	async function createTemplate() {
		if (!newTmplAppName.trim() || !newTmplName.trim()) {
			toast.error('Application name and template name are required');
			return;
		}
		creatingTemplate = true;
		try {
			await eb().send(
				new CreateConfigurationTemplateCommand({
					ApplicationName: newTmplAppName.trim(),
					TemplateName: newTmplName.trim(),
					SolutionStackName: newTmplStack || undefined
				})
			);
			toast.success(`Template "${newTmplName}" created`);
			newTmplAppName = '';
			newTmplName = '';
			newTmplStack = '';
			showCreateTemplate = false;
			// Note: DescribeApplications includes template names; reload applications to refresh template data
			await loadData();
		} catch (e) {
			toast.error('Failed to create template: ' + String(e));
		} finally {
			creatingTemplate = false;
		}
	}

	async function deleteTemplate(appName: string, templateName: string) {
		const ok = await confirmDestructive(
			`Delete configuration template "${templateName}" from "${appName}"?`
		);
		if (!ok) return;
		try {
			await eb().send(
				new DeleteConfigurationTemplateCommand({ ApplicationName: appName, TemplateName: templateName })
			);
			toast.success(`Template "${templateName}" deleted`);
			// Remove from local state
			configTemplates = configTemplates.filter(
				(t) => !(t.ApplicationName === appName && t.TemplateName === templateName)
			);
		} catch (e) {
			toast.error('Failed to delete template: ' + String(e));
		}
	}

	// Platform versions CRUD (improvement #18)
	async function createPlatformVersion() {
		if (!newPlatformName.trim() || !newPlatformVersion.trim()) {
			toast.error('Platform name and version are required');
			return;
		}
		creatingPlatform = true;
		try {
			await eb().send(
				new CreatePlatformVersionCommand({
					PlatformName: newPlatformName.trim(),
					PlatformVersion: newPlatformVersion.trim(),
					PlatformDefinitionBundle: { S3Bucket: undefined, S3Key: undefined }
				})
			);
			toast.success(`Platform "${newPlatformName}/${newPlatformVersion}" created`);
			newPlatformName = '';
			newPlatformVersion = '';
			showCreatePlatform = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to create platform version: ' + String(e));
		} finally {
			creatingPlatform = false;
		}
	}

	async function deletePlatformVersion(platformArn: string) {
		const ok = await confirmDestructive(`Delete platform version "${platformArn}"?`);
		if (!ok) return;
		try {
			await eb().send(new DeletePlatformVersionCommand({ PlatformArn: platformArn }));
			toast.success('Platform version deleted');
			await loadData();
		} catch (e) {
			toast.error('Failed to delete platform version: ' + String(e));
		}
	}

	// CNAME swap (improvement #20)
	async function swapCNAMEs() {
		if (!swappingSrc || !swappingDst) {
			toast.error('Both source and destination environments are required');
			return;
		}
		swapping = true;
		try {
			await eb().send(
				new SwapEnvironmentCNAMEsCommand({
					SourceEnvironmentName: swappingSrc,
					DestinationEnvironmentName: swappingDst
				})
			);
			toast.success(`CNAMEs swapped between "${swappingSrc}" and "${swappingDst}"`);
			showCnameSwap = false;
			swappingSrc = '';
			swappingDst = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to swap CNAMEs: ' + String(e));
		} finally {
			swapping = false;
		}
	}

	// Applications, environments, versions, solution stacks, and platform
	// versions are all region-scoped; clear them before reloading so stale
	// data from the old region never lingers.
	onRegionChange(() => {
		applications = [];
		environments = [];
		versions = [];
		solutionStacks = [];
		platformVersions = [];
		configTemplates = [];
		expandedEnvs = new Set();
		void loadData();
	});
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Leaf class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Elastic Beanstalk</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">
					Deploy and scale web applications and services
				</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<!-- CNAME Swap button (improvement #20) -->
			{#if environments.length >= 2}
				<button
					onclick={() => (showCnameSwap = true)}
					title="Swap CNAMEs"
					class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
				>
					<ArrowLeftRight class="w-4 h-4" /> Swap CNAMEs
				</button>
			{/if}
			<button
				onclick={loadData}
				title="Refresh"
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Box class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{applications.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Applications</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Server class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{environments.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Environments</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg">
				<CheckCircle class="w-5 h-5 text-emerald-600 dark:text-emerald-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{readyEnvs}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Ready</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Package class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{versions.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Versions</p>
			</div>
		</div>
	</div>

	<!-- CNAME Swap Dialog (improvement #20) -->
	{#if showCnameSwap}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Swap Environment CNAMEs</h2>
				<p class="text-sm text-gray-500 dark:text-gray-400">
					Select two environments to swap their CNAMEs (blue/green deployment).
				</p>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="swapping-src">Source Environment</label>
						<select id="swapping-src"
							bind:value={swappingSrc}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">Select...</option>
							{#each environments as env}
								<option value={env.EnvironmentName}>{env.EnvironmentName}</option>
							{/each}
						</select>
					</div>
					<div class="flex justify-center">
						<ArrowLeftRight class="w-5 h-5 text-gray-400" />
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="swapping-dst">Destination Environment</label>
						<select id="swapping-dst"
							bind:value={swappingDst}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">Select...</option>
							{#each environments.filter((e) => e.EnvironmentName !== swappingSrc) as env}
								<option value={env.EnvironmentName}>{env.EnvironmentName}</option>
							{/each}
						</select>
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button
						onclick={swapCNAMEs}
						disabled={swapping}
						class="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
					>
						{swapping ? 'Swapping...' : 'Swap CNAMEs'}
					</button>
					<button
						onclick={() => (showCnameSwap = false)}
						class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
						>Cancel</button
					>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Application Modal -->
	{#if showCreateApp}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Application</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-app-name">Application Name *</label>
						<input id="new-app-name"
							bind:value={newAppName}
							placeholder="my-application"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-app-desc">Description</label>
						<input id="new-app-desc"
							bind:value={newAppDesc}
							placeholder="Optional description"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button
						onclick={createApplication}
						disabled={creatingApp}
						class="flex-1 px-4 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700 disabled:opacity-50"
					>
						{creatingApp ? 'Creating...' : 'Create'}
					</button>
					<button
						onclick={() => (showCreateApp = false)}
						class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
						>Cancel</button
					>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Environment Modal (improvements #14, #15, #22) -->
	{#if showCreateEnv}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div
				class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg space-y-4 max-h-[90vh] overflow-y-auto"
			>
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Environment</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-env-app-name">Application Name *</label>
						<select id="new-env-app-name"
							bind:value={newEnvAppName}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">Select application...</option>
							{#each applications as app}
								<option value={app.ApplicationName}>{app.ApplicationName}</option>
							{/each}
						</select>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-env-name">Environment Name *</label>
						<input id="new-env-name"
							bind:value={newEnvName}
							placeholder="my-env"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-env-solution-stack">Solution Stack</label>
						<select id="new-env-solution-stack"
							bind:value={newEnvSolutionStack}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							{#each solutionStacks as stack}
								<option value={stack}>{stack}</option>
							{/each}
						</select>
					</div>
					<!-- Tier Type (improvement #1) -->
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-env-tier-type">Environment Tier</label>
						<select id="new-env-tier-type"
							bind:value={newEnvTierType}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="WebServer">WebServer (web app)</option>
							<option value="Worker">Worker (background processing)</option>
						</select>
					</div>
					<!-- Load Balancer Type (improvement #14, #22) -->
					{#if newEnvTierType === 'WebServer'}
						<div>
							<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
								 for="new-env-lb-type">Load Balancer Type</label>
							<select id="new-env-lb-type"
								bind:value={newEnvLBType}
								class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							>
								<option value="application">Application (ALB)</option>
								<option value="network">Network (NLB)</option>
								<option value="classic">Classic (ELB)</option>
							</select>
						</div>
					{/if}
					<!-- VPC Config (improvement #15) -->
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-env-vpc-id">VPC ID</label>
						<input id="new-env-vpc-id"
							bind:value={newEnvVPCId}
							placeholder="vpc-xxxxxxxx (optional)"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-env-subnets">Subnets</label>
						<input id="new-env-subnets"
							bind:value={newEnvSubnets}
							placeholder="subnet-xxx,subnet-yyy (optional)"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-env-desc">Description</label>
						<input id="new-env-desc"
							bind:value={newEnvDesc}
							placeholder="Optional description"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button
						onclick={createEnvironment}
						disabled={creatingEnv}
						class="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
					>
						{creatingEnv ? 'Creating...' : 'Create'}
					</button>
					<button
						onclick={() => (showCreateEnv = false)}
						class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
						>Cancel</button
					>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Version Modal (improvement #21: S3 fields) -->
	{#if showCreateVersion}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
					Create Application Version
				</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-ver-app-name">Application Name *</label>
						<select id="new-ver-app-name"
							bind:value={newVerAppName}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">Select application...</option>
							{#each applications as app}
								<option value={app.ApplicationName}>{app.ApplicationName}</option>
							{/each}
						</select>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-ver-label">Version Label *</label>
						<input id="new-ver-label"
							bind:value={newVerLabel}
							placeholder="v1.0.0"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-ver-desc">Description</label>
						<input id="new-ver-desc"
							bind:value={newVerDesc}
							placeholder="Optional description"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<!-- S3 Source Bundle (improvement #21) -->
					<div class="border-t border-gray-200 dark:border-gray-700 pt-2">
						<p class="text-xs text-gray-500 dark:text-gray-400 mb-2">
							S3 Source Bundle (optional)
						</p>
						<input
							bind:value={newVerS3Bucket}
							placeholder="S3 bucket name"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white mb-2"
						/>
						<input
							bind:value={newVerS3Key}
							placeholder="S3 object key (e.g. app/v1.zip)"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button
						onclick={createVersion}
						disabled={creatingVersion}
						class="flex-1 px-4 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700 disabled:opacity-50"
					>
						{creatingVersion ? 'Creating...' : 'Create'}
					</button>
					<button
						onclick={() => (showCreateVersion = false)}
						class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
						>Cancel</button
					>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Config Template Modal (improvement #17) -->
	{#if showCreateTemplate}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
					Create Configuration Template
				</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-tmpl-app-name">Application Name *</label>
						<select id="new-tmpl-app-name"
							bind:value={newTmplAppName}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">Select application...</option>
							{#each applications as app}
								<option value={app.ApplicationName}>{app.ApplicationName}</option>
							{/each}
						</select>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-tmpl-name">Template Name *</label>
						<input id="new-tmpl-name"
							bind:value={newTmplName}
							placeholder="my-template"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-tmpl-stack">Solution Stack</label>
						<select id="new-tmpl-stack"
							bind:value={newTmplStack}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">None</option>
							{#each solutionStacks as stack}
								<option value={stack}>{stack}</option>
							{/each}
						</select>
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button
						onclick={createTemplate}
						disabled={creatingTemplate}
						class="flex-1 px-4 py-2 bg-orange-600 text-white rounded-lg text-sm font-medium hover:bg-orange-700 disabled:opacity-50"
					>
						{creatingTemplate ? 'Creating...' : 'Create'}
					</button>
					<button
						onclick={() => (showCreateTemplate = false)}
						class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
						>Cancel</button
					>
				</div>
			</div>
		</div>
	{/if}

	<!-- Create Platform Version Modal (improvement #18) -->
	{#if showCreatePlatform}
		<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
			<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
					Create Platform Version
				</h2>
				<div class="space-y-3">
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-platform-name">Platform Name *</label>
						<input id="new-platform-name"
							bind:value={newPlatformName}
							placeholder="MyCustomPlatform"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							 for="new-platform-version">Platform Version *</label>
						<input id="new-platform-version"
							bind:value={newPlatformVersion}
							placeholder="1.0.0"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				</div>
				<div class="flex gap-3 pt-2">
					<button
						onclick={createPlatformVersion}
						disabled={creatingPlatform}
						class="flex-1 px-4 py-2 bg-cyan-600 text-white rounded-lg text-sm font-medium hover:bg-cyan-700 disabled:opacity-50"
					>
						{creatingPlatform ? 'Creating...' : 'Create'}
					</button>
					<button
						onclick={() => (showCreatePlatform = false)}
						class="flex-1 px-4 py-2 border border-gray-200 dark:border-gray-600 rounded-lg text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
						>Cancel</button
					>
				</div>
			</div>
		</div>
	{/if}

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<div class="flex gap-2 flex-wrap">
				<!-- Tabs: applications, environments, versions, templates, platforms (improvements #17, #18) -->
				{#each [['applications', 'Applications', 'green'], ['environments', 'Environments', 'blue'], ['versions', 'Versions', 'purple'], ['templates', 'Config Templates', 'orange'], ['platforms', 'Platforms', 'cyan']] as [tab, label, color]}
					<button
						onclick={() => {
							activeTab = tab as TabName;
							searchQuery = '';
						}}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab
							? `bg-${color}-600 text-white`
							: 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}"
					>
						{label}
					</button>
				{/each}
			</div>
			<div class="flex items-center gap-2">
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input
						bind:value={searchQuery}
						placeholder="Search..."
						class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48"
					/>
				</div>
				{#if activeTab === 'applications'}
					<button
						onclick={() => (showCreateApp = true)}
						class="flex items-center gap-1 px-3 py-2 bg-green-600 text-white rounded-lg text-sm font-medium hover:bg-green-700"
					>
						<Plus class="w-4 h-4" /> New
					</button>
				{:else if activeTab === 'environments'}
					<button
						onclick={() => (showCreateEnv = true)}
						class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
						disabled={applications.length === 0}
					>
						<Plus class="w-4 h-4" /> New
					</button>
				{:else if activeTab === 'versions'}
					<button
						onclick={() => (showCreateVersion = true)}
						class="flex items-center gap-1 px-3 py-2 bg-purple-600 text-white rounded-lg text-sm font-medium hover:bg-purple-700"
						disabled={applications.length === 0}
					>
						<Plus class="w-4 h-4" /> New
					</button>
				{:else if activeTab === 'templates'}
					<button
						onclick={() => (showCreateTemplate = true)}
						class="flex items-center gap-1 px-3 py-2 bg-orange-600 text-white rounded-lg text-sm font-medium hover:bg-orange-700"
						disabled={applications.length === 0}
					>
						<Plus class="w-4 h-4" /> New
					</button>
				{:else if activeTab === 'platforms'}
					<button
						onclick={() => (showCreatePlatform = true)}
						class="flex items-center gap-1 px-3 py-2 bg-cyan-600 text-white rounded-lg text-sm font-medium hover:bg-cyan-700"
					>
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
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No applications found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<Box class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{app.ApplicationName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">
											{app.Description ?? 'No description'}
										</p>
									</div>
								</div>
								<div class="flex items-center gap-3">
									<span class="text-xs text-gray-400">{(app.Versions ?? []).length} versions</span>
									<button
										onclick={() => deleteApplication(app.ApplicationName!)}
										title="Delete"
										class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'environments'}
				{#if filteredEnvs.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No environments found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredEnvs as env}
							<!-- Environment detail panel (improvement #19) -->
							<div class="rounded-lg border border-gray-100 dark:border-slate-600 overflow-hidden">
								<div
									class="flex items-center justify-between p-3 bg-gray-50 dark:bg-slate-700/50 cursor-pointer"
									onclick={() => toggleEnvDetail(env.EnvironmentName ?? '')}
									role="button"
									tabindex="0"
									onkeydown={(e) =>
										e.key === 'Enter' && toggleEnvDetail(env.EnvironmentName ?? '')}
								>
									<div class="flex items-center gap-3">
										{#if expandedEnvs.has(env.EnvironmentName ?? '')}
											<ChevronDown class="w-4 h-4 text-gray-400" />
										{:else}
											<ChevronRight class="w-4 h-4 text-gray-400" />
										{/if}
										<Server class="w-5 h-5 text-blue-500" />
										<div>
											<p class="font-medium text-gray-900 dark:text-white">
												{env.EnvironmentName}
											</p>
											<p class="text-xs text-gray-500 dark:text-gray-400">
												{env.ApplicationName} · {env.SolutionStackName ?? 'Unknown stack'}
											</p>
										</div>
									</div>
									<div class="flex items-center gap-2">
										<span
											class="text-xs px-2 py-1 rounded-full {env.Health === 'Green'
												? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
												: env.Health === 'Red'
													? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
													: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400'}"
											>{env.Health}</span
										>
										<span class="text-xs text-gray-500 dark:text-gray-400">{env.Status}</span>
										<button
											onclick={(e) => {
												e.stopPropagation();
												terminateEnvironment(env.ApplicationName!, env.EnvironmentName!);
											}}
											title="Terminate"
											class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400"
										>
											<Trash2 class="w-4 h-4" />
										</button>
									</div>
								</div>
								<!-- Expanded detail panel (improvement #19) -->
								{#if expandedEnvs.has(env.EnvironmentName ?? '')}
									<div
										class="px-4 py-3 bg-white dark:bg-slate-800 border-t border-gray-100 dark:border-slate-600 grid grid-cols-2 sm:grid-cols-3 gap-3 text-sm"
									>
										<div>
											<span class="text-xs font-medium text-gray-500 dark:text-gray-400">Tier</span>
											<p class="text-gray-900 dark:text-white">
												{env.Tier?.Name ?? 'WebServer'} / {env.Tier?.Type ?? 'Standard'}
											</p>
										</div>
										<div>
											<span class="text-xs font-medium text-gray-500 dark:text-gray-400">CNAME</span>
											<p class="text-gray-900 dark:text-white truncate">{env.CNAME ?? '—'}</p>
										</div>
										<div>
											<span class="text-xs font-medium text-gray-500 dark:text-gray-400"
												>Endpoint URL</span
											>
											<p class="text-gray-900 dark:text-white truncate">
												{env.EndpointURL ?? '—'}
											</p>
										</div>
										<div>
											<span class="text-xs font-medium text-gray-500 dark:text-gray-400"
												>Environment ID</span
											>
											<p class="text-gray-900 dark:text-white">{env.EnvironmentId ?? '—'}</p>
										</div>
										<div>
											<span class="text-xs font-medium text-gray-500 dark:text-gray-400"
												>Health Status</span
											>
											<p class="text-gray-900 dark:text-white">{env.Health ?? '—'}</p>
										</div>
										<div>
											<span class="text-xs font-medium text-gray-500 dark:text-gray-400">Status</span>
											<p class="text-gray-900 dark:text-white">{env.Status ?? '—'}</p>
										</div>
									</div>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'versions'}
				{#if filteredVersions.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No application versions found
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredVersions as ver}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<Package class="w-5 h-5 text-purple-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{ver.VersionLabel}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">
											{ver.ApplicationName} · {ver.Description ?? 'No description'}
										</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span
										class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-slate-600 text-gray-600 dark:text-gray-300"
										>{ver.Status}</span
									>
									<button
										onclick={() => deleteVersion(ver.ApplicationName!, ver.VersionLabel!)}
										title="Delete"
										class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'templates'}
				<!-- Config Templates tab (improvement #17) -->
				{#if filteredTemplates.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No configuration templates found.
						{#if applications.length > 0}
							<button
								onclick={() => (showCreateTemplate = true)}
								class="ml-1 text-orange-600 hover:underline"
							>
								Create one?
							</button>
						{/if}
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTemplates as tmpl}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<Layout class="w-5 h-5 text-orange-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{tmpl.TemplateName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">
											{tmpl.ApplicationName}
											{#if tmpl.SolutionStackName}
												· {tmpl.SolutionStackName}
											{/if}
										</p>
									</div>
								</div>
								<button
									onclick={() => deleteTemplate(tmpl.ApplicationName, tmpl.TemplateName)}
									title="Delete"
									class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400"
								>
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'platforms'}
				<!-- Platform versions tab (improvement #18) -->
				{#if filteredPlatforms.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No custom platform versions found.
						<button onclick={() => (showCreatePlatform = true)} class="ml-1 text-cyan-600 hover:underline">
							Create one?
						</button>
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPlatforms as pv}
							<div
								class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50"
							>
								<div class="flex items-center gap-3">
									<Layers class="w-5 h-5 text-cyan-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white text-xs truncate max-w-md">
											{pv.PlatformArn}
										</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{pv.PlatformStatus}</p>
									</div>
								</div>
								<button
									onclick={() => deletePlatformVersion(pv.PlatformArn!)}
									title="Delete"
									class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-gray-400 hover:text-red-600 dark:hover:text-red-400"
								>
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
