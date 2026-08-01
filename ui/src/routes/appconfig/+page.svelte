<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAppConfigClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		ListEnvironmentsCommand,
		ListConfigurationProfilesCommand,
		ListDeploymentsCommand,
		ListExtensionsCommand,
		ListExtensionAssociationsCommand,
		ListDeploymentStrategiesCommand,
		ListHostedConfigurationVersionsCommand,
		DeleteApplicationCommand,
		DeleteExtensionCommand,
		DeleteExtensionAssociationCommand,
		DeleteDeploymentStrategyCommand,
		DeleteEnvironmentCommand,
		DeleteConfigurationProfileCommand,
		CreateApplicationCommand,
		CreateExtensionCommand,
		CreateExtensionAssociationCommand,
		CreateEnvironmentCommand,
		CreateConfigurationProfileCommand,
		CreateDeploymentStrategyCommand,
		StartDeploymentCommand,
		StopDeploymentCommand,
		GetAccountSettingsCommand,
		type Application,
		type Environment,
		type ConfigurationProfile,
		type DeploymentSummary,
		type Extension,
		type ExtensionAssociationSummary,
		type DeploymentStrategy,
		type HostedConfigurationVersionSummary,
		type GrowthType,
		type ReplicateTo,
	} from '@aws-sdk/client-appconfig';
	import { toast } from 'svelte-sonner';
	import {
		Settings, Search, RefreshCw, Plus, Trash2,
		ChevronRight, Globe,
		Workflow,
		FileCode, Code2,
		XCircle, AlertCircle, Timer,
		Database,
		ArrowRight, Gauge,
		Settings2, Layout, Boxes, Shield, ExternalLink,
		Puzzle, Link, Play, Square, BarChart3, Archive,
		ChevronDown, ChevronUp, Eye
	} from 'lucide-svelte';

	const appconfig = regionalClient(getAppConfigClient);

	// Tabs
	type Tab = 'applications' | 'strategies' | 'extensions' | 'associations' | 'settings';
	let activeTab = $state<Tab>('applications');

	// ── Applications Tab ─────────────────────────────────────────────────────
	let loading = $state(false);
	let searchQuery = $state('');
	let applications = $state<Application[]>([]);
	let selectedApp = $state<Application | null>(null);
	let environments = $state<Environment[]>([]);
	let profiles = $state<ConfigurationProfile[]>([]);
	let deployments = $state<DeploymentSummary[]>([]);
	let loadingDetails = $state(false);
	let selectedEnvId = $state<string>('');
	let configVersions = $state<HostedConfigurationVersionSummary[]>([]);
	let selectedProfileId = $state<string>('');
	let loadingVersions = $state(false);
	let appDetailTab = $state<'envs' | 'profiles' | 'deployments' | 'versions'>('envs');

	// App create modal
	let showCreateModal = $state(false);
	let newAppName = $state('');
	let creating = $state(false);

	// Environment create modal
	let showCreateEnvModal = $state(false);
	let newEnvName = $state('');
	let creatingEnv = $state(false);

	// Profile create modal
	let showCreateProfileModal = $state(false);
	let newProfileName = $state('');
	let newProfileLocationUri = $state('hosted');
	let newProfileType = $state('AWS.Freeform');
	let creatingProfile = $state(false);

	// Deploy modal
	let showDeployModal = $state(false);
	let deployProfileId = $state('');
	let deployStrategyId = $state('');
	let deployVersion = $state('');
	let deploying = $state(false);

	// ── Strategies Tab ───────────────────────────────────────────────────────
	let strategies = $state<DeploymentStrategy[]>([]);
	let loadingStrategies = $state(false);
	let showCreateStratModal = $state(false);
	let newStratName = $state('');
	let newStratDesc = $state('');
	let newStratDuration = $state(10);
	let newStratBake = $state(0);
	let newStratGrowth = $state(100);
	let newStratGrowthType = $state('LINEAR');
	let newStratReplicateTo = $state('NONE');
	let creatingStrat = $state(false);

	// ── Extensions Tab ───────────────────────────────────────────────────────
	let extensions = $state<Extension[]>([]);
	let loadingExtensions = $state(false);
	let extensionSearch = $state('');
	let showCreateExtModal = $state(false);
	let newExtName = $state('');
	let newExtDesc = $state('');
	let creatingExt = $state(false);

	// ── Associations Tab ─────────────────────────────────────────────────────
	let associations = $state<ExtensionAssociationSummary[]>([]);
	let loadingAssociations = $state(false);
	let showCreateAssocModal = $state(false);
	let newAssocExtId = $state('');
	let newAssocResource = $state('');
	let creatingAssoc = $state(false);

	// ── Account Settings Tab ─────────────────────────────────────────────────
	let accountSettings = $state<{ DeletionProtection?: { Enabled?: boolean } } | null>(null);
	let loadingSettings = $state(false);

	// Derived
	const filteredApps = $derived(
		applications.filter(a => a.Name?.toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredExtensions = $derived(
		extensions.filter(e => e.Name?.toLowerCase().includes(extensionSearch.toLowerCase()))
	);

	// ── Data loaders ─────────────────────────────────────────────────────────
	async function loadApps() {
		loading = true;
		try {
			const res = await appconfig().send(new ListApplicationsCommand({}));
			applications = res.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load applications: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadStrategies() {
		loadingStrategies = true;
		try {
			const res = await appconfig().send(new ListDeploymentStrategiesCommand({}));
			strategies = res.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load strategies: ${(err as Error).message}`);
		} finally {
			loadingStrategies = false;
		}
	}

	async function loadExtensions() {
		loadingExtensions = true;
		try {
			const res = await appconfig().send(new ListExtensionsCommand({}));
			extensions = res.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load extensions: ${(err as Error).message}`);
		} finally {
			loadingExtensions = false;
		}
	}

	async function loadAssociations() {
		loadingAssociations = true;
		try {
			const res = await appconfig().send(new ListExtensionAssociationsCommand({}));
			associations = res.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load associations: ${(err as Error).message}`);
		} finally {
			loadingAssociations = false;
		}
	}

	async function loadAccountSettings() {
		loadingSettings = true;
		try {
			const res = await appconfig().send(new GetAccountSettingsCommand({}));
			accountSettings = res as typeof accountSettings;
		} catch (err: unknown) {
			toast.error(`Failed to load account settings: ${(err as Error).message}`);
		} finally {
			loadingSettings = false;
		}
	}

	async function selectApp(app: Application) {
		selectedApp = app;
		environments = [];
		profiles = [];
		deployments = [];
		configVersions = [];
		selectedEnvId = '';
		selectedProfileId = '';
		appDetailTab = 'envs';
		loadingDetails = true;
		try {
			const [envRes, profRes] = await Promise.all([
				appconfig().send(new ListEnvironmentsCommand({ ApplicationId: app.Id })),
				appconfig().send(new ListConfigurationProfilesCommand({ ApplicationId: app.Id })),
			]);
			environments = envRes.Items ?? [];
			profiles = profRes.Items ?? [];

			if (environments.length > 0) {
				selectedEnvId = environments[0].Id ?? '';
				await loadDeployments(app.Id!, selectedEnvId);
			}
		} catch (err: unknown) {
			toast.error(`Failed to load app details: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function loadDeployments(appId: string, envId: string) {
		if (!envId) return;
		loadingDetails = true;
		try {
			const depRes = await appconfig().send(new ListDeploymentsCommand({ ApplicationId: appId, EnvironmentId: envId }));
			deployments = depRes.Items ?? [];
		} catch {
			deployments = [];
		} finally {
			loadingDetails = false;
		}
	}

	async function loadConfigVersions(appId: string, profileId: string) {
		if (!profileId) return;
		loadingVersions = true;
		try {
			const res = await appconfig().send(new ListHostedConfigurationVersionsCommand({
				ApplicationId: appId,
				ConfigurationProfileId: profileId
			}));
			configVersions = res.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load versions: ${(err as Error).message}`);
		} finally {
			loadingVersions = false;
		}
	}

	// ── Application CRUD ──────────────────────────────────────────────────────
	async function createApplication() {
		if (!newAppName.trim()) return;
		creating = true;
		try {
			await appconfig().send(new CreateApplicationCommand({ Name: newAppName.trim() }));
			toast.success(`Application "${newAppName}" created`);
			showCreateModal = false;
			newAppName = '';
			await loadApps();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApplication(id: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Delete Application', message: 'Delete this AppConfig application? All configuration profiles and environments will be permanently removed.' })) return;
		try {
			await appconfig().send(new DeleteApplicationCommand({ ApplicationId: id }));
			toast.success('Application deleted');
			if (selectedApp?.Id === id) selectedApp = null;
			await loadApps();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ── Environment CRUD ──────────────────────────────────────────────────────
	async function createEnvironment() {
		if (!newEnvName.trim() || !selectedApp) return;
		creatingEnv = true;
		try {
			await appconfig().send(new CreateEnvironmentCommand({ ApplicationId: selectedApp.Id, Name: newEnvName.trim() }));
			toast.success(`Environment "${newEnvName}" created`);
			showCreateEnvModal = false;
			newEnvName = '';
			const envRes = await appconfig().send(new ListEnvironmentsCommand({ ApplicationId: selectedApp.Id }));
			environments = envRes.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed: ${(err as Error).message}`);
		} finally {
			creatingEnv = false;
		}
	}

	async function deleteEnvironment(envId: string | undefined) {
		if (!envId || !selectedApp || !await confirmDestructive({ title: 'Delete Environment', message: 'Delete this environment?' })) return;
		try {
			await appconfig().send(new DeleteEnvironmentCommand({ ApplicationId: selectedApp.Id, EnvironmentId: envId }));
			toast.success('Environment deleted');
			const envRes = await appconfig().send(new ListEnvironmentsCommand({ ApplicationId: selectedApp.Id }));
			environments = envRes.Items ?? [];
			if (selectedEnvId === envId) {
				selectedEnvId = environments[0]?.Id ?? '';
				if (selectedEnvId) await loadDeployments(selectedApp.Id!, selectedEnvId);
			}
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ── Profile CRUD ──────────────────────────────────────────────────────────
	async function createProfile() {
		if (!newProfileName.trim() || !selectedApp) return;
		creatingProfile = true;
		try {
			await appconfig().send(new CreateConfigurationProfileCommand({
				ApplicationId: selectedApp.Id,
				Name: newProfileName.trim(),
				LocationUri: newProfileLocationUri || 'hosted',
				Type: newProfileType || 'AWS.Freeform',
			}));
			toast.success(`Profile "${newProfileName}" created`);
			showCreateProfileModal = false;
			newProfileName = '';
			const profRes = await appconfig().send(new ListConfigurationProfilesCommand({ ApplicationId: selectedApp.Id }));
			profiles = profRes.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed: ${(err as Error).message}`);
		} finally {
			creatingProfile = false;
		}
	}

	async function deleteProfile(profileId: string | undefined) {
		if (!profileId || !selectedApp || !await confirmDestructive({ title: 'Delete Profile', message: 'Delete this configuration profile?' })) return;
		try {
			await appconfig().send(new DeleteConfigurationProfileCommand({ ApplicationId: selectedApp.Id, ConfigurationProfileId: profileId }));
			toast.success('Profile deleted');
			const profRes = await appconfig().send(new ListConfigurationProfilesCommand({ ApplicationId: selectedApp.Id }));
			profiles = profRes.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ── Deployment actions ────────────────────────────────────────────────────
	async function startDeployment() {
		if (!selectedApp || !deployProfileId || !deployStrategyId || !deployVersion) return;
		deploying = true;
		try {
			await appconfig().send(new StartDeploymentCommand({
				ApplicationId: selectedApp.Id,
				EnvironmentId: selectedEnvId,
				ConfigurationProfileId: deployProfileId,
				DeploymentStrategyId: deployStrategyId,
				ConfigurationVersion: deployVersion,
			}));
			toast.success('Deployment started');
			showDeployModal = false;
			deployProfileId = '';
			deployStrategyId = '';
			deployVersion = '';
			await loadDeployments(selectedApp.Id!, selectedEnvId);
		} catch (err: unknown) {
			toast.error(`Deployment failed: ${(err as Error).message}`);
		} finally {
			deploying = false;
		}
	}

	async function stopDeployment(depNum: number | undefined) {
		if (!depNum || !selectedApp || !selectedEnvId) return;
		if (!await confirmDestructive({ title: 'Stop Deployment', message: 'Roll back this deployment?' })) return;
		try {
			await appconfig().send(new StopDeploymentCommand({
				ApplicationId: selectedApp.Id,
				EnvironmentId: selectedEnvId,
				DeploymentNumber: depNum,
			}));
			toast.success('Deployment stopped');
			await loadDeployments(selectedApp.Id!, selectedEnvId);
		} catch (err: unknown) {
			toast.error(`Stop failed: ${(err as Error).message}`);
		}
	}

	// ── Strategy CRUD ─────────────────────────────────────────────────────────
	async function createStrategy() {
		if (!newStratName.trim()) return;
		creatingStrat = true;
		try {
			await appconfig().send(new CreateDeploymentStrategyCommand({
				Name: newStratName.trim(),
				Description: newStratDesc.trim() || undefined,
				DeploymentDurationInMinutes: newStratDuration,
				FinalBakeTimeInMinutes: newStratBake,
				GrowthFactor: newStratGrowth,
				GrowthType: newStratGrowthType as GrowthType,
				ReplicateTo: newStratReplicateTo as ReplicateTo,
			}));
			toast.success(`Strategy "${newStratName}" created`);
			showCreateStratModal = false;
			newStratName = '';
			newStratDesc = '';
			await loadStrategies();
		} catch (err: unknown) {
			toast.error(`Failed: ${(err as Error).message}`);
		} finally {
			creatingStrat = false;
		}
	}

	async function deleteStrategy(id: string | undefined, name: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Delete Strategy', message: `Delete deployment strategy "${name}"?` })) return;
		try {
			await appconfig().send(new DeleteDeploymentStrategyCommand({ DeploymentStrategyId: id }));
			toast.success('Strategy deleted');
			await loadStrategies();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ── Extension CRUD ────────────────────────────────────────────────────────
	async function createExtension() {
		if (!newExtName.trim()) return;
		creatingExt = true;
		try {
			await appconfig().send(new CreateExtensionCommand({
				Name: newExtName.trim(),
				Description: newExtDesc.trim() || undefined,
				Actions: {}
			}));
			toast.success(`Extension "${newExtName}" created`);
			showCreateExtModal = false;
			newExtName = '';
			newExtDesc = '';
			await loadExtensions();
		} catch (err: unknown) {
			toast.error(`Extension creation failed: ${(err as Error).message}`);
		} finally {
			creatingExt = false;
		}
	}

	async function deleteExtension(id: string | undefined, name: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Delete Extension', message: `Delete extension "${name}"?` })) return;
		try {
			await appconfig().send(new DeleteExtensionCommand({ ExtensionIdentifier: id }));
			toast.success('Extension deleted');
			await loadExtensions();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ── Association CRUD ──────────────────────────────────────────────────────
	async function createAssociation() {
		if (!newAssocExtId.trim() || !newAssocResource.trim()) return;
		creatingAssoc = true;
		try {
			await appconfig().send(new CreateExtensionAssociationCommand({
				ExtensionIdentifier: newAssocExtId.trim(),
				ResourceIdentifier: newAssocResource.trim()
			}));
			toast.success('Association created');
			showCreateAssocModal = false;
			newAssocExtId = '';
			newAssocResource = '';
			await loadAssociations();
		} catch (err: unknown) {
			toast.error(`Failed: ${(err as Error).message}`);
		} finally {
			creatingAssoc = false;
		}
	}

	async function deleteAssociation(id: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Delete Association', message: 'Delete this extension association?' })) return;
		try {
			await appconfig().send(new DeleteExtensionAssociationCommand({ ExtensionAssociationId: id }));
			toast.success('Association deleted');
			await loadAssociations();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// ── Tab change ────────────────────────────────────────────────────────────
	function handleTabChange(tab: Tab) {
		activeTab = tab;
		if (tab === 'strategies' && strategies.length === 0 && !loadingStrategies) loadStrategies();
		if (tab === 'extensions' && extensions.length === 0 && !loadingExtensions) loadExtensions();
		if (tab === 'associations' && associations.length === 0 && !loadingAssociations) loadAssociations();
		if (tab === 'settings' && !accountSettings && !loadingSettings) loadAccountSettings();
	}

	function getDeploymentStatusIcon(status: string | undefined) {
		if (status === 'COMPLETE') return 'text-emerald-500';
		if (status === 'ROLLING_BACK' || status === 'ROLLED_BACK') return 'text-rose-500';
		if (status === 'DEPLOYING' || status === 'BAKING') return 'text-amber-500 animate-pulse';
		return 'text-slate-400';
	}

	// selectedApp and its whole detail chain (environments, profiles,
	// deployments, config versions) reference an application id that is
	// only unique within the region it was fetched from, and
	// strategies/extensions/associations/accountSettings are each lazily
	// loaded once and cached (see handleTabChange) — clear all of it and
	// reload whichever tab is active. `activeTab` is read via untrack()
	// because handleTabChange also writes it: without untrack, every tab
	// switch would re-trigger this region effect and double-fetch.
	onRegionChange(() => {
		selectedApp = null;
		environments = [];
		profiles = [];
		deployments = [];
		configVersions = [];
		selectedEnvId = '';
		selectedProfileId = '';
		appDetailTab = 'envs';
		strategies = [];
		extensions = [];
		associations = [];
		accountSettings = null;
		const tab = untrack(() => activeTab);
		if (tab === 'strategies') void loadStrategies();
		else if (tab === 'extensions') void loadExtensions();
		else if (tab === 'associations') void loadAssociations();
		else if (tab === 'settings') void loadAccountSettings();
		else void loadApps();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-blue-600/20 rounded-xl shadow-inner border border-blue-500/20">
				<Settings2 class="w-8 h-8 text-blue-600" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-blue-600 to-indigo-600 dark:from-blue-400 dark:to-indigo-400 bg-clip-text text-transparent italic tracking-tight">AppConfig Orchestration</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Managed application configuration lifecycles, rollout strategies, and feature flags.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button
				onclick={() => { if (activeTab === 'applications') loadApps(); else if (activeTab === 'strategies') loadStrategies(); else if (activeTab === 'extensions') loadExtensions(); else if (activeTab === 'associations') loadAssociations(); else loadAccountSettings(); }}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {(loading || loadingStrategies || loadingExtensions || loadingAssociations) ? 'animate-spin' : ''}" />
			</button>
			{#if activeTab === 'applications'}
				<button onclick={() => showCreateModal = true} class="flex items-center gap-2 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-black shadow-lg transition-all active:scale-95 uppercase text-xs tracking-widest">
					<Plus class="w-4 h-4" /> New Application
				</button>
			{:else if activeTab === 'strategies'}
				<button onclick={() => showCreateStratModal = true} class="flex items-center gap-2 px-5 py-2.5 bg-amber-600 hover:bg-amber-700 text-white rounded-xl font-black shadow-lg transition-all active:scale-95 uppercase text-xs tracking-widest">
					<Plus class="w-4 h-4" /> New Strategy
				</button>
			{:else if activeTab === 'extensions'}
				<button onclick={() => showCreateExtModal = true} class="flex items-center gap-2 px-5 py-2.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-xl font-black shadow-lg transition-all active:scale-95 uppercase text-xs tracking-widest">
					<Plus class="w-4 h-4" /> New Extension
				</button>
			{:else if activeTab === 'associations'}
				<button onclick={() => showCreateAssocModal = true} class="flex items-center gap-2 px-5 py-2.5 bg-violet-600 hover:bg-violet-700 text-white rounded-xl font-black shadow-lg transition-all active:scale-95 uppercase text-xs tracking-widest">
					<Plus class="w-4 h-4" /> New Association
				</button>
			{/if}
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex flex-wrap gap-2 p-1 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl w-fit">
		<button onclick={() => handleTabChange('applications')} class="flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest transition-all {activeTab === 'applications' ? 'bg-blue-600 text-white shadow-lg' : 'text-slate-500 hover:text-slate-900 dark:hover:text-white'}"><Layout class="w-3.5 h-3.5" /> Applications</button>
		<button onclick={() => handleTabChange('strategies')} class="flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest transition-all {activeTab === 'strategies' ? 'bg-amber-600 text-white shadow-lg' : 'text-slate-500 hover:text-slate-900 dark:hover:text-white'}"><BarChart3 class="w-3.5 h-3.5" /> Strategies</button>
		<button onclick={() => handleTabChange('extensions')} class="flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest transition-all {activeTab === 'extensions' ? 'bg-indigo-600 text-white shadow-lg' : 'text-slate-500 hover:text-slate-900 dark:hover:text-white'}"><Puzzle class="w-3.5 h-3.5" /> Extensions</button>
		<button onclick={() => handleTabChange('associations')} class="flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest transition-all {activeTab === 'associations' ? 'bg-violet-600 text-white shadow-lg' : 'text-slate-500 hover:text-slate-900 dark:hover:text-white'}"><Link class="w-3.5 h-3.5" /> Associations</button>
		<button onclick={() => handleTabChange('settings')} class="flex items-center gap-2 px-4 py-2 rounded-xl text-xs font-black uppercase tracking-widest transition-all {activeTab === 'settings' ? 'bg-slate-600 text-white shadow-lg' : 'text-slate-500 hover:text-slate-900 dark:hover:text-white'}"><Settings class="w-3.5 h-3.5" /> Settings</button>
	</div>

	<!-- ══ APPLICATIONS TAB ══════════════════════════════════════════════════ -->
	{#if activeTab === 'applications'}
	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- App List -->
		<div class="lg:col-span-3">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input type="text" bind:value={searchQuery} placeholder="Search apps..." class="w-full pl-9 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-blue-500 outline-none italic font-bold" />
					</div>
				</div>
				<div class="max-h-[600px] overflow-y-auto divide-y divide-slate-100 dark:divide-slate-700/50">
					{#if loading && !applications.length}
						{#each Array(3) as _}<div class="p-4 animate-pulse"><div class="h-10 bg-slate-200/50 rounded-lg"></div></div>{/each}
					{:else if filteredApps.length === 0}
						<div class="p-10 text-center text-slate-400 text-sm italic font-bold">No applications.</div>
					{:else}
						{#each filteredApps as app}
							<div role="button" tabindex="0" onclick={() => selectApp(app)} onkeydown={(e) => e.key === 'Enter' && selectApp(app)}
								class="p-4 flex items-center justify-between hover:bg-blue-500/5 cursor-pointer transition-all {selectedApp?.Id === app.Id ? 'bg-blue-500/10 border-l-4 border-blue-500' : 'border-l-4 border-transparent'}">
								<div class="flex items-center gap-3 min-w-0">
									<Layout class="w-4 h-4 text-blue-600 shrink-0" />
									<div class="min-w-0">
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate">{app.Name}</div>
										<div class="text-[8px] text-slate-400 font-mono opacity-60 truncate">{app.Id}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300 shrink-0" />
							</div>
						{/each}
					{/if}
				</div>
			</div>
		</div>

		<!-- App Detail -->
		<div class="lg:col-span-9">
			{#if selectedApp}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
					<!-- App Header -->
					<div class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-blue-500/5 to-indigo-500/5 flex justify-between items-start">
						<div>
							<h2 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">{selectedApp.Name}</h2>
							<div class="flex items-center gap-2 mt-2">
								<span class="px-2 py-0.5 rounded-lg bg-blue-500/10 text-blue-600 text-[9px] font-black uppercase border border-blue-500/20">{environments.length} ENVS</span>
								<span class="px-2 py-0.5 rounded-lg bg-emerald-500/10 text-emerald-600 text-[9px] font-black uppercase border border-emerald-500/20">{profiles.length} PROFILES</span>
								<span class="px-2 py-0.5 rounded-lg bg-amber-500/10 text-amber-600 text-[9px] font-black uppercase border border-amber-500/20">{deployments.length} DEPLOYMENTS</span>
							</div>
						</div>
						<div class="flex items-center gap-2">
							<button onclick={() => showDeployModal = true} class="flex items-center gap-1.5 px-3 py-2 bg-emerald-600 text-white rounded-xl text-[10px] font-black uppercase tracking-widest hover:bg-emerald-700 transition-all" title="Start Deployment">
								<Play class="w-3.5 h-3.5" /> Deploy
							</button>
							<button onclick={() => deleteApplication(selectedApp?.Id)} title="Delete Application" class="p-2 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-xl transition-all border border-rose-500/20">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>

					<!-- Detail Sub-tabs -->
					<div class="flex border-b border-slate-100 dark:border-slate-700/50">
						<button onclick={() => { appDetailTab = 'envs'; }} class="flex items-center gap-1.5 px-4 py-3 text-[10px] font-black uppercase tracking-widest border-b-2 transition-all {appDetailTab === 'envs' ? 'border-blue-500 text-blue-600' : 'border-transparent text-slate-400 hover:text-slate-700'}"><Globe class="w-3.5 h-3.5" /> Environments</button>
					<button onclick={() => { appDetailTab = 'profiles'; }} class="flex items-center gap-1.5 px-4 py-3 text-[10px] font-black uppercase tracking-widest border-b-2 transition-all {appDetailTab === 'profiles' ? 'border-blue-500 text-blue-600' : 'border-transparent text-slate-400 hover:text-slate-700'}"><FileCode class="w-3.5 h-3.5" /> Profiles</button>
					<button onclick={() => { appDetailTab = 'deployments'; }} class="flex items-center gap-1.5 px-4 py-3 text-[10px] font-black uppercase tracking-widest border-b-2 transition-all {appDetailTab === 'deployments' ? 'border-blue-500 text-blue-600' : 'border-transparent text-slate-400 hover:text-slate-700'}"><Workflow class="w-3.5 h-3.5" /> Deployments</button>
					<button onclick={() => { appDetailTab = 'versions'; if (selectedProfileId) loadConfigVersions(selectedApp!.Id!, selectedProfileId); }} class="flex items-center gap-1.5 px-4 py-3 text-[10px] font-black uppercase tracking-widest border-b-2 transition-all {appDetailTab === 'versions' ? 'border-blue-500 text-blue-600' : 'border-transparent text-slate-400 hover:text-slate-700'}"><Archive class="w-3.5 h-3.5" /> Config Versions</button>
					</div>

					<div class="p-6">
						<!-- Environments -->
						{#if appDetailTab === 'envs'}
							<div class="flex items-center justify-between mb-4">
								<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest italic">Target Environments</h3>
								<button onclick={() => showCreateEnvModal = true} class="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600/10 text-blue-600 rounded-lg text-[10px] font-black uppercase border border-blue-500/20 hover:bg-blue-600/20 transition-all">
									<Plus class="w-3 h-3" /> Add
								</button>
							</div>
							{#if environments.length === 0}
								<div class="p-8 text-center border border-dashed border-slate-200 dark:border-slate-700 rounded-2xl text-slate-400 text-sm italic">No environments.</div>
							{:else}
								<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
									{#each environments as env}
										<div class="p-4 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 flex items-center justify-between group">
											<div class="flex items-center gap-3">
												<div class="w-2 h-2 rounded-full {env.State === 'READY_FOR_DEPLOYMENT' ? 'bg-emerald-500' : 'bg-slate-400'}"></div>
												<div>
													<div class="text-xs font-black text-slate-900 dark:text-white uppercase italic">{env.Name}</div>
													<div class="text-[8px] text-slate-400 font-mono opacity-60">{env.State}</div>
												</div>
											</div>
											<button onclick={() => deleteEnvironment(env.Id)} class="p-1.5 rounded-lg text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-all"><Trash2 class="w-3.5 h-3.5" /></button>
										</div>
									{/each}
								</div>
							{/if}
						{/if}

						<!-- Profiles -->
						{#if appDetailTab === 'profiles'}
							<div class="flex items-center justify-between mb-4">
								<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest italic">Configuration Profiles</h3>
								<button onclick={() => showCreateProfileModal = true} class="flex items-center gap-1.5 px-3 py-1.5 bg-indigo-600/10 text-indigo-600 rounded-lg text-[10px] font-black uppercase border border-indigo-500/20 hover:bg-indigo-600/20 transition-all">
									<Plus class="w-3 h-3" /> Add
								</button>
							</div>
							{#if profiles.length === 0}
								<div class="p-8 text-center border border-dashed border-slate-200 dark:border-slate-700 rounded-2xl text-slate-400 text-sm italic">No profiles.</div>
							{:else}
								<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
									{#each profiles as prof}
										<div class="py-3 flex items-center justify-between group">
											<div class="flex items-center gap-3">
												<div class="p-1.5 bg-slate-100 dark:bg-slate-800 rounded-lg"><Code2 class="w-3.5 h-3.5 text-indigo-500" /></div>
												<div>
													<div class="text-xs font-black text-slate-900 dark:text-white uppercase italic">{prof.Name}</div>
													<div class="text-[8px] text-slate-400 uppercase">{prof.Type || 'FREEFORM'} · {prof.LocationUri}</div>
												</div>
											</div>
											<div class="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
												<button onclick={() => { selectedProfileId = prof.Id ?? ''; appDetailTab = 'versions'; loadConfigVersions(selectedApp!.Id!, prof.Id!); }}
													class="p-1.5 rounded-lg text-blue-500 hover:bg-blue-500/10"><Eye class="w-3.5 h-3.5" /></button>
												<button onclick={() => deleteProfile(prof.Id)} class="p-1.5 rounded-lg text-rose-400 hover:bg-rose-500/10"><Trash2 class="w-3.5 h-3.5" /></button>
											</div>
										</div>
									{/each}
								</div>
							{/if}
						{/if}

						<!-- Deployments -->
						{#if appDetailTab === 'deployments'}
							<div class="flex items-center justify-between mb-4">
								<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest italic">Rollout History</h3>
								{#if environments.length > 1}
									<select bind:value={selectedEnvId} onchange={() => loadDeployments(selectedApp!.Id!, selectedEnvId)}
										class="text-[10px] font-black uppercase bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-lg px-2 py-1 outline-none focus:ring-1 focus:ring-blue-500">
										{#each environments as env}<option value={env.Id}>{env.Name}</option>{/each}
									</select>
								{/if}
							</div>
							{#if loadingDetails}
								<div class="p-8 flex justify-center opacity-30"><RefreshCw class="w-6 h-6 text-blue-500 animate-spin" /></div>
							{:else if deployments.length === 0}
								<div class="p-8 text-center border border-dashed border-slate-200 dark:border-slate-700 rounded-2xl text-slate-400 text-sm italic">No deployments.</div>
							{:else}
								<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
									{#each [...deployments].reverse() as dep}
										<div class="py-4 flex items-center justify-between group">
											<div class="flex items-center gap-3">
												<div class="w-2 h-2 rounded-full {getDeploymentStatusIcon(dep.State).replace('text-', 'bg-').split(' ')[0]}"></div>
												<div>
													<div class="text-xs font-black text-slate-900 dark:text-white uppercase italic">DEP-{dep.DeploymentNumber}</div>
													<div class="text-[9px] text-slate-500">v{dep.ConfigurationVersion} · <span class="{getDeploymentStatusIcon(dep.State)}">{dep.State}</span></div>
												</div>
											</div>
											<div class="flex items-center gap-3 text-[8px] text-slate-400">
												{#if dep.CompletedAt}
													<span class="flex items-center gap-1"><Timer class="w-3 h-3" />{dep.CompletedAt.toLocaleDateString()}</span>
												{/if}
												{#if dep.State !== 'COMPLETE' && dep.State !== 'ROLLED_BACK'}
													<button onclick={() => stopDeployment(dep.DeploymentNumber)} class="p-1.5 rounded-lg text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-all"><Square class="w-3 h-3" /></button>
												{/if}
											</div>
										</div>
									{/each}
								</div>
							{/if}
						{/if}

						<!-- Config Versions -->
						{#if appDetailTab === 'versions'}
							<div class="flex items-center justify-between mb-4">
								<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest italic">Hosted Config Versions</h3>
								{#if profiles.length > 0}
									<select bind:value={selectedProfileId} onchange={() => loadConfigVersions(selectedApp!.Id!, selectedProfileId)}
										class="text-[10px] font-black uppercase bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-lg px-2 py-1 outline-none focus:ring-1 focus:ring-blue-500">
										<option value="">— Select profile —</option>
										{#each profiles as prof}<option value={prof.Id}>{prof.Name}</option>{/each}
									</select>
								{/if}
							</div>
							{#if !selectedProfileId}
								<div class="p-8 text-center text-slate-400 text-sm italic">Select a profile to view versions.</div>
							{:else if loadingVersions}
								<div class="p-8 flex justify-center opacity-30"><RefreshCw class="w-6 h-6 text-blue-500 animate-spin" /></div>
							{:else if configVersions.length === 0}
								<div class="p-8 text-center border border-dashed border-slate-200 dark:border-slate-700 rounded-2xl text-slate-400 text-sm italic">No versions.</div>
							{:else}
								<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
									{#each [...configVersions].reverse() as ver}
										<div class="py-3 flex items-center justify-between">
											<div class="flex items-center gap-3">
												<div class="p-1.5 bg-slate-100 dark:bg-slate-800 rounded-lg"><Archive class="w-3.5 h-3.5 text-blue-500" /></div>
												<div>
													<div class="text-xs font-black text-slate-900 dark:text-white uppercase italic">v{ver.VersionNumber}</div>
													<div class="text-[8px] text-slate-400">{ver.ContentType}{ver.VersionLabel ? ` · ${ver.VersionLabel}` : ''}</div>
												</div>
											</div>
											<div class="text-[8px] text-slate-400 font-mono">v{ver.VersionNumber}</div>
										</div>
									{/each}
								</div>
							{/if}
						{/if}
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-24 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]"><Settings2 class="w-16 h-16 text-slate-200 dark:text-slate-700" /></div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">Select an Application</h3>
					<p class="text-slate-500 text-sm max-w-sm italic">Choose an application from the list to explore environments, profiles, deployments, and config versions.</p>
				</div>
			{/if}
		</div>
	</div>
	{/if}

	<!-- ══ STRATEGIES TAB ════════════════════════════════════════════════════ -->
	{#if activeTab === 'strategies'}
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700/50 flex items-center justify-between">
			<div class="flex items-center gap-2"><BarChart3 class="w-4 h-4 text-amber-500" /><span class="text-xs font-black text-slate-600 dark:text-slate-300 uppercase tracking-widest italic">Deployment Strategies</span></div>
			<span class="text-[10px] font-black text-slate-400 uppercase">{strategies.length} TOTAL</span>
		</div>
		{#if loadingStrategies}
			<div class="p-12 flex flex-col items-center gap-3 opacity-30"><RefreshCw class="w-8 h-8 text-amber-500 animate-spin" /><span class="text-[10px] uppercase font-black text-slate-500">Loading...</span></div>
		{:else if strategies.length === 0}
			<div class="p-16 text-center flex flex-col items-center gap-3"><BarChart3 class="w-10 h-10 text-slate-200 dark:text-slate-700" /><p class="text-sm font-black text-slate-400 italic uppercase">No deployment strategies.</p></div>
		{:else}
			<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
				{#each strategies as strat}
					<div class="p-5 flex items-center justify-between hover:bg-amber-500/5 transition-all group">
						<div class="flex items-center gap-4">
							<div class="p-2.5 bg-amber-500/10 rounded-xl border border-amber-500/20"><BarChart3 class="w-5 h-5 text-amber-600" /></div>
							<div>
								<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-sm">{strat.Name}</div>
								<div class="flex items-center gap-3 mt-1 text-[9px] text-slate-500 font-mono">
									<span>{strat.DeploymentDurationInMinutes}m duration</span>
									<span>·</span>
									<span>{strat.GrowthFactor}% growth</span>
									<span>·</span>
									<span>{strat.GrowthType}</span>
									{#if strat.FinalBakeTimeInMinutes}<span>· {strat.FinalBakeTimeInMinutes}m bake</span>{/if}
								</div>
								{#if strat.Description}<p class="text-[9px] text-slate-400 mt-1 italic">{strat.Description}</p>{/if}
							</div>
						</div>
						<button onclick={() => deleteStrategy(strat.Id, strat.Name)} class="p-2 rounded-xl text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-all border border-transparent hover:border-rose-500/20"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/each}
			</div>
		{/if}
	</div>
	{/if}

	<!-- ══ EXTENSIONS TAB ════════════════════════════════════════════════════ -->
	{#if activeTab === 'extensions'}
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700/50 flex items-center gap-3">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
				<input type="text" bind:value={extensionSearch} placeholder="Search extensions..." class="w-full pl-9 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-indigo-500 outline-none italic font-bold" />
			</div>
			<span class="text-[10px] font-black text-slate-400 uppercase">{extensions.length} TOTAL</span>
		</div>
		{#if loadingExtensions}
			<div class="p-12 flex flex-col items-center gap-3 opacity-30"><RefreshCw class="w-8 h-8 text-indigo-500 animate-spin" /><span class="text-[10px] uppercase font-black text-slate-500">Loading...</span></div>
		{:else if filteredExtensions.length === 0}
			<div class="p-16 text-center flex flex-col items-center gap-3"><Puzzle class="w-10 h-10 text-slate-200 dark:text-slate-700" /><p class="text-sm font-black text-slate-400 italic uppercase">No extensions registered.</p></div>
		{:else}
			<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
				{#each filteredExtensions as ext}
					<div class="p-5 flex items-center justify-between hover:bg-indigo-500/5 transition-all group">
						<div class="flex items-center gap-4">
							<div class="p-2.5 bg-indigo-500/10 rounded-xl border border-indigo-500/20"><Puzzle class="w-5 h-5 text-indigo-600" /></div>
							<div>
								<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-sm">{ext.Name}</div>
								<div class="flex items-center gap-2 mt-1">
									<span class="text-[8px] text-slate-400 font-mono opacity-60">{ext.Id}</span>
									<span class="px-1.5 py-0.5 rounded bg-indigo-500/10 text-indigo-600 text-[8px] font-black uppercase border border-indigo-500/20">v{ext.VersionNumber}</span>
									{#if ext.Actions && Object.keys(ext.Actions).length > 0}
										<span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-700 text-slate-500 text-[8px] font-black uppercase">{Object.keys(ext.Actions).length} HOOKS</span>
									{/if}
								</div>
								{#if ext.Description}<p class="text-[9px] text-slate-400 mt-1 italic">{ext.Description}</p>{/if}
								{#if ext.Parameters && Object.keys(ext.Parameters).length > 0}
									<div class="flex flex-wrap gap-1 mt-1">
										{#each Object.keys(ext.Parameters) as param}
											<span class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-slate-500 text-[7px] font-mono">{param}{ext.Parameters[param].Required ? '*' : ''}</span>
										{/each}
									</div>
								{/if}
							</div>
						</div>
						<button onclick={() => deleteExtension(ext.Id, ext.Name)} class="p-2 rounded-xl text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-all border border-transparent hover:border-rose-500/20"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/each}
			</div>
		{/if}
	</div>
	{/if}

	<!-- ══ ASSOCIATIONS TAB ══════════════════════════════════════════════════ -->
	{#if activeTab === 'associations'}
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700/50 flex items-center justify-between">
			<div class="flex items-center gap-2"><Link class="w-4 h-4 text-violet-500" /><span class="text-xs font-black text-slate-600 dark:text-slate-300 uppercase tracking-widest italic">Extension Associations</span></div>
			<span class="text-[10px] font-black text-slate-400 uppercase">{associations.length} TOTAL</span>
		</div>
		{#if loadingAssociations}
			<div class="p-12 flex flex-col items-center gap-3 opacity-30"><RefreshCw class="w-8 h-8 text-violet-500 animate-spin" /><span class="text-[10px] uppercase font-black text-slate-500">Loading...</span></div>
		{:else if associations.length === 0}
			<div class="p-16 text-center flex flex-col items-center gap-3"><Link class="w-10 h-10 text-slate-200 dark:text-slate-700" /><p class="text-sm font-black text-slate-400 italic uppercase">No extension associations.</p></div>
		{:else}
			<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
				{#each associations as assoc}
					<div class="p-5 flex items-center justify-between hover:bg-violet-500/5 transition-all group">
						<div class="flex items-center gap-4">
							<div class="p-2.5 bg-violet-500/10 rounded-xl border border-violet-500/20"><Link class="w-5 h-5 text-violet-600" /></div>
							<div>
								<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-sm font-mono">{assoc.Id}</div>
								<div class="flex flex-col gap-0.5 mt-1">
									<div class="flex items-center gap-1.5 text-[9px] text-slate-500"><Puzzle class="w-2.5 h-2.5 text-indigo-400" /><span class="font-mono truncate max-w-xs">{assoc.ExtensionArn}</span></div>
									<div class="flex items-center gap-1.5 text-[9px] text-slate-500"><Database class="w-2.5 h-2.5 text-slate-400" /><span class="font-mono truncate max-w-xs">{assoc.ResourceArn}</span></div>
								</div>
							</div>
						</div>
						<button onclick={() => deleteAssociation(assoc.Id)} class="p-2 rounded-xl text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-all border border-transparent hover:border-rose-500/20"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/each}
			</div>
		{/if}
	</div>
	{/if}

	<!-- ══ SETTINGS TAB ══════════════════════════════════════════════════════ -->
	{#if activeTab === 'settings'}
	<div class="max-w-2xl space-y-4">
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
			<div class="flex items-center gap-3">
				<div class="p-2.5 bg-slate-500/10 rounded-xl border border-slate-500/20"><Settings class="w-5 h-5 text-slate-600" /></div>
				<div>
					<h3 class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">Account Settings</h3>
					<p class="text-[10px] text-slate-400 mt-0.5">Global AppConfig account configuration.</p>
				</div>
			</div>
			{#if loadingSettings}
				<div class="p-8 flex justify-center opacity-30"><RefreshCw class="w-6 h-6 animate-spin text-slate-400" /></div>
			{:else if accountSettings}
				<div class="p-4 bg-slate-50 dark:bg-slate-900/50 rounded-xl border border-slate-200 dark:border-slate-700">
					<div class="flex items-center justify-between">
						<div>
							<div class="text-xs font-black text-slate-700 dark:text-slate-200 uppercase italic">Deletion Protection</div>
							<p class="text-[10px] text-slate-400 mt-0.5">Prevents accidental deletion of configuration resources.</p>
						</div>
						<div class="flex items-center gap-2">
							<div class="w-2 h-2 rounded-full {accountSettings.DeletionProtection?.Enabled ? 'bg-emerald-500' : 'bg-slate-300'}"></div>
							<span class="text-[10px] font-black text-slate-500 uppercase">{accountSettings.DeletionProtection?.Enabled ? 'ENABLED' : 'DISABLED'}</span>
						</div>
					</div>
				</div>
			{:else}
				<div class="p-8 text-center text-slate-400 text-sm italic">Failed to load settings.</div>
			{/if}
		</div>
	</div>
	{/if}
</div>

<!-- ══ MODALS ════════════════════════════════════════════════════════════════ -->

<!-- Create Application -->
{#if showCreateModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateModal = false} role="presentation"></div>
	<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2rem] shadow-2xl border border-blue-500/20 animate-in zoom-in-95 p-8">
		<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase tracking-tighter italic">New Application</h3>
		<form onsubmit={(e) => { e.preventDefault(); createApplication(); }} class="space-y-4">
			<div>
				<label for="appName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Application Name</label>
				<input id="appName" type="text" bind:value={newAppName} placeholder="my-app-config" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm italic" required />
			</div>
			<div class="flex gap-3 pt-2">
				<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
				<button type="submit" disabled={creating} class="flex-1 px-4 py-3 bg-blue-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creating ? 'Creating...' : 'Create'}</button>
			</div>
		</form>
	</div>
</div>
{/if}

<!-- Create Environment -->
{#if showCreateEnvModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateEnvModal = false} role="presentation"></div>
	<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2rem] shadow-2xl border border-blue-500/20 animate-in zoom-in-95 p-8">
		<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase tracking-tighter italic">New Environment</h3>
		<form onsubmit={(e) => { e.preventDefault(); createEnvironment(); }} class="space-y-4">
			<div>
				<label for="envName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Environment Name</label>
				<input id="envName" type="text" bind:value={newEnvName} placeholder="production" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-blue-500 font-mono text-sm italic" required />
			</div>
			<div class="flex gap-3 pt-2">
				<button type="button" onclick={() => showCreateEnvModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
				<button type="submit" disabled={creatingEnv} class="flex-1 px-4 py-3 bg-blue-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creatingEnv ? 'Creating...' : 'Create'}</button>
			</div>
		</form>
	</div>
</div>
{/if}

<!-- Create Profile -->
{#if showCreateProfileModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateProfileModal = false} role="presentation"></div>
	<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2rem] shadow-2xl border border-indigo-500/20 animate-in zoom-in-95 p-8">
		<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase tracking-tighter italic">New Configuration Profile</h3>
		<form onsubmit={(e) => { e.preventDefault(); createProfile(); }} class="space-y-4">
			<div>
				<label for="profName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Profile Name</label>
				<input id="profName" type="text" bind:value={newProfileName} placeholder="my-feature-flags" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm italic" required />
			</div>
			<div>
				<label for="profType" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Type</label>
				<select id="profType" bind:value={newProfileType} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-indigo-500 text-sm font-bold">
					<option value="AWS.Freeform">AWS.Freeform</option>
					<option value="AWS.AppConfig.FeatureFlags">AWS.AppConfig.FeatureFlags</option>
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button type="button" onclick={() => showCreateProfileModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
				<button type="submit" disabled={creatingProfile} class="flex-1 px-4 py-3 bg-indigo-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creatingProfile ? 'Creating...' : 'Create'}</button>
			</div>
		</form>
	</div>
</div>
{/if}

<!-- Deploy Modal -->
{#if showDeployModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showDeployModal = false} role="presentation"></div>
	<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2rem] shadow-2xl border border-emerald-500/20 animate-in zoom-in-95 p-8">
		<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase tracking-tighter italic">Start Deployment</h3>
		<form onsubmit={(e) => { e.preventDefault(); startDeployment(); }} class="space-y-4">
			<div>
				<label for="depEnv" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Environment</label>
				<select id="depEnv" bind:value={selectedEnvId} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-emerald-500 text-sm font-bold" required>
					{#each environments as env}<option value={env.Id}>{env.Name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="depProfile" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Configuration Profile</label>
				<select id="depProfile" bind:value={deployProfileId} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-emerald-500 text-sm font-bold" required>
					<option value="">— select —</option>
					{#each profiles as p}<option value={p.Id}>{p.Name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="depStrategy" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Deployment Strategy</label>
				<select id="depStrategy" bind:value={deployStrategyId} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-emerald-500 text-sm font-bold" required>
					<option value="">— select —</option>
					{#each strategies as s}<option value={s.Id}>{s.Name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="depVersion" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Configuration Version</label>
				<input id="depVersion" type="text" bind:value={deployVersion} placeholder="1" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-emerald-500 font-mono text-sm italic" required />
			</div>
			<div class="flex gap-3 pt-2">
				<button type="button" onclick={() => showDeployModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
				<button type="submit" disabled={deploying} class="flex-1 px-4 py-3 bg-emerald-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{deploying ? 'Deploying...' : 'Deploy'}</button>
			</div>
		</form>
	</div>
</div>
{/if}

<!-- Create Strategy -->
{#if showCreateStratModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateStratModal = false} role="presentation"></div>
	<div class="relative w-full max-w-lg bg-white dark:bg-slate-800 rounded-[2rem] shadow-2xl border border-amber-500/20 animate-in zoom-in-95 p-8">
		<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase tracking-tighter italic">New Deployment Strategy</h3>
		<form onsubmit={(e) => { e.preventDefault(); createStrategy(); }} class="space-y-4">
			<div class="grid grid-cols-2 gap-4">
				<div class="col-span-2">
					<label for="stratName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Strategy Name</label>
					<input id="stratName" type="text" bind:value={newStratName} placeholder="AllAtOnce" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-amber-500 font-mono text-sm italic" required />
				</div>
				<div>
					<label for="stratDuration" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Duration (min)</label>
					<input id="stratDuration" type="number" min="0" bind:value={newStratDuration} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-amber-500 text-sm font-bold" />
				</div>
				<div>
					<label for="stratGrowth" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Growth Factor (%)</label>
					<input id="stratGrowth" type="number" min="1" max="100" bind:value={newStratGrowth} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-amber-500 text-sm font-bold" />
				</div>
				<div>
					<label for="stratBake" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Bake Time (min)</label>
					<input id="stratBake" type="number" min="0" bind:value={newStratBake} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-amber-500 text-sm font-bold" />
				</div>
				<div>
					<label for="stratGrowthType" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Growth Type</label>
					<select id="stratGrowthType" bind:value={newStratGrowthType} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-amber-500 text-sm font-bold">
						<option value="LINEAR">LINEAR</option>
						<option value="EXPONENTIAL">EXPONENTIAL</option>
					</select>
				</div>
				<div class="col-span-2">
					<label for="stratDesc" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Description (optional)</label>
					<input id="stratDesc" type="text" bind:value={newStratDesc} placeholder="Description..." class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-amber-500 font-mono text-sm italic" />
				</div>
			</div>
			<div class="flex gap-3 pt-2">
				<button type="button" onclick={() => showCreateStratModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
				<button type="submit" disabled={creatingStrat} class="flex-1 px-4 py-3 bg-amber-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creatingStrat ? 'Creating...' : 'Create'}</button>
			</div>
		</form>
	</div>
</div>
{/if}

<!-- Create Extension -->
{#if showCreateExtModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateExtModal = false} role="presentation"></div>
	<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2rem] shadow-2xl border border-indigo-500/20 animate-in zoom-in-95 p-8">
		<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase tracking-tighter italic">Register Extension</h3>
		<form onsubmit={(e) => { e.preventDefault(); createExtension(); }} class="space-y-4">
			<div>
				<label for="extName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Extension Name</label>
				<input id="extName" type="text" bind:value={newExtName} placeholder="MyLambdaExtension" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm italic" required />
			</div>
			<div>
				<label for="extDesc" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Description (optional)</label>
				<input id="extDesc" type="text" bind:value={newExtDesc} placeholder="What does this extension do?" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm italic" />
			</div>
			<div class="flex gap-3 pt-2">
				<button type="button" onclick={() => showCreateExtModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
				<button type="submit" disabled={creatingExt} class="flex-1 px-4 py-3 bg-indigo-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creatingExt ? 'Registering...' : 'Register'}</button>
			</div>
		</form>
	</div>
</div>
{/if}

<!-- Create Association -->
{#if showCreateAssocModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
	<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateAssocModal = false} role="presentation"></div>
	<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2rem] shadow-2xl border border-violet-500/20 animate-in zoom-in-95 p-8">
		<h3 class="text-xl font-black text-slate-900 dark:text-white mb-5 uppercase tracking-tighter italic">Associate Extension</h3>
		<form onsubmit={(e) => { e.preventDefault(); createAssociation(); }} class="space-y-4">
			<div>
				<label for="assocExtId" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Extension ID or Name</label>
				<input id="assocExtId" type="text" bind:value={newAssocExtId} placeholder="Extension ID or name" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-violet-500 font-mono text-sm italic" required />
			</div>
			<div>
				<label for="assocResource" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1.5 italic">Resource ARN</label>
				<input id="assocResource" type="text" bind:value={newAssocResource} placeholder="arn:aws:appconfig:..." class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-violet-500 font-mono text-sm italic" required />
			</div>
			<div class="flex gap-3 pt-2">
				<button type="button" onclick={() => showCreateAssocModal = false} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 rounded-xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
				<button type="submit" disabled={creatingAssoc} class="flex-1 px-4 py-3 bg-violet-600 text-white rounded-xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">{creatingAssoc ? 'Associating...' : 'Associate'}</button>
			</div>
		</form>
	</div>
</div>
{/if}

<style>
	::-webkit-scrollbar { width: 6px; }
	::-webkit-scrollbar-track { background: transparent; }
	::-webkit-scrollbar-thumb { background: rgba(148, 163, 184, 0.1); border-radius: 10px; }
	::-webkit-scrollbar-thumb:hover { background: rgba(148, 163, 184, 0.2); }
</style>
