<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getServerlessRepoClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		GetApplicationCommand,
		CreateApplicationCommand,
		DeleteApplicationCommand,
		UpdateApplicationCommand,
		ListApplicationVersionsCommand,
		CreateApplicationVersionCommand,
		GetApplicationPolicyCommand,
		PutApplicationPolicyCommand,
		ListApplicationDependenciesCommand,
		CreateCloudFormationTemplateCommand,
		GetCloudFormationTemplateCommand,
		type ApplicationSummary,
		type ApplicationPolicyStatement,
		type VersionSummary,
		type ApplicationDependencySummary
	} from '@aws-sdk/client-serverlessapplicationrepository';
	import { toast } from 'svelte-sonner';
	import {
		Package,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		ChevronRight,
		Globe,
		ShieldCheck,
		Workflow,
		ExternalLink,
		Zap,
		Download,
		Star,
		User,
		Layers3,
		Tag,
		LayoutGrid,
		Activity,
		FileCode,
		GitBranch,
		Lock,
		Network,
		Edit,
		X,
		Check,
		AlertCircle,
		Clock
	} from 'lucide-svelte';

	const slr = regionalClient(getServerlessRepoClient);

	// ── State ────────────────────────────────────────────────────────────────
	let loading = $state(false);
	let applications = $state<ApplicationSummary[]>([]);
	let selectedApp = $state<ApplicationSummary | null>(null);
	let activeTab = $state<'versions' | 'policy' | 'template' | 'dependencies'>('versions');
	let searchQuery = $state('');

	// Versions
	let versions = $state<VersionSummary[]>([]);
	let loadingVersions = $state(false);

	// Policy
	let policyStatements = $state<ApplicationPolicyStatement[]>([]);
	let loadingPolicy = $state(false);

	// Template
	let templateStatus = $state('');
	let templateUrl = $state('');
	let loadingTemplate = $state(false);
	let templateId = $state('');

	// Dependencies
	let dependencies = $state<ApplicationDependencySummary[]>([]);
	let loadingDeps = $state(false);

	// Create app form
	let showCreate = $state(false);
	let creating = $state(false);
	let newName = $state('');
	let newDesc = $state('');
	let newAuthor = $state('');
	let newSourceCodeURL = $state('');
	let newSemanticVersion = $state('1.0.0');
	let newLabels = $state('');
	let newHomePageURL = $state('');
	let newLicenseURL = $state('');
	let newSpdxLicenseID = $state('Apache-2.0');

	// Create version form
	let showCreateVersion = $state(false);
	let creatingVersion = $state(false);
	let newVersionSemver = $state('');
	let newVersionSourceURL = $state('');
	let newVersionTemplateURL = $state('');

	// Edit app form
	let showEdit = $state(false);
	let editing = $state(false);
	let editDesc = $state('');
	let editAuthor = $state('');
	let editHomePageURL = $state('');

	// Policy form
	let showAddPolicy = $state(false);
	let addingPolicy = $state(false);
	let policyPrincipals = $state('*');
	let policyActions = $state<string[]>(['Deploy']);
	const allPolicyActions = ['Deploy', 'GetApplication', 'ListApplicationVersions', 'SearchApplications'];

	// Derived
	const filteredApps = $derived(
		applications.filter(
			(a) =>
				!searchQuery ||
				(a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(a.ApplicationId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	// ── Data Loading ─────────────────────────────────────────────────────────
	async function loadApps() {
		loading = true;
		try {
			const res = await slr().send(new ListApplicationsCommand({}));
			applications = res.Applications ?? [];
		} catch (err) {
			toast.error(`Failed to load applications: ${String(err)}`);
		} finally {
			loading = false;
		}
	}

	async function selectApp(app: ApplicationSummary) {
		selectedApp = app;
		activeTab = 'versions';
		await loadVersions(app);
	}

	async function loadVersions(app: ApplicationSummary) {
		loadingVersions = true;
		try {
			const res = await slr().send(
				new ListApplicationVersionsCommand({ ApplicationId: app.ApplicationId })
			);
			versions = res.Versions ?? [];
		} catch (err) {
			toast.error(`Failed to load versions: ${String(err)}`);
		} finally {
			loadingVersions = false;
		}
	}

	async function loadPolicy(app: ApplicationSummary) {
		loadingPolicy = true;
		try {
			const res = await slr().send(
				new GetApplicationPolicyCommand({ ApplicationId: app.ApplicationId })
			);
			policyStatements = res.Statements ?? [];
		} catch (err) {
			toast.error(`Failed to load policy: ${String(err)}`);
		} finally {
			loadingPolicy = false;
		}
	}

	async function loadTemplate(app: ApplicationSummary) {
		loadingTemplate = true;
		templateStatus = '';
		templateUrl = '';
		templateId = '';
		try {
			// Create a template, then poll for it
			const createRes = await slr().send(
				new CreateCloudFormationTemplateCommand({ ApplicationId: app.ApplicationId })
			);
			const tid = createRes.TemplateId ?? '';
			templateId = tid;
			templateStatus = createRes.Status ?? '';
			templateUrl = createRes.TemplateUrl ?? '';
			if (tid && templateStatus === 'ACTIVE') return;
			// Poll once more
			if (tid) {
				const getRes = await slr().send(
					new GetCloudFormationTemplateCommand({
						ApplicationId: app.ApplicationId,
						TemplateId: tid
					})
				);
				templateStatus = getRes.Status ?? '';
				templateUrl = getRes.TemplateUrl ?? '';
			}
		} catch (err) {
			toast.error(`Failed to create template: ${String(err)}`);
		} finally {
			loadingTemplate = false;
		}
	}

	async function loadDependencies(app: ApplicationSummary) {
		loadingDeps = true;
		try {
			const res = await slr().send(
				new ListApplicationDependenciesCommand({ ApplicationId: app.ApplicationId })
			);
			dependencies = res.Dependencies ?? [];
		} catch (err) {
			toast.error(`Failed to load dependencies: ${String(err)}`);
		} finally {
			loadingDeps = false;
		}
	}

	async function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		if (!selectedApp) return;
		if (tab === 'versions') await loadVersions(selectedApp);
		else if (tab === 'policy') await loadPolicy(selectedApp);
		else if (tab === 'template') await loadTemplate(selectedApp);
		else if (tab === 'dependencies') await loadDependencies(selectedApp);
	}

	// ── Feature 1: Create Application ────────────────────────────────────────
	async function createApplication() {
		if (!newName || !newAuthor) {
			toast.error('Name and Author are required');
			return;
		}
		creating = true;
		try {
			await slr().send(
				new CreateApplicationCommand({
					Name: newName,
					Description: newDesc,
					Author: newAuthor,
					SourceCodeUrl: newSourceCodeURL || undefined,
					SemanticVersion: newSemanticVersion || undefined,
					Labels: newLabels ? newLabels.split(',').map((l) => l.trim()) : undefined,
					HomePageUrl: newHomePageURL || undefined,
					LicenseUrl: newLicenseURL || undefined,
					SpdxLicenseId: newSpdxLicenseID || undefined
				})
			);
			toast.success(`Application "${newName}" created`);
			showCreate = false;
			resetCreateForm();
			await loadApps();
		} catch (err) {
			toast.error(`Failed to create: ${String(err)}`);
		} finally {
			creating = false;
		}
	}

	function resetCreateForm() {
		newName = '';
		newDesc = '';
		newAuthor = '';
		newSourceCodeURL = '';
		newSemanticVersion = '1.0.0';
		newLabels = '';
		newHomePageURL = '';
		newLicenseURL = '';
		newSpdxLicenseID = 'Apache-2.0';
	}

	// ── Feature 2: Delete Application ────────────────────────────────────────
	async function deleteApplication(app: ApplicationSummary) {
		const confirmed = await confirmDestructive({
			message: `Delete application "${app.Name}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await slr().send(new DeleteApplicationCommand({ ApplicationId: app.ApplicationId }));
			toast.success(`Application "${app.Name}" deleted`);
			if (selectedApp?.ApplicationId === app.ApplicationId) selectedApp = null;
			await loadApps();
		} catch (err) {
			toast.error(`Failed to delete: ${String(err)}`);
		}
	}

	// ── Feature 3: Update Application ────────────────────────────────────────
	function openEdit(app: ApplicationSummary) {
		editDesc = app.Description ?? '';
		editAuthor = app.Author ?? '';
		editHomePageURL = app.HomePageUrl ?? '';
		showEdit = true;
	}

	async function updateApplication() {
		if (!selectedApp) return;
		editing = true;
		try {
			await slr().send(
				new UpdateApplicationCommand({
					ApplicationId: selectedApp.ApplicationId,
					Description: editDesc || undefined,
					Author: editAuthor || undefined,
					HomePageUrl: editHomePageURL || undefined
				})
			);
			toast.success('Application updated');
			showEdit = false;
			await loadApps();
			// Refresh selected
			const updated = applications.find((a) => a.ApplicationId === selectedApp!.ApplicationId);
			if (updated) selectedApp = updated;
		} catch (err) {
			toast.error(`Failed to update: ${String(err)}`);
		} finally {
			editing = false;
		}
	}

	// ── Feature 4: Create Version ─────────────────────────────────────────────
	async function createVersion() {
		if (!selectedApp || !newVersionSemver) {
			toast.error('Semantic version is required');
			return;
		}
		creatingVersion = true;
		try {
			await slr().send(
				new CreateApplicationVersionCommand({
					ApplicationId: selectedApp.ApplicationId,
					SemanticVersion: newVersionSemver,
					SourceCodeUrl: newVersionSourceURL || undefined,
					TemplateUrl: newVersionTemplateURL || undefined
				})
			);
			toast.success(`Version ${newVersionSemver} created`);
			showCreateVersion = false;
			newVersionSemver = '';
			newVersionSourceURL = '';
			newVersionTemplateURL = '';
			await loadVersions(selectedApp);
		} catch (err) {
			toast.error(`Failed to create version: ${String(err)}`);
		} finally {
			creatingVersion = false;
		}
	}

	// ── Feature 5: Put Policy ─────────────────────────────────────────────────
	async function savePolicy() {
		if (!selectedApp) return;
		addingPolicy = true;
		try {
			const principals = policyPrincipals
				.split(',')
				.map((p) => p.trim())
				.filter(Boolean);
			const newStmts: ApplicationPolicyStatement[] = [
				...policyStatements,
				{ Actions: policyActions, Principals: principals }
			];
			const res = await slr().send(
				new PutApplicationPolicyCommand({
					ApplicationId: selectedApp.ApplicationId,
					Statements: newStmts
				})
			);
			policyStatements = res.Statements ?? [];
			toast.success('Policy updated');
			showAddPolicy = false;
			policyPrincipals = '*';
			policyActions = ['Deploy'];
		} catch (err) {
			toast.error(`Failed to update policy: ${String(err)}`);
		} finally {
			addingPolicy = false;
		}
	}

	async function removeStatement(idx: number) {
		if (!selectedApp) return;
		const newStmts = policyStatements.filter((_, i) => i !== idx);
		try {
			const res = await slr().send(
				new PutApplicationPolicyCommand({
					ApplicationId: selectedApp.ApplicationId,
					Statements: newStmts
				})
			);
			policyStatements = res.Statements ?? [];
			toast.success('Statement removed');
		} catch (err) {
			toast.error(`Failed to remove statement: ${String(err)}`);
		}
	}

	// Applications and every selected-app-scoped tab (versions, policy,
	// template, dependencies) are region-scoped, so a region switch must clear
	// them (not just reload) rather than keep showing the old region's app.
	onRegionChange(() => {
		applications = [];
		selectedApp = null;
		versions = [];
		policyStatements = [];
		templateStatus = '';
		templateUrl = '';
		templateId = '';
		dependencies = [];
		showCreate = false;
		showCreateVersion = false;
		showEdit = false;
		showAddPolicy = false;
		void loadApps();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div
		class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl"
	>
		<div class="flex items-center gap-4">
			<div class="p-3 bg-rose-600/20 rounded-xl shadow-inner border border-rose-500/20">
				<Package class="w-8 h-8 text-rose-600" />
			</div>
			<div>
				<h1
					class="text-3xl font-bold bg-gradient-to-r from-rose-600 to-orange-600 dark:from-rose-400 dark:to-orange-400 bg-clip-text text-transparent italic tracking-tight"
				>
					Serverless Application Repository
				</h1>
				<p
					class="text-slate-500 dark:text-slate-400 text-sm mt-1 uppercase tracking-widest font-black italic opacity-70"
				>
					Managed Application Discovery, Packaging &amp; One-Click Deployment Plane
				</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button
				onclick={loadApps}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => (showCreate = true)}
				class="flex items-center gap-2 px-5 py-2.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl font-black shadow-lg shadow-rose-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Publish App
			</button>
		</div>
	</div>

	<!-- Stats -->
	<div class="grid grid-cols-1 md:grid-cols-4 gap-6">
		<div
			class="p-8 bg-slate-900 rounded-3xl border border-slate-800 shadow-2xl flex items-center justify-between group/total overflow-hidden relative"
		>
			<div
				class="absolute inset-0 bg-gradient-to-br from-rose-500/10 to-transparent pointer-events-none"
			></div>
			<div class="relative z-10">
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">
					Registry Status
				</div>
				<div
					class="text-3xl font-black text-white italic tabular-nums flex items-center gap-2 uppercase"
				>
					Synched
					<div class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
				</div>
			</div>
			<Globe
				class="w-10 h-10 text-rose-500/20 group-hover/total:scale-110 transition-transform relative z-10"
			/>
		</div>
		<div
			class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat"
		>
			<div>
				<div class="text-[9px] font-black text-rose-600 uppercase tracking-widest mb-1 italic">
					Published Apps
				</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">
					{applications.length}
				</div>
			</div>
			<div class="p-3 bg-rose-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Package class="w-6 h-6 text-rose-600" />
			</div>
		</div>
		<div
			class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat"
		>
			<div>
				<div class="text-[9px] font-black text-orange-600 uppercase tracking-widest mb-1 italic">
					Selected App
				</div>
				<div
					class="text-xl font-black text-slate-900 dark:text-white italic truncate max-w-[120px]"
				>
					{selectedApp?.Name ?? '—'}
				</div>
			</div>
			<div
				class="p-3 bg-orange-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform"
			>
				<Activity class="w-6 h-6 text-orange-600" />
			</div>
		</div>
		<div
			class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat"
		>
			<div>
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">
					Vendor Trust
				</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums uppercase">
					Verified
				</div>
			</div>
			<div
				class="p-3 bg-white/50 dark:bg-slate-700 rounded-2xl group-hover/stat:rotate-12 transition-transform"
			>
				<ShieldCheck class="w-6 h-6 text-slate-400" />
			</div>
		</div>
	</div>

	<!-- Main Layout: list + detail -->
	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Application Catalog -->
		<div
			class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl overflow-hidden flex flex-col min-h-[600px]"
		>
			<div
				class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-white/20 dark:bg-slate-900/10 flex flex-col gap-3 sticky top-0 z-10 backdrop-blur-md"
			>
				<div class="flex items-center justify-between">
					<h3
						class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none"
					>
						<LayoutGrid class="w-4 h-4 text-rose-500" />
						Catalog
					</h3>
					<span
						class="px-2 py-0.5 rounded-full bg-rose-500/10 text-rose-600 text-[9px] font-black uppercase"
					>
						{applications.length}
					</span>
				</div>
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-3 h-3 text-slate-400" />
					<input
						type="text"
						bind:value={searchQuery}
						placeholder="Search..."
						class="w-full pl-8 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-[10px] focus:ring-2 focus:ring-rose-500 outline-none transition-all italic font-black uppercase"
					/>
				</div>
			</div>

			<div class="flex-1 overflow-y-auto p-4 space-y-2">
				{#if loading}
					<div class="flex flex-col items-center justify-center p-16 opacity-30">
						<Package class="w-12 h-12 text-rose-500 animate-bounce mb-4" />
						<span class="text-[10px] uppercase font-black text-slate-500 tracking-[0.2em] italic"
							>Syncing...</span
						>
					</div>
				{:else if filteredApps.length}
					{#each filteredApps as app}
						<div
							onclick={() => selectApp(app)}
							role="button"
							tabindex="0"
							onkeydown={(e) => {
								if (e.key === 'Enter' || e.key === ' ') {
									e.preventDefault();
									selectApp(app);
								}
							}}
							class="w-full text-left p-4 rounded-2xl border transition-all cursor-pointer group/row {selectedApp?.ApplicationId === app.ApplicationId
								? 'bg-rose-50 dark:bg-rose-900/20 border-rose-200 dark:border-rose-700/50'
								: 'bg-white/60 dark:bg-slate-900/40 border-slate-100 dark:border-slate-800 hover:border-rose-200 dark:hover:border-rose-700/40'}"
						>
							<div class="flex items-start justify-between gap-2">
								<div class="flex items-center gap-3 min-w-0">
									<div
										class="p-2 bg-slate-900 rounded-xl border border-slate-800 flex-shrink-0 group-hover/row:rotate-6 transition-transform"
									>
										<Zap class="w-4 h-4 text-white" />
									</div>
									<div class="min-w-0">
										<div
											class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tighter italic truncate"
										>
											{app.Name}
										</div>
										<div
											class="text-[9px] text-slate-500 italic font-bold truncate opacity-70 mt-0.5"
										>
											{app.Author ?? 'Unknown author'}
										</div>
									</div>
								</div>
								<div class="flex items-center gap-1 flex-shrink-0">
									<button
										onclick={(e) => {
											e.stopPropagation();
											selectedApp = app;
											openEdit(app);
										}}
										class="p-1.5 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-400 hover:text-slate-600 transition-all"
									>
										<Edit class="w-3 h-3" />
									</button>
									<button
										onclick={(e) => {
											e.stopPropagation();
											deleteApplication(app);
										}}
										class="p-1.5 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20 text-slate-400 hover:text-red-500 transition-all"
									>
										<Trash2 class="w-3 h-3" />
									</button>
								</div>
							</div>
							{#if app.Description}
								<p class="text-[9px] text-slate-500 italic mt-2 line-clamp-2 leading-relaxed pl-9">
									{app.Description}
								</p>
							{/if}
							{#if app.Labels && app.Labels.length > 0}
								<div class="flex flex-wrap gap-1 mt-2 pl-9">
									{#each app.Labels.slice(0, 3) as label}
										<span
											class="px-1.5 py-0.5 rounded bg-slate-100 dark:bg-slate-800 text-[8px] font-black uppercase text-slate-500 italic"
											>{label}</span
										>
									{/each}
								</div>
							{/if}
						</div>
					{/each}
				{:else}
					<div class="flex flex-col items-center justify-center p-16 text-center opacity-30">
						<Package class="w-12 h-12 text-slate-600 mb-4" />
						<p class="text-[10px] text-slate-500 font-black uppercase italic">No serverless applications found</p>
					</div>
				{/if}
			</div>
		</div>

		<!-- Detail Panel -->
		<div class="lg:col-span-2">
			{#if selectedApp}
				<div
					class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl overflow-hidden flex flex-col min-h-[600px]"
				>
					<!-- App Header -->
					<div
						class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-white/20 dark:bg-slate-900/10"
					>
						<div class="flex items-start justify-between gap-4">
							<div class="flex items-center gap-4">
								<div class="p-4 bg-slate-900 rounded-2xl border border-slate-800 shadow-xl">
									<Zap class="w-7 h-7 text-white" />
								</div>
								<div>
									<h2
										class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic"
									>
										{selectedApp.Name}
									</h2>
									<div class="flex items-center gap-3 mt-1">
										<div class="flex items-center gap-1">
											<User class="w-3 h-3 text-slate-400" />
											<span class="text-[10px] text-slate-500 italic font-black uppercase"
												>{selectedApp.Author}</span
											>
										</div>
										{#if selectedApp.SpdxLicenseId}
											<span
												class="px-1.5 py-0.5 rounded bg-emerald-500/10 text-emerald-600 text-[8px] font-black uppercase border border-emerald-500/20"
												>{selectedApp.SpdxLicenseId}</span
											>
										{/if}
									</div>
									{#if selectedApp.Description}
										<p class="text-[10px] text-slate-500 italic mt-2 max-w-md leading-relaxed">
											{selectedApp.Description}
										</p>
									{/if}
								</div>
							</div>
							<div class="flex items-center gap-2 flex-shrink-0">
								<button
									onclick={() => openEdit(selectedApp!)}
									class="flex items-center gap-1.5 px-3 py-1.5 bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-xl text-[10px] font-black uppercase tracking-widest italic transition-all"
								>
									<Edit class="w-3 h-3" /> Edit
								</button>
								<button
									onclick={() => deleteApplication(selectedApp!)}
									class="flex items-center gap-1.5 px-3 py-1.5 bg-red-50 dark:bg-red-900/20 hover:bg-red-100 border border-red-200 dark:border-red-700/50 rounded-xl text-[10px] font-black uppercase tracking-widest italic text-red-600 transition-all"
								>
									<Trash2 class="w-3 h-3" /> Delete
								</button>
							</div>
						</div>

						<!-- App ID / Links -->
						<div class="mt-4 flex flex-wrap gap-3">
							<div
								class="px-3 py-1.5 rounded-xl bg-slate-100 dark:bg-slate-800 text-[9px] font-mono text-slate-500 italic truncate max-w-xs"
							>
								{selectedApp.ApplicationId}
							</div>
							{#if selectedApp.HomePageUrl}
								<a
									href={selectedApp.HomePageUrl}
									target="_blank"
									rel="noopener"
									class="flex items-center gap-1 px-3 py-1.5 rounded-xl bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700/40 text-[9px] font-black uppercase text-blue-600 italic hover:bg-blue-100 transition-all"
								>
									<ExternalLink class="w-3 h-3" /> Homepage
								</a>
							{/if}
						</div>

						{#if selectedApp.Labels && selectedApp.Labels.length > 0}
							<div class="flex flex-wrap gap-1 mt-3">
								{#each selectedApp.Labels as label}
									<span
										class="px-2 py-0.5 rounded-full bg-rose-500/10 text-rose-600 text-[8px] font-black uppercase italic border border-rose-500/20"
										>{label}</span
									>
								{/each}
							</div>
						{/if}
					</div>

					<!-- Tabs -->
					<div
						class="flex border-b border-slate-100 dark:border-slate-700/50 bg-white/10 dark:bg-slate-900/10 px-6 gap-1"
					>
						{#each [
							{ id: 'versions', label: 'Versions', icon: GitBranch },
							{ id: 'policy', label: 'Policy', icon: Lock },
							{ id: 'template', label: 'CFN Template', icon: FileCode },
							{ id: 'dependencies', label: 'Dependencies', icon: Network }
						] as tab}
							<button
								onclick={() => switchTab(tab.id as typeof activeTab)}
								class="flex items-center gap-1.5 px-4 py-3 text-[10px] font-black uppercase tracking-widest italic border-b-2 transition-all {activeTab === tab.id
									? 'border-rose-500 text-rose-600'
									: 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}"
							>
								<tab.icon class="w-3 h-3" />
								{tab.label}
							</button>
						{/each}
					</div>

					<!-- Tab Content -->
					<div class="flex-1 overflow-y-auto p-6">
						<!-- Versions Tab -->
						{#if activeTab === 'versions'}
							<div class="space-y-4">
								<div class="flex items-center justify-between">
									<h4
										class="text-xs font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-2"
									>
										<GitBranch class="w-3.5 h-3.5 text-rose-500" />
										Application Versions
									</h4>
									<button
										onclick={() => (showCreateVersion = true)}
										class="flex items-center gap-1.5 px-3 py-1.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl text-[10px] font-black uppercase tracking-widest italic transition-all active:scale-95 shadow-sm"
									>
										<Plus class="w-3 h-3" /> New Version
									</button>
								</div>

								{#if showCreateVersion}
									<div
										class="p-4 bg-slate-50 dark:bg-slate-900/50 rounded-2xl border border-slate-200 dark:border-slate-700 space-y-3"
									>
										<div class="text-[10px] font-black uppercase text-slate-500 italic tracking-widest">
											Create New Version
										</div>
										<div class="grid grid-cols-1 gap-3">
											<div>
												<label
													class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
													 for="new-version-semver">Semantic Version *</label>
												<input id="new-version-semver"
													type="text"
													bind:value={newVersionSemver}
													placeholder="1.0.1"
													class="w-full px-3 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs focus:ring-2 focus:ring-rose-500 outline-none"
												/>
											</div>
											<div>
												<label
													class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
													 for="new-version-source-url">Source Code URL</label>
												<input id="new-version-source-url"
													type="text"
													bind:value={newVersionSourceURL}
													placeholder="https://github.com/..."
													class="w-full px-3 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs focus:ring-2 focus:ring-rose-500 outline-none"
												/>
											</div>
											<div>
												<label
													class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
													 for="new-version-template-url">Template URL</label>
												<input id="new-version-template-url"
													type="text"
													bind:value={newVersionTemplateURL}
													placeholder="https://s3.amazonaws.com/..."
													class="w-full px-3 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs focus:ring-2 focus:ring-rose-500 outline-none"
												/>
											</div>
										</div>
										<div class="flex gap-2 justify-end">
											<button
												onclick={() => (showCreateVersion = false)}
												class="px-3 py-1.5 rounded-xl bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-[10px] font-black uppercase italic text-slate-600 hover:bg-slate-50 transition-all"
												>Cancel</button
											>
											<button
												onclick={createVersion}
												disabled={creatingVersion}
												class="px-3 py-1.5 rounded-xl bg-rose-600 hover:bg-rose-700 text-white text-[10px] font-black uppercase italic transition-all disabled:opacity-50"
											>
												{creatingVersion ? 'Creating...' : 'Create'}
											</button>
										</div>
									</div>
								{/if}

								{#if loadingVersions}
									<div class="flex items-center justify-center p-12 opacity-40">
										<GitBranch class="w-8 h-8 text-rose-500 animate-pulse" />
									</div>
								{:else if versions.length}
									<div class="space-y-2">
										{#each versions as v}
											<div
												class="p-4 bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 flex items-center justify-between group/v"
											>
												<div class="flex items-center gap-3">
													<div
														class="px-2.5 py-1 rounded-lg bg-rose-500/10 text-rose-600 text-[10px] font-black uppercase font-mono border border-rose-500/20"
													>
														v{v.SemanticVersion}
													</div>
													<div class="flex items-center gap-1 text-[9px] text-slate-400 italic">
														<Clock class="w-3 h-3" />
														{v.CreationTime
															? new Date(v.CreationTime).toLocaleDateString()
															: 'Unknown'}
													</div>
												</div>
												{#if v.SourceCodeUrl}
													<a
														href={v.SourceCodeUrl}
														target="_blank"
														rel="noopener"
														class="flex items-center gap-1 text-[9px] text-blue-500 hover:text-blue-600 italic font-black uppercase"
													>
														<ExternalLink class="w-3 h-3" /> Source
													</a>
												{/if}
											</div>
										{/each}
									</div>
								{:else}
									<div class="flex flex-col items-center justify-center p-12 opacity-30">
										<GitBranch class="w-10 h-10 text-slate-400 mb-3" />
										<p class="text-[10px] text-slate-500 font-black uppercase italic">No versions yet</p>
									</div>
								{/if}
							</div>
						{/if}

						<!-- Policy Tab -->
						{#if activeTab === 'policy'}
							<div class="space-y-4">
								<div class="flex items-center justify-between">
									<h4
										class="text-xs font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-2"
									>
										<Lock class="w-3.5 h-3.5 text-rose-500" />
										Access Policy Statements
									</h4>
									<button
										onclick={() => (showAddPolicy = true)}
										class="flex items-center gap-1.5 px-3 py-1.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl text-[10px] font-black uppercase tracking-widest italic transition-all active:scale-95 shadow-sm"
									>
										<Plus class="w-3 h-3" /> Add Statement
									</button>
								</div>

								{#if showAddPolicy}
									<div
										class="p-4 bg-slate-50 dark:bg-slate-900/50 rounded-2xl border border-slate-200 dark:border-slate-700 space-y-3"
									>
										<div class="text-[10px] font-black uppercase text-slate-500 italic tracking-widest">
											New Policy Statement
										</div>
										<div>
											<label
												class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
												 for="policy-principals">Principals (comma-separated, * for all)</label>
											<input id="policy-principals"
												type="text"
												bind:value={policyPrincipals}
												placeholder="*, 123456789012"
												class="w-full px-3 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-xs focus:ring-2 focus:ring-rose-500 outline-none"
											/>
										</div>
										<fieldset class="m-0 border-0 p-0">
											<legend
												class="block text-[9px] font-black uppercase text-slate-500 italic mb-2"
												>Actions</legend
											>
											<div class="flex flex-wrap gap-2">
												{#each allPolicyActions as action}
													<label
														class="flex items-center gap-1.5 cursor-pointer text-[10px] font-black uppercase italic text-slate-600 dark:text-slate-300"
													>
														<input
															type="checkbox"
															checked={policyActions.includes(action)}
															onchange={(e) => {
																if ((e.target as HTMLInputElement).checked) {
																	policyActions = [...policyActions, action];
																} else {
																	policyActions = policyActions.filter((a) => a !== action);
																}
															}}
															class="accent-rose-600"
														/>
														{action}
													</label>
												{/each}
											</div>
										</fieldset>
										<div class="flex gap-2 justify-end">
											<button
												onclick={() => (showAddPolicy = false)}
												class="px-3 py-1.5 rounded-xl bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 text-[10px] font-black uppercase italic text-slate-600 hover:bg-slate-50 transition-all"
												>Cancel</button
											>
											<button
												onclick={savePolicy}
												disabled={addingPolicy}
												class="px-3 py-1.5 rounded-xl bg-rose-600 hover:bg-rose-700 text-white text-[10px] font-black uppercase italic transition-all disabled:opacity-50"
											>
												{addingPolicy ? 'Saving...' : 'Save'}
											</button>
										</div>
									</div>
								{/if}

								{#if loadingPolicy}
									<div class="flex items-center justify-center p-12 opacity-40">
										<Lock class="w-8 h-8 text-rose-500 animate-pulse" />
									</div>
								{:else if policyStatements.length}
									<div class="space-y-2">
										{#each policyStatements as stmt, i}
											<div
												class="p-4 bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800"
											>
												<div class="flex items-start justify-between gap-3">
													<div class="flex-1 space-y-2">
														{#if stmt.StatementId}
															<div class="text-[9px] font-mono text-slate-400 italic">
																ID: {stmt.StatementId}
															</div>
														{/if}
														<div class="flex flex-wrap gap-1">
															{#each stmt.Principals ?? [] as principal}
																<span
																	class="px-2 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 text-[9px] font-black uppercase italic border border-blue-200 dark:border-blue-700/40"
																	>{principal}</span
																>
															{/each}
														</div>
														<div class="flex flex-wrap gap-1">
															{#each stmt.Actions ?? [] as action}
																<span
																	class="px-2 py-0.5 rounded bg-rose-100 dark:bg-rose-900/30 text-rose-700 dark:text-rose-300 text-[9px] font-black uppercase italic border border-rose-200 dark:border-rose-700/40"
																	>{action}</span
																>
															{/each}
														</div>
													</div>
													<button
														onclick={() => removeStatement(i)}
														class="p-1.5 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20 text-slate-400 hover:text-red-500 transition-all flex-shrink-0"
													>
														<Trash2 class="w-3 h-3" />
													</button>
												</div>
											</div>
										{/each}
									</div>
								{:else}
									<div class="flex flex-col items-center justify-center p-12 opacity-30">
										<Lock class="w-10 h-10 text-slate-400 mb-3" />
										<p class="text-[10px] text-slate-500 font-black uppercase italic">
											No policy statements
										</p>
										<p class="text-[9px] text-slate-400 italic mt-1">
											Add statements to control access
										</p>
									</div>
								{/if}
							</div>
						{/if}

						<!-- CFN Template Tab -->
						{#if activeTab === 'template'}
							<div class="space-y-4">
								<div class="flex items-center justify-between">
									<h4
										class="text-xs font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-2"
									>
										<FileCode class="w-3.5 h-3.5 text-rose-500" />
										CloudFormation Template
									</h4>
									<button
										onclick={() => loadTemplate(selectedApp!)}
										disabled={loadingTemplate}
										class="flex items-center gap-1.5 px-3 py-1.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl text-[10px] font-black uppercase tracking-widest italic transition-all active:scale-95 shadow-sm disabled:opacity-50"
									>
										<RefreshCw class="w-3 h-3 {loadingTemplate ? 'animate-spin' : ''}" />
										{loadingTemplate ? 'Generating...' : 'Generate Template'}
									</button>
								</div>

								{#if loadingTemplate}
									<div class="flex items-center justify-center p-12 opacity-40">
										<FileCode class="w-8 h-8 text-rose-500 animate-pulse" />
									</div>
								{:else if templateId}
									<div
										class="p-5 bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 space-y-4"
									>
										<div class="flex items-center justify-between">
											<div
												class="px-2.5 py-1 rounded-lg text-[10px] font-black uppercase italic border {templateStatus === 'ACTIVE'
													? 'bg-emerald-500/10 text-emerald-600 border-emerald-500/20'
													: templateStatus === 'EXPIRED'
														? 'bg-red-500/10 text-red-600 border-red-500/20'
														: 'bg-amber-500/10 text-amber-600 border-amber-500/20'}"
											>
												{templateStatus}
											</div>
											<div class="text-[9px] font-mono text-slate-400">ID: {templateId}</div>
										</div>
										{#if templateUrl}
											<div>
												<div
													class="text-[9px] font-black uppercase text-slate-500 italic mb-1 tracking-widest"
												>
													Template URL
												</div>
												<div
													class="flex items-center gap-2 p-3 bg-slate-50 dark:bg-slate-800 rounded-xl"
												>
													<code
														class="text-[9px] font-mono text-slate-600 dark:text-slate-300 flex-1 truncate"
														>{templateUrl}</code
													>
													<a
														href={templateUrl}
														target="_blank"
														rel="noopener"
														class="flex-shrink-0 p-1 hover:text-rose-600 transition-colors text-slate-400"
													>
														<ExternalLink class="w-3 h-3" />
													</a>
												</div>
											</div>
										{:else}
											<div class="flex items-center gap-2 text-[10px] text-amber-600 italic">
												<AlertCircle class="w-4 h-4" />
												No template URL — app may not have a version with a template attached.
											</div>
										{/if}
									</div>
								{:else}
									<div class="flex flex-col items-center justify-center p-12 opacity-30">
										<FileCode class="w-10 h-10 text-slate-400 mb-3" />
										<p class="text-[10px] text-slate-500 font-black uppercase italic">
											No template generated
										</p>
										<p class="text-[9px] text-slate-400 italic mt-1">
											Click Generate Template to create a CloudFormation template
										</p>
									</div>
								{/if}
							</div>
						{/if}

						<!-- Dependencies Tab -->
						{#if activeTab === 'dependencies'}
							<div class="space-y-4">
								<div class="flex items-center justify-between">
									<h4
										class="text-xs font-black text-slate-500 uppercase tracking-widest italic flex items-center gap-2"
									>
										<Network class="w-3.5 h-3.5 text-rose-500" />
										Nested Application Dependencies
									</h4>
									<button
										onclick={() => loadDependencies(selectedApp!)}
										class="p-2 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all"
									>
										<RefreshCw
											class="w-3.5 h-3.5 text-slate-500 {loadingDeps ? 'animate-spin' : ''}"
										/>
									</button>
								</div>

								{#if loadingDeps}
									<div class="flex items-center justify-center p-12 opacity-40">
										<Network class="w-8 h-8 text-rose-500 animate-pulse" />
									</div>
								{:else if dependencies.length}
									<div class="space-y-2">
										{#each dependencies as dep}
											<div
												class="p-4 bg-white dark:bg-slate-900 rounded-2xl border border-slate-100 dark:border-slate-800 flex items-center justify-between"
											>
												<div class="flex items-center gap-3">
													<div class="p-2 bg-purple-500/10 rounded-xl">
														<Network class="w-4 h-4 text-purple-500" />
													</div>
													<div>
														<div
															class="text-xs font-black text-slate-900 dark:text-white italic uppercase tracking-tight"
														>
															{dep.ApplicationId}
														</div>
														<div class="text-[9px] text-slate-400 font-mono italic mt-0.5">
															v{dep.SemanticVersion}
														</div>
													</div>
												</div>
												<ChevronRight class="w-4 h-4 text-slate-300" />
											</div>
										{/each}
									</div>
								{:else}
									<div class="flex flex-col items-center justify-center p-12 opacity-30">
										<Network class="w-10 h-10 text-slate-400 mb-3" />
										<p class="text-[10px] text-slate-500 font-black uppercase italic">
											No nested dependencies
										</p>
										<p class="text-[9px] text-slate-400 italic mt-1">
											This application has no nested application dependencies
										</p>
									</div>
								{/if}
							</div>
						{/if}
					</div>
				</div>
			{:else}
				<div
					class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl flex flex-col items-center justify-center min-h-[600px] opacity-40"
				>
					<Package class="w-16 h-16 text-slate-400 mb-4" />
					<p class="text-sm font-black uppercase italic text-slate-500 tracking-widest">
						Select an application
					</p>
					<p class="text-[10px] text-slate-400 italic mt-2">
						Click an app from the catalog to view details
					</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Application Modal -->
{#if showCreate}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm"
	>
		<div
			class="bg-white dark:bg-slate-900 rounded-3xl shadow-2xl border border-slate-200 dark:border-slate-700 w-full max-w-lg max-h-[90vh] overflow-y-auto"
		>
			<div class="p-6 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
				<h3 class="text-lg font-black uppercase italic tracking-tight text-slate-900 dark:text-white">
					Publish New Application
				</h3>
				<button
					onclick={() => { showCreate = false; resetCreateForm(); }}
					class="p-2 rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800 transition-all"
				>
					<X class="w-4 h-4 text-slate-500" />
				</button>
			</div>
			<div class="p-6 space-y-4">
				<div class="grid grid-cols-2 gap-4">
					<div class="col-span-2">
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-name">App Name * <span class="text-rose-500">(alphanumeric + hyphens)</span></label>
						<input id="new-name"
							type="text"
							bind:value={newName}
							placeholder="my-app"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
						/>
					</div>
					<div class="col-span-2">
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-desc">Description</label>
						<textarea id="new-desc"
							bind:value={newDesc}
							placeholder="Brief description..."
							rows="2"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none resize-none"
						></textarea>
					</div>
					<div>
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-author">Author *</label>
						<input id="new-author"
							type="text"
							bind:value={newAuthor}
							placeholder="Jane Doe"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
						/>
					</div>
					<div>
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-semantic-version">Initial Version</label>
						<input id="new-semantic-version"
							type="text"
							bind:value={newSemanticVersion}
							placeholder="1.0.0"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
						/>
					</div>
					<div class="col-span-2">
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-source-code-url">Source Code URL</label>
						<input id="new-source-code-url"
							type="text"
							bind:value={newSourceCodeURL}
							placeholder="https://github.com/..."
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
						/>
					</div>
					<div>
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-spdx-license-id">SPDX License ID</label>
						<input id="new-spdx-license-id"
							type="text"
							bind:value={newSpdxLicenseID}
							placeholder="Apache-2.0"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
						/>
					</div>
					<div>
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-labels">Labels (comma-separated)</label>
						<input id="new-labels"
							type="text"
							bind:value={newLabels}
							placeholder="web, api, lambda"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
						/>
					</div>
					<div class="col-span-2">
						<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
							 for="new-home-page-url">Homepage URL</label>
						<input id="new-home-page-url"
							type="text"
							bind:value={newHomePageURL}
							placeholder="https://..."
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
						/>
					</div>
				</div>
			</div>
			<div
				class="p-6 border-t border-slate-100 dark:border-slate-800 flex justify-end gap-3"
			>
				<button
					onclick={() => { showCreate = false; resetCreateForm(); }}
					class="px-5 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-sm font-black uppercase italic text-slate-600 transition-all"
					>Cancel</button
				>
				<button
					onclick={createApplication}
					disabled={creating}
					class="px-5 py-2.5 rounded-xl bg-rose-600 hover:bg-rose-700 text-white text-sm font-black uppercase italic transition-all shadow-lg shadow-rose-600/20 disabled:opacity-50"
				>
					{creating ? 'Publishing...' : 'Publish Application'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Edit Application Modal -->
{#if showEdit && selectedApp}
	<div
		class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/40 backdrop-blur-sm"
	>
		<div
			class="bg-white dark:bg-slate-900 rounded-3xl shadow-2xl border border-slate-200 dark:border-slate-700 w-full max-w-md"
		>
			<div class="p-6 border-b border-slate-100 dark:border-slate-800 flex items-center justify-between">
				<h3 class="text-lg font-black uppercase italic tracking-tight text-slate-900 dark:text-white">
					Edit — {selectedApp.Name}
				</h3>
				<button
					onclick={() => (showEdit = false)}
					class="p-2 rounded-xl hover:bg-slate-100 dark:hover:bg-slate-800 transition-all"
				>
					<X class="w-4 h-4 text-slate-500" />
				</button>
			</div>
			<div class="p-6 space-y-4">
				<div>
					<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
						 for="edit-desc">Description</label>
					<textarea id="edit-desc"
						bind:value={editDesc}
						rows="2"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none resize-none"
					></textarea>
				</div>
				<div>
					<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
						 for="edit-author">Author</label>
					<input id="edit-author"
						type="text"
						bind:value={editAuthor}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
					/>
				</div>
				<div>
					<label class="block text-[9px] font-black uppercase text-slate-500 italic mb-1"
						 for="edit-home-page-url">Homepage URL</label>
					<input id="edit-home-page-url"
						type="text"
						bind:value={editHomePageURL}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none"
					/>
				</div>
			</div>
			<div class="p-6 border-t border-slate-100 dark:border-slate-800 flex justify-end gap-3">
				<button
					onclick={() => (showEdit = false)}
					class="px-5 py-2.5 rounded-xl bg-slate-100 dark:bg-slate-800 hover:bg-slate-200 dark:hover:bg-slate-700 text-sm font-black uppercase italic text-slate-600 transition-all"
					>Cancel</button
				>
				<button
					onclick={updateApplication}
					disabled={editing}
					class="px-5 py-2.5 rounded-xl bg-rose-600 hover:bg-rose-700 text-white text-sm font-black uppercase italic transition-all shadow-lg shadow-rose-600/20 disabled:opacity-50"
				>
					{editing ? 'Saving...' : 'Save Changes'}
				</button>
			</div>
		</div>
	</div>
{/if}

<style>
	::-webkit-scrollbar {
		width: 6px;
	}
	::-webkit-scrollbar-track {
		background: transparent;
	}
	::-webkit-scrollbar-thumb {
		background: rgba(225, 29, 72, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(225, 29, 72, 0.2);
	}
</style>
