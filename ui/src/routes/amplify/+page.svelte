<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAmplifyClient } from '$lib/aws-client';
	import {
		ListAppsCommand,
		ListBranchesCommand,
		ListJobsCommand,
		DeleteAppCommand,
		CreateAppCommand,
		ListWebhooksCommand,
		CreateWebhookCommand,
		DeleteWebhookCommand,
		StartJobCommand,
		ListDomainAssociationsCommand,
		CreateDomainAssociationCommand,
		type App,
		type Branch,
		type JobSummary,
		type Webhook,
		type DomainAssociation
	} from '@aws-sdk/client-amplify';
	import { toast } from 'svelte-sonner';
	import { 
		Layout, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Layers, Share2, Play, CheckCircle2, 
		XCircle, AlertCircle, Timer, Zap,
		ArrowRight, ExternalLink, Shield,
		GitBranch, GitCommit, Settings,
		ArrowDown, Layout as LayoutIcon, Code2, Server,
		Database, Terminal, Smartphone, Tablet, Laptop,
		Link, Network, Gauge
	} from 'lucide-svelte';

	const amplify = regionalClient(getAmplifyClient);

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let apps = $state<App[]>([]);
	let selectedApp = $state<App | null>(null);
	let branches = $state<Branch[]>([]);
	let selectedBranch = $state<Branch | null>(null);
	let jobs = $state<JobSummary[]>([]);
	let loadingDetails = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let newAppName = $state('');
	let repoUrl = $state('https://github.com/example/my-amplify-app');
	let creating = $state(false);

	// Derived
	const filteredApps = $derived(
		apps.filter(app => app.name?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadApps() {
		loading = true;
		try {
			const res = await amplify().send(new ListAppsCommand({}));
			apps = res.apps ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load apps: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectApp(app: App) {
		selectedApp = app;
		branches = [];
		selectedBranch = null;
		jobs = [];
		loadingDetails = true;
		try {
			const branchRes = await amplify().send(new ListBranchesCommand({ appId: app.appId }));
			branches = branchRes.branches ?? [];
			if (branches.length > 0) {
				await selectBranch(branches[0]);
			}
			await loadExtras(app);
		} catch (err: unknown) {
			toast.error(`Failed to load branches: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function selectBranch(branch: Branch) {
		selectedBranch = branch;
		loadingDetails = true;
		try {
			const jobRes = await amplify().send(new ListJobsCommand({ 
				appId: selectedApp?.appId, 
				branchName: branch.branchName 
			}));
			jobs = jobRes.jobSummaries ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load jobs: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	// Webhooks (build triggers) + custom domains
	let webhooks = $state<Webhook[]>([]);
	let domains = $state<DomainAssociation[]>([]);
	let loadingExtras = $state(false);
	let newWebhookBranch = $state('');
	let creatingWebhook = $state(false);
	let triggeringWebhook = $state<string | null>(null);
	let showDomainModal = $state(false);
	let newDomainName = $state('');
	let newDomainBranch = $state('');
	let newDomainPrefix = $state('');
	let creatingDomain = $state(false);

	async function loadExtras(app: App) {
		loadingExtras = true;
		try {
			const [whRes, domRes] = await Promise.all([
				amplify().send(new ListWebhooksCommand({ appId: app.appId })),
				amplify().send(new ListDomainAssociationsCommand({ appId: app.appId }))
			]);
			webhooks = whRes.webhooks ?? [];
			domains = domRes.domainAssociations ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load webhooks/domains: ${(err as Error).message}`);
		} finally {
			loadingExtras = false;
		}
	}

	async function createWebhook() {
		if (!selectedApp || !newWebhookBranch.trim()) return;
		creatingWebhook = true;
		try {
			await amplify().send(
				new CreateWebhookCommand({
					appId: selectedApp.appId,
					branchName: newWebhookBranch.trim(),
					description: `Build trigger for ${newWebhookBranch.trim()}`
				})
			);
			toast.success(`Webhook created for ${newWebhookBranch}`);
			newWebhookBranch = '';
			await loadExtras(selectedApp);
		} catch (err: unknown) {
			toast.error(`Failed to create webhook: ${(err as Error).message}`);
		} finally {
			creatingWebhook = false;
		}
	}

	async function deleteWebhook(id: string | undefined) {
		if (!id || !selectedApp) return;
		if (!(await confirmDestructive({ title: 'Delete Webhook', message: 'Delete this build-trigger webhook?' }))) return;
		try {
			await amplify().send(new DeleteWebhookCommand({ webhookId: id }));
			toast.success('Webhook deleted');
			await loadExtras(selectedApp);
		} catch (err: unknown) {
			toast.error(`Failed to delete webhook: ${(err as Error).message}`);
		}
	}

	// Trigger a build for the webhook's branch (equivalent of POSTing to the
	// webhook URL).
	async function triggerWebhook(wh: Webhook) {
		if (!selectedApp || !wh.branchName) return;
		triggeringWebhook = wh.webhookId ?? wh.branchName;
		try {
			await amplify().send(
				new StartJobCommand({
					appId: selectedApp.appId,
					branchName: wh.branchName,
					jobType: 'RELEASE'
				})
			);
			toast.success(`Build triggered for ${wh.branchName}`);
			if (selectedBranch?.branchName === wh.branchName) await selectBranch(selectedBranch);
		} catch (err: unknown) {
			toast.error(`Failed to trigger build: ${(err as Error).message}`);
		} finally {
			triggeringWebhook = null;
		}
	}

	async function createDomain() {
		if (!selectedApp || !newDomainName.trim() || !newDomainBranch.trim()) return;
		creatingDomain = true;
		try {
			await amplify().send(
				new CreateDomainAssociationCommand({
					appId: selectedApp.appId,
					domainName: newDomainName.trim(),
					subDomainSettings: [
						{ prefix: newDomainPrefix.trim(), branchName: newDomainBranch.trim() }
					]
				})
			);
			toast.success(`Domain ${newDomainName} associated`);
			showDomainModal = false;
			newDomainName = '';
			newDomainBranch = '';
			newDomainPrefix = '';
			await loadExtras(selectedApp);
		} catch (err: unknown) {
			toast.error(`Failed to associate domain: ${(err as Error).message}`);
		} finally {
			creatingDomain = false;
		}
	}

	async function createApp() {
		if (!newAppName.trim()) return;
		creating = true;
		try {
			await amplify().send(new CreateAppCommand({
				name: newAppName.trim(),
				repository: repoUrl.trim()
			}));
			toast.success(`App "${newAppName}" created`);
			showCreateModal = false;
			newAppName = '';
			await loadApps();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApp(id: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Delete Amplify App', message: 'Delete this Amplify app? All environments and hosting configurations will be removed.' })) return;
		try {
			await amplify().send(new DeleteAppCommand({ appId: id }));
			toast.success(`App deleted`);
			if (selectedApp?.appId === id) selectedApp = null;
			await loadApps();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function getJobStatusIcon(status: string | undefined) {
		if (status === 'SUCCEED') return CheckCircle2;
		if (status === 'FAILED') return XCircle;
		if (status === 'RUNNING') return RefreshCw;
		return AlertCircle;
	}

	function getJobStatusColor(status: string | undefined): string {
		if (status === 'SUCCEED') return 'text-emerald-500';
		if (status === 'FAILED') return 'text-rose-500';
		if (status === 'RUNNING') return 'text-amber-500 animate-spin';
		return 'text-slate-400';
	}

	onRegionChange(loadApps);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-amber-500/20 rounded-xl shadow-inner">
				<LayoutIcon class="w-8 h-8 text-amber-500" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-amber-600 to-orange-600 dark:from-amber-400 dark:to-orange-400 bg-clip-text text-transparent italic tracking-tight">Amplify Full-Stack</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Accelerated mobile and web application hosting with managed backend environments.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadApps}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-amber-600 hover:bg-amber-700 text-white rounded-xl font-black shadow-lg shadow-amber-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Assemble App
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- App List -->
		<div class="lg:col-span-3 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search applications..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-amber-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !apps.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse"><div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div></div>
						{/each}
					{:else}
						{#each filteredApps as app}
							<div 
								role="button"
								tabindex="0"
								onclick={() => selectApp(app)}
								onkeydown={(e) => e.key === 'Enter' && selectApp(app)}
								class="p-4 flex items-center justify-between hover:bg-amber-500/5 dark:hover:bg-amber-500/10 cursor-pointer transition-all {selectedApp?.appId === app.appId ? 'bg-amber-500/10 border-l-4 border-amber-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3">
									<Globe class="w-4 h-4 text-amber-600" />
									<div>
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate max-w-[150px]">{app.name}</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{app.defaultDomain || 'No domain assigned'}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}

						{#if !apps.length}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No Amplify apps found.</div>
						{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-9 space-y-6">
			{#if selectedApp}
				<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
					<!-- App Config -->
					<div class="lg:col-span-6 space-y-6">
						<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
							<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-amber-500/5 to-orange-500/5 flex justify-between items-start">
								<div>
									<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedApp.name}</h2>
									<div class="flex items-center gap-3 mt-4">
										<div class="px-2 py-0.5 rounded-lg bg-amber-500/10 text-amber-600 text-[9px] font-black uppercase tracking-widest border border-amber-500/20">
											WEB_APPS
										</div>
										<div class="px-2 py-0.5 rounded-lg bg-emerald-500/10 text-emerald-600 text-[9px] font-black uppercase tracking-widest border border-emerald-500/20">
											OPERATIONAL
										</div>
									</div>
								</div>
								<button 
									onclick={() => deleteApp(selectedApp?.appId)}
									class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
									title="Explode App"
								>
									<Trash2 class="w-4 h-4" />
								</button>
							</div>

							<div class="p-8 space-y-8">
								<!-- Domain -->
								<div class="space-y-4">
									<h3 class="flex items-center gap-2 text-xs font-black text-slate-500 uppercase tracking-widest italic leading-none">
										<Network class="w-4 h-4 text-amber-500" />
										Service Endpoint
									</h3>
									<div class="p-4 bg-slate-900 dark:bg-black rounded-3xl border border-slate-800 shadow-inner group/end">
										<div class="font-mono text-[10px] text-amber-400 break-all select-all flex items-start gap-3">
											<ExternalLink class="w-3.5 h-3.5 mt-0.5 shrink-0 text-slate-600 group-hover/end:text-amber-500 transition-colors" />
											{selectedApp.defaultDomain || 'N/A'}
										</div>
									</div>
								</div>

								<!-- Repo Source -->
								<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/repo">
									<div class="flex items-center justify-between mb-4">
										<div class="flex items-center gap-3">
											<div class="p-2 bg-slate-900 dark:bg-black rounded-xl">
												<Link class="w-4 h-4 text-amber-500" />
											</div>
											<div>
												<div class="text-[10px] font-black text-slate-400 uppercase tracking-widest leading-none">Source Repository</div>
												<div class="text-[11px] font-black text-slate-900 dark:text-white uppercase italic mt-1 truncate max-w-[200px]">{selectedApp.repository || 'Direct Upload'}</div>
											</div>
										</div>
									</div>
								</div>

								<!-- Environment Branches -->
								<div class="space-y-4">
									<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
										<GitBranch class="w-4 h-4 text-amber-500" />
										Provisioned Branches
									</h3>
									<div class="flex flex-wrap gap-2">
										{#each branches as branch}
											<button 
												onclick={() => selectBranch(branch)}
												class="px-4 py-2 rounded-xl flex items-center gap-2 transition-all {selectedBranch?.branchName === branch.branchName ? 'bg-amber-600 text-white shadow-lg scale-105' : 'bg-white/50 dark:bg-slate-700/50 text-slate-600 dark:text-slate-300 border border-slate-200 dark:border-slate-600 hover:bg-white dark:hover:bg-slate-700'}"
											>
												<GitBranch class="w-3.5 h-3.5" />
												<span class="text-[10px] font-black uppercase tracking-tighter font-mono">{branch.branchName}</span>
											</button>
										{/each}
									</div>
								</div>

								<!-- Build Triggers (Webhooks) -->
								<div class="space-y-4">
									<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
										<Zap class="w-4 h-4 text-amber-500" />
										Build Triggers
									</h3>
									<div class="flex gap-2">
										<input
											type="text"
											bind:value={newWebhookBranch}
											placeholder="branch name"
											class="flex-1 px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-xs font-mono outline-none focus:ring-2 focus:ring-amber-500"
										/>
										<button
											onclick={createWebhook}
											disabled={creatingWebhook || !newWebhookBranch.trim()}
											class="px-3 py-2 bg-amber-600 text-white rounded-xl text-[10px] font-black uppercase tracking-widest disabled:opacity-50"
										>
											{creatingWebhook ? '...' : 'Add Hook'}
										</button>
									</div>
									{#if loadingExtras}
										<p class="text-[10px] text-slate-400 italic">Loading…</p>
									{:else if webhooks.length === 0}
										<p class="text-[10px] text-slate-400 italic">No build-trigger webhooks.</p>
									{:else}
										<div class="space-y-2">
											{#each webhooks as wh}
												<div class="flex items-center gap-2 p-2 rounded-xl bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600">
													<GitBranch class="w-3.5 h-3.5 text-amber-500 shrink-0" />
													<span class="text-[10px] font-mono font-black truncate">{wh.branchName}</span>
													<span class="text-[8px] text-slate-400 font-mono truncate flex-1">{wh.webhookId}</span>
													<button
														onclick={() => triggerWebhook(wh)}
														disabled={triggeringWebhook === (wh.webhookId ?? wh.branchName)}
														title="Trigger build"
														class="rounded p-1 text-emerald-600 hover:bg-emerald-100 disabled:opacity-40 dark:hover:bg-emerald-900/30"
													>
														<Play class="w-3.5 h-3.5" />
													</button>
													<button
														onclick={() => deleteWebhook(wh.webhookId)}
														title="Delete webhook"
														class="rounded p-1 text-rose-600 hover:bg-rose-100 dark:hover:bg-rose-900/30"
													>
														<Trash2 class="w-3.5 h-3.5" />
													</button>
												</div>
											{/each}
										</div>
									{/if}
								</div>

								<!-- Custom Domains -->
								<div class="space-y-4">
									<div class="flex items-center justify-between">
										<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
											<Globe class="w-4 h-4 text-amber-500" />
											Custom Domains
										</h3>
										<button
											onclick={() => (showDomainModal = true)}
											class="flex items-center gap-1 px-2 py-1 bg-amber-600 text-white rounded-lg text-[9px] font-black uppercase tracking-widest"
										>
											<Plus class="w-3 h-3" /> Add
										</button>
									</div>
									{#if loadingExtras}
										<p class="text-[10px] text-slate-400 italic">Loading…</p>
									{:else if domains.length === 0}
										<p class="text-[10px] text-slate-400 italic">No custom domains associated.</p>
									{:else}
										<div class="space-y-2">
											{#each domains as dom}
												<div class="p-3 rounded-xl bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600">
													<div class="flex items-center justify-between">
														<span class="text-[11px] font-black font-mono truncate">{dom.domainName}</span>
														<span class="text-[8px] px-2 py-0.5 rounded bg-amber-500/10 text-amber-600 font-black uppercase tracking-widest">{dom.domainStatus}</span>
													</div>
													{#each dom.subDomains ?? [] as sd}
														<div class="text-[9px] text-slate-400 font-mono mt-1 truncate">
															{sd.subDomainSetting?.prefix || '@'} → {sd.subDomainSetting?.branchName}
														</div>
													{/each}
												</div>
											{/each}
										</div>
									{/if}
								</div>
							</div>
						</div>
					</div>

					<!-- Job History -->
					<div class="lg:col-span-6 space-y-6">
						<div class="bg-slate-900 dark:bg-black rounded-[2.5rem] shadow-2xl border border-slate-800 overflow-hidden h-[600px] flex flex-col animate-in slide-in-from-right-8 duration-500">
							<div class="p-6 border-b border-white/5 bg-white/5 flex items-center justify-between">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-amber-500" />
									<div>
										<h4 class="text-xs font-black text-white uppercase tracking-widest italic leading-none">Deployment Ledger</h4>
										<span class="text-[8px] font-bold text-slate-500 uppercase">Interactive Provisioning History</span>
									</div>
								</div>
								<div class="px-2 py-0.5 rounded-lg bg-white/5 border border-white/10 text-[9px] text-slate-500 font-mono italic">
									{jobs.length} JOBS
								</div>
							</div>

							<div class="flex-1 overflow-y-auto p-8 space-y-4">
								{#if loadingDetails}
									<div class="flex flex-col items-center justify-center h-full opacity-30">
										<RefreshCw class="w-10 h-10 text-amber-500 animate-spin mb-4" />
										<span class="text-[9px] uppercase font-black text-slate-500 tracking-[0.2em]">Listing Jobs...</span>
									</div>
								{:else if jobs.length}
									{#each jobs as job}
										{@const StatusIcon = getJobStatusIcon(job.status)}
										<div class="p-5 bg-white/5 rounded-3xl border border-white/10 hover:border-amber-500/30 transition-all group/job">
											<div class="flex justify-between items-start mb-4">
												<div>
													<div class="text-[9px] font-black text-slate-400 uppercase italic tracking-tighter mb-1 font-mono">#{job.jobId}</div>
													<div class="flex items-center gap-2">
														<StatusIcon class="w-3.5 h-3.5 {getJobStatusColor(job.status)}" />
														<span class="text-[10px] font-black text-white uppercase tracking-widest">{job.status}</span>
													</div>
												</div>
												<div class="text-right">
													<div class="text-[8px] font-black text-slate-600 uppercase">Commit</div>
													<span class="text-[10px] font-black text-slate-400 font-mono tracking-tighter">{job.commitId?.slice(0, 7) || 'HEAD'}</span>
												</div>
											</div>
											<div class="flex items-center justify-between pt-4 border-t border-white/5">
												<div class="flex items-center gap-2">
													<Timer class="w-3 h-3 text-slate-600" />
													<span class="text-[9px] font-bold text-slate-500 tabular-nums">{job.startTime?.toLocaleString() || 'N/A'}</span>
												</div>
												<ExternalLink class="w-3 h-3 text-slate-700 opacity-0 group-hover/job:opacity-100 transition-opacity" />
											</div>
										</div>
									{/each}
								{:else}
									<div class="flex flex-col items-center justify-center h-full opacity-20 p-12 text-center">
										<Smartphone class="w-16 h-16 text-slate-600 mb-6" />
										<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">No Deployments</h4>
										<p class="text-[10px] text-slate-600 italic">This application hasn't processed any release jobs yet. Deploy a backend or frontend branch to begin the provisioning lifecycle.</p>
									</div>
								{/if}
							</div>
						</div>

						<!-- Metrics -->
						<div class="p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl flex items-center justify-between group/metric overflow-hidden relative">
							<div class="absolute inset-0 bg-gradient-to-r from-amber-500/5 to-transparent pointer-events-none"></div>
							<div class="flex items-center gap-4 relative z-10">
								<div class="p-3 bg-amber-500/10 rounded-2xl group-hover/metric:scale-110 transition-transform">
									<Gauge class="w-6 h-6 text-amber-600" />
								</div>
								<div>
									<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic leading-none">Interface Latency</div>
									<div class="text-lg font-black text-slate-900 dark:text-white uppercase tracking-tighter">OPTIMIZED</div>
								</div>
							</div>
							<div class="text-right relative z-10">
								<div class="text-[8px] text-slate-500 uppercase font-black mb-1">Status</div>
								<div class="flex items-center gap-1.5">
									<div class="w-1.5 h-1.5 rounded-full bg-emerald-500"></div>
									<span class="text-[10px] text-slate-800 dark:text-white font-black uppercase">LIVE</span>
								</div>
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<LayoutIcon class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">Mobile/Web Orchestrator</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Oversee full-stack application lifecycles, track branch-specific backend environments, and analyze real-time deployment jobs through an high-resolution hosting topography.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateModal = false} onkeydown={(e) => { if (e.key === 'Escape') showCreateModal = false; }} role="presentation"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-amber-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Assemble Full-Stack App</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createApp(); }} class="space-y-6">
					<div>
						<label for="appName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Application Identifier</label>
						<input 
							id="appName"
							type="text" 
							bind:value={newAppName}
							placeholder="e.g. gopherstack-frontend-v5"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-amber-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div>
						<label for="repoUrl" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Source Origin (URL)</label>
						<input 
							id="repoUrl"
							type="text" 
							bind:value={repoUrl}
							placeholder="https://github.com/..."
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-amber-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-amber-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg shadow-amber-600/20 active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Assembling...' : 'Deploy Blueprint'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<!-- Domain Association Modal -->
{#if showDomainModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => (showDomainModal = false)} onkeydown={(e) => { if (e.key === 'Escape') showDomainModal = false; }} role="presentation"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-amber-500/20 overflow-hidden">
			<div class="p-8 space-y-5">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">Associate Custom Domain</h3>
				<div>
					<label for="dom-name" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 italic">Domain Name</label>
					<input id="dom-name" type="text" bind:value={newDomainName} placeholder="example.com" class="w-full px-5 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-2xl outline-none focus:ring-2 focus:ring-amber-500 font-mono text-xs" />
				</div>
				<div>
					<label for="dom-branch" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 italic">Branch</label>
					<input id="dom-branch" type="text" bind:value={newDomainBranch} placeholder="main" class="w-full px-5 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-2xl outline-none focus:ring-2 focus:ring-amber-500 font-mono text-xs" />
				</div>
				<div>
					<label for="dom-prefix" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 italic">Subdomain Prefix (blank = root)</label>
					<input id="dom-prefix" type="text" bind:value={newDomainPrefix} placeholder="www" class="w-full px-5 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-2xl outline-none focus:ring-2 focus:ring-amber-500 font-mono text-xs" />
				</div>
				<div class="flex gap-4 pt-2">
					<button type="button" onclick={() => (showDomainModal = false)} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest">Cancel</button>
					<button onclick={createDomain} disabled={creatingDomain || !newDomainName.trim() || !newDomainBranch.trim()} class="flex-1 px-4 py-3 bg-amber-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest disabled:opacity-50">
						{creatingDomain ? 'Associating…' : 'Associate'}
					</button>
				</div>
			</div>
		</div>
	</div>
{/if}

<style>
	/* Custom scrollbar */
	::-webkit-scrollbar {
		width: 6px;
	}
	::-webkit-scrollbar-track {
		background: transparent;
	}
	::-webkit-scrollbar-thumb {
		background: rgba(148, 163, 184, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(148, 163, 184, 0.2);
	}
</style>
