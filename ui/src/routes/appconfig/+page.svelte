<script lang="ts">
	import { onMount } from 'svelte';
	import { getAppConfigClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		ListEnvironmentsCommand,
		ListConfigurationProfilesCommand,
		ListDeploymentsCommand,
		DeleteApplicationCommand,
		CreateApplicationCommand,
		type Application,
		type Environment,
		type ConfigurationProfile,
		type DeploymentSummary
	} from '@aws-sdk/client-appconfig';
	import { toast } from 'svelte-sonner';
	import { 
		Settings, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Layers, Zap, Terminal, Workflow,
		FileCode, Sliders, Play, CheckCircle2, Code2,
		XCircle, AlertCircle, Timer, Server,
		Database, Share2, ArrowRight, Gauge,
		Settings2, Layout, Boxes, Shield
	} from 'lucide-svelte';
	const appconfig = getAppConfigClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let applications = $state<Application[]>([]);
	let selectedApp = $state<Application | null>(null);
	let environments = $state<Environment[]>([]);
	let profiles = $state<ConfigurationProfile[]>([]);
	let deployments = $state<DeploymentSummary[]>([]);
	let loadingDetails = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let newAppName = $state('');
	let creating = $state(false);

	// Derived
	const filteredApps = $derived(
		applications.filter(a => a.Name?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadApps() {
		loading = true;
		try {
			const res = await appconfig.send(new ListApplicationsCommand({}));
			applications = res.Items ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load AppConfig: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectApp(app: Application) {
		selectedApp = app;
		environments = [];
		profiles = [];
		deployments = [];
		loadingDetails = true;
		try {
			const envRes = await appconfig.send(new ListEnvironmentsCommand({ ApplicationId: app.Id }));
			environments = envRes.Items ?? [];

			const profRes = await appconfig.send(new ListConfigurationProfilesCommand({ ApplicationId: app.Id }));
			profiles = profRes.Items ?? [];

			if (environments.length > 0) {
				const depRes = await appconfig.send(new ListDeploymentsCommand({ 
					ApplicationId: app.Id, 
					EnvironmentId: environments[0].Id 
				}));
				deployments = depRes.Items ?? [];
			}
		} catch (err: unknown) {
			toast.error(`Failed to load app details: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function createApplication() {
		if (!newAppName.trim()) return;
		creating = true;
		try {
			await appconfig.send(new CreateApplicationCommand({ Name: newAppName.trim() }));
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
		if (!id || !confirm(`Delete AppConfig application? This will permanently remove all profiles and environments.`)) return;
		try {
			await appconfig.send(new DeleteApplicationCommand({ ApplicationId: id }));
			toast.success(`Application deleted`);
			if (selectedApp?.Id === id) selectedApp = null;
			await loadApps();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function getDeploymentStatusIcon(status: string | undefined) {
		if (status === 'COMPLETE') return CheckCircle2;
		if (status === 'ROLLING_BACK') return XCircle;
		if (status === 'DEPLOYING') return RefreshCw;
		return AlertCircle;
	}

	function getDeploymentStatusColor(status: string | undefined): string {
		if (status === 'COMPLETE') return 'text-emerald-500';
		if (status === 'ROLLING_BACK') return 'text-rose-500';
		if (status === 'DEPLOYING') return 'text-amber-500 animate-spin';
		return 'text-slate-400';
	}

	onMount(() => {
		loadApps();
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
				onclick={loadApps}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-black shadow-lg shadow-blue-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Assemble Application
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
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-blue-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !applications.length}
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
								class="p-4 flex items-center justify-between hover:bg-blue-500/5 dark:hover:bg-blue-500/10 cursor-pointer transition-all {selectedApp?.Id === app.Id ? 'bg-blue-500/10 border-l-4 border-blue-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3">
									<Layout class="w-4 h-4 text-blue-600" />
									<div>
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate max-w-[150px]">{app.Name}</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{app.Id}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}

						{#if !applications.length}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No applications detected.</div>
						{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-9 space-y-6">
			{#if selectedApp}
				<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
					<!-- Topology Map -->
					<div class="lg:col-span-7 space-y-6">
						<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
							<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-blue-500/5 to-indigo-500/5 flex justify-between items-start">
								<div>
									<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedApp.Name}</h2>
									<div class="flex items-center gap-3 mt-4">
										<div class="px-2 py-0.5 rounded-lg bg-blue-500/10 text-blue-600 text-[9px] font-black uppercase tracking-widest border border-blue-500/20">
											{environments.length} ENVIRONMENTS
										</div>
										<div class="px-2 py-0.5 rounded-lg bg-emerald-500/10 text-emerald-600 text-[9px] font-black uppercase tracking-widest border border-emerald-500/20">
											{profiles.length} PROFILES
										</div>
									</div>
								</div>
								<button 
									onclick={() => deleteApplication(selectedApp?.Id)}
									class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
									title="Purge Application"
								>
									<Trash2 class="w-4 h-4" />
								</button>
							</div>

							<div class="p-8 space-y-8">
								<!-- Environment Explorer -->
								<div class="space-y-4">
									<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
										<Globe class="w-4 h-4 text-blue-500" />
										Target Environments
									</h3>
									<div class="flex flex-wrap gap-2">
										{#each environments || [] as env}
											<div class="px-4 py-3 bg-white/60 dark:bg-slate-900/60 rounded-2xl border border-slate-100 dark:border-slate-700/50 shadow-sm flex items-center gap-3 group/env">
												<div class="w-2 h-2 rounded-full {env.State === 'READY_FOR_DEPLOYMENT' ? 'bg-emerald-500' : 'bg-slate-400'} group-hover/env:scale-125 transition-transform"></div>
												<span class="text-[10px] font-black text-slate-900 dark:text-white uppercase italic">{env.Name}</span>
											</div>
										{:else}
											<div class="p-6 w-full text-center border border-dashed border-slate-200 dark:border-slate-700 rounded-3xl text-[10px] text-slate-400 italic font-black uppercase tracking-widest">No target environments provisioned.</div>
										{/each}
									</div>
								</div>

								<!-- Profile Registry -->
								<div class="space-y-4">
									<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
										<FileCode class="w-4 h-4 text-indigo-500" />
										Configuration Sources
									</h3>
									<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
										{#each profiles || [] as prof}
											<div class="p-5 bg-white/60 dark:bg-slate-900/60 rounded-3xl border border-slate-100 dark:border-slate-700/50 shadow-sm group/prof hover:border-blue-500/30 transition-all flex justify-between items-center">
												<div class="flex items-center gap-3">
													<div class="p-2 bg-slate-900 dark:bg-black rounded-xl">
														<Code2 class="w-3.5 h-3.5 text-blue-500" />
													</div>
													<div>
														<div class="text-[10px] font-black text-slate-900 dark:text-white uppercase italic leading-none">{prof.Name}</div>
														<div class="text-[8px] text-slate-400 uppercase font-black mt-1">{prof.Type || 'FREEFORM'}</div>
													</div>
												</div>
												<ArrowRight class="w-3.5 h-3.5 text-slate-300 opacity-0 group-hover/prof:opacity-100 transition-opacity" />
											</div>
										{/each}
									</div>
								</div>
							</div>
						</div>
					</div>

					<!-- Deployment History -->
					<div class="lg:col-span-5 space-y-6">
						<div class="bg-slate-900 dark:bg-black rounded-[2.5rem] shadow-2xl border border-slate-800 overflow-hidden h-[600px] flex flex-col animate-in slide-in-from-right-8 duration-500">
							<div class="p-6 border-b border-white/5 bg-white/5 flex items-center justify-between">
								<div class="flex items-center gap-3">
									<Workflow class="w-5 h-5 text-blue-500" />
									<div>
										<h4 class="text-xs font-black text-white uppercase tracking-widest italic leading-none">Rollout Ledger</h4>
										<span class="text-[8px] font-bold text-slate-500 uppercase">Interactive Deployment History</span>
									</div>
								</div>
								<div class="flex items-center gap-1.5 px-3 py-1 rounded-full bg-blue-500/10 border border-blue-500/20">
									<span class="w-1.5 h-1.5 rounded-full bg-blue-500 animate-pulse"></span>
									<span class="text-[9px] font-black text-blue-600 uppercase tracking-widest italic">REALTIME</span>
								</div>
							</div>

							<div class="flex-1 overflow-y-auto p-8 space-y-4">
								{#if loadingDetails}
									<div class="flex flex-col items-center justify-center h-full opacity-30">
										<RefreshCw class="w-10 h-10 text-blue-500 animate-spin mb-4" />
										<span class="text-[9px] uppercase font-black text-slate-500 tracking-[0.2em]">Listing Rollouts...</span>
									</div>
								{:else if deployments.length}
									{#each deployments as dep}
										{@const StatusIcon = getDeploymentStatusIcon(dep.State)}
										<div class="p-5 bg-white/5 rounded-3xl border border-white/10 hover:border-blue-500/30 transition-all group/job">
											<div class="flex justify-between items-start mb-4">
												<div>
													<div class="text-[9px] font-black text-slate-400 uppercase italic tracking-tighter mb-1 font-mono">DEP-{dep.DeploymentNumber}</div>
													<div class="flex items-center gap-2">
														<StatusIcon class="w-3.5 h-3.5 {getDeploymentStatusColor(dep.State)}" />
														<span class="text-[10px] font-black text-white uppercase tracking-widest">{dep.State}</span>
													</div>
												</div>
												<div class="text-right">
													<div class="text-[8px] font-black text-slate-600 uppercase">Version</div>
													<span class="text-[10px] font-black text-slate-400 tabular-nums italic">v{dep.ConfigurationVersion}</span>
												</div>
											</div>
											<div class="flex items-center justify-between pt-4 border-t border-white/5">
												<div class="flex items-center gap-2">
													<Timer class="w-3 h-3 text-slate-600" />
													<span class="text-[9px] font-bold text-slate-500 tabular-nums">{dep.CompletedAt?.toLocaleString() || 'In Progress'}</span>
												</div>
												<ExternalLink class="w-3 h-3 text-slate-700 opacity-0 group-hover/job:opacity-100 transition-opacity" />
											</div>
										</div>
									{/each}
								{:else}
									<div class="flex flex-col items-center justify-center h-full opacity-20 p-12 text-center">
										<Boxes class="w-16 h-16 text-slate-600 mb-6" />
										<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">No Deployments</h4>
										<p class="text-[10px] text-slate-600 italic">This application hasn't executed any configuration rollouts yet. Deploy a profile to a target environment to begin the configuration lifecycle.</p>
									</div>
								{/if}
							</div>
						</div>

						<!-- Metrics -->
						<div class="p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl flex items-center justify-between group/metric overflow-hidden relative">
							<div class="absolute inset-0 bg-gradient-to-r from-blue-500/5 to-transparent pointer-events-none"></div>
							<div class="flex items-center gap-4 relative z-10">
								<div class="p-3 bg-blue-500/10 rounded-2xl group-hover/metric:scale-110 transition-transform">
									<Gauge class="w-6 h-6 text-blue-600" />
								</div>
								<div>
									<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic leading-none">Propagation Speed</div>
									<div class="text-lg font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">HARDENED</div>
								</div>
							</div>
							<div class="text-right relative z-10">
								<div class="text-[8px] text-slate-500 uppercase font-black mb-1">Status</div>
								<div class="flex items-center gap-1.5">
									<div class="w-1.5 h-1.5 rounded-full bg-emerald-500"></div>
									<span class="text-[10px] text-slate-800 dark:text-white font-black uppercase">SYNCED</span>
								</div>
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<Settings2 class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">Configuration Topography</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Oversee application configuration lifecycles, manage environment-specific rollouts, and monitor real-time configuration propagation through an enterprise-grade control plane.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateModal = false}></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-blue-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Assemble Application</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createApplication(); }} class="space-y-6">
					<div>
						<label for="appName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Application Identifier</label>
						<input 
							id="appName"
							type="text" 
							bind:value={newAppName}
							placeholder="e.g. gopherstack-microservices-config"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-blue-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div class="p-5 bg-blue-500/5 rounded-2xl border border-blue-500/10">
						<div class="flex items-center gap-2 mb-2">
							<Shield class="w-3.5 h-3.5 text-blue-500" />
							<span class="text-[10px] font-black text-blue-600 uppercase tracking-widest leading-none">Configuration Baseline</span>
						</div>
						<p class="text-[9px] text-blue-800 dark:text-blue-400 leading-relaxed font-bold uppercase tracking-tight italic">
							Registering an application allows you to group profiles and environments. Configuration validators will be enforced during rollout.
						</p>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-blue-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Assembling...' : 'Provision Blueprint'}
						</button>
					</div>
				</form>
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
