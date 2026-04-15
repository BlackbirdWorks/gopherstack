<script lang="ts">
	import { onMount } from 'svelte';
	import { getFISClient } from '$lib/aws-client';
	import {
		ListExperimentsCommand,
		ListExperimentTemplatesCommand,
		GetExperimentCommand,
		GetExperimentTemplateCommand,
		StartExperimentCommand,
		StopExperimentCommand,
		type ExperimentSummary,
		type ExperimentTemplateSummary,
		type Experiment
	} from '@aws-sdk/client-fis';
	import { toast } from 'svelte-sonner';
	import { 
		ZapOff, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Link, Share2, Shield, CheckCircle2, 
		XCircle, AlertCircle, Layout, Boxes, 
		Archive, Timer, Settings, Workflow, 
		Terminal, ExternalLink, Gauge, BarChart3, 
		Wifi, Smartphone, Laptop, DatabaseBackup, 
		Route, Radio, Router, Cpu, Sliders,
		Layers, Workflow as WorkflowIcon,
		Globe2, Server, Target, Zap,
		Database, HardDrive, ListTree,
		ServerCrash, Binary, Scan, Radar,
		SearchCode, KeyRound, ShieldQuestion,
		Layers3, Boxes as ClusterIcon,
		PieChart, TrendingUp, Filter,
		Share, LayoutGrid, ActivitySquare,
		Smartphone as Mobile, Play, VideoOff,
		Eye, Radar as Sensors, RadioTower, Tv, 
		AppWindow, Binary as AppIcon,
		UserCircle, MousePointer2,
		CloudRain, CloudLightning,
		FastForward, History, Navigation,
		Orbit, Map, MapPin, Building,
		Skull, Bomb, Flame, Zap as Spark,
		Activity as Pulse, PlayCircle, StopCircle,
		ShieldAlert, Ban, Timer as ClockIcon
	} from 'lucide-svelte';

	const fis = getFISClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let experiments = $state<ExperimentSummary[]>([]);
	let templates = $state<ExperimentTemplateSummary[]>([]);
	let selectedExperiment = $state<Experiment | null>(null);
	let loadingDetails = $state(false);

	// Tabs
	let activeTab = $state<'experiments' | 'templates'>('experiments');

	// Derived
	const filteredExperiments = $derived(
		experiments.filter(e => e.id?.toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredTemplates = $derived(
		templates.filter(t => t.description?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const stats = $derived({
		activeExperiments: experiments.filter(e => e.state?.status === 'running').length,
		totalTemplates: templates.length,
		chaosIntensity: 'SEVERE'
	});

	// Actions
	async function loadFIS() {
		loading = true;
		try {
			const expRes = await fis.send(new ListExperimentsCommand({}));
			experiments = expRes.experiments ?? [];

			const tempRes = await fis.send(new ListExperimentTemplatesCommand({}));
			templates = tempRes.experimentTemplates ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load FIS: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectExperiment(id: string | undefined) {
		if (!id) return;
		loadingDetails = true;
		try {
			const res = await fis.send(new GetExperimentCommand({ id }));
			selectedExperiment = res.experiment ?? null;
		} catch (err: unknown) {
			toast.error(`Failed to load experiment details: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function stopExperiment(id: string | undefined) {
		if (!id || !confirm(`Cease Fault Injection cycle? State restoration artifacts will be prioritized.`)) return;
		try {
			await fis.send(new StopExperimentCommand({ id }));
			toast.success(`Experiment cessation triggered`);
			await selectExperiment(id);
			await loadFIS();
		} catch (err: unknown) {
			toast.error(`Cessation failed: ${(err as Error).message}`);
		}
	}

	function getStatusColor(status: string | undefined): string {
		if (status === 'running') return 'bg-amber-500 animate-pulse';
		if (status === 'completed') return 'bg-emerald-500';
		if (status === 'failed' || status === 'stopped') return 'bg-rose-500';
		if (status === 'initiating') return 'bg-blue-500 animate-pulse';
		return 'bg-slate-400';
	}

	onMount(() => {
		loadFIS();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-rose-600/20 rounded-xl shadow-inner border border-rose-500/20">
				<ZapOff class="w-8 h-8 text-rose-600 animate-pulse" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-rose-600 to-amber-600 dark:from-rose-400 dark:to-amber-400 bg-clip-text text-transparent italic tracking-tight">FIS</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Managed AWS Fault Injection Service (FIS) experiments and orchestration telemetry.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadFIS}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Sync chaos fleet"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				class="flex items-center gap-2 px-5 py-2.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl font-black shadow-lg shadow-rose-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<PlayCircle class="w-5 h-5" />
				Ignite Chaos
			</button>
		</div>
	</div>

	<!-- Chaos Matrix tabs -->
	<div class="flex gap-2 p-1 bg-slate-100 dark:bg-slate-900/50 rounded-2xl w-fit border border-slate-200 dark:border-slate-800">
		<button 
			onclick={() => activeTab = 'experiments'}
			class="px-6 py-2 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all {activeTab === 'experiments' ? 'bg-white dark:bg-slate-800 text-rose-600 shadow-sm border border-slate-200 dark:border-slate-700' : 'text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}"
		>
			Live Experiments
		</button>
		<button 
			onclick={() => activeTab = 'templates'}
			class="px-6 py-2 rounded-xl text-[10px] font-black uppercase tracking-widest transition-all {activeTab === 'templates' ? 'bg-white dark:bg-slate-800 text-rose-600 shadow-sm border border-slate-200 dark:border-slate-700' : 'text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}"
		>
			Experiment Templates
		</button>
	</div>

	<!-- Chaos Metrics -->
	<div class="grid grid-cols-1 md:grid-cols-3 gap-6">
		<div class="p-8 bg-slate-900 rounded-3xl border border-slate-800 shadow-2xl flex items-center justify-between group/total overflow-hidden relative">
			<div class="absolute inset-0 bg-gradient-to-br from-rose-500/10 to-transparent pointer-events-none"></div>
			<div class="relative z-10">
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Active Experiments</div>
				<div class="text-3xl font-black text-white italic tabular-nums">{stats.activeExperiments}<span class="text-xs uppercase ml-1 opacity-40">/ {experiments.length} SYNCED</span></div>
			</div>
			<Skull class="w-10 h-10 text-rose-500/20 group-hover/total:scale-110 transition-transform relative z-10" />
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-rose-600 uppercase tracking-widest mb-1 italic">Chaos Stratagem</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{stats.totalTemplates}</div>
			</div>
			<div class="p-3 bg-rose-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Bomb class="w-6 h-6 text-rose-600" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Intensity Pulse</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{stats.chaosIntensity}</div>
			</div>
			<div class="p-3 bg-white/50 dark:bg-slate-700 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Flame class="w-6 h-6 text-slate-400" />
			</div>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Chaos Ledger -->
		<div class="lg:col-span-4 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search chaos assets..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if activeTab === 'experiments'}
						{#each filteredExperiments as e}
							<div 
								role="button"
								tabindex="0"
								onclick={() => selectExperiment(e.id)}
								onkeydown={(key) => key.key === 'Enter' && selectExperiment(e.id)}
								class="p-4 flex items-center justify-between hover:bg-rose-500/5 transition-all group/ks cursor-pointer {selectedExperiment?.id === e.id ? 'bg-rose-500/10 border-l-4 border-rose-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3">
									<div class="w-2 h-2 rounded-full {getStatusColor(e.state?.status)}"></div>
									<div>
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-[11px] truncate max-w-[150px]">{e.id}</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{e.experimentTemplateId} | {e.state?.status?.toUpperCase()}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}
					{:else}
						{#each filteredTemplates as t}
							<div 
								class="p-4 flex items-center justify-between hover:bg-rose-500/5 transition-all group/app border-l-4 border-transparent cursor-pointer"
							>
								<div class="flex items-center gap-3">
									<div class="w-2 h-2 rounded-full bg-rose-500/20"></div>
									<div>
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-[11px] truncate max-w-[150px]">{t.description || t.id}</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{t.id}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}
					{/if}

					{#if (!experiments.length && activeTab === 'experiments') || (!templates.length && activeTab === 'templates')}
						<div class="p-12 text-center text-slate-400 text-sm italic font-bold uppercase">No registered chaos assets.</div>
					{/if}
				</div>
			</div>
		</div>

		<!-- Main Control Panel -->
		<div class="lg:col-span-8 space-y-6">
			{#if selectedExperiment}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
					<!-- Header -->
					<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-rose-500/5 to-amber-500/5 flex justify-between items-start">
						<div>
							<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedExperiment.id}</h2>
							<div class="flex items-center gap-3 mt-4">
								<div class="px-3 py-1 rounded-xl {getStatusColor(selectedExperiment.state?.status)} text-white text-[10px] font-black uppercase tracking-widest border border-white/10 shadow-sm">
									{selectedExperiment.state?.status}
								</div>
								<div class="px-2 py-1 rounded-lg bg-rose-900/10 dark:bg-rose-400/10 text-rose-600 dark:text-rose-400 text-[9px] font-black uppercase tracking-widest border border-rose-500/20">
									TEMPLATE: {selectedExperiment.experimentTemplateId}
								</div>
							</div>
						</div>
						<div class="flex gap-2">
							<button 
								onclick={() => stopExperiment(selectedExperiment?.id)}
								class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
								title="Stop Chaos"
							>
								<StopCircle class="w-4 h-4" />
							</button>
						</div>
					</div>

					<div class="p-8 space-y-8">
						<!-- Chaos Diagnostics -->
						<div class="grid grid-cols-1 md:grid-cols-3 gap-6">
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<Pulse class="w-3.5 h-3.5 text-rose-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Blast Radius</span>
								</div>
								<div class="text-[14px] font-black text-rose-600 uppercase italic">10% (SECTOR_A)</div>
							</div>
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<Wifi class="w-3.5 h-3.5 text-rose-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Target Health</span>
								</div>
								<div class="text-[14px] font-black text-rose-500 uppercase italic">DEGRADED</div>
							</div>
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<ShieldAlert class="w-3.5 h-3.5 text-rose-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Safety Lever</span>
									<span class="text-[8px] text-emerald-500 uppercase ml-auto">ARMED</span>
								</div>
								<div class="text-[14px] font-black text-emerald-500 uppercase italic">TLS_ACTIVE</div>
							</div>
						</div>

						<!-- Core Meta Stats -->
						<div class="grid grid-cols-1 md:grid-cols-2 gap-6 pt-4">
							<div class="p-8 bg-slate-50 dark:bg-slate-900/50 rounded-[2.5rem] border border-slate-100 dark:border-slate-700/50">
								<h3 class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-6 italic flex items-center gap-2">
									<Target class="w-4 h-4 text-rose-500" />
									Chaos Stratagem
								</h3>
								<div class="space-y-4">
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">Actions</span>
										<span class="text-rose-600 dark:text-rose-300 font-mono italic">{Object.keys(selectedExperiment.actions || {}).length} TASKS</span>
									</div>
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">Targets</span>
										<span class="text-rose-600 dark:text-rose-300 font-mono italic">{Object.keys(selectedExperiment.targets || {}).length} NODES</span>
									</div>
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic border-t border-slate-200 dark:border-slate-700 pt-4 mt-4">
										<span class="text-slate-500">Stop Condition</span>
										<span class="text-emerald-500 font-mono italic">{selectedExperiment.stopConditions?.[0]?.source || 'STATIC'}</span>
									</div>
								</div>
							</div>
							<div class="p-8 bg-slate-50 dark:bg-slate-900/50 rounded-[2.5rem] border border-slate-100 dark:border-slate-700/50">
								<h3 class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-6 italic flex items-center gap-2">
									<History class="w-4 h-4 text-emerald-500" />
									Lifecycle Trace
								</h3>
								<div class="space-y-4">
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">Ignited At</span>
										<span class="text-emerald-500 font-mono italic">{selectedExperiment.startTime?.toLocaleDateString()}</span>
									</div>
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">Cessation At</span>
										<span class="text-emerald-500 font-mono italic">{selectedExperiment.endTime?.toLocaleDateString() || 'PULSE_ACTIVE'}</span>
									</div>
								</div>
							</div>
						</div>

						<!-- Action Ledger -->
						<div class="pt-4 space-y-4">
							<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic px-1 flex items-center justify-between">
								<span class="flex items-center gap-2">
									<ListTree class="w-4 h-4 text-rose-500" />
									Fault Injection Ledger
								</span>
								<span class="text-[8px] opacity-60">SYNCING_CHAOS</span>
							</div>
							
							<div class="space-y-3">
								{#each Object.entries(selectedExperiment.actions || {}) as [name, action]}
									<div class="p-5 bg-slate-900 dark:bg-black rounded-3xl border border-slate-800 flex items-center justify-between transition-all hover:bg-slate-800">
										<div class="flex items-center gap-4">
											<div class="p-2.5 bg-white/5 rounded-xl">
												<Binary class="w-4 h-4 text-white" />
											</div>
											<div>
												<div class="text-[10px] font-black text-white uppercase italic tracking-tighter leading-none mb-1 font-mono">{name}</div>
												<div class="text-[8px] text-slate-500 uppercase font-bold tracking-widest italic">{action.actionId} | TARGETS: {action.targetId}</div>
											</div>
										</div>
										<div class="text-right">
											<div class="flex items-center gap-2 justify-end mb-1">
												<div class="w-1.5 h-1.5 rounded-full {getStatusColor(action.state?.status)}"></div>
												<div class="text-[9px] font-black text-white italic uppercase tracking-tight italic font-black">{action.state?.status}</div>
											</div>
											<div class="text-[8px] font-mono text-rose-400 italic font-black uppercase tracking-widest">TRACE_ACTIVE</div>
										</div>
									</div>
								{:else}
									<div class="h-32 bg-slate-50 dark:bg-slate-900/50 rounded-[2rem] flex items-center justify-center animate-pulse">
										<span class="text-[10px] uppercase font-black text-slate-500 tracking-widest italic">Scanning fault ledger...</span>
									</div>
								{/each}
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<ZapOff class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">Global Chaos Matrix</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Coordinate high-performance chaos orchestration, monitor distributed fault injection experiments, and orchestrate global resilience lifecycles through an enterprise-grade Amazon FIS managed plane.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

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
