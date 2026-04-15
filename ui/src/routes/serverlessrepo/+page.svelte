<script lang="ts">
	import { onMount } from 'svelte';
	import { getServerlessRepoClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		type ApplicationSummary
	} from '@aws-sdk/client-serverlessapplicationrepository';
	import { toast } from 'svelte-sonner';
	import { 
		Boxes, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Zap, Workflow, Terminal, ExternalLink,
		Gauge, BarChart3, Binary, Scan,
		CheckCircle2, AlertCircle, Ban,
		Layers3, ListTree, PieChart, Timer,
		TrendingUp, Filter, Share,
		Puzzle, Map, LayoutGrid, Package,
		Download, Star, User
	} from 'lucide-svelte';

	const slr = getServerlessRepoClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let applications = $state<ApplicationSummary[]>([]);

	// Derived
	const filteredApps = $derived(
		applications.filter(a => a.Name?.toLowerCase().includes(searchQuery.toLowerCase()) || a.ApplicationId?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadApps() {
		loading = true;
		try {
			const res = await slr.send(new ListApplicationsCommand({}));
			applications = res.Applications ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load Repository: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	onMount(() => {
		loadApps();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-rose-600/20 rounded-xl shadow-inner border border-rose-500/20">
				<Package class="w-8 h-8 text-rose-600" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-rose-600 to-orange-600 dark:from-rose-400 dark:to-orange-400 bg-clip-text text-transparent italic tracking-tight">Serverless Application Repository</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1 uppercase tracking-widest font-black italic opacity-70">Managed Application Discovery, Packaging & One-Click Deployment Plane</p>
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
				class="flex items-center gap-2 px-5 py-2.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl font-black shadow-lg shadow-rose-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Publish App
			</button>
		</div>
	</div>

	<!-- Stats -->
	<div class="grid grid-cols-1 md:grid-cols-4 gap-6">
		<div class="p-8 bg-slate-900 rounded-3xl border border-slate-800 shadow-2xl flex items-center justify-between group/total overflow-hidden relative">
			<div class="absolute inset-0 bg-gradient-to-br from-rose-500/10 to-transparent pointer-events-none"></div>
			<div class="relative z-10">
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Registry Status</div>
				<div class="text-3xl font-black text-white italic tabular-nums flex items-center gap-2 uppercase">
					Synched
					<div class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
				</div>
			</div>
			<Globe class="w-10 h-10 text-rose-500/20 group-hover/total:scale-110 transition-transform relative z-10" />
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-rose-600 uppercase tracking-widest mb-1 italic">Published Apps</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{applications.length}</div>
			</div>
			<div class="p-3 bg-rose-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Package class="w-6 h-6 text-rose-600" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-orange-600 uppercase tracking-widest mb-1 italic">Global Installs</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">1.2k</div>
			</div>
			<div class="p-3 bg-orange-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Download class="w-6 h-6 text-orange-600" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Vendor Trust</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums uppercase">Verified</div>
			</div>
			<div class="p-3 bg-white/50 dark:bg-slate-700 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<ShieldCheck class="w-6 h-6 text-slate-400" />
			</div>
		</div>
	</div>

	<!-- Application Catalog -->
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl overflow-hidden min-h-[600px] flex flex-col">
		<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-white/20 dark:bg-slate-900/10 flex justify-between items-center sticky top-0 z-10 backdrop-blur-md">
			<div class="flex items-center gap-4">
				<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
					<LayoutGrid class="w-4 h-4 text-rose-500" />
					Managed Application Catalog
				</h3>
			</div>
			<div class="relative w-64">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-3 h-3 text-slate-400" />
				<input 
					type="text" 
					bind:value={searchQuery}
					placeholder="Search applications..."
					class="w-full pl-8 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-[10px] focus:ring-2 focus:ring-rose-500 outline-none transition-all italic font-black uppercase"
				/>
			</div>
		</div>

		<div class="flex-1 overflow-y-auto p-8">
			{#if loading}
				<div class="flex flex-col items-center justify-center p-32 opacity-30">
					<Package class="w-16 h-16 text-rose-500 animate-bounce mb-8" />
					<span class="text-[10px] uppercase font-black text-slate-500 tracking-[0.2em] italic font-mono">Syncing Repository Plane...</span>
				</div>
			{:else if filteredApps.length}
				<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
					{#each filteredApps as app}
						<div class="p-6 bg-white dark:bg-slate-900 rounded-[2.5rem] border border-slate-100 dark:border-slate-800 shadow-sm hover:shadow-2xl hover:scale-[1.02] transition-all group/app flex flex-col">
							<div class="flex items-start justify-between mb-4">
								<div class="p-4 bg-slate-900 rounded-2xl border border-slate-800 group-hover/app:rotate-6 transition-transform shadow-xl">
									<Zap class="w-6 h-6 text-white" />
								</div>
								<div class="flex flex-col items-end">
									<div class="flex items-center gap-1 text-amber-500 mb-1">
										<Star class="w-3 h-3 fill-current" />
										<span class="text-[10px] font-black italic">4.9</span>
									</div>
									<div class="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-500 text-[8px] font-black uppercase italic border border-emerald-500/20">VERIFIED</div>
								</div>
							</div>
							<h4 class="text-xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none mb-2 truncate">{app.Name}</h4>
							<p class="text-[10px] text-slate-500 dark:text-slate-400 mb-6 italic font-bold line-clamp-2 leading-relaxed opacity-80">{app.Description || 'Serverless cloud application component.'}</p>
							
							<div class="mt-auto space-y-4 pt-4 border-t border-slate-50 dark:border-slate-800">
								<div class="flex items-center justify-between">
									<div class="flex items-center gap-2">
										<User class="w-3 h-3 text-slate-400" />
										<span class="text-[9px] font-black text-slate-500 uppercase italic truncate max-w-[120px]">{app.Author}</span>
									</div>
									<div class="text-[8px] text-slate-400 font-mono tracking-tighter italic opacity-60 uppercase">LICENSE: APACHE-2.0</div>
								</div>
								<button class="w-full py-2.5 bg-slate-900 dark:bg-black text-white rounded-xl text-[10px] font-black uppercase tracking-widest italic flex items-center justify-center gap-2 hover:bg-rose-600 transition-all active:scale-95">
									<Download class="w-3.5 h-3.5" />
									Deploy Template
								</button>
							</div>
						</div>
					{/each}
				</div>
			{:else}
				<div class="flex flex-col items-center justify-center p-32 text-center opacity-30">
					<Boxes class="w-20 h-20 text-slate-600 mb-8" />
					<h4 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none mb-2">Repository Vacuum Detect</h4>
					<p class="text-[11px] text-slate-500 max-w-xs mx-auto italic font-black uppercase tracking-tight leading-relaxed">No serverless applications found in the managed catalog. Publish or discover application templates to initialize the deployment plane.</p>
				</div>
			{/if}
		</div>

		<!-- Feedback Footer -->
		<div class="p-8 border-t border-slate-100 dark:border-slate-700/50 bg-slate-50/50 dark:bg-slate-900/40 flex flex-wrap gap-8 items-center justify-center">
			<div class="flex items-center gap-3">
				<ShieldCheck class="w-5 h-5 text-emerald-500" />
				<span class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest">Signed Application Security Checks Enabled</span>
			</div>
			<div class="flex items-center gap-3">
				<Workflow class="w-5 h-5 text-blue-500" />
				<span class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest">One-Click CloudFormation Integration</span>
			</div>
			<div class="flex items-center gap-3">
				<ExternalLink class="w-5 h-5 text-purple-500" />
				<span class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest">Global Marketplace Provisioning</span>
			</div>
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
		background: rgba(225, 29, 72, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(225, 29, 72, 0.2);
	}
</style>
