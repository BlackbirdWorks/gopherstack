<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getLightsailClient } from '$lib/aws-client';
	import {
		GetInstancesCommand,
		GetRelationalDatabasesCommand,
		GetStaticIpsCommand,
		DeleteInstanceCommand,
		CreateInstancesCommand,
		type Instance,
		type RelationalDatabase,
		type StaticIp
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { 
		Zap, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Link, Share2, Shield, CheckCircle2, 
		XCircle, AlertCircle, Layout, Boxes, 
		Archive, Timer, Settings, Workflow, 
		Terminal, ExternalLink, Gauge, BarChart3, 
		Wifi, Smartphone, Laptop, DatabaseBackup, 
		Route, Radio, Router, Cpu, Sliders,
		Layers, Workflow as WorkflowIcon,
		Globe2, Server, Target, ZapOff,
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
		Sun, Sunrise, Lightbulb, Sparkles as Flare, GaugeCircle
	} from 'lucide-svelte';

	const lightsail = regionalClient(getLightsailClient);

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let instances = $state<Instance[]>([]);
	let databases = $state<RelationalDatabase[]>([]);
	let staticIps = $state<StaticIp[]>([]);
	let selectedInstance = $state<Instance | null>(null);
	let loadingDetails = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let instanceName = $state('');
	let creating = $state(false);

	// Derived
	const filteredInstances = $derived(
		instances.filter(i => i.name?.toLowerCase().includes(searchQuery.toLowerCase()) || i.publicIpAddress?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const stats = $derived({
		active: instances.filter(i => i.state?.name === 'running').length,
		dbCount: databases.length,
		staticIpCount: staticIps.length,
		total: instances.length
	});

	// Actions
	async function loadLightsail() {
		loading = true;
		try {
			const instRes = await lightsail().send(new GetInstancesCommand({}));
			instances = instRes.instances ?? [];

			const dbRes = await lightsail().send(new GetRelationalDatabasesCommand({}));
			databases = dbRes.relationalDatabases ?? [];

			const ipRes = await lightsail().send(new GetStaticIpsCommand({}));
			staticIps = ipRes.staticIps ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load Lightsail: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function terminateInstance(name: string | undefined) {
		if (!name || !await confirmDestructive({ title: 'Terminate Instance', message: 'Terminate this Lightsail instance? All ephemeral data and the public IP will be permanently released.', confirmLabel: 'Terminate' })) return;
		try {
			await lightsail().send(new DeleteInstanceCommand({ instanceName: name }));
			toast.success(`Instance termination initiated`);
			selectedInstance = null;
			await loadLightsail();
		} catch (err: unknown) {
			toast.error(`Termination failed: ${(err as Error).message}`);
		}
	}

	async function createInstance() {
		if (!instanceName.trim()) return;
		creating = true;
		try {
			await lightsail().send(new CreateInstancesCommand({
				instanceNames: [instanceName.trim()],
				availabilityZone: 'us-east-1a',
				blueprintId: 'ubuntu_22_04',
				bundleId: 'nano_3_0'
			}));
			toast.success(`Instance "${instanceName}" provisioned`);
			showCreateModal = false;
			instanceName = '';
			await loadLightsail();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	function getStateColor(state: string | undefined): string {
		if (state === 'running') return 'bg-emerald-500';
		if (state === 'pending' || state === 'starting' || state === 'stopping') return 'bg-amber-500 animate-pulse';
		if (state === 'stopped' || state === 'terminated') return 'bg-rose-500';
		return 'bg-slate-400';
	}

	onRegionChange(loadLightsail);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-amber-500/20 rounded-xl shadow-inner border border-amber-400/20">
				<Sun class="w-8 h-8 text-amber-500 animate-pulse" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-amber-500 to-orange-500 dark:from-amber-400 dark:to-orange-400 bg-clip-text text-transparent italic tracking-tight">Lightsail Compute Deck</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Simplified VPS orchestration, managed cloud resource hosting, and real-time instance telemetry.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadLightsail}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Sync instances"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-amber-500 hover:bg-amber-600 text-white rounded-xl font-black shadow-lg shadow-amber-500/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Launch Flare
			</button>
		</div>
	</div>

	<!-- Fleet Metrics -->
	<div class="grid grid-cols-1 md:grid-cols-4 gap-6">
		<div class="p-8 bg-slate-900 rounded-3xl border border-slate-800 shadow-2xl flex items-center justify-between group/total overflow-hidden relative">
			<div class="absolute inset-0 bg-gradient-to-br from-amber-500/10 to-transparent pointer-events-none"></div>
			<div class="relative z-10">
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Active Flares</div>
				<div class="text-3xl font-black text-white italic tabular-nums">{stats.active}<span class="text-xs uppercase ml-1 opacity-40">/ {stats.total}</span></div>
			</div>
			<Server class="w-10 h-10 text-amber-500/20 group-hover/total:scale-110 transition-transform relative z-10" />
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-amber-600 uppercase tracking-widest mb-1 italic">Managed Databases</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{stats.dbCount}</div>
			</div>
			<div class="p-3 bg-amber-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Database class="w-6 h-6 text-amber-600" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Static IP Pool</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{stats.staticIpCount}</div>
			</div>
			<div class="p-3 bg-white/50 dark:bg-slate-700 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Globe2 class="w-6 h-6 text-slate-400" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Resource Health</div>
				<div class="text-3xl font-black text-emerald-500 italic tabular-nums">SYNC</div>
			</div>
			<div class="p-3 bg-emerald-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<ActivitySquare class="w-6 h-6 text-emerald-600" />
			</div>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Instance List -->
		<div class="lg:col-span-4 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search compute flares..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-amber-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#each filteredInstances as i}
						<div 
							role="button"
							tabindex="0"
							onclick={() => selectedInstance = i}
							onkeydown={(e) => e.key === 'Enter' && (selectedInstance = i)}
							class="p-4 flex items-center justify-between hover:bg-amber-500/5 transition-all group/ls cursor-pointer {selectedInstance?.name === i.name ? 'bg-amber-500/10 border-l-4 border-amber-500 shadow-inner' : 'border-l-4 border-transparent'}"
						>
							<div class="flex items-center gap-3">
								<div class="w-2 h-2 rounded-full {getStateColor(i.state?.name)}"></div>
								<div>
									<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-[11px] truncate max-w-[150px]">{i.name}</div>
									<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{i.publicIpAddress || 'PRIVATE_GRID'}</div>
								</div>
							</div>
							<ChevronRight class="w-4 h-4 text-slate-300" />
						</div>
					{/each}

					{#if !instances.length}
						<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No active compute flares detected.</div>
					{/if}
				</div>
			</div>
		</div>

		<!-- Main Control Panel -->
		<div class="lg:col-span-8 space-y-6">
			{#if selectedInstance}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
					<!-- Header -->
					<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-amber-500/5 to-orange-500/5 flex justify-between items-start">
						<div>
							<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedInstance.name}</h2>
							<div class="flex items-center gap-3 mt-4">
								<div class="px-3 py-1 rounded-xl {getStateColor(selectedInstance.state?.name)} text-white text-[10px] font-black uppercase tracking-widest border border-white/10 shadow-sm">
									{selectedInstance.state?.name?.toUpperCase()}
								</div>
								<div class="px-2 py-1 rounded-lg bg-amber-900/10 dark:bg-amber-400/10 text-amber-600 dark:text-amber-300 text-[9px] font-black uppercase tracking-widest border border-amber-500/20">
									BUNDLE: {selectedInstance.bundleId}
								</div>
							</div>
						</div>
						<div class="flex gap-2">
							<button 
								onclick={() => terminateInstance(selectedInstance?.name)}
								class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
								title="Explode Flare"
							>
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>

					<div class="p-8 space-y-8">
						<!-- Performance Metrics -->
						<div class="grid grid-cols-1 md:grid-cols-3 gap-6">
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<Target class="w-3.5 h-3.5 text-amber-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Blueprint Stratagem</span>
								</div>
								<div class="text-[14px] font-black text-slate-800 dark:text-white uppercase italic truncate max-w-[150px]">{selectedInstance.blueprintId}</div>
							</div>
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<Wifi class="w-3.5 h-3.5 text-amber-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Connectivity</span>
								</div>
								<div class="text-[14px] font-black text-slate-800 dark:text-white uppercase italic">{selectedInstance.isStaticIp ? 'STATIC_LOCKED' : 'EPHEMERAL'}</div>
							</div>
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<GaugeCircle class="w-3.5 h-3.5 text-amber-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Instance Load</span>
								</div>
								<div class="text-[14px] font-black text-emerald-500 uppercase italic">LOW_IMPACT</div>
							</div>
						</div>

						<!-- Core Meta Stats -->
						<div class="grid grid-cols-1 md:grid-cols-2 gap-6 pt-4">
							<div class="p-8 bg-slate-50 dark:bg-slate-900/50 rounded-[2.5rem] border border-slate-100 dark:border-slate-700/50">
								<h3 class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-6 italic flex items-center gap-2">
									<ShieldCheck class="w-4 h-4 text-amber-500" />
									Networking Boundary
								</h3>
								<div class="space-y-4 font-mono">
									<div class="flex justify-between items-center text-[9px] uppercase font-black italic">
										<span class="text-slate-500">Public Protocol</span>
										<span class="text-emerald-500">{selectedInstance.publicIpAddress || 'NONE'}</span>
									</div>
									<div class="flex justify-between items-center text-[9px] uppercase font-black italic">
										<span class="text-slate-500">Private Protocol</span>
										<span class="text-slate-400">{selectedInstance.privateIpAddress}</span>
									</div>
								</div>
							</div>
							<div class="p-8 bg-slate-50 dark:bg-slate-900/50 rounded-[2.5rem] border border-slate-100 dark:border-slate-700/50">
								<h3 class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-6 italic flex items-center gap-2">
									<Globe2 class="w-4 h-4 text-amber-500" />
									Geographic Sync
								</h3>
								<div class="space-y-4">
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">Availability Zone</span>
										<span class="text-emerald-500">{selectedInstance.location?.availabilityZone}</span>
									</div>
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">Region Registry</span>
										<span class="text-slate-400">{selectedInstance.location?.regionName}</span>
									</div>
								</div>
							</div>
						</div>

						<!-- Advanced Properties -->
						<div class="pt-4 space-y-4">
							<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic px-1 flex items-center gap-2">
								<Sliders class="w-4 h-4 text-amber-500" />
								Compute Hardware Stratagem
							</div>
							<div class="p-8 bg-slate-900 dark:bg-black rounded-[2.5rem] border border-slate-800 shadow-inner grid grid-cols-2 gap-8">
								<div>
									<div class="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">VCPU Count</div>
									<div class="text-[12px] font-black text-white uppercase italic tracking-tighter leadning-none">0.5 CORE</div>
								</div>
								<div>
									<div class="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">RAM Allocation</div>
									<div class="text-[12px] font-black text-white uppercase italic tracking-tighter leadning-none">512 MB</div>
								</div>
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<Lightbulb class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">Simplified Compute Deck</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Provision high-performance managed VPS flares, coordinate distributed cloud resource stratagems, and monitor global instance telemetry through an enterprise-grade Lightsail managed plane.</p>
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
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Launch Flare</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createInstance(); }} class="space-y-6">
					<div>
						<label for="iName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Flare Identifier</label>
						<input 
							id="iName"
							type="text" 
							bind:value={instanceName}
							placeholder="e.g. bishop-test-vps"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-amber-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div class="p-5 bg-amber-500/5 rounded-2xl border border-amber-500/10">
						<div class="flex items-center gap-2 mb-2">
							<ShieldCheck class="w-3.5 h-3.5 text-amber-500" />
							<span class="text-[10px] font-black text-amber-600 uppercase tracking-widest leading-none">Compute Baseline</span>
						</div>
						<p class="text-[9px] text-amber-800 dark:text-amber-400 leading-relaxed font-bold uppercase tracking-tight italic">
							Registering a Lightsail flare provisions a managed VPS instance with automatic networking and EBS persistence. Initial deployment typically completes in 2-3 minutes.
						</p>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-amber-500 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Syncing...' : 'Ignite Flare'}
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
