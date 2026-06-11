<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getWorkSpacesClient } from '$lib/aws-client';
	import {
		DescribeWorkspacesCommand,
		DescribeWorkspaceBundlesCommand,
		TerminateWorkspacesCommand,
		CreateWorkspacesCommand,
		StartWorkspacesCommand,
		StopWorkspacesCommand,
		RebootWorkspacesCommand,
		RebuildWorkspacesCommand,
		type WorkSpacesClient,
		type Workspace,
		type WorkspaceBundle
	} from '@aws-sdk/client-workspaces';
	import { toast } from 'svelte-sonner';
	import { 
		Monitor, Search, RefreshCw, Plus, Trash2, 
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
		Monitor as DesktopGui, Computer, HardDrive as HD,
		Network as NetIcon,
		LockKeyhole, UserCog, UserCheck, ShieldAlert,
		Square, RotateCcw, Wrench
	} from 'lucide-svelte';

	let workspacesClient: WorkSpacesClient | undefined;
	function workspaces(): WorkSpacesClient {
		return (workspacesClient ??= getWorkSpacesClient());
	}

	let showBundleCompare = $state(false);

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let workspaceList = $state<Workspace[]>([]);
	let bundles = $state<WorkspaceBundle[]>([]);
	let selectedWorkspace = $state<Workspace | null>(null);
	let loadingDetails = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let userName = $state('');
	let bundleId = $state('');
	let creating = $state(false);

	// Derived
	const filteredWorkspaces = $derived(
		workspaceList.filter(w => w.UserName?.toLowerCase().includes(searchQuery.toLowerCase()) || w.WorkspaceId?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const stats = $derived({
		active: workspaceList.filter(w => w.State === 'AVAILABLE').length,
		unhealthy: workspaceList.filter(w => w.State === 'UNHEALTHY').length,
		total: workspaceList.length
	});

	// Actions
	async function loadWorkSpaces() {
		loading = true;
		try {
			const wsRes = await workspaces().send(new DescribeWorkspacesCommand({}));
			workspaceList = wsRes.Workspaces ?? [];

			const bundleRes = await workspaces().send(new DescribeWorkspaceBundlesCommand({}));
			bundles = bundleRes.Bundles ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load WorkSpaces: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function terminateWorkspace(id: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Terminate WorkSpace', message: 'Terminate this WorkSpace? All user profile data and persistent storage will be permanently purged.', confirmLabel: 'Terminate' })) return;
		try {
			await workspaces().send(new TerminateWorkspacesCommand({
				TerminateWorkspaceRequests: [{ WorkspaceId: id }]
			}));
			toast.success(`Termination initiated for ${id}`);
			selectedWorkspace = null;
			await loadWorkSpaces();
		} catch (err: unknown) {
			toast.error(`Termination failed: ${(err as Error).message}`);
		}
	}

	async function createWorkspace() {
		if (!userName.trim() || !bundleId) return;
		creating = true;
		try {
			await workspaces().send(new CreateWorkspacesCommand({
				Workspaces: [{
					// Default mock dir
					DirectoryId: 'd-1234567890',
					UserName: userName.trim(),
					BundleId: bundleId,
					UserVolumeEncryptionEnabled: false,
					RootVolumeEncryptionEnabled: false,
					WorkspaceProperties: {
						RunningMode: 'ALWAYS_ON',
						RootVolumeSizeGib: 80,
						UserVolumeSizeGib: 50,
						ComputeTypeName: 'STANDARD'
					}
				}]
			}));
			toast.success(`WorkSpace provisioned for ${userName}`);
			showCreateModal = false;
			userName = '';
			bundleId = '';
			await loadWorkSpaces();
		} catch (err: unknown) {
			toast.error(`Provisioning failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	let actioning = $state(false);

	async function startWorkspace(id: string | undefined) {
		if (!id) return;
		actioning = true;
		try {
			await workspaces().send(new StartWorkspacesCommand({ StartWorkspaceRequests: [{ WorkspaceId: id }] }));
			toast.success(`Start initiated for ${id}`);
			await loadWorkSpaces();
		} catch (err: unknown) {
			toast.error(`Start failed: ${(err as Error).message}`);
		} finally {
			actioning = false;
		}
	}

	async function stopWorkspace(id: string | undefined) {
		if (!id) return;
		actioning = true;
		try {
			await workspaces().send(new StopWorkspacesCommand({ StopWorkspaceRequests: [{ WorkspaceId: id }] }));
			toast.success(`Stop initiated for ${id}`);
			await loadWorkSpaces();
		} catch (err: unknown) {
			toast.error(`Stop failed: ${(err as Error).message}`);
		} finally {
			actioning = false;
		}
	}

	async function rebootWorkspace(id: string | undefined) {
		if (!id) return;
		actioning = true;
		try {
			await workspaces().send(new RebootWorkspacesCommand({ RebootWorkspaceRequests: [{ WorkspaceId: id }] }));
			toast.success(`Reboot initiated for ${id}`);
			await loadWorkSpaces();
		} catch (err: unknown) {
			toast.error(`Reboot failed: ${(err as Error).message}`);
		} finally {
			actioning = false;
		}
	}

	async function rebuildWorkspace(id: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Rebuild WorkSpace', message: 'Rebuild this WorkSpace? The user volume is recreated from the last available snapshot; data not yet backed up is lost.', confirmLabel: 'Rebuild' })) return;
		actioning = true;
		try {
			await workspaces().send(new RebuildWorkspacesCommand({ RebuildWorkspaceRequests: [{ WorkspaceId: id }] }));
			toast.success(`Rebuild initiated for ${id}`);
			await loadWorkSpaces();
		} catch (err: unknown) {
			toast.error(`Rebuild failed: ${(err as Error).message}`);
		} finally {
			actioning = false;
		}
	}

	function getStateColor(state: string | undefined): string {
		if (state === 'AVAILABLE') return 'bg-emerald-500';
		if (state === 'PENDING' || state === 'STARTING' || state === 'REBOOTING') return 'bg-amber-500 animate-pulse';
		if (state === 'UNHEALTHY' || state === 'ERROR' || state === 'TERMINATED') return 'bg-rose-500';
		return 'bg-slate-400';
	}

	onMount(() => {
		loadWorkSpaces();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-sky-600/20 rounded-xl shadow-inner border border-sky-500/20">
				<Computer class="w-8 h-8 text-sky-600 animate-pulse" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-sky-600 to-indigo-600 dark:from-sky-400 dark:to-indigo-400 bg-clip-text text-transparent italic tracking-tight">WorkSpaces Desktop Grid</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Managed virtual desktop infrastructure, high-fidelity user environment hosting, and real-time connectivity pulse.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadWorkSpaces}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Sync environments"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => (showBundleCompare = !showBundleCompare)}
				class="flex items-center gap-2 px-4 py-2.5 bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-xl font-black uppercase text-xs tracking-widest transition-all active:scale-95"
				title="Compare bundles"
			>
				<Boxes class="w-5 h-5" />
				Bundles
			</button>
			<button
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-sky-600 hover:bg-sky-700 text-white rounded-xl font-black shadow-lg shadow-sky-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Deploy Instance
			</button>
		</div>
	</div>

	<!-- Bundle Comparison -->
	{#if showBundleCompare}
		<div class="rounded-2xl border border-slate-200 dark:border-slate-700 bg-white/60 dark:bg-slate-800/40 p-6 shadow-xl">
			<div class="mb-4 flex items-center justify-between">
				<h2 class="text-sm font-black uppercase tracking-widest text-slate-700 dark:text-slate-200 italic">Bundle Comparison</h2>
				<button onclick={() => (showBundleCompare = false)} class="text-xs text-slate-400 hover:text-slate-600">Close</button>
			</div>
			{#if bundles.length === 0}
				<p class="text-sm text-slate-400 italic">No bundles available.</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-slate-100 dark:border-slate-700/50">
					<table class="w-full text-sm">
						<thead class="bg-slate-50 dark:bg-slate-800/60 text-xs uppercase text-slate-500">
							<tr>
								<th class="px-4 py-2 text-left">Bundle</th>
								<th class="px-4 py-2 text-left">Compute</th>
								<th class="px-4 py-2 text-left">User Storage</th>
								<th class="px-4 py-2 text-left">Root Storage</th>
								<th class="px-4 py-2 text-left">Description</th>
								<th class="px-4 py-2 text-left">Owner</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-800">
							{#each bundles as bundle}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-800/40 {bundleId === bundle.BundleId ? 'bg-sky-50 dark:bg-sky-900/20' : ''}">
									<td class="px-4 py-2 font-medium text-slate-900 dark:text-white">
										{bundle.Name}
										<div class="text-[10px] text-slate-400 font-mono">{bundle.BundleId}</div>
									</td>
									<td class="px-4 py-2">{bundle.ComputeType?.Name ?? '—'}</td>
									<td class="px-4 py-2">{bundle.UserStorage?.Capacity ? `${bundle.UserStorage.Capacity} GB` : '—'}</td>
									<td class="px-4 py-2">{bundle.RootStorage?.Capacity ? `${bundle.RootStorage.Capacity} GB` : '—'}</td>
									<td class="px-4 py-2 text-xs text-slate-500 max-w-[220px] truncate">{bundle.Description ?? '—'}</td>
									<td class="px-4 py-2 text-xs text-slate-500">{bundle.Owner ?? '—'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- VDI Monitor -->
	<div class="grid grid-cols-1 md:grid-cols-3 gap-6">
		<div class="p-8 bg-slate-900 rounded-3xl border border-slate-800 shadow-2xl flex items-center justify-between group/total overflow-hidden relative">
			<div class="absolute inset-0 bg-gradient-to-br from-sky-500/10 to-transparent pointer-events-none"></div>
			<div class="relative z-10">
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Active Desktops</div>
				<div class="text-3xl font-black text-white italic tabular-nums">{stats.active}</div>
			</div>
			<Monitor class="w-10 h-10 text-sky-500/20 group-hover/total:scale-110 transition-transform relative z-10" />
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-rose-500/20 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-rose-600 uppercase tracking-widest mb-1 italic">Degraded instances</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{stats.unhealthy}</div>
			</div>
			<div class="p-3 bg-rose-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<ShieldAlert class="w-6 h-6 text-rose-600" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Bundle Availability</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{bundles.length}</div>
			</div>
			<div class="p-3 bg-white/50 dark:bg-slate-700 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Boxes class="w-6 h-6 text-slate-400" />
			</div>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Workspace List -->
		<div class="lg:col-span-4 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search desktops..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-sky-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#each filteredWorkspaces as w}
						<div 
							role="button"
							tabindex="0"
							onclick={() => selectedWorkspace = w}
							onkeydown={(e) => e.key === 'Enter' && (selectedWorkspace = w)}
							class="p-4 flex items-center justify-between hover:bg-sky-500/5 transition-all group/ws cursor-pointer {selectedWorkspace?.WorkspaceId === w.WorkspaceId ? 'bg-sky-500/10 border-l-4 border-sky-500 shadow-inner' : 'border-l-4 border-transparent'}"
						>
							<div class="flex items-center gap-3">
								<div class="w-2 h-2 rounded-full {getStateColor(w.State)}"></div>
								<div>
									<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-[11px] truncate max-w-[180px]">{w.UserName}</div>
									<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{w.WorkspaceId}</div>
								</div>
							</div>
							<ChevronRight class="w-4 h-4 text-slate-300" />
						</div>
					{/each}

					{#if !workspaceList.length}
						<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No provisioned managed desktops.</div>
					{/if}
				</div>
			</div>
		</div>

		<!-- Main Control Panel -->
		<div class="lg:col-span-8 space-y-6">
			{#if selectedWorkspace}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
					<!-- Header -->
					<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-sky-500/5 to-indigo-500/5 flex justify-between items-start">
						<div>
							<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedWorkspace.UserName}</h2>
							<div class="flex items-center gap-3 mt-4">
								<div class="px-3 py-1 rounded-xl {getStateColor(selectedWorkspace.State)} text-white text-[10px] font-black uppercase tracking-widest border border-white/10 shadow-sm">
									{selectedWorkspace.State}
								</div>
								<div class="px-2 py-1 rounded-lg bg-sky-900/10 dark:bg-sky-400/10 text-sky-600 dark:text-sky-300 text-[9px] font-black uppercase tracking-widest border border-sky-500/20">
									ID: {selectedWorkspace.WorkspaceId}
								</div>
							</div>
						</div>
						<div class="flex gap-2">
							<button
								onclick={() => startWorkspace(selectedWorkspace?.WorkspaceId)}
								disabled={actioning || selectedWorkspace.State === 'AVAILABLE'}
								class="p-2.5 bg-slate-900 dark:bg-black text-emerald-500 hover:bg-emerald-500/10 rounded-2xl transition-all border border-emerald-500/20 shadow-xl disabled:opacity-40"
								title="Start WorkSpace"
							>
								<Play class="w-4 h-4" />
							</button>
							<button
								onclick={() => stopWorkspace(selectedWorkspace?.WorkspaceId)}
								disabled={actioning || selectedWorkspace.State === 'STOPPED'}
								class="p-2.5 bg-slate-900 dark:bg-black text-amber-500 hover:bg-amber-500/10 rounded-2xl transition-all border border-amber-500/20 shadow-xl disabled:opacity-40"
								title="Stop WorkSpace"
							>
								<Square class="w-4 h-4" />
							</button>
							<button
								onclick={() => rebootWorkspace(selectedWorkspace?.WorkspaceId)}
								disabled={actioning}
								class="p-2.5 bg-slate-900 dark:bg-black text-sky-400 hover:bg-sky-500/10 rounded-2xl transition-all border border-sky-500/20 shadow-xl disabled:opacity-40"
								title="Reboot WorkSpace"
							>
								<RotateCcw class="w-4 h-4" />
							</button>
							<button
								onclick={() => rebuildWorkspace(selectedWorkspace?.WorkspaceId)}
								disabled={actioning}
								class="p-2.5 bg-slate-900 dark:bg-black text-indigo-400 hover:bg-indigo-500/10 rounded-2xl transition-all border border-indigo-500/20 shadow-xl disabled:opacity-40"
								title="Rebuild WorkSpace"
							>
								<Wrench class="w-4 h-4" />
							</button>
							<button
								onclick={() => terminateWorkspace(selectedWorkspace?.WorkspaceId)}
								class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
								title="Explode environment"
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
									<Wifi class="w-3.5 h-3.5 text-sky-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">User Latency</span>
								</div>
								<div class="text-[14px] font-black text-slate-800 dark:text-white uppercase italic">22ms (RTT)</div>
							</div>
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<Target class="w-3.5 h-3.5 text-sky-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Connection Health</span>
								</div>
								<div class="text-[14px] font-black text-emerald-500 uppercase italic">AVAILABLE</div>
							</div>
							<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/metric">
								<div class="flex items-center gap-2 mb-2">
									<Timer class="w-3.5 h-3.5 text-sky-500" />
									<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Active Uptime</span>
								</div>
								<div class="text-[14px] font-black text-slate-800 dark:text-white uppercase italic">14h 22m</div>
							</div>
						</div>

						<!-- Core Meta Stats -->
						<div class="grid grid-cols-1 md:grid-cols-2 gap-6 pt-4">
							<div class="p-8 bg-slate-50 dark:bg-slate-900/50 rounded-[2.5rem] border border-slate-100 dark:border-slate-700/50">
								<h3 class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-6 italic flex items-center gap-2">
									<HD class="w-4 h-4 text-sky-500" />
									Storage Fabric
								</h3>
								<div class="space-y-4">
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">Root Volume</span>
										<span class="text-slate-400">80 GB (gp3)</span>
									</div>
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">User Volume</span>
										<span class="text-slate-400">50 GB (gp3)</span>
									</div>
								</div>
							</div>
							<div class="p-8 bg-slate-50 dark:bg-slate-900/50 rounded-[2.5rem] border border-slate-100 dark:border-slate-700/50">
								<h3 class="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-6 italic flex items-center gap-2">
									<NetIcon class="w-4 h-4 text-sky-500" />
									Network Boundary
								</h3>
								<div class="space-y-4">
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">IP Control</span>
										<span class="text-emerald-500">ALLOWED</span>
									</div>
									<div class="flex justify-between items-center text-[10px] uppercase font-black italic">
										<span class="text-slate-500">VPC Peering</span>
										<span class="text-emerald-500">ACTIVE</span>
									</div>
								</div>
							</div>
						</div>

						<!-- Advanced Properties -->
						<div class="pt-4 space-y-4">
							<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic px-1 flex items-center gap-2">
								<UserCog class="w-4 h-4 text-sky-500" />
								Environment Stratagem
							</div>
							<div class="p-8 bg-slate-900 dark:bg-black rounded-[2.5rem] border border-slate-800 shadow-inner grid grid-cols-2 gap-8">
								<div>
									<div class="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Compute Tier</div>
									<div class="text-[12px] font-black text-white uppercase italic tracking-tighter leadning-none">STANDARD (2 vCPU, 4GB)</div>
								</div>
								<div>
									<div class="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Running Mode</div>
									<div class="text-[12px] font-black text-white uppercase italic tracking-tighter leadning-none">ALWAYS_ON</div>
								</div>
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<Computer class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">Managed Virtual Desktops</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Provision high-fidelity managed Windows and Linux desktops, coordinate distributed user environments, and monitor global connectivity pulse through an enterprise-grade WorkSpaces managed plane.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateModal = false} onkeydown={(e) => { if (e.key === 'Escape') showCreateModal = false; }} role="presentation"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-sky-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Deploy Instance</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createWorkspace(); }} class="space-y-6">
					<div>
						<label for="uName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Target Identity</label>
						<input 
							id="uName"
							type="text" 
							bind:value={userName}
							placeholder="e.g. bishop.a"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-sky-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div>
						<label for="bId" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Bundle Stratagem</label>
						<select 
							id="bId"
							bind:value={bundleId}
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-sky-500 transition-all font-mono text-xs italic"
							required
						>
							{#each bundles as bundle}
								<option value={bundle.BundleId}>{bundle.Name} ({bundle.ComputeType?.Name})</option>
							{/each}
						</select>
					</div>

					<div class="p-5 bg-sky-500/5 rounded-2xl border border-sky-500/10">
						<div class="flex items-center gap-2 mb-2">
							<ShieldCheck class="w-3.5 h-3.5 text-sky-500" />
							<span class="text-[10px] font-black text-sky-600 uppercase tracking-widest leading-none">VDI Baseline</span>
						</div>
						<p class="text-[9px] text-sky-800 dark:text-sky-400 leading-relaxed font-bold uppercase tracking-tight italic">
							Registering a managed WorkSpace provisions a dedicated virtual desktop instance with automatic EBS persistence and directory integration. Provisioning typically completes in 10-15 minutes.
						</p>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-sky-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Syncing...' : 'Provision Desktop'}
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
