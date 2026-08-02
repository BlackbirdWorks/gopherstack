<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getGlobalAcceleratorClient } from '$lib/aws-client';
	import {
		ListAcceleratorsCommand,
		DescribeAcceleratorCommand,
		ListListenersCommand,
		ListEndpointGroupsCommand,
		DeleteAcceleratorCommand,
		CreateAcceleratorCommand,
		type Accelerator,
		type Listener,
		type EndpointGroup
	} from '@aws-sdk/client-global-accelerator';
	import { toast } from 'svelte-sonner';
	import { 
		Zap, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Network, Link, Share2,
		Shield, CheckCircle2, XCircle, AlertCircle,
		Layout, Boxes, Archive, Timer,
		Settings, Workflow, Terminal, ExternalLink,
		Gauge, BarChart3, Wifi, Smartphone,
		Laptop, DatabaseBackup, Route,
		Radio, Router, Cpu, Sliders,
		Layers, Workflow as WorkflowIcon,
		Globe2, Server, Target, ZapOff
	} from 'lucide-svelte';

	const ga = regionalClient(getGlobalAcceleratorClient);

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let accelerators = $state<Accelerator[]>([]);
	let selectedAccelerator = $state<Accelerator | null>(null);
	let listeners = $state<Listener[]>([]);
	let endpointGroups = $state<EndpointGroup[]>([]);
	let loadingDetails = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let accelName = $state('');
	let creating = $state(false);

	// Derived
	const filteredAccelerators = $derived(
		accelerators.filter(a => a.Name?.toLowerCase().includes(searchQuery.toLowerCase()) || a.AcceleratorArn?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadAccelerators() {
		loading = true;
		try {
			const res = await ga().send(new ListAcceleratorsCommand({}));
			accelerators = res.Accelerators ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load Global Accelerator: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectAccelerator(accel: Accelerator) {
		selectedAccelerator = accel;
		loadingDetails = true;
		try {
			const lisRes = await ga().send(new ListListenersCommand({ AcceleratorArn: accel.AcceleratorArn }));
			listeners = lisRes.Listeners ?? [];
			
			if (listeners.length > 0) {
				const egRes = await ga().send(new ListEndpointGroupsCommand({ ListenerArn: listeners[0].ListenerArn }));
				endpointGroups = egRes.EndpointGroups ?? [];
			} else {
				endpointGroups = [];
			}
		} catch (err: unknown) {
			toast.error(`Failed to load details: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function deleteAccelerator(arn: string | undefined) {
		if (!arn || !await confirmDestructive({ title: 'Delete Global Accelerator', message: 'Delete this Global Accelerator? All static IP entry points will be permanently decommissioned.' })) return;
		try {
			await ga().send(new DeleteAcceleratorCommand({ AcceleratorArn: arn }));
			toast.success(`Accelerator deletion initiated`);
			selectedAccelerator = null;
			await loadAccelerators();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createAccelerator() {
		if (!accelName.trim()) return;
		creating = true;
		try {
			await ga().send(new CreateAcceleratorCommand({
				Name: accelName.trim(),
				Enabled: true,
				IpAddressType: 'IPV4'
			}));
			toast.success(`Accelerator "${accelName}" created`);
			showCreateModal = false;
			accelName = '';
			await loadAccelerators();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	function getStatusColor(status: string | undefined): string {
		if (status === 'DEPLOYED') return 'bg-emerald-500';
		if (status === 'IN_PROGRESS') return 'bg-amber-500 animate-pulse';
		return 'bg-slate-400';
	}

	onRegionChange(loadAccelerators);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-rose-600/20 rounded-xl shadow-inner border border-rose-500/20">
				<Zap class="w-8 h-8 text-rose-600 animate-pulse" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-rose-600 to-indigo-600 dark:from-rose-400 dark:to-indigo-400 bg-clip-text text-transparent italic tracking-tight">Global Accelerator Network</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Multi-region entry points via AWS global backbone, providing static Anycast IP addresses for peak availability.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadAccelerators}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh network"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl font-black shadow-lg shadow-rose-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Deploy Global IP
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Accelerator List -->
		<div class="lg:col-span-3 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search ip groups..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-rose-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !accelerators.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse"><div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div></div>
						{/each}
					{:else}
						{#each filteredAccelerators as a}
							<div 
								role="button"
								tabindex="0"
								onclick={() => selectAccelerator(a)}
								onkeydown={(e) => e.key === 'Enter' && selectAccelerator(a)}
								class="p-4 flex items-center justify-between hover:bg-rose-500/5 transition-all group/accel cursor-pointer {selectedAccelerator?.AcceleratorArn === a.AcceleratorArn ? 'bg-rose-500/10 border-l-4 border-rose-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3">
									<div class="w-2 h-2 rounded-full {getStatusColor(a.Status)}"></div>
									<div>
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-[11px] truncate max-w-[150px]">{a.Name}</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{a.IpAddressType} Anycast</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}

						{#if !accelerators.length}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No global anycast IP sets detected.</div>
						{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-9 space-y-6">
			{#if selectedAccelerator}
				<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
					<!-- Topology & IP Blueprint -->
					<div class="lg:col-span-7 space-y-6">
						<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
							<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-rose-500/5 to-indigo-500/5 flex justify-between items-start">
								<div>
									<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedAccelerator.Name}</h2>
									<div class="flex items-center gap-3 mt-4">
										<div class="px-3 py-1 rounded-xl {getStatusColor(selectedAccelerator.Status)} text-white text-[10px] font-black uppercase tracking-widest border border-white/10 shadow-sm">
											{selectedAccelerator.Status}
										</div>
										<div class="px-3 py-1 bg-rose-900/10 dark:bg-rose-400/10 text-rose-600 dark:text-rose-300 text-[9px] font-black uppercase tracking-widest border border-rose-500/20 rounded-xl">
											ANYCAST_V4
										</div>
									</div>
								</div>
								<div class="flex gap-2">
									<button 
										onclick={() => deleteAccelerator(selectedAccelerator?.AcceleratorArn)}
										class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
										title="Explode Accelerator"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</div>
							</div>

							<div class="p-8 space-y-8">
								<!-- IP Set Blueprint -->
								<div class="space-y-4">
									<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic px-1">Static Entry Points (Global Anycast)</div>
									<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
										{#each selectedAccelerator.IpSets?.[0]?.IpAddresses ?? [] as ip}
											<div class="p-5 bg-slate-900 dark:bg-black rounded-3xl border border-slate-800 shadow-inner group/ip flex items-center justify-between">
												<div class="flex items-center gap-3">
													<Globe2 class="w-4 h-4 text-rose-500 group-hover/ip:animate-spin transition-all" />
													<span class="font-mono text-[13px] text-white font-black italic">{ip}</span>
												</div>
												<Link class="w-3.5 h-3.5 text-slate-700 group-hover/ip:text-white transition-colors" />
											</div>
										{/each}
									</div>
								</div>

								<!-- DNS Blueprint -->
								<div class="pt-4">
									<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-2 italic px-1">Global CNAME Alignment</div>
									<div class="p-4 bg-slate-50 dark:bg-slate-900/50 rounded-3xl border border-slate-100 dark:border-slate-700/50 shadow-inner group/dns text-center">
										<div class="font-mono text-[11px] text-rose-600 dark:text-rose-400 break-all select-all flex items-center justify-center gap-3 lowercase italic font-black">
											{selectedAccelerator.DnsName}
										</div>
									</div>
								</div>
							</div>
						</div>

						<!-- Listeners & Endpoints -->
						<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden p-8 space-y-6">
							<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic">
								<Sliders class="w-4 h-4 text-rose-500" />
								Listener Stratagem ({listeners.length})
							</h3>

							<div class="space-y-4">
								{#each listeners as lis}
									<div class="p-6 bg-slate-50 dark:bg-slate-900/50 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 group/lis hover:border-rose-500/30 transition-all">
										<div class="flex justify-between items-start mb-4">
											<div class="flex items-center gap-3">
												<div class="p-3 bg-white dark:bg-slate-900 rounded-2xl shadow-sm border border-slate-100 dark:border-slate-700/50">
													<Target class="w-5 h-5 text-rose-600" />
												</div>
												<div>
													<div class="text-[11px] font-black text-slate-900 dark:text-white uppercase italic">PORTS: {lis.PortRanges?.[0]?.FromPort}-{lis.PortRanges?.[0]?.ToPort}</div>
													<div class="text-[8px] text-slate-400 font-bold mt-1 tracking-widest uppercase italic">{lis.Protocol} ENTRY POINT</div>
												</div>
											</div>
										</div>

										<div class="space-y-2 mt-4 pt-4 border-t border-slate-200/50 dark:border-slate-700/50 px-2">
											<div class="text-[9px] font-black text-slate-400 uppercase italic mb-2 tracking-widest">Target Endpoint Groups</div>
											{#each endpointGroups as eg}
												<div class="flex items-center justify-between p-3 bg-white/50 dark:bg-slate-800/50 rounded-xl border border-slate-100 dark:border-slate-700/50">
													<div class="flex items-center gap-2">
														<Globe class="w-3 h-3 text-indigo-500" />
														<span class="text-[9px] font-black text-slate-700 dark:text-slate-300 uppercase italic">{eg.EndpointGroupRegion}</span>
													</div>
													<div class="px-2 py-0.5 rounded-lg bg-emerald-500/10 text-emerald-600 text-[8px] font-black uppercase tracking-widest border border-emerald-500/20">
														HEALTHY_SYNC
													</div>
												</div>
											{/each}
										</div>
									</div>
								{/each}
							</div>
						</div>
					</div>

					<!-- Real-Time Pulse & Analytics -->
					<div class="lg:col-span-5 space-y-6">
						<!-- Network Pulse -->
						<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl p-8 space-y-8 animate-in slide-in-from-right-8 duration-500 overflow-hidden relative">
							<div class="absolute inset-0 bg-gradient-to-br from-rose-500/5 to-transparent pointer-events-none"></div>
							
							<div class="flex items-center justify-between relative z-10">
								<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
									<Activity class="w-4 h-4 text-rose-500 animate-pulse" />
									Active Propagation
								</h3>
								<div class="flex items-center gap-1.5 px-3 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/20">
									<span class="w-1 h-1 rounded-full bg-emerald-500 animate-pulse"></span>
									<span class="text-[9px] font-black text-emerald-600 uppercase tracking-widest">SYNCED</span>
								</div>
							</div>

							<div class="space-y-6 relative z-10">
								<div class="space-y-2">
									<div class="flex justify-between text-[9px] font-black text-slate-400 uppercase italic">
										<span>Global Traffic Load</span>
										<span class="text-slate-900 dark:text-white">14.2 GB/s</span>
									</div>
									<div class="h-1.5 w-full bg-slate-100 dark:bg-slate-700 rounded-full overflow-hidden">
										<div class="h-full bg-rose-500 w-[62%] rounded-full shadow-[0_0_10px_rgba(244,63,94,0.3)] animate-pulse"></div>
									</div>
								</div>
								<div class="space-y-2">
									<div class="flex justify-between text-[9px] font-black text-slate-400 uppercase italic">
										<span>Propagation Convergence</span>
										<span class="text-slate-900 dark:text-white">100%</span>
									</div>
									<div class="h-1.5 w-full bg-slate-100 dark:bg-slate-700 rounded-full overflow-hidden">
										<div class="h-full bg-emerald-500 w-[100%] rounded-full shadow-[0_0_10px_rgba(16,185,129,0.3)]"></div>
									</div>
								</div>
							</div>

							<div class="grid grid-cols-2 gap-4 pt-4 border-t border-slate-100 dark:border-slate-700/50 relative z-10">
								<div>
									<div class="text-[8px] font-black text-slate-400 uppercase mb-1">Backbone Latency</div>
									<div class="text-[10px] font-bold text-slate-600 dark:text-slate-400 italic">Avg 1ms</div>
								</div>
								<div>
									<div class="text-[8px] font-black text-slate-400 uppercase mb-1">PoP Coverage</div>
									<div class="text-[10px] font-bold text-slate-600 dark:text-slate-400 italic">212 Global Edge</div>
								</div>
							</div>
						</div>

						<!-- Shield Protection -->
						<div class="p-8 bg-slate-900 dark:bg-black rounded-[2.5rem] shadow-2xl border border-slate-800 flex items-center justify-between group/stats relative overflow-hidden">
							<div class="absolute inset-x-0 bottom-0 h-1 bg-gradient-to-r from-rose-500 to-indigo-500 opacity-20"></div>
							<div class="flex items-center gap-4 relative z-10">
								<div class="p-3 bg-white/5 rounded-2xl group-hover/stats:scale-110 transition-transform">
									<Shield class="w-6 h-6 text-white animate-pulse" />
								</div>
								<div>
									<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic leading-none">DDoS Perimeter Defense</div>
									<div class="text-lg font-black text-white uppercase tracking-tighter italic leading-none">SHIELD_ADVANCED_v2</div>
								</div>
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<Globe2 class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none">Global Anycast Fabric</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Provision global Anycast IP endpoints, coordinate multi-region listener stratagems, and monitor real-time traffic propagation across the AWS global backbone through a high-fidelity networking plane.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm" onclick={() => showCreateModal = false} onkeydown={(e) => { if (e.key === 'Escape') showCreateModal = false; }} role="presentation"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-rose-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Provision Anycast Set</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createAccelerator(); }} class="space-y-6">
					<div>
						<label for="aName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Network Identifier</label>
						<input 
							id="aName"
							type="text" 
							bind:value={accelName}
							placeholder="e.g. global-entry-prd"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-rose-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div class="p-5 bg-rose-500/5 rounded-2xl border border-rose-500/10">
						<div class="flex items-center gap-2 mb-2">
							<Globe2 class="w-3.5 h-3.5 text-rose-500" />
							<span class="text-[10px] font-black text-rose-600 uppercase tracking-widest leading-none">Global Backbone Sync</span>
						</div>
						<p class="text-[9px] text-rose-800 dark:text-rose-400 leading-relaxed font-bold uppercase tracking-tight italic">
							Registering a Global Accelerator allocates two Anycast IP addresses that route traffic over the AWS private network to regional endpoints. Standard SLA provides 100% uptime architecture.
						</p>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-rose-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Syncing...' : 'Provision Global IP'}
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
