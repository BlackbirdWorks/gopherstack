<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getShieldClient } from '$lib/aws-client';
	import {
		ListProtectionsCommand,
		DescribeSubscriptionCommand,
		GetSubscriptionStateCommand,
		CreateSubscriptionCommand,
		CreateProtectionCommand,
		DeleteProtectionCommand,
		type Protection,
		type Subscription
	} from '@aws-sdk/client-shield';
	import { toast } from 'svelte-sonner';
	import { 
		Shield, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Lock, ShieldAlert, BarChart3, Zap,
		Layout, Boxes, Archive, Timer,
		Settings, Workflow, Terminal, ExternalLink,
		Gauge, Radar, Sliders, Target,
		CheckCircle2, AlertTriangle, AlertCircle,
		ShieldQuestion, History, Ban
	} from 'lucide-svelte';

	const shield = getShieldClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let protections = $state<Protection[]>([]);
	let subscription = $state<Subscription | null>(null);
	let subscriptionState = $state<string>('INSPECTING');
	let loadingState = $state(false);

	// Derived
	const filteredProtections = $derived(
		protections.filter(p => p.Name?.toLowerCase().includes(searchQuery.toLowerCase()) || p.ResourceArn?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadShield() {
		loading = true;
		try {
			const res = await shield.send(new ListProtectionsCommand({}));
			protections = res.Protections ?? [];

			const subRes = await shield.send(new DescribeSubscriptionCommand({}));
			subscription = subRes.Subscription ?? null;

			const stateRes = await shield.send(new GetSubscriptionStateCommand({}));
			subscriptionState = stateRes.SubscriptionState || 'INACTIVE';
		} catch (err: unknown) {
			toast.error(`Failed to load Shield: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function activateSubscription() {
		loadingState = true;
		try {
			await shield.send(new CreateSubscriptionCommand({}));
			toast.success(`Shield Advanced subscription activated`);
			await loadShield();
		} catch (err: unknown) {
			toast.error(`Activation failed: ${(err as Error).message}`);
		} finally {
			loadingState = false;
		}
	}

	async function deleteProtection(id: string | undefined) {
		if (!id || !await confirmDestructive({ title: 'Remove DDoS Protection', message: 'Remove Shield Advanced protection from this resource? The resource will no longer be monitored.', confirmLabel: 'Remove' })) return;
		try {
			await shield.send(new DeleteProtectionCommand({ ProtectionId: id }));
			toast.success(`Protection removed`);
			await loadShield();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		loadShield();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-indigo-600/20 rounded-xl shadow-inner border border-indigo-500/20">
				<Radar class="w-8 h-8 text-indigo-600 animate-pulse" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-indigo-600 to-blue-600 dark:from-indigo-400 dark:to-blue-400 bg-clip-text text-transparent italic tracking-tight text-shadow-glow">Shield Advanced</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1 uppercase tracking-widest font-black italic opacity-70">Distributed Denial of Service (DDoS) Protection Nerve Center</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadShield}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			{#if subscriptionState !== 'ACTIVE'}
				<button 
					onclick={activateSubscription}
					class="flex items-center gap-2 px-5 py-2.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-xl font-black shadow-lg shadow-indigo-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
				>
					<ShieldCheck class="w-5 h-5" />
					Enable Shield Advanced
				</button>
			{:else}
				<span class="rounded-xl border border-emerald-500/30 bg-emerald-500/10 px-3 py-2 text-xs font-black uppercase tracking-wider text-emerald-700 dark:text-emerald-300">Subscription Active</span>
			{/if}
			<button type="button" class="rounded-xl border border-indigo-500/30 bg-indigo-500/10 px-3 py-2 text-xs font-black uppercase tracking-wider text-indigo-700 dark:text-indigo-300">+ Add Protection</button>
		</div>
	</div>

	<!-- Top Stats -->
	<div class="grid grid-cols-1 md:grid-cols-4 gap-6">
		<div class="p-8 bg-slate-900 rounded-3xl border border-slate-800 shadow-2xl flex items-center justify-between group/total overflow-hidden relative">
			<div class="absolute inset-0 bg-gradient-to-br from-indigo-500/10 to-transparent pointer-events-none"></div>
			<div class="relative z-10">
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">Security Posture</div>
				<div class="text-3xl font-black text-white italic tabular-nums flex items-center gap-2">
					{subscriptionState}
					<div class="w-2 h-2 rounded-full {subscriptionState === 'ACTIVE' ? 'bg-emerald-500 animate-pulse' : 'bg-rose-500'}"></div>
				</div>
			</div>
			<ShieldCheck class="w-10 h-10 text-indigo-500/20 group-hover/total:scale-110 transition-transform relative z-10" />
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-emerald-600 uppercase tracking-widest mb-1 italic">Protected Assets</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{protections.length}</div>
			</div>
			<div class="p-3 bg-emerald-500/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Lock class="w-6 h-6 text-emerald-600" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-rose-600 uppercase tracking-widest mb-1 italic">Active Threats</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">0</div>
			</div>
			<div class="p-3 bg-rose-600/10 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<ShieldAlert class="w-6 h-6 text-rose-600" />
			</div>
		</div>
		<div class="p-8 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl flex items-center justify-between group/stat">
			<div>
				<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic">DRT Availability</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums uppercase">Online</div>
			</div>
			<div class="p-3 bg-white/50 dark:bg-slate-700 rounded-2xl group-hover/stat:rotate-12 transition-transform">
				<Activity class="w-6 h-6 text-slate-400" />
			</div>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Protections Ledger -->
		<div class="lg:col-span-12 space-y-6">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl overflow-hidden min-h-[500px] flex flex-col">
				<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-white/20 dark:bg-slate-900/10 flex justify-between items-center sticky top-0 z-10 backdrop-blur-md">
					<div class="flex items-center gap-4">
						<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic leading-none">
							<Target class="w-4 h-4 text-indigo-500" />
							Distributed Resource Protection Ledger
						</h3>
					</div>
					<div class="relative w-64">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-3 h-3 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Filter protections..."
							class="w-full pl-8 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-[10px] focus:ring-2 focus:ring-indigo-500 outline-none transition-all italic font-black uppercase"
						/>
					</div>
				</div>

				<div class="flex-1 overflow-y-auto">
					{#if loading}
						<div class="flex flex-col items-center justify-center p-32 opacity-30">
							<Radar class="w-16 h-16 text-indigo-500 animate-spin mb-8" />
							<span class="text-[10px] uppercase font-black text-slate-500 tracking-[0.2em] italic font-mono">Sequencing Perimeter Matrix...</span>
						</div>
					{:else if filteredProtections.length}
						<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
							{#each filteredProtections as p}
								<div class="p-8 hover:bg-indigo-500/5 transition-all group/p shadow-sm">
									<div class="flex flex-col md:flex-row md:items-center justify-between gap-6">
										<div class="flex items-start gap-6">
											<div class="p-4 bg-slate-900 rounded-3xl flex items-center justify-center shrink-0 border border-slate-800 group-hover/p:scale-105 transition-transform shadow-xl">
												<Lock class="w-6 h-6 text-white" />
											</div>
											<div>
												<h4 class="text-xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none mb-3 break-all">{p.Name}</h4>
												<p class="text-[10px] text-slate-500 dark:text-slate-400 max-w-2xl leading-relaxed italic font-bold opacity-60 mb-4 font-mono truncate">{p.ResourceArn}</p>
												
												<div class="flex flex-wrap items-center gap-6">
													<div class="flex items-center gap-2">
														<ShieldCheck class="w-3.5 h-3.5 text-emerald-500" />
														<span class="text-[9px] font-black text-slate-600 uppercase italic">Posture: REINFORCED</span>
													</div>
													<div class="flex items-center gap-2">
														<Globe class="w-3.5 h-3.5 text-slate-400" />
														<span class="text-[9px] font-black text-slate-600 uppercase italic">Edge Propagation: COMPLETE</span>
													</div>
													<div class="flex items-center gap-2 text-indigo-500">
														<Zap class="w-3.5 h-3.5" />
														<span class="text-[9px] font-black uppercase italic">Automatic Layer 7 Sync</span>
													</div>
												</div>
											</div>
										</div>
										<div class="flex flex-row md:flex-col items-center gap-3 md:items-end">
											<button 
												onclick={() => deleteProtection(p.Id)}
												class="p-3 bg-white/50 dark:bg-slate-700/50 text-rose-500 hover:bg-rose-500 hover:text-white rounded-xl transition-all border border-slate-200 dark:border-slate-600 shadow-sm"
												title="Decommission Protection"
											>
												<Trash2 class="w-4 h-4" />
											</button>
											<div class="text-[8px] text-slate-400 font-mono tracking-tighter italic opacity-60 uppercase shrink-0">PROTECTION_ID: {p.Id?.slice(0, 12)}...</div>
										</div>
									</div>
								</div>
							{/each}
						</div>
					{:else}
						<div class="flex flex-col items-center justify-center p-32 text-center opacity-30">
							<ShieldQuestion class="w-20 h-20 text-slate-600 mb-8" />
							<h4 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none mb-2">Perimeter Exposed</h4>
							<p class="text-[11px] text-slate-500 max-w-xs mx-auto italic font-black uppercase tracking-tight leading-relaxed">No resources are currently registered for Shield Advanced protection. Distributed denial-of-service surfacing remains unmitigated.</p>
						</div>
					{/if}
				</div>

				<!-- Visual Feedback Footer -->
				<div class="p-8 border-t border-slate-100 dark:border-slate-700/50 bg-slate-50/50 dark:bg-slate-900/40 grid grid-cols-1 md:grid-cols-2 gap-8">
					<div class="flex items-center gap-4 group/audit bg-white dark:bg-slate-800 p-5 rounded-3xl shadow-sm border border-slate-100 dark:border-slate-700/50 transition-all hover:scale-[1.02]">
						<div class="p-3 bg-emerald-500/10 rounded-2xl group-hover/audit:rotate-12 transition-transform">
							<ShieldCheck class="w-6 h-6 text-emerald-600" />
						</div>
						<div>
							<div class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest mb-1 leading-none">Security Response Team (DRT)</div>
							<div class="text-[12px] font-black text-slate-900 dark:text-white uppercase italic leading-none tracking-tighter">ACCESS_GRANTED / 24X7_COVERAGE</div>
						</div>
					</div>
					<div class="flex items-center gap-4 group/audit bg-white dark:bg-slate-800 p-5 rounded-3xl shadow-sm border border-slate-100 dark:border-slate-700/50 transition-all hover:scale-[1.02]">
						<div class="p-3 bg-indigo-500/10 rounded-2xl group-hover/audit:rotate-12 transition-transform">
							<BarChart3 class="w-6 h-6 text-indigo-600" />
						</div>
						<div>
							<div class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest mb-1 leading-none">Protection Telemetry Sync</div>
							<div class="text-[12px] font-black text-slate-900 dark:text-white uppercase italic leading-none tracking-tighter">GLOBAL_MESH_HEALTH: NOMINAL</div>
						</div>
					</div>
				</div>
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
		background: rgba(99, 102, 241, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(99, 102, 241, 0.2);
	}
</style>
