<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getShieldClient } from '$lib/aws-client';
	import {
		ListProtectionsCommand,
		DescribeSubscriptionCommand,
		GetSubscriptionStateCommand,
		CreateSubscriptionCommand,
		CreateProtectionCommand,
		DeleteProtectionCommand,
		ListAttacksCommand,
		DescribeAttackStatisticsCommand,
		AssociateDRTRoleCommand,
		AssociateDRTLogBucketCommand,
		EnableApplicationLayerAutomaticResponseCommand,
		DisableApplicationLayerAutomaticResponseCommand,
		type Protection,
		type Subscription,
		type AttackSummary
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
		ShieldQuestion, History, Ban, Play, Pause,
		PhoneCall, FileText, UserCheck, Siren
	} from 'lucide-svelte';

	const shield = regionalClient(getShieldClient);

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let protections = $state<Protection[]>([]);
	let subscription = $state<Subscription | null>(null);
	let subscriptionState = $state<string>('INSPECTING');
	let loadingState = $state(false);
	let attacks = $state<AttackSummary[]>([]);
	let attackStats = $state<{ count: number; fromInclusive: number; toExclusive: number } | null>(null);
	let activeTab = $state<'protections' | 'timeline' | 'drt'>('protections');

	// ALAR state
	let alarResourceArn = $state('');
	let alarAction = $state<'BLOCK' | 'COUNT'>('COUNT');
	let alarLoading = $state(false);

	// DRT engagement state
	let drtRoleArn = $state('');
	let drtLogBucket = $state('');
	let drtLoading = $state(false);

	// Derived
	const filteredProtections = $derived(
		protections.filter(p => p.Name?.toLowerCase().includes(searchQuery.toLowerCase()) || p.ResourceArn?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadShield() {
		loading = true;
		try {
			const res = await shield().send(new ListProtectionsCommand({}));
			protections = res.Protections ?? [];

			const subRes = await shield().send(new DescribeSubscriptionCommand({}));
			subscription = subRes.Subscription ?? null;

			const stateRes = await shield().send(new GetSubscriptionStateCommand({}));
			subscriptionState = stateRes.SubscriptionState || 'INACTIVE';

			// Load attack timeline
			const attackRes = await shield().send(new ListAttacksCommand({}));
			attacks = attackRes.AttackSummaries ?? [];

			// Load attack statistics
			const statsRes = await shield().send(new DescribeAttackStatisticsCommand({}));
			const items = statsRes.DataItems ?? [];
			const range = statsRes.TimeRange;
			const total = items.reduce((sum: number, item: { AttackCount?: number }) => sum + (item.AttackCount ?? 0), 0);
			attackStats = {
				count: total,
				fromInclusive: range?.FromInclusive ? Number(range.FromInclusive) : 0,
				toExclusive: range?.ToExclusive ? Number(range.ToExclusive) : 0
			};
		} catch (err: unknown) {
			toast.error(`Failed to load Shield: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function activateSubscription() {
		loadingState = true;
		try {
			await shield().send(new CreateSubscriptionCommand({}));
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
			await shield().send(new DeleteProtectionCommand({ ProtectionId: id }));
			toast.success(`Protection removed`);
			await loadShield();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function enableALAR() {
		if (!alarResourceArn.trim()) {
			toast.error('Resource ARN is required');
			return;
		}
		alarLoading = true;
		try {
			const action = alarAction === 'BLOCK' ? { Block: {} } : { Count: {} };
			await shield().send(new EnableApplicationLayerAutomaticResponseCommand({
				ResourceArn: alarResourceArn.trim(),
				Action: action
			}));
			toast.success(`ALAR enabled (${alarAction}) for resource`);
			alarResourceArn = '';
		} catch (err: unknown) {
			toast.error(`ALAR enable failed: ${(err as Error).message}`);
		} finally {
			alarLoading = false;
		}
	}

	async function disableALAR() {
		if (!alarResourceArn.trim()) {
			toast.error('Resource ARN is required');
			return;
		}
		if (!await confirmDestructive({ title: 'Disable ALAR', message: 'Disable automatic layer 7 response for this resource?', confirmLabel: 'Disable' })) return;
		alarLoading = true;
		try {
			await shield().send(new DisableApplicationLayerAutomaticResponseCommand({
				ResourceArn: alarResourceArn.trim()
			}));
			toast.success('ALAR disabled for resource');
			alarResourceArn = '';
		} catch (err: unknown) {
			toast.error(`ALAR disable failed: ${(err as Error).message}`);
		} finally {
			alarLoading = false;
		}
	}

	async function associateDRTRole() {
		if (!drtRoleArn.trim()) {
			toast.error('IAM Role ARN is required');
			return;
		}
		drtLoading = true;
		try {
			await shield().send(new AssociateDRTRoleCommand({ RoleArn: drtRoleArn.trim() }));
			toast.success('DRT IAM role associated');
			drtRoleArn = '';
		} catch (err: unknown) {
			toast.error(`DRT role association failed: ${(err as Error).message}`);
		} finally {
			drtLoading = false;
		}
	}

	async function associateDRTLogBucket() {
		if (!drtLogBucket.trim()) {
			toast.error('S3 bucket name is required');
			return;
		}
		drtLoading = true;
		try {
			await shield().send(new AssociateDRTLogBucketCommand({ LogBucket: drtLogBucket.trim() }));
			toast.success('DRT log bucket associated');
			drtLogBucket = '';
		} catch (err: unknown) {
			toast.error(`DRT log bucket association failed: ${(err as Error).message}`);
		} finally {
			drtLoading = false;
		}
	}

	onRegionChange(loadShield);
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
				<div class="text-[9px] font-black text-rose-600 uppercase tracking-widest mb-1 italic">Recorded Attacks</div>
				<div class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums">{attackStats?.count ?? 0}</div>
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

	<!-- Tab Navigation -->
	<div class="flex gap-2 border-b border-slate-200 dark:border-slate-700">
		<button
			onclick={() => activeTab = 'protections'}
			class="px-5 py-3 text-xs font-black uppercase tracking-widest transition-all border-b-2 {activeTab === 'protections' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}"
		>
			<span class="flex items-center gap-2"><Lock class="w-3.5 h-3.5" /> Protections</span>
		</button>
		<button
			onclick={() => activeTab = 'timeline'}
			class="px-5 py-3 text-xs font-black uppercase tracking-widest transition-all border-b-2 {activeTab === 'timeline' ? 'border-rose-500 text-rose-600 dark:text-rose-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}"
		>
			<span class="flex items-center gap-2"><History class="w-3.5 h-3.5" /> Attack Timeline</span>
		</button>
		<button
			onclick={() => activeTab = 'drt'}
			class="px-5 py-3 text-xs font-black uppercase tracking-widest transition-all border-b-2 {activeTab === 'drt' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}"
		>
			<span class="flex items-center gap-2"><PhoneCall class="w-3.5 h-3.5" /> DRT Engagement</span>
		</button>
	</div>

	{#if activeTab === 'protections'}
	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Protections Ledger -->
		<div class="lg:col-span-8 space-y-6">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl overflow-hidden min-h-[400px] flex flex-col">
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
							<p class="text-[11px] text-slate-500 max-w-xs mx-auto italic font-black uppercase tracking-tight leading-relaxed">No resources are currently registered for Shield Advanced protection.</p>
						</div>
					{/if}
				</div>
			</div>
		</div>

		<!-- ALAR Setup Panel -->
		<div class="lg:col-span-4 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl p-6">
				<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic mb-5">
					<Zap class="w-4 h-4 text-indigo-500" />
					ALAR — Auto Layer 7 Response
				</h3>
				<p class="text-[10px] text-slate-500 italic mb-4 leading-relaxed">Enable Shield Advanced to automatically deploy WAF rules in response to Layer 7 DDoS attacks on a protected resource.</p>
				<div class="space-y-3">
					<div>
						<label class="text-[9px] font-black uppercase tracking-widest text-slate-500 italic mb-1 block">Resource ARN</label>
						<input
							type="text"
							bind:value={alarResourceArn}
							placeholder="arn:aws:elasticloadbalancing:..."
							class="w-full px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-[10px] focus:ring-2 focus:ring-indigo-500 outline-none font-mono"
						/>
					</div>
					<div>
						<label class="text-[9px] font-black uppercase tracking-widest text-slate-500 italic mb-1 block">Mitigation Action</label>
						<div class="flex gap-2">
							<button
								onclick={() => alarAction = 'COUNT'}
								class="flex-1 py-2 rounded-xl text-[9px] font-black uppercase tracking-widest transition-all border {alarAction === 'COUNT' ? 'bg-amber-500/20 border-amber-500/50 text-amber-700 dark:text-amber-300' : 'border-slate-200 dark:border-slate-600 text-slate-500'}"
							>
								Count
							</button>
							<button
								onclick={() => alarAction = 'BLOCK'}
								class="flex-1 py-2 rounded-xl text-[9px] font-black uppercase tracking-widest transition-all border {alarAction === 'BLOCK' ? 'bg-rose-500/20 border-rose-500/50 text-rose-700 dark:text-rose-300' : 'border-slate-200 dark:border-slate-600 text-slate-500'}"
							>
								Block
							</button>
						</div>
					</div>
					<div class="flex gap-2 pt-1">
						<button
							onclick={enableALAR}
							disabled={alarLoading}
							class="flex-1 py-2.5 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white rounded-xl text-[9px] font-black uppercase tracking-widest transition-all flex items-center justify-center gap-1.5"
						>
							<Play class="w-3 h-3" /> Enable
						</button>
						<button
							onclick={disableALAR}
							disabled={alarLoading}
							class="flex-1 py-2.5 bg-white/50 dark:bg-slate-700/50 hover:bg-rose-500/10 disabled:opacity-50 text-rose-600 rounded-xl text-[9px] font-black uppercase tracking-widest transition-all border border-rose-200 dark:border-rose-900 flex items-center justify-center gap-1.5"
						>
							<Ban class="w-3 h-3" /> Disable
						</button>
					</div>
				</div>
			</div>
		</div>
	</div>

	{:else if activeTab === 'timeline'}
	<!-- Attack Timeline -->
	<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl overflow-hidden">
		<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-white/20 dark:bg-slate-900/10 flex justify-between items-center">
			<h3 class="text-xs font-black text-slate-500 uppercase tracking-widest flex items-center gap-2 italic">
				<History class="w-4 h-4 text-rose-500" />
				DDoS Attack Timeline
			</h3>
			{#if attackStats}
				<div class="flex items-center gap-4">
					<span class="text-[9px] font-black uppercase tracking-widest text-slate-500 italic">
						{attackStats.count} total attacks in range
					</span>
					{#if attackStats.fromInclusive > 0}
						<span class="text-[9px] font-mono text-slate-400 italic">
							{new Date(attackStats.fromInclusive * 1000).toLocaleDateString()} – {new Date(attackStats.toExclusive * 1000).toLocaleDateString()}
						</span>
					{/if}
				</div>
			{/if}
		</div>

		{#if loading}
			<div class="flex flex-col items-center justify-center p-32 opacity-30">
				<History class="w-16 h-16 text-rose-500 animate-spin mb-8" />
				<span class="text-[10px] uppercase font-black text-slate-500 tracking-[0.2em] italic">Loading attack history...</span>
			</div>
		{:else if attacks.length}
			<div class="divide-y divide-slate-100 dark:divide-slate-700/50">
				{#each attacks as attack}
					<div class="p-6 hover:bg-rose-500/5 transition-all group/a flex items-center gap-6">
						<div class="p-3 bg-rose-900/20 rounded-2xl border border-rose-900/30 shrink-0">
							<ShieldAlert class="w-5 h-5 text-rose-500" />
						</div>
						<div class="flex-1 min-w-0">
							<div class="text-xs font-black text-slate-900 dark:text-white uppercase tracking-tight italic mb-1 font-mono truncate">
								{attack.AttackId}
							</div>
							<div class="text-[10px] text-slate-500 italic font-bold truncate">{attack.ResourceArn}</div>
						</div>
						<div class="text-right shrink-0">
							{#if attack.StartTime}
								<div class="text-[9px] font-black uppercase tracking-widest text-slate-500 italic">
									{new Date(attack.StartTime).toLocaleString()}
								</div>
							{/if}
							{#if attack.EndTime}
								<div class="text-[9px] text-slate-400 italic font-mono">
									→ {new Date(attack.EndTime).toLocaleString()}
								</div>
							{/if}
						</div>
						<div class="w-2 h-2 rounded-full bg-rose-500 animate-pulse shrink-0"></div>
					</div>
				{/each}
			</div>
		{:else}
			<div class="flex flex-col items-center justify-center p-32 text-center opacity-30">
				<CheckCircle2 class="w-20 h-20 text-emerald-500 mb-8" />
				<h4 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic leading-none mb-2">All Clear</h4>
				<p class="text-[11px] text-slate-500 max-w-xs mx-auto italic font-black uppercase tracking-tight leading-relaxed">No DDoS attacks recorded in the current observation window. Perimeter is nominal.</p>
			</div>
		{/if}

		<!-- Attack stats footer -->
		<div class="p-6 border-t border-slate-100 dark:border-slate-700/50 bg-slate-50/50 dark:bg-slate-900/40 grid grid-cols-1 md:grid-cols-3 gap-4">
			<div class="flex items-center gap-3 bg-white dark:bg-slate-800 p-4 rounded-2xl border border-slate-100 dark:border-slate-700/50">
				<BarChart3 class="w-5 h-5 text-indigo-500 shrink-0" />
				<div>
					<div class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest">Total Attacks</div>
					<div class="text-sm font-black text-slate-900 dark:text-white italic">{attackStats?.count ?? '—'}</div>
				</div>
			</div>
			<div class="flex items-center gap-3 bg-white dark:bg-slate-800 p-4 rounded-2xl border border-slate-100 dark:border-slate-700/50">
				<Timer class="w-5 h-5 text-amber-500 shrink-0" />
				<div>
					<div class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest">Observation Window</div>
					<div class="text-sm font-black text-slate-900 dark:text-white italic">365 days</div>
				</div>
			</div>
			<div class="flex items-center gap-3 bg-white dark:bg-slate-800 p-4 rounded-2xl border border-slate-100 dark:border-slate-700/50">
				<Gauge class="w-5 h-5 text-emerald-500 shrink-0" />
				<div>
					<div class="text-[9px] font-black text-slate-500 uppercase italic tracking-widest">Threat Level</div>
					<div class="text-sm font-black text-slate-900 dark:text-white italic">{(attackStats?.count ?? 0) > 0 ? 'ELEVATED' : 'NOMINAL'}</div>
				</div>
			</div>
		</div>
	</div>

	{:else}
	<!-- DRT Engagement Flows -->
	<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
		<!-- DRT Role Association -->
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl p-8">
			<div class="flex items-center gap-3 mb-6">
				<div class="p-3 bg-amber-500/10 rounded-2xl">
					<UserCheck class="w-6 h-6 text-amber-600" />
				</div>
				<div>
					<h3 class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">DRT IAM Role</h3>
					<p class="text-[10px] text-slate-500 italic">Grant the DRT access to act in your account</p>
				</div>
			</div>
			<p class="text-[10px] text-slate-500 italic mb-5 leading-relaxed">
				Associate an IAM role that grants the Shield Response Team (DRT) permission to manage your AWS resources during a DDoS event. The role must trust the <span class="font-mono text-indigo-500">drt.shield.amazonaws.com</span> principal.
			</p>
			<div class="space-y-3">
				<div>
					<label class="text-[9px] font-black uppercase tracking-widest text-slate-500 italic mb-1 block">IAM Role ARN</label>
					<input
						type="text"
						bind:value={drtRoleArn}
						placeholder="arn:aws:iam::123456789012:role/ShieldDRTRole"
						class="w-full px-3 py-2.5 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-[10px] focus:ring-2 focus:ring-amber-500 outline-none font-mono"
					/>
				</div>
				<button
					onclick={associateDRTRole}
					disabled={drtLoading}
					class="w-full py-3 bg-amber-500 hover:bg-amber-600 disabled:opacity-50 text-white rounded-xl text-[10px] font-black uppercase tracking-widest transition-all flex items-center justify-center gap-2 shadow-lg shadow-amber-500/20"
				>
					<UserCheck class="w-4 h-4" /> Associate DRT Role
				</button>
			</div>
			<div class="mt-6 p-4 bg-amber-50 dark:bg-amber-900/10 rounded-2xl border border-amber-200 dark:border-amber-900/30">
				<div class="flex items-start gap-2">
					<AlertTriangle class="w-4 h-4 text-amber-600 shrink-0 mt-0.5" />
					<p class="text-[9px] text-amber-700 dark:text-amber-400 italic leading-relaxed font-bold">
						The DRT can only access your resources during active DDoS events and only for the duration of the engagement. All actions are logged to CloudTrail.
					</p>
				</div>
			</div>
		</div>

		<!-- DRT Log Bucket -->
		<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-3xl shadow-xl p-8">
			<div class="flex items-center gap-3 mb-6">
				<div class="p-3 bg-indigo-500/10 rounded-2xl">
					<FileText class="w-6 h-6 text-indigo-600" />
				</div>
				<div>
					<h3 class="text-sm font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">DRT Log Bucket</h3>
					<p class="text-[10px] text-slate-500 italic">Share flow logs with the Shield Response Team</p>
				</div>
			</div>
			<p class="text-[10px] text-slate-500 italic mb-5 leading-relaxed">
				Associate an S3 bucket containing VPC Flow Logs or Route 53 Resolver DNS query logs. The DRT uses these logs to analyze traffic patterns and craft targeted mitigations during an event.
			</p>
			<div class="space-y-3">
				<div>
					<label class="text-[9px] font-black uppercase tracking-widest text-slate-500 italic mb-1 block">S3 Bucket Name</label>
					<input
						type="text"
						bind:value={drtLogBucket}
						placeholder="my-shield-flow-logs-bucket"
						class="w-full px-3 py-2.5 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-[10px] focus:ring-2 focus:ring-indigo-500 outline-none font-mono"
					/>
				</div>
				<button
					onclick={associateDRTLogBucket}
					disabled={drtLoading}
					class="w-full py-3 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white rounded-xl text-[10px] font-black uppercase tracking-widest transition-all flex items-center justify-center gap-2 shadow-lg shadow-indigo-600/20"
				>
					<FileText class="w-4 h-4" /> Associate Log Bucket
				</button>
			</div>
			<div class="mt-6 p-4 bg-indigo-50 dark:bg-indigo-900/10 rounded-2xl border border-indigo-200 dark:border-indigo-900/30">
				<div class="flex items-start gap-2">
					<Info class="w-4 h-4 text-indigo-600 shrink-0 mt-0.5" />
					<p class="text-[9px] text-indigo-700 dark:text-indigo-400 italic leading-relaxed font-bold">
						The S3 bucket must have a bucket policy that grants the DRT read access. Shield Advanced will validate bucket permissions when you associate the bucket.
					</p>
				</div>
			</div>
		</div>

		<!-- DRT Engagement Checklist -->
		<div class="lg:col-span-2 bg-slate-900 rounded-3xl border border-slate-800 shadow-2xl p-8">
			<div class="flex items-center gap-3 mb-6">
				<Siren class="w-6 h-6 text-rose-400 animate-pulse" />
				<h3 class="text-sm font-black text-white uppercase tracking-tighter italic">DRT Engagement Workflow</h3>
			</div>
			<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
				{#each [
					{ step: '01', title: 'Detect Attack', desc: 'Shield Advanced detects volumetric or application-layer DDoS traffic against a protected resource.', icon: Radar, color: 'text-rose-400' },
					{ step: '02', title: 'Auto Mitigate', desc: 'Shield applies inline network mitigations automatically. ALAR deploys WAF rules for Layer 7 events if enabled.', icon: Zap, color: 'text-amber-400' },
					{ step: '03', title: 'DRT Engages', desc: 'If mitigations are insufficient, the Shield Response Team engages using associated IAM role and log access.', icon: PhoneCall, color: 'text-emerald-400' }
				] as step}
					<div class="bg-slate-800/60 rounded-2xl p-5 border border-slate-700/50">
						<div class="flex items-center gap-3 mb-3">
							<span class="text-[9px] font-black text-slate-500 font-mono">STEP {step.step}</span>
							<svelte:component this={step.icon} class="w-4 h-4 {step.color}" />
						</div>
						<h4 class="text-sm font-black text-white italic mb-2 tracking-tight">{step.title}</h4>
						<p class="text-[10px] text-slate-400 italic leading-relaxed">{step.desc}</p>
					</div>
				{/each}
			</div>
		</div>
	</div>
	{/if}
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
