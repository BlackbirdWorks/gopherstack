<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { Activity, Zap, Box, Shield, Settings, Terminal, BarChart3, Cloud, Layout, Boxes, Search, Globe, Cpu, Database, Flame, Gauge, HardDrive, KeyRound, Laptop, ListTree, MousePointer2, Network, Radio, Radar, Route, Scan, SearchCode, Server, Share2, ShieldAlert, ShieldCheck, ShieldQuestion, Sliders, Smartphone, Target, Timer, TrendingUp, UserCircle, Wifi, Workflow, ZapOff } from 'lucide-svelte';
	import { dashboardClient } from '$lib/api/connect-client';
	import type { CapturedRequest } from '$lib/api/gopherstack/dashboard/v1/dashboard_pb';
	import { sidebarCategories } from '$lib/nav';

	type PortAllocation = {
		label: string;
		port: number;
	};

	type PortBucket = {
		end: number;
		entries: PortAllocation[];
		start: number;
	};

	const totalServices = sidebarCategories.reduce((acc, cat) => acc + cat.routes.length, 0);
	const maxLiveEvents = 8;

	let events = $state<CapturedRequest[]>([]);
	let eventStreamConnected = $state(false);
	let abortController: AbortController | null = null;
	let destroying = false;

	let portRangeStart = $state(5000);
	let portRangeEnd = $state(10000);
	let allocatedPorts = $state<PortAllocation[]>([]);
	let hoveredBucket = $state<PortBucket | null>(null);
	let systemStateTimer: ReturnType<typeof setInterval> | null = null;

	const portBuckets = $derived.by(() => {
		const bucketCount = 40;
		const span = Math.max(1, portRangeEnd - portRangeStart);
		const bucketSize = Math.max(1, Math.ceil(span / bucketCount));
		const buckets: PortBucket[] = [];

		for (let i = 0; i < bucketCount; i++) {
			const start = portRangeStart + i * bucketSize;
			const end = i === bucketCount - 1 ? portRangeEnd : Math.min(portRangeEnd, start + bucketSize);
			buckets.push({ start, end, entries: [] });
		}

		for (const entry of allocatedPorts) {
			if (entry.port < portRangeStart || entry.port >= portRangeEnd) {
				continue;
			}

			const index = Math.min(bucketCount - 1, Math.floor((entry.port - portRangeStart) / bucketSize));
			buckets[index]?.entries.push(entry);
		}

		return buckets;
	});

	function guessService(req: CapturedRequest): string {
		const headers = req.headers ?? {};
		if (headers['X-Amz-Target']) {
			return headers['X-Amz-Target'].split('.')[0];
		}
		if (req.path.startsWith('/latest/meta-data') || req.path.startsWith('/latest/api/token')) {
			return 'IMDS';
		}

		const auth = headers['Authorization'] ?? '';
		if (auth.includes('Credential=')) {
			const parts = auth.split('Credential=')[1]?.split('/');
			if (parts && parts.length > 3) {
				return parts[3].toUpperCase();
			}
		}

		return 'HTTP';
	}

	function formatTime(req: CapturedRequest): string {
		if (!req.timestamp) {
			return 'now';
		}

		const nowSeconds = Math.floor(Date.now() / 1000);
		const requestSeconds = Number(req.timestamp.seconds ?? 0);
		const delta = Math.max(0, nowSeconds - requestSeconds);

		if (delta < 60) {
			return 'now';
		}
		if (delta < 3600) {
			return `${Math.floor(delta / 60)}m`;
		}

		return `${Math.floor(delta / 3600)}h`;
	}

	function formatPortLabel(label: string): string {
		if (label.startsWith('lambda-url:')) {
			return `lambda url: ${label.replace('lambda-url:', '')}`;
		}
		if (label.startsWith('lambda:')) {
			return `lambda: ${label.replace('lambda:', '')}`;
		}

		return label;
	}

	async function loadSystemState() {
		try {
			const response = await fetch('/dashboard/api/system/state');
			if (!response.ok) {
				return;
			}

			const payload = (await response.json()) as {
				allocated?: PortAllocation[];
				portRangeEnd?: number;
				portRangeStart?: number;
			};

			portRangeStart = Number(payload.portRangeStart ?? portRangeStart);
			portRangeEnd = Number(payload.portRangeEnd ?? portRangeEnd);
			allocatedPorts = (payload.allocated ?? []).toSorted((a, b) => a.port - b.port);
		} catch {
			// Keep the dashboard view resilient when system-state polling fails.
		}
	}

	async function startEventStream() {
		abortController = new AbortController();
		try {
			const stream = await dashboardClient.streamConsole({}, { signal: abortController.signal });
			eventStreamConnected = true;
			for await (const response of stream) {
				if (response.request) {
					events = [response.request, ...events].slice(0, maxLiveEvents);
				}
			}
		} catch (err: unknown) {
			const e = err as Error;
			if (e.name !== 'AbortError' && !destroying) {
				eventStreamConnected = false;
			}
		}
	}

	function stopEventStream() {
		if (abortController) {
			abortController.abort();
			abortController = null;
			eventStreamConnected = false;
		}
	}

	onMount(() => {
		void loadSystemState();
		void startEventStream();
		systemStateTimer = setInterval(() => {
			void loadSystemState();
		}, 3000);
	});

	onDestroy(() => {
		destroying = true;
		stopEventStream();
		if (systemStateTimer) {
			clearInterval(systemStateTimer);
			systemStateTimer = null;
		}
	});
	
	const systemStats = [
		{ label: 'Active Services', value: totalServices, icon: Server, color: 'text-emerald-500' },
		{ label: 'Uptime', value: '99.98%', icon: Activity, color: 'text-indigo-500' },
		{ label: 'Latency', value: '1ms', icon: Gauge, color: 'text-cyan-500' },
		{ label: 'Cloud Faults', value: 'Active', icon: Flame, color: 'text-rose-500' },
	];

	const quickLinks = [
		{ label: 'FIS Chaos', href: '/dashboard/fis', icon: ZapOff, desc: 'Inject faults & test resilience' },
		{ label: 'API Console', href: '/dashboard/console', icon: Terminal, desc: 'Real-time API interaction' },
		{ label: 'Metrics', href: '/dashboard/metrics', icon: BarChart3, desc: 'System-wide performance telemetry' },
		{ label: 'Settings', href: '/dashboard/settings', icon: Settings, desc: 'Configure Gopherstack instance' },
	];
</script>

<div class="space-y-8 pb-20">
	<!-- Hero Section -->
	<section class="relative overflow-hidden rounded-[2.5rem] bg-slate-900 border border-slate-800 p-8 md:p-12 shadow-2xl group">
		<div class="absolute inset-0 bg-gradient-to-br from-indigo-500/20 via-transparent to-rose-500/10 pointer-events-none"></div>
		<div class="absolute -top-24 -right-24 w-96 h-96 bg-indigo-600/10 rounded-full blur-[100px] group-hover:bg-indigo-600/20 transition-all duration-1000"></div>
		
		<div class="relative z-10 flex flex-col md:flex-row items-center gap-12">
			<div class="flex-1 space-y-6">
				<div class="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-indigo-500/10 border border-indigo-500/20 text-indigo-400 text-xs font-bold uppercase tracking-widest italic animate-pulse">
					<Activity class="w-3 h-3" /> System Operational
				</div>
				<h1 class="text-4xl md:text-6xl font-black text-white italic tracking-tighter leading-none">
					GOPHER<span class="text-indigo-500">STACK</span>
				</h1>
				<p class="text-slate-400 text-lg max-w-xl leading-relaxed italic">
					High-performance local AWS cloud stack orchestration. Distributed tracing, real-time metrics, and cloud-native resilience simulation at scale.
				</p>
				<div class="flex flex-wrap gap-4 pt-4">
					<a href="/dashboard/docs" class="flex items-center gap-2 px-6 py-3 bg-white text-slate-900 rounded-2xl font-black italic uppercase text-sm hover:scale-105 active:scale-95 transition-all shadow-xl">
						<Layout class="w-4 h-4" /> Documentation
					</a>
					<a href="/dashboard/console" class="flex items-center gap-2 px-6 py-3 bg-slate-800 text-white border border-slate-700/50 rounded-2xl font-black italic uppercase text-sm hover:bg-slate-700 transition-all">
						<Terminal class="w-4 h-4" /> Launch Console
					</a>
				</div>
			</div>
			
			<div class="w-full md:w-auto flex flex-col gap-4">
				<div class="p-6 bg-white/5 backdrop-blur-md rounded-3xl border border-white/10 shadow-2xl transform hover:-rotate-2 transition-transform duration-500">
					<div class="flex items-center gap-4 mb-4">
						<div class="p-3 bg-indigo-500/20 rounded-2xl italic">
							<Cpu class="w-6 h-6 text-indigo-400" />
						</div>
						<div>
							<div class="text-[10px] font-black text-slate-500 uppercase tracking-widest italic">Stack Density</div>
							<div class="text-2xl font-black text-white italic tabular-nums">{totalServices} Services</div>
						</div>
					</div>
					<div class="space-y-2">
						{#each [75, 45, 90] as width}
							<div class="h-1 w-full bg-slate-800 rounded-full overflow-hidden">
								<div class="h-full bg-indigo-500/50 rounded-full animate-in slide-in-from-left duration-1000 ease-out" style="width: {width}%"></div>
							</div>
						{/each}
					</div>
				</div>
				
				<div class="p-6 bg-white/5 backdrop-blur-md rounded-3xl border border-white/10 shadow-2xl transform translate-x-4 md:translate-x-8 hover:rotate-2 transition-transform duration-500">
					<div class="flex items-center gap-4">
						<div class="p-3 bg-rose-500/20 rounded-2xl italic">
							<Zap class="w-6 h-6 text-rose-400" />
						</div>
						<div>
							<div class="text-[10px] font-black text-slate-500 uppercase tracking-widest italic">Chaos Engine</div>
							<div class="text-2xl font-black text-white italic tracking-tighter uppercase whitespace-nowrap">TLS_ARMED</div>
						</div>
					</div>
				</div>
			</div>
		</div>
	</section>

	<!-- Stats Grid -->
	<section class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">
		{#each systemStats as stat}
			<div class="p-6 bg-white dark:bg-slate-800/40 backdrop-blur-xl border border-slate-200 dark:border-slate-700/50 rounded-3xl shadow-sm hover:shadow-xl transition-all group overflow-hidden relative">
				<div class="absolute top-0 right-0 p-4 opacity-5 group-hover:opacity-10 transition-opacity">
					<stat.icon class="w-24 h-24 transform translate-x-8 translate-y-8" />
				</div>
				<div class="relative z-10 flex items-center justify-between">
					<div>
						<p class="text-[10px] font-black text-slate-400 dark:text-slate-500 uppercase tracking-widest mb-1 italic">{stat.label}</p>
						<p class="text-3xl font-black text-slate-900 dark:text-white italic tabular-nums tracking-tighter">{stat.value}</p>
					</div>
					<div class="p-3 bg-slate-100 dark:bg-slate-700 rounded-2xl group-hover:scale-110 transition-transform">
						<stat.icon class="w-6 h-6 {stat.color}" />
					</div>
				</div>
			</div>
		{/each}
	</section>

	<!-- Main Content Grid -->
	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Quick Links -->
		<section class="lg:col-span-2 space-y-4">
			<div class="flex items-center justify-between px-2">
				<h2 class="text-lg font-black text-slate-900 dark:text-white italic uppercase tracking-widest flex items-center gap-2">
					<Layout class="w-5 h-5 text-indigo-500" /> Control Deck
				</h2>
			</div>
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each quickLinks as link}
					<a href={link.href} class="p-6 bg-white dark:bg-slate-800/40 backdrop-blur-xl border border-slate-200 dark:border-slate-700/50 rounded-[2rem] shadow-sm hover:shadow-md hover:border-indigo-500/30 transition-all group overflow-hidden flex items-start gap-4">
						<div class="p-4 bg-slate-50 dark:bg-slate-700/50 rounded-2xl group-hover:bg-indigo-500/10 group-hover:scale-110 transition-all duration-300">
							<link.icon class="w-6 h-6 text-slate-600 dark:text-slate-400 group-hover:text-indigo-500 transition-colors" />
						</div>
						<div class="flex-1">
							<div class="flex items-center justify-between mb-1">
								<h3 class="font-black text-slate-900 dark:text-white italic uppercase text-sm tracking-tight">{link.label}</h3>
								<Target class="w-3 h-3 text-slate-300 dark:text-slate-600 opacity-0 group-hover:opacity-100 transition-opacity" />
							</div>
							<p class="text-slate-500 dark:text-slate-400 text-xs italic tracking-tight">{link.desc}</p>
						</div>
					</a>
				{/each}
			</div>
		</section>

		<section class="space-y-4">
			<div class="flex items-center justify-between px-2">
				<h2 class="text-lg font-black text-slate-900 dark:text-white italic uppercase tracking-widest flex items-center gap-2">
					<Activity class="w-5 h-5 text-rose-500" /> Event Stream
				</h2>
				<span class={`text-[10px] px-2 py-1 rounded-full border font-black uppercase tracking-widest ${eventStreamConnected ? 'text-emerald-600 border-emerald-200 bg-emerald-50 dark:text-emerald-400 dark:border-emerald-800 dark:bg-emerald-950/40' : 'text-red-600 border-red-200 bg-red-50 dark:text-red-400 dark:border-red-800 dark:bg-red-950/40'}`}>
					{eventStreamConnected ? 'live' : 'offline'}
				</span>
			</div>
			<div class="bg-slate-900 rounded-[2rem] border border-slate-800 p-6 shadow-2xl min-h-[400px]">
				<div class="space-y-4">
					{#if events.length === 0}
						<p class="text-[11px] text-slate-500 italic">No live events yet. Trigger any AWS API call to populate this stream.</p>
					{:else}
						{#each events as event}
							<div class="flex gap-4 group/item">
								<div class="mt-1 w-1.5 h-1.5 rounded-full bg-indigo-400 shadow-[0_0_8px_currentColor]"></div>
								<div class="flex-1 min-w-0">
									<div class="flex justify-between items-center mb-0.5">
										<span class="text-[9px] font-black text-slate-600 uppercase tracking-widest">{guessService(event)}</span>
										<span class="text-[9px] font-mono text-slate-700">{formatTime(event)}</span>
									</div>
									<p class="text-[11px] font-bold text-slate-300 italic truncate group-hover/item:text-white transition-colors">
										{event.method} {event.path} ({event.status})
									</p>
								</div>
							</div>
						{/each}
					{/if}
				</div>
				<div class="mt-8 pt-6 border-t border-slate-800">
					<div class="flex items-center justify-between mb-4">
						<span class="text-[10px] font-black text-slate-500 uppercase tracking-widest italic">Port Allocation</span>
						<span class="text-[10px] font-mono text-indigo-400 italic font-black">{portRangeStart}-{portRangeEnd}</span>
					</div>
					<div class="grid grid-cols-10 gap-1.5">
						{#each portBuckets as bucket}
							<button
								type="button"
								class={`aspect-square rounded-[2px] transition-all ${bucket.entries.length > 0 ? 'bg-indigo-500 shadow-[0_0_8px_rgba(99,102,241,0.5)] hover:scale-110' : 'bg-slate-800'} ${bucket.entries.length > 0 ? 'cursor-pointer' : 'cursor-default'}`}
								onmouseenter={() => {
									hoveredBucket = bucket.entries.length > 0 ? bucket : null;
								}}
								onmouseleave={() => {
									hoveredBucket = null;
								}}
								aria-label={bucket.entries.length > 0 ? `${bucket.entries.length} allocations in ${bucket.start}-${bucket.end}` : `No allocations in ${bucket.start}-${bucket.end}`}
							></button>
						{/each}
					</div>
					<div class="mt-4 min-h-8">
						{#if hoveredBucket && hoveredBucket.entries.length > 0}
							<p class="text-[10px] text-indigo-300 font-mono">
								{#if hoveredBucket.entries.length === 1}
									{formatPortLabel(hoveredBucket.entries[0].label)} on port {hoveredBucket.entries[0].port}
								{:else}
									{hoveredBucket.entries.length} services allocated in this range: {hoveredBucket.entries[0].port} ({formatPortLabel(hoveredBucket.entries[0].label)})
								{/if}
							</p>
						{:else if allocatedPorts.length === 0}
							<p class="text-[10px] text-slate-500 italic">No allocated ports currently in use.</p>
						{:else}
							<p class="text-[10px] text-slate-500 italic">Hover an active block to see service and port.</p>
						{/if}
					</div>
				</div>
			</div>
		</section>

	</div>
</div>

<style>
	:global(body) {
		background-attachment: fixed;
	}
</style>

