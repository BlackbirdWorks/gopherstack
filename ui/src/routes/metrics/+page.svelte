<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { dashboardClient } from '$lib/api/connect-client';
	import type { DashboardMetrics } from '$lib/api/gopherstack/dashboard/v1/dashboard_pb';
	import { toast } from 'svelte-sonner';

	let metrics = $state<DashboardMetrics | null>(null);
	let isConnected = $state(false);
	let abortController = new AbortController();
	let destroying = false;

	const allServices = ['DynamoDB', 'S3', 'SSM', 'IAM', 'STS', 'SNS', 'SQS', 'KMS', 'SecretsManager', 'ECR', 'AppSync'];
	let enabledServices = $state<Set<string>>(new Set(allServices));
	let showFilterDropdown = $state(false);
	let sparklineBars = $state<number[]>(Array.from({ length: 20 }, () => 0));

	function rankOperation(n: string): number {
		if (n.startsWith('DynamoDB::')) return 1;
		if (n.startsWith('S3::')) return 2;
		if (n.startsWith('SSM::')) return 3;
		return 4;
	}

	const filteredOperations = $derived(
		(metrics?.operations ?? []).filter((op) => {
			const svc = op.operation.split('::')[0];
			return enabledServices.has(svc);
		}).toSorted((a, b) => {
			const diff = rankOperation(a.operation) - rankOperation(b.operation);
			return diff === 0 ? Number(b.count) - Number(a.count) : diff;
		})
	);

	const allEnabled = $derived(enabledServices.size === allServices.length);

	function toggleService(svc: string) {
		const next = new Set(enabledServices);
		if (next.has(svc)) next.delete(svc); else next.add(svc);
		enabledServices = next;
	}

	function toggleAll(checked: boolean) {
		enabledServices = checked ? new Set(allServices) : new Set();
	}

	function svcColor(service: string): string {
		switch (service) {
			case 'DynamoDB': return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300';
			case 'S3': return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300';
			case 'SSM': return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-300';
			default: return 'bg-slate-100 text-slate-800 dark:bg-slate-700 dark:text-slate-300';
		}
	}

	function workerSvcColor(svc: string): string {
		const upper = svc.toUpperCase();
		if (upper === 'DYNAMODB') return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300';
		if (upper === 'S3') return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300';
		return 'bg-slate-100 text-slate-800 dark:bg-slate-700 dark:text-slate-300';
	}

	function queueDepthColor(depth: number): string {
		if (depth > 10) return 'text-orange-600 dark:text-orange-400';
		if (depth > 0) return 'text-yellow-600 dark:text-yellow-400';
		return 'text-green-600 dark:text-green-400';
	}

	function parseOp(operation: string): { service: string; action: string } {
		const parts = operation.split('::');
		return parts.length > 1 ? { service: parts[0], action: parts[1] } : { service: 'SDK', action: parts[0] };
	}

	async function startMetricsStream() {
		try {
			const stream = await dashboardClient.streamMetrics({}, { signal: abortController.signal });
			isConnected = true;
			for await (const response of stream) {
				if (response.dashboard) {
					const dashboard = response.dashboard;
					metrics = dashboard;
					const totalOps = (dashboard.operations ?? []).reduce((sum, op) => sum + Number(op.count), 0);
					sparklineBars = [...sparklineBars.slice(1), totalOps];
				}
			}
		} catch (err: unknown) {
			const e = err as Error;
			if (e.name !== 'AbortError' && !destroying) {
				isConnected = false;
				toast.error(`Metrics stream disconnected: ${e.message}`);
			}
		}
	}

	onMount(() => { startMetricsStream(); });
	onDestroy(() => { destroying = true; abortController.abort(); });
</script>

<div class="container mx-auto space-y-8">
	<div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 border-b border-slate-200 dark:border-slate-700 pb-6">
		<div>
			<h1 class="text-4xl font-extrabold tracking-tight text-blue-600 dark:text-blue-400">Performance Metrics</h1>
			<p class="text-slate-500 dark:text-slate-400 mt-2">Real-time health and performance statistics for Gopherstack.</p>
		</div>
		<div class="flex items-center gap-3">
			<div class="hidden sm:flex items-end h-9 gap-[3px] px-3 py-1.5 bg-white dark:bg-slate-900 rounded-lg border border-slate-200 dark:border-white/5 shadow-inner" title="Live Traffic Heatmap">
				{#each sparklineBars as bar}
					{@const maxBar = Math.max(...sparklineBars, 1)}
					{@const height = Math.max(4, (bar / maxBar) * 24)}
					{@const opacity = 0.3 + (bar / maxBar) * 0.7}
					<div class="w-1.5 rounded-sm bg-blue-500 transition-all duration-300" style="height: {height}px; opacity: {opacity}"></div>
				{/each}
			</div>
			<div class={`flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-bold border ${isConnected ? 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-400 border-green-200 dark:border-green-800' : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400 border-red-200 dark:border-red-800'}`}>
				<span class="relative flex h-2 w-2">
					{#if isConnected}
						<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
						<span class="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
					{:else}
						<span class="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
					{/if}
				</span>
				{isConnected ? 'LIVE' : 'DISCONNECTED'}
			</div>
		</div>
	</div>

	{#if !metrics}
		<div class="flex flex-col items-center justify-center py-24 opacity-60">
			<svg class="w-12 h-12 text-slate-200 animate-spin dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none">
				<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
				<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
			</svg>
			<p class="mt-4 font-medium text-slate-600 dark:text-slate-300">Gathering insights...</p>
		</div>
	{:else}
		{#if metrics.deadlocks && metrics.deadlocks.length > 0}
			<section>
				<div class="bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-800 rounded-2xl shadow-lg overflow-hidden">
					<div class="flex items-center gap-4 text-red-700 dark:text-red-400 p-6">
						<svg class="stroke-current shrink-0 h-6 w-6" fill="none" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" /></svg>
						<div>
							<h3 class="font-bold text-lg">Potential Deadlocks Detected!</h3>
							<p class="text-sm opacity-90">One or more locks have been held for an unusually long time while other goroutines are waiting.</p>
						</div>
					</div>
					<div class="px-6 pb-6">
						<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-3">
							{#each metrics.deadlocks as d}
								<div class="bg-red-100 dark:bg-red-900/40 rounded-xl p-4 border border-red-200 dark:border-red-800 shadow-sm">
									<div class="flex justify-between items-start mb-2">
										<span class="font-mono text-xs font-bold text-red-800 dark:text-red-300">{d.lock}</span>
										<span class="bg-red-200 text-red-900 text-[10px] font-bold px-1.5 py-0.5 rounded dark:bg-red-800 dark:text-red-100">HELD</span>
									</div>
									<div class="text-sm font-bold truncate mb-1 text-red-950 dark:text-red-200">Op: {d.operation}</div>
									<div class="flex justify-between text-xs font-medium text-red-700 dark:text-red-400">
										<span>Held: {d.heldSec.toFixed(1)}s</span>
										<span class="font-bold uppercase tracking-wider">{d.waiters} waiting</span>
									</div>
								</div>
							{/each}
						</div>
					</div>
				</div>
			</section>
		{/if}

		<section>
			<h2 class="text-xl font-bold mb-4 flex items-center gap-2 text-slate-900 dark:text-white">
				<svg class="h-5 w-5 text-purple-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
				Go Runtime
			</h2>
			<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Goroutines</div>
					<div class="text-3xl font-bold py-1 text-blue-600 dark:text-blue-400">{metrics.runtime?.goroutines ?? 0}</div>
					<div class="text-xs text-slate-500 dark:text-slate-400">Active routines</div>
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Heap Memory</div>
					<div class="text-3xl font-bold py-1 text-purple-600 dark:text-purple-400">{(metrics.runtime?.heapAllocMb ?? 0).toFixed(2)} MB</div>
					<div class="text-xs text-slate-500 dark:text-slate-400">{((metrics.runtime?.heapAllocMb ?? 0) / (metrics.runtime?.heapSysMb || 1) * 100).toFixed(1)}% of {(metrics.runtime?.heapSysMb ?? 0).toFixed(2)} MB total</div>
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<div class="text-sm font-medium text-slate-500 dark:text-slate-400">GC Cycles</div>
					<div class="text-3xl font-bold py-1 text-yellow-600 dark:text-yellow-400">{metrics.runtime?.numGc ?? 0}</div>
					<div class="text-xs text-slate-500 dark:text-slate-400">Last pause: {(metrics.runtime?.lastGcPauseMs ?? 0).toFixed(2)} ms</div>
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Total Allocated</div>
					<div class="text-3xl font-bold py-1 text-slate-600 dark:text-slate-300">{(metrics.runtime?.totalAllocMb ?? 0).toFixed(2)} MB</div>
					<div class="text-xs text-slate-500 dark:text-slate-400">Lifetime total</div>
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Heap In-Use</div>
					<div class="text-3xl font-bold py-1 text-teal-600 dark:text-teal-400">{(metrics.runtime?.heapInuseMb ?? 0).toFixed(2)} MB</div>
					<div class="text-xs text-slate-500 dark:text-slate-400">Actively referenced spans</div>
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Services</div>
					<div class="text-3xl font-bold py-1 text-indigo-600 dark:text-indigo-400">{metrics.runtime?.numServices ?? 0}</div>
					<div class="text-xs text-slate-500 dark:text-slate-400">Registered AWS emulators</div>
				</div>
			</div>
		</section>

		<section>
			<div class="flex flex-col md:flex-row justify-between items-start md:items-end mb-6 gap-4">
				<div class="flex items-center gap-2">
					<h2 class="text-xl font-bold flex items-center gap-2 text-slate-900 dark:text-white">
						<svg class="h-5 w-5 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>
						Operation Latencies
					</h2>
					<span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-blue-900 dark:text-blue-300 font-mono">{filteredOperations.length} operations tracked</span>
				</div>
				<div class="relative">
					<button onclick={() => { showFilterDropdown = !showFilterDropdown; }} class="inline-flex items-center px-4 py-2 text-sm font-medium text-slate-900 bg-white border border-slate-300 rounded-lg hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:bg-slate-700">
						Filter Services
						{#if enabledServices.size > 0 && enabledServices.size < allServices.length}
							<span class="ml-2 bg-blue-100 text-blue-800 text-xs font-semibold px-2 py-0.5 rounded dark:bg-blue-200 dark:text-blue-800">{enabledServices.size}</span>
						{/if}
						<svg class="w-2.5 h-2.5 ml-2.5" fill="none" viewBox="0 0 10 6"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 4 4 4-4" /></svg>
					</button>
					{#if showFilterDropdown}
						<div class="absolute right-0 z-10 mt-1 bg-white rounded-lg shadow-lg w-60 dark:bg-slate-700 border border-slate-200 dark:border-slate-600">
							<div class="p-3">
								<button type="button" onclick={() => toggleAll(!allEnabled)} class="inline-flex items-center cursor-pointer mb-2" role="switch" aria-checked={allEnabled}>
									<div class="relative shrink-0 w-9 h-5 rounded-full transition-colors {allEnabled ? 'bg-blue-600' : 'bg-slate-200 dark:bg-slate-600'}">
										<div class="absolute top-[2px] left-[2px] bg-white border border-slate-300 rounded-full h-4 w-4 transition-transform {allEnabled ? 'translate-x-4 border-white' : ''}"></div>
									</div>
									<span class="ml-3 text-sm font-medium text-slate-900 dark:text-slate-300">All Services</span>
								</button>
							</div>
							<ul class="h-48 px-3 pb-3 overflow-y-auto text-sm text-slate-700 dark:text-slate-200">
								{#each allServices as svc}
									<li>
										<div class="flex items-center p-2 rounded hover:bg-slate-100 dark:hover:bg-slate-600">
											<input type="checkbox" checked={enabledServices.has(svc)} onchange={() => toggleService(svc)} class="w-4 h-4 text-blue-600 bg-slate-100 border-slate-300 rounded focus:ring-blue-500 dark:bg-slate-600 dark:border-slate-500" />
											<span class="w-full ml-2 text-sm font-medium text-slate-900 dark:text-slate-300">{svc}</span>
										</div>
									</li>
								{/each}
							</ul>
						</div>
					{/if}
				</div>
			</div>
			<div class="relative overflow-x-auto shadow-sm sm:rounded-xl border border-slate-200 dark:border-slate-700 bg-white/50 dark:bg-slate-800/50 backdrop-blur-sm">
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50/50 dark:bg-slate-800/50 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Operation</th>
							<th class="px-6 py-3 text-center">Requests</th>
							<th class="px-6 py-3 text-right">P50</th>
							<th class="px-6 py-3 text-right">P95</th>
							<th class="px-6 py-3 text-right">P99</th>
							<th class="px-6 py-3 text-right">Avg</th>
							<th class="px-6 py-3 text-right">Max</th>
						</tr>
					</thead>
					<tbody>
						{#if filteredOperations.length === 0}
							<tr><td colspan="7" class="text-center py-8 text-slate-400 italic">No operations recorded yet</td></tr>
						{:else}
							{#each filteredOperations as op}
								{@const parsed = parseOp(op.operation)}
								<tr class="bg-white border-b border-slate-200 dark:border-slate-700 dark:bg-slate-800/50 hover:bg-slate-50/80 dark:hover:bg-slate-700/50 transition-colors">
									<td class="px-6 py-4 font-medium"><div class="flex items-center gap-2"><span class="text-[10px] font-bold px-1.5 py-0.5 rounded {svcColor(parsed.service)} uppercase">{parsed.service}</span><code class="text-blue-600 dark:text-blue-400 font-bold text-xs">{parsed.action}</code></div></td>
									<td class="px-6 py-4 text-center"><div><span class="font-bold text-slate-900 dark:text-white">{op.count}</span></div><div class="text-[10px] text-slate-400 uppercase tracking-tighter">requests</div></td>
									<td class="px-6 py-4 text-right font-mono text-blue-600 dark:text-blue-500 font-semibold">{op.p50Ms.toFixed(2)}<span class="text-xs opacity-40 ml-0.5">ms</span></td>
									<td class="px-6 py-4 text-right font-mono text-purple-600 dark:text-purple-500 font-semibold">{op.p95Ms.toFixed(2)}<span class="text-xs opacity-40 ml-0.5">ms</span></td>
									<td class="px-6 py-4 text-right font-mono text-yellow-600 dark:text-yellow-500 font-semibold">{op.p99Ms.toFixed(2)}<span class="text-xs opacity-40 ml-0.5">ms</span></td>
									<td class="px-6 py-4 text-right font-mono text-slate-600 dark:text-slate-400">{op.avgMs.toFixed(2)}<span class="text-xs opacity-40 ml-0.5">ms</span></td>
									<td class="px-6 py-4 text-right font-mono text-slate-900 dark:text-white font-bold">{op.maxMs.toFixed(2)}<span class="text-xs opacity-40 ml-0.5">ms</span></td>
								</tr>
							{/each}
						{/if}
					</tbody>
				</table>
			</div>
		</section>

		<section>
			<h2 class="text-xl font-bold mb-4 flex items-center gap-2 text-slate-900 dark:text-white">
				<svg class="h-5 w-5 text-orange-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" /></svg>
				Service Workers
			</h2>
			{#if !metrics.workers || metrics.workers.length === 0}
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl text-center text-slate-500">No active background workers detected.</div>
			{:else}
				<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
					{#each metrics.workers as w}
						<div class="p-5 bg-white border border-slate-200 rounded-xl shadow dark:bg-slate-800 dark:border-slate-700">
							<div class="flex justify-between items-start mb-4">
								<div class="flex flex-col gap-1">
									<span class="text-[10px] font-bold px-1.5 py-0.5 rounded {workerSvcColor(w.service)} uppercase w-fit">{w.service.toUpperCase()}</span>
									<h4 class="text-sm font-bold text-slate-900 dark:text-white truncate">{w.worker}</h4>
								</div>
								<div class="flex flex-col items-end">
									<span class="text-2xl font-black {queueDepthColor(Number(w.queueDepth))}">{w.queueDepth}</span>
									<span class="text-[9px] uppercase tracking-tighter text-slate-400">pending</span>
								</div>
							</div>
							<div class="space-y-3 pt-3 border-t border-slate-100 dark:border-slate-700">
								<div class="flex justify-between text-xs">
									<span class="text-slate-500 dark:text-slate-400">Tasks Processed</span>
									<span class="font-mono font-bold text-slate-600 dark:text-slate-300">{Number(w.tasksTotal).toLocaleString()}</span>
								</div>
								<div class="flex justify-between text-xs">
									<span class="text-slate-500 dark:text-slate-400">Items Handled</span>
									<span class="font-mono font-bold text-blue-600 dark:text-blue-400">{Number(w.itemsProcessedTotal).toLocaleString()}</span>
								</div>
								{#if Number(w.errorsTotal) > 0}
									<div class="flex justify-between text-xs">
										<span class="text-red-500 font-medium">Worker Errors</span>
										<span class="font-mono font-bold text-red-600 dark:text-red-400">{Number(w.errorsTotal).toLocaleString()}</span>
									</div>
								{/if}
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</section>
	{/if}
</div>
