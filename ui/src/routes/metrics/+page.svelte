<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { dashboardClient } from '$lib/api/connect-client';
	import type { DashboardMetrics } from '$lib/api/gopherstack/dashboard/v1/dashboard_pb';
	import { toast } from 'svelte-sonner';

	let metrics = $state<DashboardMetrics | null>(null);
	let isConnected = $state(false);
	let abortController = new AbortController();

	async function startMetricsStream() {
		try {
			const stream = await dashboardClient.streamMetrics({}, { signal: abortController.signal });
			isConnected = true;
			for await (const response of stream) {
				if (response.dashboard) {
					metrics = response.dashboard;
				}
			}
		} catch (err: unknown) {
			const e = err as Error;
			if (e.name !== 'AbortError') {
				isConnected = false;
				toast.error(`Metrics stream disconnected: ${e.message}`);
			}
		}
	}

	onMount(() => {
		startMetricsStream();
	});

	onDestroy(() => {
		abortController.abort();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex justify-between items-center bg-white/40 dark:bg-zinc-900/40 p-4 rounded-xl shadow-lg border border-white/60 dark:border-zinc-800/60 backdrop-blur-md">
		<h1 class="text-2xl font-bold tracking-tight text-zinc-900 dark:text-zinc-100">Performance Metrics</h1>
		<div class="flex items-center gap-2">
			<span class="relative flex h-3 w-3">
				{#if isConnected}
					<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
					<span class="relative inline-flex rounded-full h-3 w-3 bg-green-500"></span>
				{:else}
					<span class="relative inline-flex rounded-full h-3 w-3 bg-red-500"></span>
				{/if}
			</span>
			<span class="text-sm font-medium text-zinc-600 dark:text-zinc-300">
				{isConnected ? 'Live' : 'Disconnected'}
			</span>
		</div>
	</div>

	{#if !metrics}
		<div class="flex items-center justify-center p-12">
			<p class="text-zinc-500">Waiting for metrics...</p>
		</div>
	{:else}
		<!-- Runtime Metrics Summary -->
		<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
			<div class="p-4 bg-white/50 dark:bg-zinc-900/50 backdrop-blur rounded-lg border border-white/20 dark:border-zinc-800/50 shadow">
				<p class="text-xs font-semibold text-zinc-500 uppercase">Goroutines</p>
				<p class="text-2xl font-bold text-zinc-900 dark:text-zinc-100">{metrics.runtime?.goroutines || 0}</p>
			</div>
			<div class="p-4 bg-white/50 dark:bg-zinc-900/50 backdrop-blur rounded-lg border border-white/20 dark:border-zinc-800/50 shadow">
				<p class="text-xs font-semibold text-zinc-500 uppercase">Heap In Use</p>
				<p class="text-2xl font-bold text-zinc-900 dark:text-zinc-100">{metrics.runtime?.heapInuseMb.toFixed(1) || 0} MB</p>
			</div>
			<div class="p-4 bg-white/50 dark:bg-zinc-900/50 backdrop-blur rounded-lg border border-white/20 dark:border-zinc-800/50 shadow">
				<p class="text-xs font-semibold text-zinc-500 uppercase">Services Running</p>
				<p class="text-2xl font-bold text-zinc-900 dark:text-zinc-100">{metrics.runtime?.numServices || 0}</p>
			</div>
			<div class="p-4 bg-white/50 dark:bg-zinc-900/50 backdrop-blur rounded-lg border border-white/20 dark:border-zinc-800/50 shadow">
				<p class="text-xs font-semibold text-zinc-500 uppercase">Total Alloc</p>
				<p class="text-2xl font-bold text-zinc-900 dark:text-zinc-100">{metrics.runtime?.totalAllocMb.toFixed(1) || 0} MB</p>
			</div>
		</div>

		<!-- API Operations Metrics -->
		<div class="p-5 bg-white/60 dark:bg-zinc-900/60 backdrop-blur-lg rounded-xl border border-white/40 dark:border-zinc-800 shadow-md">
			<h2 class="text-lg font-bold mb-4">Operations Summary</h2>
			{#if (!metrics.operations || metrics.operations.length === 0)}
				<p class="text-sm text-zinc-500">No operation metrics recorded.</p>
			{:else}
				<div class="overflow-x-auto">
					<table class="min-w-full divide-y divide-zinc-200 dark:divide-zinc-800">
						<thead>
							<tr>
								<th class="px-4 py-2 text-left text-xs font-medium text-zinc-500 uppercase">Operation</th>
								<th class="px-4 py-2 text-right text-xs font-medium text-zinc-500 uppercase">Count</th>
								<th class="px-4 py-2 text-right text-xs font-medium text-zinc-500 uppercase">Errors</th>
								<th class="px-4 py-2 text-right text-xs font-medium text-zinc-500 uppercase">Avg (ms)</th>
								<th class="px-4 py-2 text-right text-xs font-medium text-zinc-500 uppercase">P95 (ms)</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-zinc-200 dark:divide-zinc-800">
							{#each metrics.operations as op}
								<tr>
									<td class="px-4 py-3 whitespace-nowrap text-sm font-medium text-zinc-900 dark:text-zinc-100">{op.operation}</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm text-right text-zinc-600 dark:text-zinc-400">{op.count}</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm text-right text-zinc-600 dark:text-zinc-400">
										{#if op.errorCount > 0}
											<span class="text-red-500 font-bold">{op.errorCount}</span>
										{:else}
											0
										{/if}
									</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm text-right text-zinc-600 dark:text-zinc-400">{op.avgMs.toFixed(2)}</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm text-right text-zinc-600 dark:text-zinc-400">{op.p95Ms.toFixed(2)}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>

		<!-- Background Workers Metrics -->
		<div class="p-5 bg-white/60 dark:bg-zinc-900/60 backdrop-blur-lg rounded-xl border border-white/40 dark:border-zinc-800 shadow-md">
			<h2 class="text-lg font-bold mb-4">Background Workers</h2>
			{#if (!metrics.workers || metrics.workers.length === 0)}
				<p class="text-sm text-zinc-500">No active background workers.</p>
			{:else}
				<div class="overflow-x-auto">
					<table class="min-w-full divide-y divide-zinc-200 dark:divide-zinc-800">
						<thead>
							<tr>
								<th class="px-4 py-2 text-left text-xs font-medium text-zinc-500 uppercase">Service</th>
								<th class="px-4 py-2 text-left text-xs font-medium text-zinc-500 uppercase">Worker</th>
								<th class="px-4 py-2 text-right text-xs font-medium text-zinc-500 uppercase">Queue Depth</th>
								<th class="px-4 py-2 text-right text-xs font-medium text-zinc-500 uppercase">Tasks Total</th>
								<th class="px-4 py-2 text-right text-xs font-medium text-zinc-500 uppercase">Errors</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-zinc-200 dark:divide-zinc-800">
							{#each metrics.workers as worker}
								<tr>
									<td class="px-4 py-3 whitespace-nowrap text-sm font-medium text-zinc-900 dark:text-zinc-100">{worker.service}</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm font-medium text-zinc-600 dark:text-zinc-300">{worker.worker}</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm text-right text-zinc-600 dark:text-zinc-400">{worker.queueDepth}</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm text-right text-zinc-600 dark:text-zinc-400">{worker.tasksTotal}</td>
									<td class="px-4 py-3 whitespace-nowrap text-sm text-right text-zinc-600 dark:text-zinc-400">
										{#if worker.errorsTotal > 0}
											<span class="text-red-500 font-bold">{worker.errorsTotal}</span>
										{:else}
											0
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>
