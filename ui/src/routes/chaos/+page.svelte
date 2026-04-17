<script lang="ts">
	import { onMount } from 'svelte';
	import { Zap, AlertCircle, Activity, Settings, Gauge, TrendingUp, Lock, Unlock, RefreshCw } from 'lucide-svelte';

	let faults = $state<any[]>([]);
	let networkEffects = $state({ latency: 0, jitter: 0 });
	let activity = $state<any[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	// Form state
	let service = $state('s3');
	let probability = $state(1.0);
	let errorCode = $state('ServiceUnavailable');
	let statusCode = $state(503);
	let showAddForm = $state(false);
	let latencyInput = $state(0);
	let jitterInput = $state(0);

	onMount(async () => {
		await loadChaosState();
	});

	async function loadChaosState() {
		try {
			loading = true;
			const response = await fetch('/_gopherstack/chaos/query');
			if (!response.ok) throw new Error('Failed to load chaos state');
			const data = await response.json();
			faults = data.faults || [];
			networkEffects = data.effects || { latency: 0, jitter: 0 };
			activity = data.activity || [];
			latencyInput = networkEffects.latency || 0;
			jitterInput = networkEffects.jitter || 0;
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		} finally {
			loading = false;
		}
	}

	async function addFault() {
		try {
			const response = await fetch('/_gopherstack/chaos/faults', {
				method: 'PATCH',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify([{
					service,
					probability: parseFloat(String(probability)),
					error: { statusCode: parseInt(String(statusCode)), code: errorCode }
				}])
			});
			if (!response.ok) throw new Error('Failed to add fault');
			showAddForm = false;
			await loadChaosState();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function deleteFault(index: number) {
		try {
			const response = await fetch('/_gopherstack/chaos/faults/by-index', {
				method: 'DELETE',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ index })
			});
			if (!response.ok) throw new Error('Failed to delete fault');
			await loadChaosState();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function clearFaults() {
		if (!confirm('Clear all faults?')) return;
		try {
			const response = await fetch('/_gopherstack/chaos/faults/clear', { method: 'POST' });
			if (!response.ok) throw new Error('Failed to clear faults');
			await loadChaosState();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function applyNetworkEffects() {
		try {
			const response = await fetch('/_gopherstack/chaos/effects', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ latency: latencyInput, jitter: jitterInput })
			});
			if (!response.ok) throw new Error('Failed to apply network effects');
			await loadChaosState();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}

	async function resetNetworkEffects() {
		try {
			const response = await fetch('/_gopherstack/chaos/effects/reset', { method: 'POST' });
			if (!response.ok) throw new Error('Failed to reset network effects');
			await loadChaosState();
		} catch (e) {
			error = e instanceof Error ? e.message : 'Unknown error';
		}
	}
</script>

<div class="space-y-6">
	<!-- Header -->
	<div>
		<div class="flex items-center gap-3 mb-2">
			<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
				<Zap class="w-6 h-6 text-amber-600 dark:text-amber-400" />
			</div>
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Chaos Engineering</h1>
		</div>
		<p class="text-slate-600 dark:text-slate-300">Inject faults and network effects to test resilience</p>
	</div>

	<!-- Error Alert -->
	{#if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg flex gap-3">
			<AlertCircle class="w-5 h-5 text-red-600 dark:text-red-400 flex-shrink-0 mt-0.5" />
			<div class="text-sm text-red-700 dark:text-red-300">{error}</div>
		</div>
	{/if}

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Fault Rules Section -->
		<div class="lg:col-span-2 space-y-4">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
				<div class="flex items-center justify-between mb-4">
					<h2 class="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2">
						<Lock class="w-5 h-5 text-red-500" />
						Fault Rules
					</h2>
					<div class="flex gap-2">
						{#if faults.length > 0}
							<button onclick={clearFaults} class="px-3 py-1.5 text-sm font-medium text-red-700 dark:text-red-300 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg hover:bg-red-100 dark:hover:bg-red-900/30 transition-colors">
								Clear All
							</button>
						{/if}
						<button onclick={() => showAddForm = !showAddForm} class="px-3 py-1.5 text-sm font-medium text-white bg-indigo-600 rounded-lg hover:bg-indigo-700 transition-colors">
							Add Fault
						</button>
					</div>
				</div>

				{#if showAddForm}
					<div class="p-4 bg-slate-50 dark:bg-slate-700/50 rounded-lg border border-slate-200 dark:border-slate-600 mb-4 space-y-3">
						<div>
							<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Service</label>
							<input type="text" bind:value={service} class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-800 dark:text-white text-sm" placeholder="s3, dynamodb, lambda, etc" />
						</div>
						<div class="grid grid-cols-2 gap-3">
							<div>
								<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Probability (0-1)</label>
								<input type="number" bind:value={probability} min="0" max="1" step="0.1" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-800 dark:text-white text-sm" />
							</div>
							<div>
								<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Status Code</label>
								<input type="number" bind:value={statusCode} class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-800 dark:text-white text-sm" />
							</div>
						</div>
						<div>
							<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Error Code</label>
							<input type="text" bind:value={errorCode} class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg dark:bg-slate-800 dark:text-white text-sm" placeholder="ServiceUnavailable" />
						</div>
						<div class="flex gap-2">
							<button onclick={addFault} class="flex-1 px-4 py-2 bg-indigo-600 text-white rounded-lg font-medium text-sm hover:bg-indigo-700 transition-colors">
								Add
							</button>
							<button onclick={() => showAddForm = false} class="flex-1 px-4 py-2 bg-slate-200 dark:bg-slate-700 text-slate-700 dark:text-slate-200 rounded-lg font-medium text-sm hover:bg-slate-300 dark:hover:bg-slate-600 transition-colors">
								Cancel
							</button>
						</div>
					</div>
				{/if}

				{#if faults.length === 0}
					<div class="text-center py-8 text-slate-500 dark:text-slate-400">
						<AlertCircle class="w-8 h-8 mx-auto mb-2 opacity-50" />
						<p>No fault rules active</p>
					</div>
				{:else}
					<div class="space-y-2">
						{#each faults as fault, idx}
							<div class="p-3 bg-slate-50 dark:bg-slate-700 rounded-lg flex items-start justify-between">
								<div class="text-sm">
									<p class="font-medium text-slate-900 dark:text-white">{fault.service}</p>
									<p class="text-slate-600 dark:text-slate-400">{fault.probability * 100}% probability → {fault.statusCode}</p>
									<p class="text-xs text-slate-500 dark:text-slate-500">{fault.errorCode}</p>
								</div>
								<button onclick={() => deleteFault(idx)} class="text-red-600 dark:text-red-400 hover:text-red-700 dark:hover:text-red-300 transition-colors">
									✕
								</button>
							</div>
						{/each}
					</div>
				{/if}
			</div>

			<!-- Activity Log -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2 mb-4">
					<Activity class="w-5 h-5 text-blue-500" />
					Activity Log
				</h2>
				{#if activity.length === 0}
					<div class="text-center py-8 text-slate-500 dark:text-slate-400">
						<Activity class="w-8 h-8 mx-auto mb-2 opacity-50" />
						<p>No activity recorded</p>
					</div>
				{:else}
					<div class="space-y-2 max-h-96 overflow-y-auto">
						{#each activity.slice(0, 50) as event}
							<div class="p-2 text-xs bg-slate-50 dark:bg-slate-700 rounded flex justify-between">
								<span class="text-slate-700 dark:text-slate-300">{event.timestamp} - {event.service}.{event.operation} {event.triggered ? '🔴 Triggered' : '✓ Passed'}</span>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		</div>

		<!-- Network Effects Section -->
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
			<h2 class="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2 mb-4">
				<Gauge class="w-5 h-5 text-purple-500" />
				Network Effects
			</h2>

			<div class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">
						Latency: <span class="text-indigo-600 dark:text-indigo-400 font-semibold">{latencyInput}ms</span>
					</label>
					<input type="range" bind:value={latencyInput} min="0" max="10000" step="100" class="w-full" />
				</div>

				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-2">
						Jitter: <span class="text-indigo-600 dark:text-indigo-400 font-semibold">{jitterInput}ms</span>
					</label>
					<input type="range" bind:value={jitterInput} min="0" max="5000" step="100" class="w-full" />
				</div>

				<div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-900/50 rounded-lg p-3 text-xs text-blue-700 dark:text-blue-300">
					<p class="font-semibold mb-1">Current Settings:</p>
					<p>Latency: {networkEffects.latency}ms</p>
					<p>Jitter: {networkEffects.jitter}ms</p>
				</div>

				<div class="flex gap-2">
					<button onclick={applyNetworkEffects} class="flex-1 px-4 py-2 bg-indigo-600 text-white rounded-lg font-medium text-sm hover:bg-indigo-700 transition-colors flex items-center justify-center gap-2">
						<TrendingUp class="w-4 h-4" />
						Apply
					</button>
					<button onclick={resetNetworkEffects} class="flex-1 px-4 py-2 bg-slate-200 dark:bg-slate-700 text-slate-700 dark:text-slate-200 rounded-lg font-medium text-sm hover:bg-slate-300 dark:hover:bg-slate-600 transition-colors flex items-center justify-center gap-2">
						<RefreshCw class="w-4 h-4" />
						Reset
					</button>
				</div>
			</div>

			<!-- Stats -->
			<div class="mt-6 pt-6 border-t border-slate-200 dark:border-slate-700">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-3">Statistics</h3>
				<div class="space-y-2 text-sm">
					<div class="flex justify-between">
						<span class="text-slate-600 dark:text-slate-400">Active Faults:</span>
						<span class="font-semibold text-slate-900 dark:text-white">{faults.length}</span>
					</div>
					<div class="flex justify-between">
						<span class="text-slate-600 dark:text-slate-400">Activity Events:</span>
						<span class="font-semibold text-slate-900 dark:text-white">{activity.length}</span>
					</div>
				</div>
			</div>
		</div>
	</div>
</div>

<style>
	input[type='range'] {
		accent-color: rgb(79, 70, 229);
	}
</style>
