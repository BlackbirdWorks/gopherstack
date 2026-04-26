<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { toast } from 'svelte-sonner';

	interface HealthData {
		memory: {
			alloc: number;
			totalAlloc: number;
			sys: number;
			numGC: number;
			goroutines: number;
		};
		uptime: string;
	}

	let health = $state<HealthData | null>(null);
	let isConnected = $state(false);
	let interval: ReturnType<typeof setInterval>;

	async function fetchHealth() {
		try {
			const resp = await fetch('/dashboard/api/system/health');
			if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
			health = await resp.json();
			isConnected = true;
		} catch (err: unknown) {
			isConnected = false;
			const errorMessage = err instanceof Error ? err.message : String(err);
			toast.error(`Failed to fetch health: ${errorMessage}`);
		}
	}

	function formatBytes(bytes: number) {
		if (bytes === 0) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
	}

	onMount(() => {
		fetchHealth();
		interval = setInterval(fetchHealth, 5000);
	});

	onDestroy(() => {
		clearInterval(interval);
	});
</script>

<div class="container mx-auto space-y-8">
	<div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 border-b border-slate-200 dark:border-slate-700 pb-6">
		<div>
			<h1 class="text-4xl font-extrabold tracking-tight text-indigo-600 dark:text-indigo-400">Resource Health</h1>
			<p class="text-slate-500 dark:text-slate-400 mt-2">In-memory resource utilization and systemic health tracking.</p>
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
			{isConnected ? 'CONNECTED' : 'DISCONNECTED'}
		</div>
	</div>

	{#if !health}
		<div class="flex flex-col items-center justify-center py-24 opacity-60">
			<svg class="w-12 h-12 text-slate-200 animate-spin dark:text-slate-600 fill-indigo-600" viewBox="0 0 100 101" fill="none">
				<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
				<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
			</svg>
			<p class="mt-4 font-medium text-slate-600 dark:text-slate-300">Gathering telemetry...</p>
		</div>
	{:else}
		{@const pct = (health.memory.alloc / health.memory.sys) * 100}
		<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
			<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-sm rounded-2xl">
				<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Allocated Memory</div>
				<div class="text-3xl font-bold py-2 text-indigo-600 dark:text-indigo-400">{formatBytes(health.memory.alloc)}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400">Current heap allocation</div>
			</div>
			<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-sm rounded-2xl">
				<div class="text-sm font-medium text-slate-500 dark:text-slate-400">System Memory</div>
				<div class="text-3xl font-bold py-2 text-blue-600 dark:text-blue-400">{formatBytes(health.memory.sys)}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400">Total memory from OS</div>
			</div>
			<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-sm rounded-2xl">
				<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Goroutines</div>
				<div class="text-3xl font-bold py-2 text-emerald-600 dark:text-emerald-400">{health.memory.goroutines}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400">Active threads</div>
			</div>
			<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 shadow-sm rounded-2xl">
				<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Uptime</div>
				<div class="text-3xl font-bold py-2 text-amber-600 dark:text-amber-400 truncate">{health.uptime}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400">Since last restart</div>
			</div>
		</div>

		<section class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-2xl overflow-hidden">
			<div class="p-6 border-b border-slate-200 dark:border-slate-700 bg-slate-50/50 dark:bg-slate-900/50">
				<h2 class="text-xl font-bold text-slate-900 dark:text-white flex items-center gap-2">
					<svg class="h-5 w-5 text-indigo-500" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>
					GC Performance
				</h2>
			</div>
			<div class="p-6">
				<div class="grid grid-cols-1 md:grid-cols-2 gap-8">
					<div>
						<div class="text-sm font-medium text-slate-500 dark:text-slate-400 mb-1">Total Collections</div>
						<div class="text-4xl font-black text-slate-900 dark:text-white">{health.memory.numGC}</div>
						<p class="text-sm text-slate-500 mt-2">Number of completed GC cycles.</p>
					</div>
					<div>
						<div class="text-sm font-medium text-slate-500 dark:text-slate-400 mb-1">Memory Pressure</div>
						<div class="w-full bg-slate-100 dark:bg-slate-700 rounded-full h-4 mb-2">
							<div class="bg-indigo-600 h-4 rounded-full transition-all duration-500" style="width: {Math.min(100, pct)}%"></div>
						</div>
						<div class="flex justify-between text-xs font-mono">
							<span>Alloc: {formatBytes(health.memory.alloc)}</span>
							<span>Sys: {formatBytes(health.memory.sys)}</span>
						</div>
					</div>
				</div>
			</div>
		</section>

		<section class="grid grid-cols-1 md:grid-cols-3 gap-6">
			<div class="md:col-span-2 p-6 bg-indigo-50 dark:bg-indigo-900/20 border border-indigo-100 dark:border-indigo-800 rounded-2xl">
				<h3 class="text-lg font-bold text-indigo-900 dark:text-indigo-300 mb-2">Janitor Status</h3>
				<p class="text-sm text-indigo-700 dark:text-indigo-400 mb-4">Background janitors are actively sweeping the following services for leaks:</p>
				<div class="flex flex-wrap gap-2">
					{#each ['SQS', 'ACM', 'Support', 'BedrockRuntime', 'SecretsManager', 'DynamoDB', 'EventBridge', 'KMS'] as svc}
						<span class="px-3 py-1 bg-white dark:bg-indigo-900/40 border border-indigo-200 dark:border-indigo-700 rounded-full text-xs font-bold text-indigo-600 dark:text-indigo-400">
							{svc} Janitor: ACTIVE
						</span>
					{/each}
				</div>
			</div>
			<div class="p-6 bg-slate-50 dark:bg-slate-900/50 border border-slate-200 dark:border-slate-800 rounded-2xl">
				<h3 class="text-lg font-bold text-slate-900 dark:text-white mb-2">Leak Auditing</h3>
				<p class="text-xs text-slate-500 dark:text-slate-400">System is configured with <code>X-Gopherstack-Memory-Stats</code> enabled on all API responses.</p>
			</div>
		</section>
	{/if}
</div>
