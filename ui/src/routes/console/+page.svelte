<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { dashboardClient } from '$lib/api/connect-client';
	import type { CapturedRequest } from '$lib/api/gopherstack/dashboard/v1/dashboard_pb';
	import { toast } from 'svelte-sonner';

	let requests = $state<CapturedRequest[]>([]);
	let isConnected = $state(false);
	let abortController = new AbortController();

	async function startConsoleStream() {
		try {
			const stream = await dashboardClient.streamConsole({}, { signal: abortController.signal });
			isConnected = true;
			for await (const response of stream) {
				if (response.request) {
					// Prepend new requests, keep up to 100
					requests = [response.request, ...requests].slice(0, 100);
				}
			}
		} catch (err: unknown) {
			const e = err as Error;
			if (e.name !== 'AbortError') {
				isConnected = false;
				toast.error(`Console stream disconnected: ${e.message}`);
			}
		}
	}

	onMount(() => {
		startConsoleStream();
	});

	onDestroy(() => {
		abortController.abort();
	});

	let selectedRequest = $state<CapturedRequest | null>(null);

	function openDetails(req: CapturedRequest) {
		selectedRequest = req;
	}

	function closeDetails() {
		selectedRequest = null;
	}
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex justify-between items-center bg-white/40 dark:bg-zinc-900/40 p-4 rounded-xl shadow-lg border border-white/60 dark:border-zinc-800/60 backdrop-blur-md">
		<h1 class="text-2xl font-bold tracking-tight text-zinc-900 dark:text-zinc-100">Live API Console</h1>
		<div class="flex items-center gap-4">
			<button
				onclick={() => requests = []}
				class="text-xs px-2 py-1 rounded bg-zinc-200 dark:bg-zinc-800 text-zinc-700 dark:text-zinc-300 hover:bg-zinc-300 dark:hover:bg-zinc-700 transition"
			>
				Clear
			</button>
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
	</div>

	<!-- Requests List -->
	<div class="grid grid-cols-1 md:grid-cols-[1fr_350px] gap-6 items-start">
		<div class="overflow-hidden rounded-xl bg-white/50 shadow backdrop-blur ring-1 ring-black/5 dark:bg-zinc-900/50 dark:ring-white/10 flex flex-col max-h-[700px]">
			{#if requests.length === 0}
				<div class="flex flex-col items-center justify-center p-12 h-64">
					<p class="text-zinc-500">Waiting for API requests...</p>
					<p class="text-xs text-zinc-400 mt-2">Make a request to Gopherstack locally to see it appear here.</p>
				</div>
			{:else}
				<div class="overflow-auto grow">
					<table class="min-w-full divide-y divide-zinc-200 dark:divide-zinc-800 relative">
						<thead class="bg-zinc-50/80 dark:bg-zinc-800/80 backdrop-blur sticky top-0 z-10">
							<tr>
								<th scope="col" class="py-3.5 pl-4 pr-3 text-left text-xs font-semibold uppercase sm:pl-6 text-zinc-500">Time</th>
								<th scope="col" class="px-3 py-3.5 text-left text-xs font-semibold uppercase text-zinc-500">Method</th>
								<th scope="col" class="px-3 py-3.5 text-left text-xs font-semibold uppercase text-zinc-500 border-r border-zinc-200 dark:border-zinc-700">Path</th>
								<th scope="col" class="px-3 py-3.5 text-left text-xs font-semibold uppercase text-zinc-500">Status</th>
								<th scope="col" class="px-3 py-3.5 text-right text-xs font-semibold uppercase text-zinc-500">Duration</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-zinc-200 dark:divide-zinc-800 bg-transparent cursor-pointer">
							{#each requests as req}
								<tr 
									class={`hover:bg-blue-50 dark:hover:bg-blue-900/20 transition ${selectedRequest === req ? 'bg-blue-50 dark:bg-blue-900/30' : ''}`}
									onclick={() => openDetails(req)}
								>
									<td class="whitespace-nowrap py-3 pl-4 pr-3 text-sm text-zinc-500 sm:pl-6">
										{req.timestamp ? new Date(Number(req.timestamp.seconds) * 1000).toLocaleTimeString() : ''}
									</td>
									<td class="whitespace-nowrap px-3 py-3 text-sm font-medium">
										<span class={`rounded px-2 py-0.5 text-xs font-bold ${
											req.method === 'GET' ? 'bg-blue-100 text-blue-800 dark:bg-blue-900/50 dark:text-blue-200' :
											req.method === 'POST' ? 'bg-green-100 text-green-800 dark:bg-green-900/50 dark:text-green-200' :
											req.method === 'PUT' ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/50 dark:text-yellow-200' :
											req.method === 'DELETE' ? 'bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-200' :
											'bg-zinc-100 text-zinc-800 dark:bg-zinc-800 dark:text-zinc-200'
										}`}>
											{req.method}
										</span>
									</td>
									<td class="px-3 py-3 text-sm text-zinc-900 dark:text-zinc-100 font-mono truncate max-w-[200px] border-r border-zinc-200 dark:border-zinc-700" title={req.path}>
										{req.path}
									</td>
									<td class="whitespace-nowrap px-3 py-3 text-sm font-medium">
										<span class={`rounded-full flex items-center justify-center w-8 h-6 text-xs ${
											req.status >= 200 && req.status < 300 ? 'bg-green-100 text-green-800 dark:bg-green-900/50 dark:text-green-300' :
											req.status >= 400 && req.status < 500 ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/50 dark:text-yellow-300' :
											req.status >= 500 ? 'bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-300' :
											'bg-zinc-100 text-zinc-800 dark:bg-zinc-800 dark:text-zinc-300'
										}`}>
											{req.status}
										</span>
									</td>
									<td class="whitespace-nowrap px-3 py-3 text-sm text-right text-zinc-500">
										{req.durationMs}ms
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>

		<!-- Details Panel -->
		<div class="rounded-xl border border-white/40 bg-white/45 p-4 shadow-lg backdrop-blur-md dark:border-zinc-800/70 dark:bg-zinc-900/40 min-h-[400px]">
			{#if selectedRequest}
				<div class="flex justify-between items-start mb-4">
					<h3 class="font-bold text-lg">Request Details</h3>
					<button onclick={closeDetails} class="text-zinc-400 hover:text-zinc-600 dark:hover:text-zinc-200">
						✕
					</button>
				</div>
				<div class="space-y-4">
					<div>
						<p class="text-xs font-semibold text-zinc-500 uppercase mb-1">Request ID</p>
						<p class="text-sm font-mono break-all bg-white/60 dark:bg-zinc-950/50 p-2 rounded">{selectedRequest.id || 'N/A'}</p>
					</div>
					<div>
						<p class="text-xs font-semibold text-zinc-500 uppercase mb-1">Headers</p>
						<div class="bg-white/60 dark:bg-zinc-950/50 p-2 rounded max-h-48 overflow-y-auto">
							{#each Object.entries(selectedRequest.headers || {}) as [key, value]}
								<div class="text-xs font-mono mb-1 break-all">
									<span class="font-bold text-blue-600 dark:text-blue-400">{key}:</span> {value}
								</div>
							{/each}
						</div>
					</div>
					<div>
						<p class="text-xs font-semibold text-zinc-500 uppercase mb-1">Body Preview</p>
						<div class="bg-white/60 dark:bg-zinc-950/50 p-2 rounded max-h-48 overflow-y-auto font-mono text-xs whitespace-pre-wrap break-all">
							{selectedRequest.body || '(Empty body)'}
						</div>
					</div>
				</div>
			{:else}
				<div class="h-full flex flex-col items-center justify-center text-zinc-400">
					<p>Select a request to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>
