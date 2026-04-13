<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { dashboardClient } from '$lib/api/connect-client';
	import type { CapturedRequest } from '$lib/api/gopherstack/dashboard/v1/dashboard_pb';
	import { toast } from 'svelte-sonner';

	let requests = $state<CapturedRequest[]>([]);
	let isConnected = $state(false);
	let autoRefresh = $state(true);
	let abortController: AbortController | null = null;
	let destroying = false;
	let searchQuery = $state('');
	let drawerOpen = $state(false);
	let selectedRequest = $state<CapturedRequest | null>(null);

	const filteredRequests = $derived(
		searchQuery.trim() === ''
			? requests
			: requests.filter((r) => {
					const q = searchQuery.toLowerCase();
					return (
						r.method.toLowerCase().includes(q) ||
						r.path.toLowerCase().includes(q) ||
						r.status.toString().includes(q) ||
						guessService(r).toLowerCase().includes(q)
					);
				})
	);

	function guessService(req: CapturedRequest): string {
		const headers = req.headers ?? {};
		if (headers['X-Amz-Target']) {
			return headers['X-Amz-Target'].split('.')[0];
		}
		if (req.path.startsWith('/latest/meta-data') || req.path.startsWith('/latest/api/token')) return 'IMDS (EC2)';
		const auth = headers['Authorization'] ?? '';
		if (auth.includes('Credential=')) {
			const parts = auth.split('Credential=')[1]?.split('/');
			if (parts && parts.length > 3) return parts[3].toUpperCase();
		}
		return 'HTTP';
	}

	function displayPath(req: CapturedRequest): string {
		if (req.path === '/') {
			const headers = req.headers ?? {};
			if (headers['X-Amz-Target']) {
				const targetParts = headers['X-Amz-Target'].split('.');
				if (targetParts.length > 1) return targetParts[1].split('_')[0];
			}
			if (req.body?.includes('Action=')) {
				const match = req.body.match(/(?:^|&)Action=([^&]+)/);
				if (match?.[1]) return match[1];
			}
		}
		return req.path;
	}

	function statusColor(status: number): string {
		if (status >= 200 && status < 300) return 'bg-emerald-100 text-emerald-800 dark:bg-emerald-900 dark:text-emerald-300';
		if (status >= 400 && status < 500) return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-300';
		if (status >= 500) return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-300';
		return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300';
	}

	function methodColor(method: string): string {
		switch (method.toUpperCase()) {
			case 'GET': return 'text-blue-600 dark:text-blue-400';
			case 'POST': return 'text-emerald-600 dark:text-emerald-400';
			case 'PUT': return 'text-amber-600 dark:text-amber-400';
			case 'DELETE': return 'text-red-600 dark:text-red-400';
			default: return 'text-slate-600 dark:text-slate-400';
		}
	}

	function formatBody(req: CapturedRequest): string {
		if (!req.body) return '';
		const ct = (req.headers ?? {})['Content-Type'] ?? '';
		try {
			if (ct.includes('json')) return JSON.stringify(JSON.parse(req.body), null, 2);
			if (ct.includes('x-www-form-urlencoded')) return req.body.split('&').join('\n');
		} catch {}
		return req.body;
	}

	function formatTime(req: CapturedRequest): string {
		if (!req.timestamp) return '';
		return new Date(Number(req.timestamp.seconds) * 1000).toLocaleTimeString();
	}

	function openDrawer(req: CapturedRequest) {
		selectedRequest = req;
		drawerOpen = true;
	}

	function closeDrawer() {
		drawerOpen = false;
	}

	async function startStream() {
		abortController = new AbortController();
		try {
			const stream = await dashboardClient.streamConsole({}, { signal: abortController.signal });
			isConnected = true;
			for await (const response of stream) {
				if (response.request) {
					requests = [response.request, ...requests].slice(0, 200);
				}
			}
		} catch (err: unknown) {
			const e = err as Error;
			if (e.name !== 'AbortError' && !destroying) {
				isConnected = false;
				toast.error(`Console stream disconnected: ${e.message}`);
			}
		}
	}

	function stopStream() {
		if (abortController) {
			abortController.abort();
			abortController = null;
			isConnected = false;
		}
	}

	function toggleAutoRefresh(enabled: boolean) {
		autoRefresh = enabled;
		if (enabled) startStream(); else stopStream();
	}

	onMount(() => { if (autoRefresh) startStream(); });
	onDestroy(() => { destroying = true; stopStream(); });
</script>

<div class="container mx-auto h-full flex flex-col space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row justify-between items-start md:items-center gap-4 border-b border-slate-200 dark:border-slate-700 pb-6">
		<div>
			<h1 class="text-4xl font-extrabold tracking-tight text-blue-600 dark:text-blue-400">Live API Console</h1>
			<p class="text-slate-500 dark:text-slate-400 mt-2">View real-time AWS API requests hitting Gopherstack</p>
		</div>
		<div class="flex items-center gap-3">
			<div class={`flex items-center gap-2 px-3 py-1.5 rounded-full text-xs font-bold border ${isConnected ? 'bg-emerald-50 dark:bg-emerald-900/20 text-emerald-700 dark:text-emerald-400 border-emerald-200 dark:border-emerald-800' : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400 border-red-200 dark:border-red-800'}`}>
				<span class="relative flex h-2 w-2">
					{#if isConnected}
						<span class="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
						<span class="relative inline-flex rounded-full h-2 w-2 bg-emerald-500"></span>
					{:else}
						<span class="relative inline-flex rounded-full h-2 w-2 bg-red-500"></span>
					{/if}
				</span>
				{isConnected ? 'Recording' : 'Paused'}
			</div>
			<button type="button" onclick={() => toggleAutoRefresh(!autoRefresh)} class="relative inline-flex items-center cursor-pointer group" role="switch" aria-checked={autoRefresh}>
				<div class="relative shrink-0 w-11 h-6 rounded-full transition-colors {autoRefresh ? 'bg-indigo-600' : 'bg-slate-200 dark:bg-slate-700'}">
					<div class="absolute top-[2px] left-[2px] bg-white border border-slate-300 rounded-full h-5 w-5 transition-transform {autoRefresh ? 'translate-x-5 border-white' : ''}"></div>
				</div>
				<span class="ml-3 text-sm font-medium text-slate-700 dark:text-slate-300">Auto</span>
			</button>
		</div>
	</div>

	<!-- Filter Bar -->
	<div class="flex gap-4 items-center bg-white/80 dark:bg-slate-800/80 backdrop-blur-md p-4 rounded-xl shadow-sm border border-slate-200 dark:border-slate-700">
		<div class="relative flex-grow">
			<div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
				<svg class="w-4 h-4 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" /></svg>
			</div>
			<input type="text" bind:value={searchQuery} class="bg-white/50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-indigo-500 focus:border-indigo-500 block w-full pl-10 p-2.5 dark:bg-slate-800 dark:border-slate-600 dark:text-white" placeholder="Filter logs (e.g. s3, POST, 404)..." />
		</div>
		<button onclick={() => { requests = []; }} class="text-indigo-600 bg-indigo-50 hover:bg-indigo-100 border border-indigo-200 focus:ring-4 focus:outline-none focus:ring-indigo-300 font-medium rounded-lg text-sm px-4 py-2.5 dark:bg-indigo-900/30 dark:border-indigo-800 dark:text-indigo-400 dark:hover:bg-indigo-900/50">
			Clear
		</button>
	</div>

	<!-- Log Grid -->
	<div class="flex-grow rounded-xl shadow-sm border border-slate-200 dark:border-slate-700 overflow-hidden flex flex-col min-h-[500px] bg-white/50 dark:bg-slate-800/50 backdrop-blur-sm">
		<!-- Header Row -->
		<div class="grid grid-cols-12 gap-4 border-b border-slate-200 dark:border-slate-700 bg-slate-50/80 dark:bg-slate-800/80 p-3 text-xs font-semibold text-slate-500 uppercase tracking-wider">
			<div class="col-span-1">Status</div>
			<div class="col-span-1">Method</div>
			<div class="col-span-4 pl-4">Path</div>
			<div class="col-span-2">Service</div>
			<div class="col-span-2">Time</div>
			<div class="col-span-2 text-right">Duration</div>
		</div>

		<!-- Log List -->
		<div class="flex-grow overflow-y-auto">
			{#if filteredRequests.length === 0}
				<div class="flex flex-col items-center justify-center h-full text-slate-400 py-12">
					{#if requests.length === 0}
						<svg class="w-12 h-12 mb-4 opacity-40" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" /></svg>
						<p>Waiting for requests...</p>
						<p class="text-xs mt-2">Try running a command via AWS CLI targeting http://localhost:8000</p>
					{:else}
						<p>No logs match your filter.</p>
					{/if}
				</div>
			{:else}
				{#each filteredRequests as req}
					<button type="button" class="grid grid-cols-12 gap-4 p-3 w-full text-left border border-transparent border-b-slate-100 dark:border-b-slate-800 rounded-lg hover:border-indigo-200 dark:hover:border-indigo-500/40 hover:bg-white dark:hover:bg-slate-800 cursor-pointer hover:-translate-y-0.5 hover:shadow-lg dark:hover:shadow-indigo-500/20 transition-all duration-200 relative" onclick={() => openDrawer(req)}>
						<div class="col-span-1 flex items-center">
							<span class={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${statusColor(req.status)}`}>{req.status}</span>
						</div>
						<div class="col-span-1 flex items-center">
							<span class={`font-mono text-xs font-bold ${methodColor(req.method)}`}>{req.method}</span>
						</div>
						<div class="col-span-4 pl-4 flex items-center overflow-hidden">
							<span class="truncate font-mono text-xs text-slate-700 dark:text-slate-300" title={displayPath(req)}>{displayPath(req)}</span>
						</div>
						<div class="col-span-2 flex items-center overflow-hidden">
							<span class="truncate text-xs text-slate-500 uppercase font-semibold">{guessService(req)}</span>
						</div>
						<div class="col-span-2 flex items-center text-xs text-slate-500">{formatTime(req)}</div>
						<div class="col-span-2 flex items-center justify-end text-xs font-mono text-slate-600 dark:text-slate-400">{Number(req.durationMs).toFixed(2)}ms</div>
					</button>
				{/each}
			{/if}
		</div>
	</div>
</div>

<!-- Slide-out Drawer -->
{#if drawerOpen && selectedRequest}
	<!-- Backdrop -->
	<button type="button" class="fixed inset-0 z-40 bg-black/30 backdrop-blur-sm transition-opacity" onclick={closeDrawer} aria-label="Close drawer"></button>

	<!-- Drawer Panel -->
	<div class="fixed top-0 right-0 z-50 h-screen p-6 overflow-y-auto bg-white dark:bg-slate-900 border-l border-slate-200 dark:border-slate-700 shadow-2xl w-[500px] sm:w-[600px]">
		<div class="flex justify-between items-center mb-6">
			<h3 class="inline-flex items-center text-lg font-semibold text-slate-800 dark:text-white">
				<svg class="w-5 h-5 mr-2 text-indigo-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
				Request Details
			</h3>
			<button type="button" onclick={closeDrawer} class="text-slate-400 hover:bg-slate-200 hover:text-slate-900 rounded-lg p-1.5 dark:hover:bg-slate-700 dark:hover:text-white">
				<svg class="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M6 18L18 6M6 6l12 12" /></svg>
			</button>
		</div>

		<div class="space-y-5 text-sm">
			<!-- Overview -->
			<div>
				<div class="text-xs text-slate-500 uppercase tracking-wider mb-2">Overview</div>
				<div class="bg-white/80 dark:bg-slate-800/80 backdrop-blur-md p-4 rounded-xl border border-slate-200 dark:border-slate-700">
					<div class="grid grid-cols-2 gap-4">
						<div>
							<span class="text-xs text-slate-500">Method</span>
							<p class={`font-bold font-mono ${methodColor(selectedRequest.method)}`}>{selectedRequest.method}</p>
						</div>
						<div>
							<span class="text-xs text-slate-500">Status</span>
							<p class="font-bold font-mono text-slate-900 dark:text-slate-100">{selectedRequest.status}</p>
						</div>
						<div class="col-span-2">
							<span class="text-xs text-slate-500">Path</span>
							<p class="font-mono text-xs break-all text-slate-800 dark:text-slate-200">{selectedRequest.path}</p>
						</div>
						<div>
							<span class="text-xs text-slate-500">Time</span>
							<p class="font-mono text-xs text-slate-700 dark:text-slate-300">{selectedRequest.timestamp ? new Date(Number(selectedRequest.timestamp.seconds) * 1000).toISOString() : 'N/A'}</p>
						</div>
						<div>
							<span class="text-xs text-slate-500">Duration</span>
							<p class="font-mono text-xs text-slate-700 dark:text-slate-300">{Number(selectedRequest.durationMs).toFixed(2)}ms</p>
						</div>
					</div>
				</div>
			</div>

			<!-- Headers -->
			<div>
				<div class="text-xs text-slate-500 uppercase tracking-wider mb-2">Headers</div>
				<div class="p-4 rounded-xl border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-950/50">
					<div class="grid grid-cols-12 gap-2 text-xs font-mono">
						{#each Object.entries(selectedRequest.headers ?? {}) as [key, value]}
							<div class="col-span-4 text-slate-500 break-words">{key}</div>
							<div class="col-span-8 text-slate-800 dark:text-slate-200 break-words">{value}</div>
						{/each}
					</div>
				</div>
			</div>

			<!-- Request Payload -->
			<div>
				<div class="text-xs text-slate-500 uppercase tracking-wider mb-2">Request Payload</div>
				{#if selectedRequest.body}
					<div class="bg-slate-50 dark:bg-slate-950 border border-slate-200 dark:border-slate-700 text-slate-800 dark:text-slate-300 p-4 rounded-lg overflow-x-auto font-mono text-xs whitespace-pre-wrap break-all">{formatBody(selectedRequest)}</div>
				{:else}
					<div class="text-slate-400 italic p-4">No Body</div>
				{/if}
			</div>
		</div>
	</div>
{/if}
