<script lang="ts">
	import { onDestroy } from 'svelte';
	import { toast } from 'svelte-sonner';
	import {
		PostToConnectionCommand,
		DeleteConnectionCommand
	} from '@aws-sdk/client-apigatewaymanagementapi';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAPIGatewayManagementAPIClient } from '$lib/aws-client';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import {
		Activity,
		BarChart2,
		Clock,
		Copy,
		Eraser,
		Plus,
		RefreshCw,
		Radio,
		Search,
		Send,
		Trash2,
		Wifi,
		Zap
	} from 'lucide-svelte';

	type Connection = {
		connectionId: string;
		sourceIp: string;
		userAgent: string;
		connectedAt: string;
		lastActiveAt: string;
		postedMessages: number;
		bytesSent: number;
	};

	type LifecycleEvent = {
		at: string;
		type: 'connected' | 'message' | 'disconnected' | 'ping' | 'broadcast';
		detail?: string;
		bytes?: number;
	};

	type Message = {
		receivedAt: string;
		connectionId: string;
		data: string;
		bytes: number;
	};

	type Stats = {
		activeConnections: number;
		bufferedMessages: number;
		totalConnections: number;
		totalDisconnections: number;
		totalMessages: number;
		totalBroadcasts: number;
		totalBytesSent: number;
		totalRejected: number;
	};

	const apigwmgmt = regionalClient(getAPIGatewayManagementAPIClient);
	const adminBase = '/_gopherstack/apigwmgmt';

	const messageTemplates = [
		{ label: 'Ping JSON', body: '{"type":"ping","ts":"' + new Date().toISOString() + '"}' },
		{ label: 'Notification', body: '{"event":"notification","title":"Hello","priority":"normal"}' },
		{ label: 'Chat', body: '{"event":"chat","from":"server","text":"Welcome!"}' },
		{ label: 'Heartbeat', body: '{"type":"heartbeat","seq":1}' },
		{ label: 'Plain text', body: 'hello from gopherstack' }
	];

	let connections = $state<Connection[]>([]);
	let stats = $state<Stats | null>(null);
	let selected = $state<Connection | null>(null);
	let messages = $state<Message[]>([]);
	let timeline = $state<LifecycleEvent[]>([]);
	let activeTab = $state<'messages' | 'timeline'>('messages');

	let loading = $state(false);
	let searchQuery = $state('');
	let autoRefresh = $state(false);
	let refreshIntervalSec = $state(5);
	let refreshTimer: ReturnType<typeof setInterval> | null = null;
	let prettyJSON = $state(true);

	// Send message form
	let messageBody = $state('');
	let sending = $state(false);

	// Simulate connection form
	let showSimulate = $state(false);
	let simConnectionId = $state('');
	let simSourceIp = $state('127.0.0.1');
	let simUserAgent = $state('Gopherstack UI');
	let simulating = $state(false);

	// Broadcast modal
	let showBroadcast = $state(false);
	let broadcastBody = $state('');
	let broadcasting = $state(false);

	// Prune modal
	let showPrune = $state(false);
	let pruneSeconds = $state(60);
	let pruning = $state(false);

	const filteredConnections = $derived(
		searchQuery
			? connections.filter((c) => {
					const q = searchQuery.toLowerCase();
					return (
						c.connectionId.toLowerCase().includes(q) ||
						c.sourceIp.toLowerCase().includes(q) ||
						c.userAgent.toLowerCase().includes(q)
					);
				})
			: connections
	);

	const totalBufferedBytes = $derived(messages.reduce((acc, m) => acc + m.bytes, 0));

	async function loadConnections() {
		loading = true;
		try {
			const res = await fetch(`${adminBase}/connections`);
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			const data = (await res.json()) as { connections: Connection[] };
			connections = data.connections ?? [];
		} catch (err) {
			toast.error('Failed to load connections: ' + String(err));
		} finally {
			loading = false;
		}
	}

	async function loadStats() {
		try {
			const res = await fetch(`${adminBase}/stats`);
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			stats = (await res.json()) as Stats;
		} catch (err) {
			toast.error('Failed to load stats: ' + String(err));
		}
	}

	async function loadMessages(id: string) {
		try {
			const res = await fetch(`${adminBase}/connections/${encodeURIComponent(id)}/messages`);
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			const data = (await res.json()) as { messages: Message[] };
			messages = data.messages ?? [];
		} catch (err) {
			toast.error('Failed to load messages: ' + String(err));
		}
	}

	async function loadTimeline(id: string) {
		try {
			const res = await fetch(`${adminBase}/connections/${encodeURIComponent(id)}/timeline`);
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			const data = (await res.json()) as { events: LifecycleEvent[] };
			timeline = data.events ?? [];
		} catch (err) {
			toast.error('Failed to load timeline: ' + String(err));
		}
	}

	async function refreshAll() {
		await Promise.all([loadConnections(), loadStats()]);
		if (selected) {
			const stillActive = connections.find((c) => c.connectionId === selected!.connectionId);
			if (stillActive) {
				selected = stillActive;
				await Promise.all([loadMessages(selected.connectionId), loadTimeline(selected.connectionId)]);
			} else {
				selected = null;
				messages = [];
				timeline = [];
			}
		}
	}

	function selectConnection(conn: Connection) {
		selected = conn;
		messages = [];
		timeline = [];
		void loadMessages(conn.connectionId);
		void loadTimeline(conn.connectionId);
	}

	async function sendMessage() {
		if (!selected || !messageBody) {
			return;
		}
		sending = true;
		try {
			await apigwmgmt().send(
				new PostToConnectionCommand({
					ConnectionId: selected.connectionId,
					Data: new TextEncoder().encode(messageBody)
				})
			);
			toast.success('Message posted');
			messageBody = '';
			await loadMessages(selected.connectionId);
			await loadTimeline(selected.connectionId);
			await loadStats();
		} catch (err) {
			toast.error('Failed to post message: ' + String(err));
		} finally {
			sending = false;
		}
	}

	async function pingSelected() {
		if (!selected) {
			return;
		}
		try {
			const res = await fetch(
				`${adminBase}/connections/${encodeURIComponent(selected.connectionId)}/ping`,
				{ method: 'POST' }
			);
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			toast.success('Connection pinged');
			await refreshAll();
		} catch (err) {
			toast.error('Ping failed: ' + String(err));
		}
	}

	async function clearMessages() {
		if (!selected) {
			return;
		}
		const ok = await confirmDestructive({
			title: 'Clear messages?',
			message: `Drop all stored messages for ${selected.connectionId}.`,
			confirmLabel: 'Clear'
		});
		if (!ok) {
			return;
		}
		try {
			const res = await fetch(
				`${adminBase}/connections/${encodeURIComponent(selected.connectionId)}/messages`,
				{ method: 'DELETE' }
			);
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			toast.success('Messages cleared');
			messages = [];
			await loadStats();
		} catch (err) {
			toast.error('Clear failed: ' + String(err));
		}
	}

	async function deleteSelected() {
		if (!selected) {
			return;
		}
		const ok = await confirmDestructive({
			title: 'Disconnect connection?',
			message: `Terminate ${selected.connectionId}. This is irreversible.`,
			confirmLabel: 'Disconnect'
		});
		if (!ok) {
			return;
		}
		try {
			await apigwmgmt().send(new DeleteConnectionCommand({ ConnectionId: selected.connectionId }));
			toast.success('Connection deleted');
			selected = null;
			messages = [];
			timeline = [];
			await refreshAll();
		} catch (err) {
			toast.error('Delete failed: ' + String(err));
		}
	}

	async function simulateConnection() {
		if (!simConnectionId.trim()) {
			toast.error('Connection ID is required');
			return;
		}
		simulating = true;
		try {
			const res = await fetch(`${adminBase}/connections`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					connectionId: simConnectionId.trim(),
					sourceIp: simSourceIp.trim(),
					userAgent: simUserAgent.trim()
				})
			});
			if (!res.ok) {
				const body = (await res.json().catch(() => ({}))) as { message?: string };
				throw new Error(body.message ?? `HTTP ${res.status}`);
			}
			toast.success(`Connection ${simConnectionId} created`);
			showSimulate = false;
			simConnectionId = '';
			await refreshAll();
		} catch (err) {
			toast.error('Simulate failed: ' + String(err));
		} finally {
			simulating = false;
		}
	}

	async function broadcastMessage() {
		if (!broadcastBody) {
			toast.error('Body is required');
			return;
		}
		broadcasting = true;
		try {
			const res = await fetch(`${adminBase}/broadcast`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ data: broadcastBody })
			});
			if (!res.ok) {
				const body = (await res.json().catch(() => ({}))) as { message?: string };
				throw new Error(body.message ?? `HTTP ${res.status}`);
			}
			const data = (await res.json()) as { delivered: number };
			toast.success(`Broadcast delivered to ${data.delivered} connection(s)`);
			showBroadcast = false;
			broadcastBody = '';
			await refreshAll();
		} catch (err) {
			toast.error('Broadcast failed: ' + String(err));
		} finally {
			broadcasting = false;
		}
	}

	async function pruneIdle() {
		pruning = true;
		try {
			const res = await fetch(`${adminBase}/prune`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ idleSeconds: pruneSeconds })
			});
			if (!res.ok) {
				throw new Error(`HTTP ${res.status}`);
			}
			const data = (await res.json()) as { pruned: string[] };
			toast.success(`Pruned ${data.pruned.length} idle connection(s)`);
			showPrune = false;
			await refreshAll();
		} catch (err) {
			toast.error('Prune failed: ' + String(err));
		} finally {
			pruning = false;
		}
	}

	function copyConnectionId(id: string) {
		void navigator.clipboard.writeText(id);
		toast.success('Connection ID copied');
	}

	function applyTemplate(body: string) {
		messageBody = body;
	}

	function formatBytes(n: number): string {
		if (n < 1024) return `${n} B`;
		if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
		return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
	}

	function formatRelative(iso: string): string {
		const ms = Date.now() - new Date(iso).getTime();
		const sec = Math.floor(ms / 1000);
		if (sec < 1) return 'just now';
		if (sec < 60) return `${sec}s ago`;
		const min = Math.floor(sec / 60);
		if (min < 60) return `${min}m ago`;
		const hr = Math.floor(min / 60);
		if (hr < 24) return `${hr}h ago`;
		return new Date(iso).toLocaleString();
	}

	function isIdle(c: Connection): boolean {
		return Date.now() - new Date(c.lastActiveAt).getTime() > 60_000;
	}

	function tryPrettyJSON(raw: string): string {
		if (!prettyJSON) return raw;
		try {
			return JSON.stringify(JSON.parse(raw), null, 2);
		} catch {
			return raw;
		}
	}

	function eventBadgeClass(t: LifecycleEvent['type']): string {
		switch (t) {
			case 'connected':
				return 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300';
			case 'message':
				return 'bg-indigo-100 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300';
			case 'broadcast':
				return 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300';
			case 'ping':
				return 'bg-sky-100 text-sky-700 dark:bg-sky-900/30 dark:text-sky-300';
			case 'disconnected':
				return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300';
			default:
				return 'bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-300';
		}
	}

	function startAutoRefresh() {
		if (refreshTimer) {
			clearInterval(refreshTimer);
		}
		refreshTimer = setInterval(() => {
			void refreshAll();
		}, refreshIntervalSec * 1000);
	}

	function stopAutoRefresh() {
		if (refreshTimer) {
			clearInterval(refreshTimer);
			refreshTimer = null;
		}
	}

	$effect(() => {
		if (autoRefresh) {
			startAutoRefresh();
		} else {
			stopAutoRefresh();
		}
	});

	// Connection IDs are not unique across regions, and `refreshAll()` both
	// reads and writes `selected`/`connections` (its "is the selection still
	// active" lookup) -- wrapping it directly in `onRegionChange` would turn
	// those reads into effect dependencies that its own writes then
	// re-trigger, a self-retriggering loop. Instead, clear the drill-down
	// state up front (a connection selected in the old region cannot exist
	// in the new one) and reload only the list + stats; neither
	// `loadConnections` nor `loadStats` reads `selected` or `connections`,
	// so no `untrack` is needed here. The periodic auto-refresh timer below
	// is unaffected: it only calls `refreshAll()`, which talks to the
	// gopherstack-only admin endpoints over plain `fetch`, never the AWS
	// client, so it can never fire against a stale-region client.
	onRegionChange(() => {
		selected = null;
		messages = [];
		timeline = [];
		void loadConnections();
		void loadStats();
	});

	onDestroy(() => {
		stopAutoRefresh();
	});
</script>

<div class="space-y-6 p-6">
	<!-- Header -->
	<div
		class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800"
	>
		<div class="flex items-start justify-between gap-4">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">
					API Gateway Management API
				</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
					Inspect simulated WebSocket connections, send PostToConnection messages, and trace the
					lifecycle of each connection.
				</p>
			</div>
			<div class="flex flex-wrap items-center gap-2">
				<button
					type="button"
					onclick={() => void refreshAll()}
					class="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100 dark:hover:bg-slate-600"
				>
					<RefreshCw size={14} class={loading ? 'animate-spin' : ''} />
					Refresh
				</button>
				<label
					class="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100"
				>
					<input type="checkbox" bind:checked={autoRefresh} class="rounded" />
					Auto-refresh
					<input
						type="number"
						min="2"
						max="60"
						bind:value={refreshIntervalSec}
						class="w-14 rounded border border-slate-200 bg-white px-2 py-0.5 text-xs text-slate-700 dark:border-slate-600 dark:bg-slate-800 dark:text-slate-100"
					/>
					s
				</label>
				<button
					type="button"
					onclick={() => (showBroadcast = true)}
					class="flex items-center gap-2 rounded-lg bg-purple-600 px-4 py-2 text-sm font-medium text-white hover:bg-purple-700"
				>
					<Radio size={14} />
					Broadcast
				</button>
				<button
					type="button"
					onclick={() => (showPrune = true)}
					class="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100 dark:hover:bg-slate-600"
				>
					<Eraser size={14} />
					Prune idle
				</button>
				<button
					type="button"
					onclick={() => (showSimulate = true)}
					class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
				>
					<Plus size={14} />
					Simulate
				</button>
			</div>
		</div>
	</div>

	<!-- Stats -->
	{#if stats}
		<div class="grid gap-4 md:grid-cols-2 lg:grid-cols-4 xl:grid-cols-8">
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="flex items-center gap-2 text-xs uppercase text-slate-500 dark:text-slate-400">
					<Wifi size={12} /> Active
				</div>
				<div class="mt-2 text-2xl font-bold text-emerald-600 dark:text-emerald-400">
					{stats.activeConnections}
				</div>
			</div>
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="flex items-center gap-2 text-xs uppercase text-slate-500 dark:text-slate-400">
					<Activity size={12} /> Buffered msgs
				</div>
				<div class="mt-2 text-2xl font-bold text-slate-900 dark:text-white">
					{stats.bufferedMessages}
				</div>
			</div>
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Total connects</div>
				<div class="mt-2 text-2xl font-bold text-slate-900 dark:text-white">
					{stats.totalConnections}
				</div>
			</div>
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Disconnects</div>
				<div class="mt-2 text-2xl font-bold text-slate-900 dark:text-white">
					{stats.totalDisconnections}
				</div>
			</div>
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Messages posted</div>
				<div class="mt-2 text-2xl font-bold text-slate-900 dark:text-white">
					{stats.totalMessages}
				</div>
			</div>
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Broadcasts</div>
				<div class="mt-2 text-2xl font-bold text-slate-900 dark:text-white">
					{stats.totalBroadcasts}
				</div>
			</div>
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="flex items-center gap-2 text-xs uppercase text-slate-500 dark:text-slate-400">
					<BarChart2 size={12} /> Bytes sent
				</div>
				<div class="mt-2 text-2xl font-bold text-slate-900 dark:text-white">
					{formatBytes(stats.totalBytesSent)}
				</div>
			</div>
			<div
				class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
			>
				<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Rejected</div>
				<div class="mt-2 text-2xl font-bold text-rose-600 dark:text-rose-400">
					{stats.totalRejected}
				</div>
			</div>
		</div>
	{/if}

	<div class="grid gap-6 lg:grid-cols-[1fr,2fr]">
		<!-- Connection list -->
		<div
			class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800"
		>
			<div class="flex items-center justify-between gap-2">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white">
					Connections ({filteredConnections.length})
				</h2>
			</div>
			<div class="mt-3 flex items-center gap-2 rounded-lg border border-slate-200 px-3 dark:border-slate-700">
				<Search size={14} class="text-slate-400" />
				<input
					type="text"
					placeholder="Search id, IP, or user-agent"
					bind:value={searchQuery}
					class="w-full bg-transparent py-2 text-sm text-slate-900 outline-none dark:text-white"
				/>
			</div>
			<ul class="mt-3 max-h-[60vh] space-y-2 overflow-y-auto">
				{#if filteredConnections.length === 0}
					<li class="rounded-lg border border-dashed border-slate-200 p-4 text-center text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
						{loading ? 'Loading…' : 'No connections — simulate one to get started.'}
					</li>
				{:else}
					{#each filteredConnections as conn (conn.connectionId)}
						<li>
							<button
								type="button"
								onclick={() => selectConnection(conn)}
								class="w-full rounded-lg border border-slate-200 px-3 py-2 text-left transition-colors hover:border-indigo-300 hover:bg-indigo-50 dark:border-slate-700 dark:hover:border-indigo-500 dark:hover:bg-slate-700/40 {selected?.connectionId ===
								conn.connectionId
									? 'border-indigo-400 bg-indigo-50 dark:border-indigo-400 dark:bg-slate-700/60'
									: ''}"
							>
								<div class="flex items-center justify-between gap-2">
									<span class="truncate font-mono text-sm text-slate-900 dark:text-white">
										{conn.connectionId}
									</span>
									<span
										class="inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-xs {isIdle(
											conn
										)
											? 'bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300'
											: 'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/30 dark:text-emerald-300'}"
									>
										<Clock size={10} />
										{isIdle(conn) ? 'idle' : 'live'}
									</span>
								</div>
								<div class="mt-1 flex flex-wrap gap-x-3 text-xs text-slate-500 dark:text-slate-400">
									<span>{conn.sourceIp}</span>
									<span>{conn.postedMessages} msgs</span>
									<span>{formatBytes(conn.bytesSent)}</span>
									<span>{formatRelative(conn.lastActiveAt)}</span>
								</div>
							</button>
						</li>
					{/each}
				{/if}
			</ul>
		</div>

		<!-- Detail pane -->
		<div
			class="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800"
		>
			{#if !selected}
				<div class="p-12 text-center text-sm text-slate-500 dark:text-slate-400">
					Select a connection from the list to inspect messages, lifecycle events, and send a
					payload.
				</div>
			{:else}
				<div class="border-b border-slate-200 p-4 dark:border-slate-700">
					<div class="flex flex-wrap items-center justify-between gap-3">
						<div class="min-w-0">
							<div class="flex items-center gap-2">
								<span class="truncate font-mono text-base font-semibold text-slate-900 dark:text-white">
									{selected.connectionId}
								</span>
								<button
									type="button"
									title="Copy connection ID"
									onclick={() => copyConnectionId(selected!.connectionId)}
									class="text-slate-400 hover:text-indigo-600 dark:hover:text-indigo-400"
								>
									<Copy size={14} />
								</button>
							</div>
							<div class="mt-1 flex flex-wrap gap-x-4 text-xs text-slate-500 dark:text-slate-400">
								<span>IP {selected.sourceIp}</span>
								<span>UA {selected.userAgent}</span>
								<span>connected {formatRelative(selected.connectedAt)}</span>
								<span>last active {formatRelative(selected.lastActiveAt)}</span>
							</div>
						</div>
						<div class="flex flex-wrap gap-2">
							<button
								type="button"
								onclick={() => void pingSelected()}
								class="flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100 dark:hover:bg-slate-600"
							>
								<Zap size={12} /> Ping
							</button>
							<button
								type="button"
								onclick={() => void clearMessages()}
								class="flex items-center gap-1 rounded-lg border border-slate-200 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-100 dark:hover:bg-slate-600"
							>
								<Eraser size={12} /> Clear msgs
							</button>
							<button
								type="button"
								onclick={() => void deleteSelected()}
								class="flex items-center gap-1 rounded-lg border border-rose-200 bg-rose-50 px-3 py-1.5 text-xs font-medium text-rose-700 hover:bg-rose-100 dark:border-rose-900/40 dark:bg-rose-900/20 dark:text-rose-300 dark:hover:bg-rose-900/30"
							>
								<Trash2 size={12} /> Disconnect
							</button>
						</div>
					</div>
				</div>

				<!-- Send message -->
				<div class="border-b border-slate-200 p-4 dark:border-slate-700">
					<label
						for="msg-body"
						class="mb-1 block text-xs font-semibold uppercase text-slate-500 dark:text-slate-400"
					>
						Send message (PostToConnection)
					</label>
					<div class="flex flex-wrap gap-2">
						{#each messageTemplates as tpl}
							<button
								type="button"
								onclick={() => applyTemplate(tpl.body)}
								class="rounded-full border border-slate-200 bg-white px-2.5 py-1 text-xs text-slate-600 hover:bg-slate-50 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-200 dark:hover:bg-slate-600"
							>
								{tpl.label}
							</button>
						{/each}
					</div>
					<textarea
						id="msg-body"
						bind:value={messageBody}
						rows="4"
						placeholder="Payload (text or JSON)"
						class="mt-2 w-full rounded-lg border border-slate-300 bg-white px-3 py-2 font-mono text-sm text-slate-900 outline-none focus:border-indigo-500 dark:border-slate-600 dark:bg-slate-900 dark:text-white"
					></textarea>
					<div class="mt-2 flex items-center justify-between">
						<span class="text-xs text-slate-500 dark:text-slate-400">
							{new Blob([messageBody]).size} bytes (max 128 KiB)
						</span>
						<button
							type="button"
							disabled={!messageBody || sending}
							onclick={() => void sendMessage()}
							class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
						>
							<Send size={14} />
							{sending ? 'Sending…' : 'Send'}
						</button>
					</div>
				</div>

				<!-- Tabs -->
				<div class="border-b border-slate-200 px-4 dark:border-slate-700">
					<nav class="flex gap-4">
						<button
							type="button"
							onclick={() => (activeTab = 'messages')}
							class="border-b-2 px-2 py-3 text-sm font-medium {activeTab === 'messages'
								? 'border-indigo-600 text-indigo-600 dark:text-indigo-400'
								: 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200'}"
						>
							Messages ({messages.length})
						</button>
						<button
							type="button"
							onclick={() => (activeTab = 'timeline')}
							class="border-b-2 px-2 py-3 text-sm font-medium {activeTab === 'timeline'
								? 'border-indigo-600 text-indigo-600 dark:text-indigo-400'
								: 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200'}"
						>
							Timeline ({timeline.length})
						</button>
					</nav>
				</div>

				<!-- Tab content -->
				<div class="max-h-[55vh] overflow-y-auto p-4">
					{#if activeTab === 'messages'}
						<div class="mb-3 flex items-center justify-between">
							<span class="text-xs text-slate-500 dark:text-slate-400">
								{messages.length} messages · {formatBytes(totalBufferedBytes)} buffered
							</span>
							<label
								class="flex items-center gap-2 text-xs text-slate-600 dark:text-slate-300"
							>
								<input type="checkbox" bind:checked={prettyJSON} class="rounded" />
								Pretty-print JSON
							</label>
						</div>
						{#if messages.length === 0}
							<div class="rounded-lg border border-dashed border-slate-200 p-6 text-center text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
								No messages yet. Send one above to populate the buffer.
							</div>
						{:else}
							<ul class="space-y-2">
								{#each messages as msg, i (i)}
									<li
										class="rounded-lg border border-slate-200 bg-slate-50 p-3 dark:border-slate-700 dark:bg-slate-900/40"
									>
										<div class="flex items-center justify-between gap-2 text-xs text-slate-500 dark:text-slate-400">
											<span>{new Date(msg.receivedAt).toLocaleString()}</span>
											<span>{msg.bytes} B</span>
										</div>
										<pre
											class="mt-1 whitespace-pre-wrap break-all font-mono text-xs text-slate-800 dark:text-slate-200">{tryPrettyJSON(
												msg.data
											)}</pre>
									</li>
								{/each}
							</ul>
						{/if}
					{:else if timeline.length === 0}
						<div class="rounded-lg border border-dashed border-slate-200 p-6 text-center text-sm text-slate-500 dark:border-slate-700 dark:text-slate-400">
							No lifecycle events yet.
						</div>
					{:else}
						<ol class="relative border-l border-slate-200 pl-4 dark:border-slate-700">
							{#each timeline as ev, i (i)}
								<li class="mb-4 last:mb-0">
									<span
										class="absolute -left-1.5 mt-1 h-3 w-3 rounded-full {eventBadgeClass(
											ev.type
										)}"
									></span>
									<div class="flex flex-wrap items-baseline gap-2">
										<span class="rounded px-1.5 py-0.5 text-xs font-medium {eventBadgeClass(ev.type)}">
											{ev.type}
										</span>
										<span class="text-xs text-slate-500 dark:text-slate-400">
											{new Date(ev.at).toLocaleString()}
										</span>
										{#if ev.bytes}
											<span class="text-xs text-slate-500 dark:text-slate-400">
												{ev.bytes} B
											</span>
										{/if}
										{#if ev.detail}
											<span class="text-xs text-slate-500 dark:text-slate-400">
												{ev.detail}
											</span>
										{/if}
									</div>
								</li>
							{/each}
						</ol>
					{/if}
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Simulate modal -->
{#if showSimulate}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white">Simulate connection</h2>
			<form
				onsubmit={(event) => {
					event.preventDefault();
					void simulateConnection();
				}}
				class="mt-4 space-y-3"
			>
				<input
					required
					bind:value={simConnectionId}
					placeholder="Connection ID"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
				/>
				<input
					bind:value={simSourceIp}
					placeholder="Source IP"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
				/>
				<input
					bind:value={simUserAgent}
					placeholder="User agent"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
				/>
				<div class="flex justify-end gap-3">
					<button
						type="button"
						onclick={() => (showSimulate = false)}
						class="rounded-lg px-4 py-2 text-sm text-slate-600 dark:text-slate-300"
					>
						Cancel
					</button>
					<button
						type="submit"
						disabled={simulating}
						class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
					>
						{simulating ? 'Creating…' : 'Create'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Broadcast modal -->
{#if showBroadcast}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white">Broadcast to all connections</h2>
			<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">
				Each active connection receives a copy of this payload.
			</p>
			<form
				onsubmit={(event) => {
					event.preventDefault();
					void broadcastMessage();
				}}
				class="mt-4 space-y-3"
			>
				<textarea
					required
					rows="4"
					bind:value={broadcastBody}
					placeholder="Payload"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 font-mono text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
				></textarea>
				<div class="flex justify-end gap-3">
					<button
						type="button"
						onclick={() => (showBroadcast = false)}
						class="rounded-lg px-4 py-2 text-sm text-slate-600 dark:text-slate-300"
					>
						Cancel
					</button>
					<button
						type="submit"
						disabled={broadcasting}
						class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-medium text-white hover:bg-purple-700 disabled:opacity-50"
					>
						{broadcasting ? 'Sending…' : 'Broadcast'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Prune modal -->
{#if showPrune}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white">Prune idle connections</h2>
			<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">
				Disconnects every connection idle for at least the threshold below.
			</p>
			<form
				onsubmit={(event) => {
					event.preventDefault();
					void pruneIdle();
				}}
				class="mt-4 space-y-3"
			>
				<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-200">
					Idle for at least
					<input
						type="number"
						min="1"
						bind:value={pruneSeconds}
						class="w-24 rounded-lg border border-slate-300 px-2 py-1 dark:border-slate-600 dark:bg-slate-900 dark:text-white"
					/>
					seconds
				</label>
				<div class="flex justify-end gap-3">
					<button
						type="button"
						onclick={() => (showPrune = false)}
						class="rounded-lg px-4 py-2 text-sm text-slate-600 dark:text-slate-300"
					>
						Cancel
					</button>
					<button
						type="submit"
						disabled={pruning}
						class="rounded-lg bg-rose-600 px-4 py-2 text-sm font-medium text-white hover:bg-rose-700 disabled:opacity-50"
					>
						{pruning ? 'Pruning…' : 'Prune'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
