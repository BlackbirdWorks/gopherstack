<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';

	type Connection = {
		connectionId: string;
		sourceIp: string;
		userAgent: string;
		connectedAt: string;
		lastActiveAt: string;
		postedMessages: number;
	};

	type PostedMessage = {
		connectionId: string;
		receivedAt: string;
		// Backend serialises message bodies as base64-encoded JSON.
		data: string;
	};

	const supportedOperations = ['PostToConnection', 'GetConnection', 'DeleteConnection'];
	const apiBase = '/dashboard/api/apigatewaymanagementapi';

	let loading = $state(false);
	let connections = $state<Connection[]>([]);
	let selectedId = $state<string | null>(null);
	let messages = $state<PostedMessage[]>([]);
	let messagesLoading = $state(false);

	let showCreateModal = $state(false);
	let newConnectionId = $state('');
	let newSourceIp = $state('');
	let newUserAgent = $state('');

	let messageBody = $state('');
	let messageSending = $state(false);

	function formatTime(iso: string): string {
		if (!iso) return '—';
		const d = new Date(iso);
		if (Number.isNaN(d.getTime())) return iso;
		return d.toLocaleString();
	}

	function relativeAge(iso: string): string {
		if (!iso) return '—';
		const ageMs = Date.now() - new Date(iso).getTime();
		if (Number.isNaN(ageMs) || ageMs < 0) return '—';
		const seconds = Math.floor(ageMs / 1000);
		if (seconds < 60) return `${seconds}s ago`;
		const minutes = Math.floor(seconds / 60);
		if (minutes < 60) return `${minutes}m ago`;
		const hours = Math.floor(minutes / 60);
		if (hours < 24) return `${hours}h ago`;
		const days = Math.floor(hours / 24);
		return `${days}d ago`;
	}

	function decodeMessage(data: string): string {
		if (!data) return '';
		try {
			return atob(data);
		} catch {
			return data;
		}
	}

	async function loadConnections() {
		loading = true;
		try {
			const response = await fetch(`${apiBase}/connections`);
			const body = (await response.json()) as { connections?: Connection[] };
			connections = (body.connections ?? []).toSorted(
				(a, b) => new Date(b.connectedAt).getTime() - new Date(a.connectedAt).getTime()
			);
		} catch (err) {
			toast.error(`Failed to load connections: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadMessages(connectionId: string) {
		messagesLoading = true;
		try {
			const response = await fetch(`${apiBase}/connections/${encodeURIComponent(connectionId)}/messages`);
			const body = (await response.json()) as { messages?: PostedMessage[] };
			messages = body.messages ?? [];
		} catch (err) {
			toast.error(`Failed to load messages: ${(err as Error).message}`);
		} finally {
			messagesLoading = false;
		}
	}

	async function selectConnection(connectionId: string) {
		selectedId = connectionId;
		messageBody = '';
		await loadMessages(connectionId);
	}

	async function createConnection() {
		const id = newConnectionId.trim();
		if (!id) {
			toast.error('Connection ID is required');
			return;
		}
		try {
			const response = await fetch(`${apiBase}/connections`, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					connectionId: id,
					sourceIp: newSourceIp.trim() || undefined,
					userAgent: newUserAgent.trim() || undefined
				})
			});
			if (!response.ok) {
				throw new Error(((await response.json()) as { error?: string }).error ?? 'request failed');
			}
			toast.success(`Connection ${id} created`);
			showCreateModal = false;
			newConnectionId = '';
			newSourceIp = '';
			newUserAgent = '';
			await loadConnections();
			await selectConnection(id);
		} catch (err) {
			toast.error(`Failed to create connection: ${(err as Error).message}`);
		}
	}

	async function deleteConnection(connectionId: string) {
		const ok = await confirmDestructive({
			title: 'Delete connection',
			message: `Delete connection ${connectionId}? Message history will be lost.`
		});
		if (!ok) return;
		try {
			const response = await fetch(`${apiBase}/connections/${encodeURIComponent(connectionId)}`, {
				method: 'DELETE'
			});
			if (!response.ok && response.status !== 204) {
				throw new Error(((await response.json()) as { error?: string }).error ?? 'request failed');
			}
			toast.success(`Connection ${connectionId} deleted`);
			if (selectedId === connectionId) {
				selectedId = null;
				messages = [];
			}
			await loadConnections();
		} catch (err) {
			toast.error(`Failed to delete connection: ${(err as Error).message}`);
		}
	}

	async function sendMessage() {
		if (!selectedId) return;
		const body = messageBody;
		if (!body) {
			toast.error('Message body is empty');
			return;
		}
		messageSending = true;
		try {
			const response = await fetch(
				`${apiBase}/connections/${encodeURIComponent(selectedId)}/messages`,
				{
					method: 'POST',
					headers: { 'Content-Type': 'application/json' },
					body: JSON.stringify({ data: body })
				}
			);
			if (!response.ok && response.status !== 202) {
				throw new Error(((await response.json()) as { error?: string }).error ?? 'request failed');
			}
			toast.success('Message posted');
			messageBody = '';
			await loadMessages(selectedId);
			await loadConnections();
		} catch (err) {
			toast.error(`Failed to post message: ${(err as Error).message}`);
		} finally {
			messageSending = false;
		}
	}

	const selectedConnection = $derived(
		selectedId ? connections.find((c) => c.connectionId === selectedId) : undefined
	);

	const totalMessages = $derived(connections.reduce((sum, c) => sum + (c.postedMessages ?? 0), 0));

	onMount(() => {
		void loadConnections();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">API Gateway Management API</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
			Send WebSocket frames to simulated connections, inspect per-connection message history, and
			watch lifecycle activity.
		</p>
	</div>

	<div
		class="grid gap-4 rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 sm:grid-cols-3"
	>
		<div>
			<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Active connections</div>
			<div class="mt-1 text-2xl font-semibold text-slate-900 dark:text-white">{connections.length}</div>
		</div>
		<div>
			<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Total messages posted</div>
			<div class="mt-1 text-2xl font-semibold text-slate-900 dark:text-white">{totalMessages}</div>
		</div>
		<div>
			<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Supported Operations</div>
			<div class="mt-2 flex flex-wrap gap-2">
				{#each supportedOperations as operation}
					<span
						class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700 dark:bg-slate-700 dark:text-slate-200"
					>
						{operation}
					</span>
				{/each}
			</div>
		</div>
	</div>

	<div class="grid gap-6 lg:grid-cols-[2fr_3fr]">
		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800"
		>
			<div class="mb-4 flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Connections</h2>
				<div class="flex gap-2">
					<button
						type="button"
						onclick={loadConnections}
						class="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
					>
						Refresh
					</button>
					<button
						type="button"
						onclick={() => (showCreateModal = true)}
						class="rounded-lg bg-indigo-600 px-3 py-1.5 text-xs font-medium text-white hover:bg-indigo-700"
					>
						+ Simulate Connection
					</button>
				</div>
			</div>

			{#if loading}
				<p class="text-sm text-slate-500 dark:text-slate-400">Loading connections…</p>
			{:else if connections.length === 0}
				<p class="text-sm text-slate-500 dark:text-slate-400">
					No simulated connections — click <em>Simulate Connection</em> to add one.
				</p>
			{:else}
				<ul class="space-y-2">
					{#each connections as connection (connection.connectionId)}
						<li>
							<button
								type="button"
								onclick={() => selectConnection(connection.connectionId)}
								class="w-full rounded-lg border px-3 py-3 text-left transition hover:bg-slate-50 dark:hover:bg-slate-700 {selectedId ===
								connection.connectionId
									? 'border-indigo-500 bg-indigo-50 dark:border-indigo-400 dark:bg-indigo-900/30'
									: 'border-slate-200 dark:border-slate-700'}"
							>
								<div class="flex items-center justify-between">
									<span class="font-medium text-slate-900 dark:text-white">
										{connection.connectionId}
									</span>
									<span
										class="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-700 dark:bg-slate-700 dark:text-slate-200"
									>
										{connection.postedMessages} msg
									</span>
								</div>
								<div class="mt-1 text-xs text-slate-500 dark:text-slate-400">
									{connection.sourceIp} · {connection.userAgent}
								</div>
								<div class="mt-1 text-xs text-slate-400 dark:text-slate-500">
									Last active {relativeAge(connection.lastActiveAt)}
								</div>
							</button>
						</li>
					{/each}
				</ul>
			{/if}
		</div>

		<div
			class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800"
		>
			{#if !selectedConnection}
				<div class="flex h-full flex-col items-center justify-center text-center">
					<p class="text-sm text-slate-500 dark:text-slate-400">
						Select a connection on the left to send messages and view history.
					</p>
				</div>
			{:else}
				<div class="mb-4 flex items-start justify-between gap-4">
					<div>
						<h2 class="text-lg font-semibold text-slate-900 dark:text-white">
							{selectedConnection.connectionId}
						</h2>
						<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">
							{selectedConnection.sourceIp} · {selectedConnection.userAgent}
						</p>
					</div>
					<button
						type="button"
						onclick={() => deleteConnection(selectedConnection.connectionId)}
						class="rounded-lg border border-rose-300 px-3 py-1.5 text-xs font-medium text-rose-600 hover:bg-rose-50 dark:border-rose-700 dark:text-rose-300 dark:hover:bg-rose-900/30"
					>
						Delete
					</button>
				</div>

				<section class="mb-6">
					<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-200">Lifecycle</h3>
					<ol class="space-y-2 border-l-2 border-slate-200 pl-4 dark:border-slate-700">
						<li>
							<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Connected</div>
							<div class="text-sm text-slate-900 dark:text-white">
								{formatTime(selectedConnection.connectedAt)}
							</div>
						</li>
						<li>
							<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Last active</div>
							<div class="text-sm text-slate-900 dark:text-white">
								{formatTime(selectedConnection.lastActiveAt)} ({relativeAge(
									selectedConnection.lastActiveAt
								)})
							</div>
						</li>
						<li>
							<div class="text-xs uppercase text-slate-500 dark:text-slate-400">Posted messages</div>
							<div class="text-sm text-slate-900 dark:text-white">
								{selectedConnection.postedMessages}
							</div>
						</li>
					</ol>
				</section>

				<section class="mb-6">
					<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-200">Send Message</h3>
					<form
						onsubmit={(event) => {
							event.preventDefault();
							void sendMessage();
						}}
						class="space-y-2"
					>
						<textarea
							bind:value={messageBody}
							rows="4"
							placeholder={'{"action":"echo","payload":"hello"}'}
							class="w-full rounded-lg border border-slate-300 px-3 py-2 font-mono text-xs dark:border-slate-600 dark:bg-slate-900 dark:text-white"
						></textarea>
						<div class="flex items-center justify-between">
							<span class="text-xs text-slate-500 dark:text-slate-400"
								>Up to 128KB per frame</span
							>
							<button
								type="submit"
								disabled={messageSending || messageBody.length === 0}
								class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-60"
							>
								{messageSending ? 'Sending…' : 'PostToConnection'}
							</button>
						</div>
					</form>
				</section>

				<section>
					<div class="mb-2 flex items-center justify-between">
						<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">
							Message History ({messages.length})
						</h3>
						<button
							type="button"
							onclick={() => selectedId && loadMessages(selectedId)}
							class="text-xs text-indigo-600 hover:underline dark:text-indigo-400"
						>
							Refresh
						</button>
					</div>
					{#if messagesLoading}
						<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
					{:else if messages.length === 0}
						<p class="text-sm text-slate-500 dark:text-slate-400">
							No messages yet. Use the form above to send one.
						</p>
					{:else}
						<ul class="max-h-96 space-y-2 overflow-y-auto pr-1">
							{#each messages.slice().reverse() as message, idx (idx)}
								<li
									class="rounded-lg border border-slate-200 bg-slate-50 p-3 text-xs dark:border-slate-700 dark:bg-slate-900"
								>
									<div class="mb-1 flex items-center justify-between">
										<span class="text-slate-500 dark:text-slate-400">
											{formatTime(message.receivedAt)}
										</span>
										<span class="text-slate-400 dark:text-slate-500">
											{relativeAge(message.receivedAt)}
										</span>
									</div>
									<pre
										class="whitespace-pre-wrap break-words font-mono text-slate-900 dark:text-slate-100">{decodeMessage(
											message.data
										)}</pre>
								</li>
							{/each}
						</ul>
					{/if}
				</section>
			{/if}
		</div>
	</div>
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white">Simulate Connection</h2>
			<form
				onsubmit={(event) => {
					event.preventDefault();
					void createConnection();
				}}
				class="mt-4 space-y-3"
			>
				<label class="block">
					<span class="block text-xs font-medium text-slate-700 dark:text-slate-200"
						>Connection ID</span
					>
					<input
						bind:value={newConnectionId}
						required
						class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
						placeholder="e.g. conn-001"
					/>
				</label>
				<label class="block">
					<span class="block text-xs font-medium text-slate-700 dark:text-slate-200"
						>Source IP <span class="text-slate-400">(optional)</span></span
					>
					<input
						bind:value={newSourceIp}
						class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
						placeholder="127.0.0.1"
					/>
				</label>
				<label class="block">
					<span class="block text-xs font-medium text-slate-700 dark:text-slate-200"
						>User Agent <span class="text-slate-400">(optional)</span></span
					>
					<input
						bind:value={newUserAgent}
						class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
						placeholder="Gopherstack UI"
					/>
				</label>
				<div class="flex justify-end gap-3 pt-2">
					<button
						type="button"
						onclick={() => (showCreateModal = false)}
						class="rounded-lg px-4 py-2 text-sm text-slate-600 dark:text-slate-300"
						>Cancel</button
					>
					<button
						type="submit"
						class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
						>Create</button
					>
				</div>
			</form>
		</div>
	</div>
{/if}
