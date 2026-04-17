<script lang="ts">
	import { onMount } from 'svelte';
	import { getSQSClient } from '$lib/aws-client';
	import {
		ListQueuesCommand,
		GetQueueAttributesCommand,
		CreateQueueCommand,
		DeleteQueueCommand,
		SendMessageCommand,
		ReceiveMessageCommand,
		PurgeQueueCommand,
		type Message
	} from '@aws-sdk/client-sqs';
	import { toast } from 'svelte-sonner';
	import { MessageSquare, Search, RefreshCw, Plus, Trash2, Send, Inbox, Flame, ChevronDown, ChevronUp, Copy } from 'lucide-svelte';

	const sqs = getSQSClient();

	let loading = $state(false);
	let queues = $state<Array<{ url: string; attrs: Record<string, string> }>>([]);
	let searchQuery = $state('');
	let selectedQueue = $state<{ url: string; attrs: Record<string, string> } | null>(null);

	// Create queue modal
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newQueueName = $state('');
	let newQueueFifo = $state(false);
	let newVisibilityTimeout = $state(30);

	// Send message modal
	let showSendModal = $state(false);
	let sending = $state(false);
	let msgBody = $state('');
	let msgGroupId = $state('');
	let msgDelay = $state(0);

	// Receive messages
	let messages = $state<Message[]>([]);
	let receivingMessages = $state(false);
	let expandedMsg = $state<string | null>(null);

	const filteredQueues = $derived(
		queues.filter((q) => queueName(q.url).toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function queueName(url: string): string {
		return url.split('/').pop() ?? url;
	}

	function isFifo(url: string): boolean {
		return url.endsWith('.fifo');
	}

	function formatCount(n: string | undefined): string {
		return n ? parseInt(n).toLocaleString() : '0';
	}

	async function loadQueues() {
		loading = true;
		try {
			const res = await sqs.send(new ListQueuesCommand({ MaxResults: 100 }));
			const urls = res.QueueUrls ?? [];
			const enriched = await Promise.all(
				urls.slice(0, 30).map(async (url) => {
					try {
						const attrs = await sqs.send(new GetQueueAttributesCommand({
							QueueUrl: url,
							AttributeNames: ['All']
						}));
						return { url, attrs: attrs.Attributes ?? {} };
					} catch {
						return { url, attrs: {} };
					}
				})
			);
			queues = enriched;
		} catch (err: unknown) {
			toast.error(`Failed to load queues: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createQueue() {
		if (!newQueueName.trim()) return;
		creating = true;
		try {
			const name = newQueueFifo ? `${newQueueName.trim()}.fifo` : newQueueName.trim();
			const attrs: Record<string, string> = {
				VisibilityTimeout: String(newVisibilityTimeout)
			};
			if (newQueueFifo) {
				attrs.FifoQueue = 'true';
				attrs.ContentBasedDeduplication = 'true';
			}
			await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attrs }));
			toast.success(`Queue "${name}" created`);
			showCreateModal = false;
			newQueueName = '';
			newQueueFifo = false;
			newVisibilityTimeout = 30;
			await loadQueues();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteQueue(url: string) {
		const name = queueName(url);
		if (!confirm(`Delete queue "${name}"?`)) return;
		try {
			await sqs.send(new DeleteQueueCommand({ QueueUrl: url }));
			toast.success(`Queue "${name}" deleted`);
			if (selectedQueue?.url === url) { selectedQueue = null; messages = []; }
			await loadQueues();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function selectQueue(q: typeof queues[0]) {
		selectedQueue = q;
		messages = [];
	}

	async function sendMessage() {
		if (!selectedQueue || !msgBody.trim()) return;
		sending = true;
		try {
			await sqs.send(new SendMessageCommand({
				QueueUrl: selectedQueue.url,
				MessageBody: msgBody,
				MessageGroupId: isFifo(selectedQueue.url) ? (msgGroupId || 'default') : undefined,
				DelaySeconds: msgDelay > 0 ? msgDelay : undefined
			}));
			toast.success('Message sent');
			showSendModal = false;
			msgBody = '';
			msgGroupId = '';
			msgDelay = 0;
			// Refresh queue attrs to get new count
			const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: selectedQueue.url, AttributeNames: ['All'] }));
			selectedQueue = { ...selectedQueue, attrs: attrs.Attributes ?? {} };
		} catch (err: unknown) {
			toast.error(`Send failed: ${(err as Error).message}`);
		} finally {
			sending = false;
		}
	}

	async function receiveMessages() {
		if (!selectedQueue) return;
		receivingMessages = true;
		try {
			const res = await sqs.send(new ReceiveMessageCommand({
				QueueUrl: selectedQueue.url,
				MaxNumberOfMessages: 10,
				WaitTimeSeconds: 1,
				AttributeNames: ['All'],
				MessageAttributeNames: ['All']
			}));
			messages = res.Messages ?? [];
			if (messages.length === 0) {
				toast.info('No messages available');
			}
		} catch (err: unknown) {
			toast.error(`Receive failed: ${(err as Error).message}`);
		} finally {
			receivingMessages = false;
		}
	}

	async function purgeQueue(url: string) {
		if (!confirm(`Purge all messages from "${queueName(url)}"? This cannot be undone.`)) return;
		try {
			await sqs.send(new PurgeQueueCommand({ QueueUrl: url }));
			toast.success('Queue purged');
			messages = [];
		} catch (err: unknown) {
			toast.error(`Purge failed: ${(err as Error).message}`);
		}
	}

	async function copyUrl(url: string) {
		await navigator.clipboard.writeText(url);
		toast.success('URL copied');
	}

	onMount(() => { loadQueues(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg">
				<MessageSquare class="w-6 h-6 text-yellow-600 dark:text-yellow-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">SQS Queues</h1>
				<p class="text-slate-600 dark:text-slate-300">Simple Queue Service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadQueues()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button onclick={() => { showCreateModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Create Queue
			</button>
		</div>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input
			type="text"
			bind:value={searchQuery}
			placeholder="Search queues..."
			class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
		/>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Queue List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading queues...</p>
				</div>
			{:else if filteredQueues.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<MessageSquare class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No queues found</p>
				</div>
			{:else}
				{#each filteredQueues as q}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectQueue(q)}
						onkeypress={(e) => { if (e.key === 'Enter') selectQueue(q); }}
						class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-4 hover:border-indigo-400 transition-colors cursor-pointer {selectedQueue?.url === q.url ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
					>
						<div class="flex items-center justify-between">
							<div class="min-w-0 flex-1">
								<p class="font-medium text-slate-900 dark:text-white truncate">{queueName(q.url)}</p>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
									~{formatCount(q.attrs.ApproximateNumberOfMessages)} messages
								</p>
							</div>
							<div class="flex items-center gap-1 ml-2 flex-shrink-0">
								{#if isFifo(q.url)}
									<span class="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">FIFO</span>
								{/if}
								<button onclick={(e) => { e.stopPropagation(); deleteQueue(q.url); }} class="p-1 text-slate-400 hover:text-red-500">
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
						</div>
					</div>
				{/each}
			{/if}
		</div>

		<!-- Queue Detail -->
		<div class="lg:col-span-2">
			{#if selectedQueue}
				<div class="space-y-4">
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
						<div class="flex items-start justify-between mb-4">
							<div>
								<h2 class="text-xl font-bold text-slate-900 dark:text-white">{queueName(selectedQueue.url)}</h2>
								<button onclick={() => copyUrl(selectedQueue?.url ?? '')} class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500 mt-1 font-mono">
									<Copy class="w-3 h-3" />
									{selectedQueue.url}
								</button>
							</div>
							<div class="flex gap-2">
								<button onclick={() => { showSendModal = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
									<Send class="w-4 h-4" />Send
								</button>
								<button onclick={() => receiveMessages()} disabled={receivingMessages} class="px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-1.5 text-sm disabled:opacity-50">
									<Inbox class="w-4 h-4" />{receivingMessages ? '...' : 'Receive'}
								</button>
								<button onclick={() => purgeQueue(selectedQueue?.url ?? '')} class="px-3 py-1.5 bg-red-600 text-white rounded-lg hover:bg-red-700 flex items-center gap-1.5 text-sm">
									<Flame class="w-4 h-4" />Purge
								</button>
							</div>
						</div>
						<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
							{#each [
								['Messages Available', formatCount(selectedQueue.attrs.ApproximateNumberOfMessages)],
								['In Flight', formatCount(selectedQueue.attrs.ApproximateNumberOfMessagesNotVisible)],
								['Delayed', formatCount(selectedQueue.attrs.ApproximateNumberOfMessagesDelayed)],
								['Visibility Timeout', `${selectedQueue.attrs.VisibilityTimeout ?? 30}s`]
							] as [label, value]}
								<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
									<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
									<p class="font-semibold text-slate-900 dark:text-white mt-0.5">{value}</p>
								</div>
							{/each}
						</div>
					</div>

					<!-- Messages Panel -->
					{#if messages.length > 0}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
							<h3 class="font-semibold text-slate-900 dark:text-white mb-3">Received Messages ({messages.length})</h3>
							<div class="space-y-2">
								{#each messages as msg}
									<div class="border border-slate-200 dark:border-slate-600 rounded-lg overflow-hidden">
										<button
											onclick={() => { expandedMsg = expandedMsg === msg.MessageId ? null : (msg.MessageId ?? null); }}
											class="w-full flex items-center justify-between p-3 text-left hover:bg-slate-50 dark:hover:bg-slate-700/50"
										>
											<span class="font-mono text-xs text-slate-600 dark:text-slate-400 truncate">{msg.MessageId}</span>
											{#if expandedMsg === msg.MessageId}
												<ChevronUp class="w-4 h-4 text-slate-400 flex-shrink-0" />
											{:else}
												<ChevronDown class="w-4 h-4 text-slate-400 flex-shrink-0" />
											{/if}
										</button>
										{#if expandedMsg === msg.MessageId}
											<div class="border-t border-slate-200 dark:border-slate-600 p-3 bg-slate-50 dark:bg-slate-700/30">
												<pre class="text-xs text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-all">{msg.Body}</pre>
												<p class="text-xs text-slate-500 dark:text-slate-400 mt-2 font-mono truncate">Receipt: {msg.ReceiptHandle}</p>
											</div>
										{/if}
									</div>
								{/each}
							</div>
						</div>
					{/if}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<MessageSquare class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a queue to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Queue Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Queue</h2>
			<form onsubmit={(e) => { e.preventDefault(); createQueue(); }} class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Queue Name</label>
					<input
						type="text"
						bind:value={newQueueName}
						placeholder="e.g. order-processing"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						required
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Visibility Timeout (seconds)</label>
					<input
						type="number"
						bind:value={newVisibilityTimeout}
						min="0" max="43200"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
					/>
				</div>
				<label class="flex items-center gap-2 cursor-pointer">
					<input type="checkbox" bind:checked={newQueueFifo} class="rounded" />
					<span class="text-sm text-slate-700 dark:text-slate-300">FIFO queue</span>
				</label>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
					<button type="submit" disabled={creating} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creating ? 'Creating...' : 'Create Queue'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Send Message Modal -->
{#if showSendModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Send Message</h2>
			<form onsubmit={(e) => { e.preventDefault(); sendMessage(); }} class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message Body</label>
					<textarea
						bind:value={msgBody}
						rows={4}
						placeholder="Enter message body..."
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
						required
					></textarea>
				</div>
				{#if selectedQueue && isFifo(selectedQueue.url)}
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message Group ID</label>
						<input
							type="text"
							bind:value={msgGroupId}
							placeholder="e.g. order-group-1"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						/>
					</div>
				{/if}
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Delay Seconds</label>
					<input
						type="number"
						bind:value={msgDelay}
						min="0" max="900"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
					/>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showSendModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
					<button type="submit" disabled={sending} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
						<Send class="w-4 h-4" />
						{sending ? 'Sending...' : 'Send Message'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
