<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { ListOrdered, Plus, Trash2, Send, Eraser, Download, RefreshCw, X, Inbox, Copy } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		DEFAULT_ACCOUNT,
		DEFAULT_RECEIVE_VISIBILITY,
		MAX_MESSAGES,
		MAX_VISIBILITY_SECONDS,
		QueueError,
		clearMessages,
		createQueue,
		deleteMessage,
		deleteQueue,
		fromBase64,
		listQueues,
		peekMessages,
		putMessage,
		receiveMessages,
		toBase64,
		validateQueueName,
		validateSeconds,
		type QueueInfo,
		type QueueMessage
	} from '#lib/azurequeue.ts';

	const account = DEFAULT_ACCOUNT;

	let queues = $state<QueueInfo[]>([]);
	let current = $state<string | null>(null);
	let peeked = $state<QueueMessage[]>([]);
	let received = $state<QueueMessage[]>([]);
	let selected = $state<QueueMessage | null>(null);
	let loading = $state(true);
	let loadingMessages = $state(false);
	let loadError = $state('');

	let queueModal = $state<Modal | null>(null);
	let queueName = $state('');
	let queueTouched = $state(false);
	let creating = $state(false);

	let sendModal = $state<Modal | null>(null);
	let sendText = $state('');
	let sendBase64 = $state(false);
	let sendVisibility = $state('');
	let sendTtl = $state('');
	let sending = $state(false);

	let recvCount = $state('1');
	let recvVisibility = $state(String(DEFAULT_RECEIVE_VISIBILITY));
	let receiving = $state(false);

	const nameError = $derived(validateQueueName(queueName.trim()));
	const visError = $derived(validateSeconds(sendVisibility, 0, MAX_VISIBILITY_SECONDS));
	const ttlError = $derived(validateSeconds(sendTtl, 1, MAX_VISIBILITY_SECONDS, true));
	const countError = $derived(validateSeconds(recvCount, 1, MAX_MESSAGES));
	const recvVisError = $derived(validateSeconds(recvVisibility, 1, MAX_VISIBILITY_SECONDS));
	const sendBlocked = $derived(Boolean(visError || ttlError) || sending);
	const decoded = $derived(selected ? fromBase64(selected.text) : null);

	function describe(e: unknown): string {
		if (e instanceof QueueError) return e.code ? `${e.code}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	async function loadQueues() {
		loading = true;
		loadError = '';
		try {
			queues = (await listQueues(account)).toSorted((a, b) => a.name.localeCompare(b.name));
			if (current && !queues.some((q) => q.name === current)) selectQueue(null);
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load queues: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadMessages() {
		if (!current) return;
		loadingMessages = true;
		try {
			peeked = await peekMessages(account, current);
			if (selected && !peeked.some((m) => m.id === selected?.id) && !received.some((m) => m.id === selected?.id)) selected = null;
		} catch (e) {
			toast.error(`Failed to peek messages: ${describe(e)}`);
		} finally {
			loadingMessages = false;
		}
	}

	async function refresh() {
		await loadQueues();
		await loadMessages();
	}

	function selectQueue(name: string | null) {
		current = name;
		peeked = [];
		received = [];
		selected = null;
		if (name) void loadMessages();
	}

	function openNewQueue() {
		queueName = '';
		queueTouched = false;
		queueModal?.open();
	}

	async function submitQueue(event: Event) {
		event.preventDefault();
		queueTouched = true;
		if (nameError) return;
		creating = true;
		const name = queueName.trim();
		try {
			const created = await createQueue(account, name);
			toast.success(created ? `Created queue ${name}` : `Queue ${name} already exists`);
			queueModal?.close('created');
			await loadQueues();
			selectQueue(name);
		} catch (e) {
			toast.error(`Failed to create ${name}: ${describe(e)}`);
		} finally {
			creating = false;
		}
	}

	async function removeQueue(q: QueueInfo) {
		if (!(await confirmDestructive({ title: 'Delete queue', message: `Delete queue "${q.name}" and all of its messages? This cannot be undone.` }))) return;
		try {
			await deleteQueue(account, q.name);
			toast.success(`Deleted queue ${q.name}`);
			if (current === q.name) selectQueue(null);
			await loadQueues();
		} catch (e) {
			toast.error(`Failed to delete ${q.name}: ${describe(e)}`);
		}
	}

	function openSend() {
		sendText = '';
		sendBase64 = false;
		sendVisibility = '';
		sendTtl = '';
		sendModal?.open();
	}

	async function submitSend(event: Event) {
		event.preventDefault();
		if (!current || sendBlocked) return;
		sending = true;
		const body = sendBase64 ? toBase64(sendText) : sendText;
		try {
			await putMessage(account, current, body, {
				visibilityTimeout: sendVisibility.trim() === '' ? undefined : Number(sendVisibility),
				ttl: sendTtl.trim() === '' ? undefined : Number(sendTtl)
			});
			toast.success('Message sent');
			sendModal?.close('sent');
			await loadMessages();
		} catch (e) {
			toast.error(`Failed to send message: ${describe(e)}`);
		} finally {
			sending = false;
		}
	}

	async function receive() {
		if (!current || countError || recvVisError) return;
		receiving = true;
		try {
			const got = await receiveMessages(account, current, Number(recvCount || '1'), Number(recvVisibility || DEFAULT_RECEIVE_VISIBILITY));
			received = [...got, ...received];
			toast.success(got.length === 0 ? 'No visible messages to receive' : `Received ${got.length} message${got.length === 1 ? '' : 's'}`);
			await loadMessages();
		} catch (e) {
			toast.error(`Failed to receive messages: ${describe(e)}`);
		} finally {
			receiving = false;
		}
	}

	async function removeMessage(m: QueueMessage) {
		if (!current) return;
		try {
			await deleteMessage(account, current, m.id, m.popReceipt);
			toast.success(`Deleted message ${m.id}`);
			received = received.filter((r) => r.id !== m.id);
			if (selected?.id === m.id) selected = null;
			await loadMessages();
		} catch (e) {
			toast.error(`Failed to delete message: ${describe(e)}`);
		}
	}

	async function clearAll() {
		if (!current) return;
		if (!(await confirmDestructive({ title: 'Clear queue', message: `Delete every message in "${current}"? This cannot be undone.` }))) return;
		try {
			await clearMessages(account, current);
			toast.success(`Cleared ${current}`);
			received = [];
			selected = null;
			await loadMessages();
		} catch (e) {
			toast.error(`Failed to clear ${current}: ${describe(e)}`);
		}
	}

	async function copy(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const th = 'px-4 py-3 font-medium';
	const td = 'px-4 py-3 text-gray-600 dark:text-gray-400';

	onMount(loadQueues);
</script>

<div class="space-y-6 pb-20" data-testid="queue-dashboard">
	<PageHeader
		icon={ListOrdered}
		title="Queue Storage"
		description="Azure Queue Storage: queues and messages"
		color="blue"
		onRefresh={refresh}
	>
		{#snippet actions()}
			<button
				class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
				onclick={openNewQueue}
				disabled={loading || Boolean(loadError)}
			>
				<Plus class="h-4 w-4" /> New queue
			</button>
		{/snippet}
	</PageHeader>

	<p class="text-xs text-gray-500 dark:text-gray-400">
		Storage account <span class="font-mono" data-testid="queue-account">{account}</span>
	</p>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="queue-error">
			<p class="font-medium">Could not reach Azure Queue Storage</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadQueues}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<section aria-label="Queues" class="space-y-2">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Queues</h2>
				{#if loading}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if queues.length === 0}
					<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="queue-none">
						<Inbox class="h-10 w-10 mb-2 opacity-30" />
						<p class="text-sm">No queues</p>
						<p class="text-xs">Create a queue to get started</p>
					</div>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
						{#each queues as q (q.name)}
							<li class="flex items-center {current === q.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="queue-row">
								<button class="flex-1 truncate px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectQueue(q.name)}>
									{q.name}
								</button>
								<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {q.name}" aria-label="Delete queue {q.name}" onclick={() => removeQueue(q)}>
									<Trash2 class="h-4 w-4" />
								</button>
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<section aria-label="Messages" class="space-y-4 min-w-0">
				{#if !current}
					<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="queue-select-hint">
						<ListOrdered class="h-12 w-12 mb-3 opacity-30" />
						<p>Select a queue to view its messages</p>
					</div>
				{:else}
					<div class="flex flex-wrap items-center justify-between gap-2">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white break-all" data-testid="queue-current">{current}</h2>
						<div class="flex flex-wrap items-center gap-2">
							<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700" onclick={openSend}>
								<Send class="h-4 w-4" /> Send message
							</button>
							<button class="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950" onclick={clearAll}>
								<Eraser class="h-4 w-4" /> Clear queue
							</button>
						</div>
					</div>

					<div>
						<h3 class="mb-2 text-sm font-semibold text-gray-700 dark:text-gray-300">Peek (visible messages, oldest first, up to {MAX_MESSAGES})</h3>
						{#if loadingMessages && peeked.length === 0}
							<div class="flex justify-center py-6"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
						{:else if peeked.length === 0}
							<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="queue-empty">
								<Inbox class="h-10 w-10 mb-2 opacity-30" />
								<p class="text-sm">No visible messages</p>
								<p class="text-xs">Send a message, or wait for hidden ones to become visible</p>
							</div>
						{:else}
							<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
								<table class="w-full text-sm">
									<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
										<tr>
											<th class={th}>Message ID</th>
											<th class={th}>Text</th>
											<th class={th}>Dequeues</th>
											<th class={th}>Inserted</th>
											<th class={th}>Expires</th>
										</tr>
									</thead>
									<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
										{#each peeked as m (m.id)}
											<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {selected?.id === m.id ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="queue-message-row" onclick={() => (selected = m)}>
												<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-gray-100">{m.id}</td>
												<td class="{td} max-w-xs truncate">{m.text}</td>
												<td class={td}>{m.dequeueCount ?? '—'}</td>
												<td class={td}>{m.insertionTime}</td>
												<td class={td}>{m.expirationTime}</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>

					<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-4 space-y-3">
						<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Receive</h3>
						<p class="text-xs text-gray-500 dark:text-gray-400">Receiving hides messages for the visibility timeout and returns the pop receipt needed to delete them.</p>
						<div class="flex flex-wrap items-end gap-3">
							<label class="space-y-1 text-sm">
								<span class="block font-medium text-gray-700 dark:text-gray-300">Count (1-{MAX_MESSAGES})</span>
								<input class="{input} w-28" bind:value={recvCount} inputmode="numeric" aria-label="Receive count" />
							</label>
							<label class="space-y-1 text-sm">
								<span class="block font-medium text-gray-700 dark:text-gray-300">Visibility timeout (s)</span>
								<input class="{input} w-40" bind:value={recvVisibility} inputmode="numeric" aria-label="Receive visibility timeout" />
							</label>
							<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={receive} disabled={receiving || Boolean(countError || recvVisError)}>
								<Download class="h-4 w-4" /> {receiving ? 'Receiving…' : 'Receive'}
							</button>
						</div>
						{#if countError}<p class="text-xs text-red-600 dark:text-red-400">Count: {countError}</p>{/if}
						{#if recvVisError}<p class="text-xs text-red-600 dark:text-red-400">Visibility timeout: {recvVisError}</p>{/if}

						{#if received.length > 0}
							<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
								<table class="w-full text-sm" data-testid="queue-received">
									<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
										<tr>
											<th class={th}>Message ID</th>
											<th class={th}>Text</th>
											<th class={th}>Dequeues</th>
											<th class={th}>Next visible</th>
											<th class="{th} text-right">Actions</th>
										</tr>
									</thead>
									<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
										{#each received as m (m.id + m.popReceipt)}
											<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {selected?.id === m.id ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="queue-received-row" onclick={() => (selected = m)}>
												<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-gray-100">{m.id}</td>
												<td class="{td} max-w-xs truncate">{m.text}</td>
												<td class={td}>{m.dequeueCount ?? '—'}</td>
												<td class={td}>{m.timeNextVisible}</td>
												<td class="px-4 py-3 text-right">
													<button class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {m.id}" aria-label="Delete message {m.id}" onclick={(ev) => { ev.stopPropagation(); void removeMessage(m); }}><Trash2 class="h-4 w-4" /></button>
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>

					{#if selected}
						<section class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-4" data-testid="queue-message-detail" aria-label="Message {selected.id}">
							<div class="flex items-start justify-between gap-4">
								<div class="min-w-0">
									<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">Message {selected.id}</h3>
									<p class="text-xs text-gray-500 dark:text-gray-400">{selected.popReceipt ? 'Received (hidden until the visibility timeout elapses)' : 'Peeked (no pop receipt)'}</p>
								</div>
								<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (selected = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
							</div>
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
								<dt class={dt}>Dequeue count</dt><dd class={dd}>{selected.dequeueCount ?? '—'}</dd>
								<dt class={dt}>Inserted</dt><dd class={dd}>{selected.insertionTime || '—'}</dd>
								<dt class={dt}>Expires</dt><dd class={dd}>{selected.expirationTime || '—'}</dd>
								<dt class={dt}>Next visible</dt><dd class={dd}>{selected.timeNextVisible || '—'}</dd>
								<dt class={dt}>Pop receipt</dt><dd class="{dd} font-mono text-xs">{selected.popReceipt || '—'}</dd>
								<dt class={dt}>Text</dt>
								<dd class="{dd} flex items-start gap-2">
									<pre class="whitespace-pre-wrap font-mono text-xs" data-testid="queue-message-text">{selected.text}</pre>
									<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy message text" onclick={() => selected && copy(selected.text)}><Copy class="h-3.5 w-3.5" /></button>
								</dd>
								{#if decoded !== null}
									<dt class={dt}>Decoded (base64)</dt><dd class="{dd}"><pre class="whitespace-pre-wrap font-mono text-xs" data-testid="queue-message-decoded">{decoded}</pre></dd>
								{/if}
							</dl>
							{#if selected.popReceipt}
								<div class="flex gap-2">
									<button class="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950" onclick={() => selected && removeMessage(selected)}><Trash2 class="h-4 w-4" /> Delete message</button>
								</div>
							{/if}
						</section>
					{/if}

					<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="queue-unsupported-note">
						Queue metadata, approximate message count, access policies and updating a message in place are not modeled or exposed by this page. Message text is stored exactly as sent (SDKs normally base64-encode it).
					</p>
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={queueModal} title="New queue" titleId="queue-create-title">
	{#snippet children()}
		<form id="queue-create-form" class="space-y-3" onsubmit={submitQueue} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Name</span>
				<input class={input} bind:value={queueName} placeholder="my-queue" autocomplete="off" aria-label="Name" />
				{#if queueTouched && nameError}<span class="block text-xs text-red-600 dark:text-red-400">{nameError}</span>{/if}
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => queueModal?.close()}>Cancel</button>
		<button type="submit" form="queue-create-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={creating}>
			{creating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<Modal bind:this={sendModal} title="Send message" titleId="queue-send-title">
	{#snippet children()}
		<form id="queue-send-form" class="space-y-3" onsubmit={submitSend} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Message text</span>
				<textarea class={input} rows="4" bind:value={sendText} aria-label="Message text"></textarea>
			</label>
			<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
				<input type="checkbox" bind:checked={sendBase64} aria-label="Base64-encode" /> Base64-encode the text before sending
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Visibility timeout (seconds, optional)</span>
				<input class={input} bind:value={sendVisibility} placeholder="0" inputmode="numeric" aria-label="Visibility timeout" />
				{#if visError}<span class="block text-xs text-red-600 dark:text-red-400">{visError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Time to live (seconds, -1 = never, optional)</span>
				<input class={input} bind:value={sendTtl} placeholder="604800" inputmode="numeric" aria-label="Time to live" />
				{#if ttlError}<span class="block text-xs text-red-600 dark:text-red-400">{ttlError}</span>{/if}
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => sendModal?.close()}>Cancel</button>
		<button type="submit" form="queue-send-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={sendBlocked}>
			{sending ? 'Sending…' : 'Send'}
		</button>
	{/snippet}
</Modal>
