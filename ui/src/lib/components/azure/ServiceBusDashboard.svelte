<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Radio, Plus, Trash2, Send, Download, RefreshCw, X, Inbox, Copy, Undo2, Check, RotateCcw, Layers } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		BusError,
		MAX_LOCK_SECONDS,
		MAX_RECEIVE_BATCH,
		MAX_TTL_SECONDS,
		abandonMessage,
		completeMessage,
		createQueue,
		createSubscription,
		createTopic,
		deleteEntity,
		deleteSubscription,
		formatDuration,
		listQueues,
		listSubscriptions,
		listTopics,
		receiveAndDelete,
		receiveMessages,
		resubmit,
		sendMessage,
		validateEntityName,
		validateMessageId,
		validateWhole,
		type EntityConfig,
		type QueueInfo,
		type ReceivedMessage,
		type SubscriptionInfo,
		type Target,
		type TopicInfo
	} from '#lib/azureservicebus.ts';

	type Selection =
		| { kind: 'queue'; name: string }
		| { kind: 'topic'; name: string }
		| { kind: 'sub'; topic: string; name: string };
	type CreateKind = 'queue' | 'topic' | 'sub';

	let queues = $state<QueueInfo[]>([]);
	let topics = $state<TopicInfo[]>([]);
	let subs = $state<Record<string, SubscriptionInfo[]>>({});
	let sel = $state<Selection | null>(null);
	let deadLetter = $state(false);
	let received = $state<ReceivedMessage[]>([]);
	let selected = $state<ReceivedMessage | null>(null);
	let loading = $state(true);
	let loadError = $state('');

	let createModal = $state<Modal | null>(null);
	let createKind = $state<CreateKind>('queue');
	let createTopicName = $state('');
	let createName = $state('');
	let createLock = $state('');
	let createMax = $state('');
	let createTtl = $state('');
	let createTouched = $state(false);
	let creating = $state(false);

	let sendModal = $state<Modal | null>(null);
	let sendBody = $state('');
	let sendContentType = $state('text/plain');
	let sendMessageId = $state('');
	let sendLabel = $state('');
	let sendCorrelation = $state('');
	let sendReplyTo = $state('');
	let sendSession = $state('');
	let sendTtl = $state('');
	let sending = $state(false);

	let recvCount = $state('1');
	let busy = $state(false);

	const target = $derived<Target | null>(
		sel === null
			? null
			: sel.kind === 'sub'
				? { entity: sel.topic, subscription: sel.name, deadLetter }
				: sel.kind === 'queue'
					? { entity: sel.name, deadLetter }
					: null
	);
	const canReceive = $derived(target !== null);
	const queueInfo = $derived(sel?.kind === 'queue' ? (queues.find((q) => q.name === sel?.name) ?? null) : null);
	const topicInfo = $derived(sel?.kind === 'topic' ? (topics.find((t) => t.name === sel?.name) ?? null) : null);
	const subInfo = $derived(
		sel?.kind === 'sub' ? ((subs[sel.topic] ?? []).find((s) => s.name === sel?.name) ?? null) : null
	);
	const sendTarget = $derived(sel?.kind === 'queue' || sel?.kind === 'topic' ? sel.name : null);

	const nameError = $derived(validateEntityName(createName.trim()));
	const lockError = $derived(validateWhole(createLock, 1, MAX_LOCK_SECONDS));
	const maxError = $derived(validateWhole(createMax, 1, 1000));
	const ttlError = $derived(validateWhole(createTtl, 1, MAX_TTL_SECONDS));
	const createBlocked = $derived(Boolean(lockError || maxError || ttlError) || creating);
	const sendTtlError = $derived(validateWhole(sendTtl, 1, MAX_TTL_SECONDS));
	const sendIdError = $derived(validateMessageId(sendMessageId.trim()));
	const sendBlocked = $derived(Boolean(sendTtlError || sendIdError) || sending);
	const countError = $derived(validateWhole(recvCount, 1, MAX_RECEIVE_BATCH));

	function describe(e: unknown): string {
		if (e instanceof BusError) return e.code ? `${e.code}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	async function loadAll() {
		loading = true;
		loadError = '';
		try {
			const [q, t] = await Promise.all([listQueues(), listTopics()]);
			queues = q.toSorted((a, b) => a.name.localeCompare(b.name));
			topics = t.toSorted((a, b) => a.name.localeCompare(b.name));
			const entries = await Promise.all(topics.map(async (x) => [x.name, (await listSubscriptions(x.name)).toSorted((a, b) => a.name.localeCompare(b.name))] as const));
			subs = Object.fromEntries(entries);
			if (sel && !exists(sel)) select(null);
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load Service Bus: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	function exists(s: Selection): boolean {
		if (s.kind === 'queue') return queues.some((q) => q.name === s.name);
		if (s.kind === 'topic') return topics.some((t) => t.name === s.name);
		return (subs[s.topic] ?? []).some((x) => x.name === s.name);
	}

	function select(next: Selection | null) {
		sel = next;
		deadLetter = false;
		received = [];
		selected = null;
	}

	function setDeadLetter(on: boolean) {
		deadLetter = on;
		received = [];
		selected = null;
	}

	function isSel(s: Selection): boolean {
		if (!sel || sel.kind !== s.kind) return false;
		if (s.kind === 'sub' && sel.kind === 'sub') return sel.topic === s.topic && sel.name === s.name;
		return sel.name === s.name;
	}

	function openCreate(kind: CreateKind, topic = '') {
		createKind = kind;
		createTopicName = topic;
		createName = '';
		createLock = '';
		createMax = '';
		createTtl = '';
		createTouched = false;
		createModal?.open();
	}

	async function submitCreate(event: Event) {
		event.preventDefault();
		createTouched = true;
		if (nameError || createBlocked) return;
		creating = true;
		const name = createName.trim();
		const cfg: EntityConfig = {};
		if (createLock.trim()) cfg.lockSeconds = Number(createLock);
		if (createMax.trim()) cfg.maxDeliveryCount = Number(createMax);
		if (createTtl.trim()) cfg.ttlSeconds = Number(createTtl);
		try {
			let created: boolean;
			let next: Selection;
			if (createKind === 'queue') {
				created = await createQueue(name, cfg);
				next = { kind: 'queue', name };
			} else if (createKind === 'topic') {
				created = await createTopic(name, cfg);
				next = { kind: 'topic', name };
			} else {
				created = await createSubscription(createTopicName, name, cfg);
				next = { kind: 'sub', topic: createTopicName, name };
			}
			toast.success(created ? `Created ${name}` : `${name} already exists`);
			createModal?.close('created');
			await loadAll();
			select(next);
		} catch (e) {
			toast.error(`Failed to create ${name}: ${describe(e)}`);
		} finally {
			creating = false;
		}
	}

	async function removeQueueOrTopic(kind: 'queue' | 'topic', name: string) {
		const what = kind === 'queue' ? 'queue' : 'topic and its subscriptions';
		if (!(await confirmDestructive({ title: `Delete ${kind}`, message: `Delete ${what} "${name}" and all of its messages? This cannot be undone.` }))) return;
		try {
			await deleteEntity(name);
			toast.success(`Deleted ${name}`);
			await loadAll();
		} catch (e) {
			toast.error(`Failed to delete ${name}: ${describe(e)}`);
		}
	}

	async function removeSubscription(topic: string, name: string) {
		if (!(await confirmDestructive({ title: 'Delete subscription', message: `Delete subscription "${name}" and its messages? This cannot be undone.` }))) return;
		try {
			await deleteSubscription(topic, name);
			toast.success(`Deleted subscription ${name}`);
			await loadAll();
		} catch (e) {
			toast.error(`Failed to delete ${name}: ${describe(e)}`);
		}
	}

	function openSend() {
		sendBody = '';
		sendContentType = 'text/plain';
		sendMessageId = '';
		sendLabel = '';
		sendCorrelation = '';
		sendReplyTo = '';
		sendSession = '';
		sendTtl = '';
		sendModal?.open();
	}

	async function submitSend(event: Event) {
		event.preventDefault();
		if (!sendTarget || sendBlocked) return;
		sending = true;
		try {
			await sendMessage(sendTarget, sendBody, {
				contentType: sendContentType.trim() || undefined,
				messageId: sendMessageId.trim() || undefined,
				label: sendLabel.trim() || undefined,
				correlationId: sendCorrelation.trim() || undefined,
				replyTo: sendReplyTo.trim() || undefined,
				sessionId: sendSession.trim() || undefined,
				ttlSeconds: sendTtl.trim() ? Number(sendTtl) : undefined
			});
			toast.success(sel?.kind === 'topic' ? `Sent to every subscription of ${sendTarget}` : 'Message sent');
			sendModal?.close('sent');
		} catch (e) {
			toast.error(`Failed to send message: ${describe(e)}`);
		} finally {
			sending = false;
		}
	}

	async function receive() {
		if (!target || countError) return;
		busy = true;
		try {
			const got = await receiveMessages(target, Number(recvCount || '1'));
			received = [...got, ...received];
			toast.success(got.length === 0 ? 'No messages available' : `Locked ${got.length} message${got.length === 1 ? '' : 's'}`);
		} catch (e) {
			toast.error(`Failed to receive: ${describe(e)}`);
		} finally {
			busy = false;
		}
	}

	async function receiveDelete() {
		if (!target) return;
		busy = true;
		try {
			const m = await receiveAndDelete(target);
			if (m) {
				selected = m;
				toast.success(`Received and deleted ${m.messageId}`);
			} else {
				toast.success('No messages available');
			}
		} catch (e) {
			toast.error(`Failed to receive: ${describe(e)}`);
		} finally {
			busy = false;
		}
	}

	function drop(m: ReceivedMessage) {
		received = received.filter((r) => r.lockToken !== m.lockToken);
		if (selected?.lockToken === m.lockToken) selected = null;
	}

	async function complete(m: ReceivedMessage) {
		if (!target) return;
		try {
			await completeMessage(target, m);
			toast.success(`Completed ${m.messageId}`);
			drop(m);
		} catch (e) {
			toast.error(`Failed to complete: ${describe(e)}`);
		}
	}

	async function abandon(m: ReceivedMessage) {
		if (!target) return;
		try {
			await abandonMessage(target, m);
			toast.success(`Abandoned ${m.messageId}; it is available again`);
			drop(m);
		} catch (e) {
			toast.error(`Failed to abandon: ${describe(e)}`);
		}
	}

	async function doResubmit(m: ReceivedMessage) {
		if (sel?.kind !== 'queue') return;
		try {
			await resubmit(sel.name, m);
			toast.success(`Resubmitted ${m.messageId} to ${sel.name}`);
			drop(m);
		} catch (e) {
			toast.error(`Failed to resubmit: ${describe(e)}`);
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
	const iconBtn = 'rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800';
	const rowBtn = 'flex-1 truncate px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50';

	onMount(loadAll);
</script>

<div class="space-y-6 pb-20" data-testid="servicebus-dashboard">
	<PageHeader icon={Radio} title="Service Bus" description="Azure Service Bus: queues, topics, subscriptions and messages" color="blue" onRefresh={loadAll}>
		{#snippet actions()}
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={() => openCreate('queue')} disabled={loading || Boolean(loadError)}>
				<Plus class="h-4 w-4" /> New queue
			</button>
			<button class="flex items-center gap-2 rounded-lg border border-blue-300 dark:border-blue-800 px-3 py-2 text-sm text-blue-700 dark:text-blue-300 hover:bg-blue-50 dark:hover:bg-blue-950 disabled:opacity-50" onclick={() => openCreate('topic')} disabled={loading || Boolean(loadError)}>
				<Plus class="h-4 w-4" /> New topic
			</button>
		{/snippet}
	</PageHeader>

	<p class="text-xs text-gray-500 dark:text-gray-400">
		The emulator serves a single implicit namespace; it is not linked to the Microsoft.ServiceBus namespaces in Resource Manager.
	</p>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="servicebus-error">
			<p class="font-medium">Could not reach Azure Service Bus</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadAll}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[20rem_1fr]">
			<div class="space-y-5">
				<section aria-label="Queues" class="space-y-2">
					<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Queues</h2>
					{#if loading}
						<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
					{:else if queues.length === 0}
						<div class="flex flex-col items-center py-6 text-gray-500 dark:text-gray-400" data-testid="servicebus-no-queues">
							<Inbox class="h-8 w-8 mb-2 opacity-30" />
							<p class="text-sm">No queues</p>
						</div>
					{:else}
						<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
							{#each queues as q (q.name)}
								<li class="flex items-center {isSel({ kind: 'queue', name: q.name }) ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="servicebus-queue-row">
									<button class={rowBtn} onclick={() => select({ kind: 'queue', name: q.name })}>{q.name}</button>
									<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {q.name}" aria-label="Delete queue {q.name}" onclick={() => removeQueueOrTopic('queue', q.name)}><Trash2 class="h-4 w-4" /></button>
								</li>
							{/each}
						</ul>
					{/if}
				</section>

				<section aria-label="Topics" class="space-y-2">
					<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Topics</h2>
					{#if loading}
						<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
					{:else if topics.length === 0}
						<div class="flex flex-col items-center py-6 text-gray-500 dark:text-gray-400" data-testid="servicebus-no-topics">
							<Layers class="h-8 w-8 mb-2 opacity-30" />
							<p class="text-sm">No topics</p>
						</div>
					{:else}
						<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
							{#each topics as t (t.name)}
								<li data-testid="servicebus-topic-row">
									<div class="flex items-center {isSel({ kind: 'topic', name: t.name }) ? 'bg-blue-50 dark:bg-blue-950/40' : ''}">
										<button class={rowBtn} onclick={() => select({ kind: 'topic', name: t.name })}>{t.name}</button>
										<button class={iconBtn} title="New subscription on {t.name}" aria-label="New subscription on {t.name}" onclick={() => openCreate('sub', t.name)}><Plus class="h-4 w-4" /></button>
										<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {t.name}" aria-label="Delete topic {t.name}" onclick={() => removeQueueOrTopic('topic', t.name)}><Trash2 class="h-4 w-4" /></button>
									</div>
									{#each subs[t.name] ?? [] as s (s.name)}
										<div class="flex items-center border-t border-gray-50 dark:border-gray-800/60 pl-4 {isSel({ kind: 'sub', topic: t.name, name: s.name }) ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="servicebus-sub-row">
											<button class="{rowBtn} font-normal" onclick={() => select({ kind: 'sub', topic: t.name, name: s.name })}>{s.name}</button>
											<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {s.name}" aria-label="Delete subscription {s.name}" onclick={() => removeSubscription(t.name, s.name)}><Trash2 class="h-4 w-4" /></button>
										</div>
									{/each}
								</li>
							{/each}
						</ul>
					{/if}
				</section>
			</div>

			<section aria-label="Entity" class="space-y-4 min-w-0">
				{#if !sel}
					<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="servicebus-select-hint">
						<Radio class="h-12 w-12 mb-3 opacity-30" />
						<p>Select a queue, topic or subscription</p>
					</div>
				{:else}
					<div class="flex flex-wrap items-center justify-between gap-2">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white break-all" data-testid="servicebus-current">
							{sel.kind === 'sub' ? `${sel.topic} / ${sel.name}` : sel.name}
							<span class="ml-2 rounded bg-gray-100 dark:bg-slate-800 px-2 py-0.5 text-xs font-normal text-gray-600 dark:text-gray-400">{sel.kind === 'sub' ? 'subscription' : sel.kind}</span>
						</h2>
						{#if sendTarget}
							<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700" onclick={openSend}>
								<Send class="h-4 w-4" /> Send message
							</button>
						{/if}
					</div>

					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 rounded-lg border border-gray-200 dark:border-gray-700 p-4 text-sm" data-testid="servicebus-props">
						{#if queueInfo}
							<dt class={dt}>Lock duration</dt><dd class={dd}>{formatDuration(queueInfo.lockSeconds)}</dd>
							<dt class={dt}>Max delivery count</dt><dd class={dd}>{queueInfo.maxDeliveryCount ?? '—'}</dd>
							<dt class={dt}>Default message TTL</dt><dd class={dd}>{formatDuration(queueInfo.ttlSeconds)}</dd>
						{:else if topicInfo}
							<dt class={dt}>Default message TTL</dt><dd class={dd}>{formatDuration(topicInfo.ttlSeconds)}</dd>
							<dt class={dt}>Subscriptions</dt><dd class={dd}>{(subs[topicInfo.name] ?? []).length}</dd>
						{:else if subInfo}
							<dt class={dt}>Lock duration</dt><dd class={dd}>{formatDuration(subInfo.lockSeconds)}</dd>
							<dt class={dt}>Max delivery count</dt><dd class={dd}>{subInfo.maxDeliveryCount ?? '—'}</dd>
						{/if}
					</dl>

					{#if sel.kind === 'topic'}
						<p class="text-sm text-gray-600 dark:text-gray-400" data-testid="servicebus-topic-hint">
							Messages sent to a topic are copied to every subscription. Select a subscription to receive them.
						</p>
					{:else if canReceive}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-4 space-y-3">
							<div class="flex flex-wrap items-center justify-between gap-2">
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Receive</h3>
								<div class="inline-flex rounded-md border border-gray-300 dark:border-gray-600 text-sm" role="group" aria-label="Sub-queue">
									<button class="px-3 py-1 {!deadLetter ? 'bg-blue-600 text-white' : 'hover:bg-gray-50 dark:hover:bg-slate-800'} rounded-l-md" aria-pressed={!deadLetter} onclick={() => setDeadLetter(false)}>Active</button>
									<button class="px-3 py-1 {deadLetter ? 'bg-blue-600 text-white' : 'hover:bg-gray-50 dark:hover:bg-slate-800'} rounded-r-md" aria-pressed={deadLetter} onclick={() => setDeadLetter(true)}>Dead-letter</button>
								</div>
							</div>
							<p class="text-xs text-gray-500 dark:text-gray-400">
								The emulator has no non-destructive peek: receiving locks a message (raising its delivery count) until you complete or abandon it, or the lock expires.
								{#if !deadLetter}Abandoning a message dead-letters it once its delivery count reaches the max delivery count.{:else}Dead-lettered messages can be completed (removed){#if sel.kind === 'queue'} or resubmitted to the queue{/if}.{/if}
							</p>
							<div class="flex flex-wrap items-end gap-3">
								<label class="space-y-1 text-sm">
									<span class="block font-medium text-gray-700 dark:text-gray-300">Count (1-{MAX_RECEIVE_BATCH})</span>
									<input class="{input} w-28" bind:value={recvCount} inputmode="numeric" aria-label="Receive count" />
								</label>
								<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={receive} disabled={busy || Boolean(countError)}>
									<Download class="h-4 w-4" /> {busy ? 'Working…' : 'Receive (peek-lock)'}
								</button>
								<button class="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 px-3 py-2 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50" onclick={receiveDelete} disabled={busy}>
									<Trash2 class="h-4 w-4" /> Receive &amp; delete one
								</button>
							</div>
							{#if countError}<p class="text-xs text-red-600 dark:text-red-400">Count: {countError}</p>{/if}

							{#if received.length === 0}
								<div class="flex flex-col items-center py-6 text-gray-500 dark:text-gray-400" data-testid="servicebus-received-empty">
									<Inbox class="h-8 w-8 mb-2 opacity-30" />
									<p class="text-sm">No locked messages</p>
									<p class="text-xs">Receive messages from the {deadLetter ? 'dead-letter queue' : 'entity'} to inspect them</p>
								</div>
							{:else}
								<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
									<table class="w-full text-sm" data-testid="servicebus-received">
										<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
											<tr>
												<th class={th}>Message ID</th>
												<th class={th}>Body</th>
												<th class={th}>Delivery</th>
												<th class={th}>Locked until</th>
												<th class="{th} text-right">Actions</th>
											</tr>
										</thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each received as m (m.lockToken)}
												<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {selected?.lockToken === m.lockToken ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="servicebus-received-row" onclick={() => (selected = m)}>
													<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-gray-100">{m.messageId}</td>
													<td class="{td} max-w-xs truncate">{m.body}</td>
													<td class={td}>{m.deliveryCount ?? '—'}</td>
													<td class={td}>{m.lockedUntil || '—'}</td>
													<td class="px-4 py-3 text-right whitespace-nowrap">
														<button class="{iconBtn} text-green-600" title="Complete {m.messageId}" aria-label="Complete message {m.messageId}" onclick={(ev) => { ev.stopPropagation(); void complete(m); }}><Check class="h-4 w-4" /></button>
														<button class="{iconBtn} text-amber-600" title="Abandon {m.messageId}" aria-label="Abandon message {m.messageId}" onclick={(ev) => { ev.stopPropagation(); void abandon(m); }}><Undo2 class="h-4 w-4" /></button>
														{#if deadLetter && sel.kind === 'queue'}
															<button class="{iconBtn} text-blue-600" title="Resubmit {m.messageId}" aria-label="Resubmit message {m.messageId}" onclick={(ev) => { ev.stopPropagation(); void doResubmit(m); }}><RotateCcw class="h-4 w-4" /></button>
														{/if}
													</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
						</div>

						{#if selected}
							<section class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-4" data-testid="servicebus-message-detail" aria-label="Message {selected.messageId}">
								<div class="flex items-start justify-between gap-4">
									<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">Message {selected.messageId}</h3>
									<button class={iconBtn} onclick={() => (selected = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
								</div>
								<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
									<dt class={dt}>Sequence number</dt><dd class={dd}>{selected.sequenceNumber ?? '—'}</dd>
									<dt class={dt}>Delivery count</dt><dd class={dd}>{selected.deliveryCount ?? '—'}</dd>
									<dt class={dt}>Enqueued</dt><dd class={dd}>{selected.enqueuedTime || '—'}</dd>
									<dt class={dt}>Locked until</dt><dd class={dd}>{selected.lockedUntil || '—'}</dd>
									<dt class={dt}>Content type</dt><dd class={dd}>{selected.contentType || '—'}</dd>
									<dt class={dt}>Label</dt><dd class={dd}>{selected.label || '—'}</dd>
									<dt class={dt}>Correlation ID</dt><dd class={dd}>{selected.correlationId || '—'}</dd>
									<dt class={dt}>Reply to</dt><dd class={dd}>{selected.replyTo || '—'}</dd>
									<dt class={dt}>Session ID</dt><dd class={dd}>{selected.sessionId || '—'}</dd>
									<dt class={dt}>Lock token</dt><dd class="{dd} font-mono text-xs">{selected.lockToken || '—'}</dd>
									<dt class={dt}>Body</dt>
									<dd class="{dd} flex items-start gap-2">
										<pre class="whitespace-pre-wrap font-mono text-xs" data-testid="servicebus-message-body">{selected.body}</pre>
										<button class={iconBtn} aria-label="Copy message body" onclick={() => selected && copy(selected.body)}><Copy class="h-3.5 w-3.5" /></button>
									</dd>
								</dl>
							</section>
						{/if}
					{/if}

					<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="servicebus-unsupported-note">
						Not modeled by the emulator, so not offered here: message counts, non-locking peek, application properties, scheduled enqueue, explicit dead-lettering, subscription rules and filters (every subscription receives every message), sessions and AMQP.
					</p>
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={createModal} title={createKind === 'queue' ? 'New queue' : createKind === 'topic' ? 'New topic' : `New subscription on ${createTopicName}`} titleId="servicebus-create-title">
	{#snippet children()}
		<form id="servicebus-create-form" class="space-y-3" onsubmit={submitCreate} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Name</span>
				<input class={input} bind:value={createName} autocomplete="off" aria-label="Name" />
				{#if createTouched && nameError}<span class="block text-xs text-red-600 dark:text-red-400">{nameError}</span>{/if}
			</label>
			{#if createKind !== 'topic'}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Lock duration (seconds, optional, max {MAX_LOCK_SECONDS})</span>
					<input class={input} bind:value={createLock} placeholder="60" inputmode="numeric" aria-label="Lock duration" />
					{#if lockError}<span class="block text-xs text-red-600 dark:text-red-400">{lockError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Max delivery count (optional)</span>
					<input class={input} bind:value={createMax} placeholder="10" inputmode="numeric" aria-label="Max delivery count" />
					{#if maxError}<span class="block text-xs text-red-600 dark:text-red-400">{maxError}</span>{/if}
				</label>
			{/if}
			{#if createKind !== 'sub'}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Default message TTL (seconds, optional)</span>
					<input class={input} bind:value={createTtl} placeholder="1209600" inputmode="numeric" aria-label="Default message TTL" />
					{#if ttlError}<span class="block text-xs text-red-600 dark:text-red-400">{ttlError}</span>{/if}
				</label>
			{/if}
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => createModal?.close()}>Cancel</button>
		<button type="submit" form="servicebus-create-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={createBlocked}>
			{creating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<Modal bind:this={sendModal} title="Send message to {sendTarget ?? ''}" titleId="servicebus-send-title">
	{#snippet children()}
		<form id="servicebus-send-form" class="space-y-3" onsubmit={submitSend} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Body</span>
				<textarea class={input} rows="4" bind:value={sendBody} aria-label="Message body"></textarea>
			</label>
			<div class="grid gap-3 sm:grid-cols-2">
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Content type</span>
					<input class={input} bind:value={sendContentType} aria-label="Content type" />
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Message ID (optional)</span>
					<input class={input} bind:value={sendMessageId} aria-label="Message ID" />
					{#if sendIdError}<span class="block text-xs text-red-600 dark:text-red-400">{sendIdError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Label / subject (optional)</span>
					<input class={input} bind:value={sendLabel} aria-label="Label" />
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Correlation ID (optional)</span>
					<input class={input} bind:value={sendCorrelation} aria-label="Correlation ID" />
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Reply to (optional)</span>
					<input class={input} bind:value={sendReplyTo} aria-label="Reply to" />
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Session ID (optional)</span>
					<input class={input} bind:value={sendSession} aria-label="Session ID" />
				</label>
			</div>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Time to live (seconds, optional)</span>
				<input class={input} bind:value={sendTtl} placeholder="1209600" inputmode="numeric" aria-label="Time to live" />
				{#if sendTtlError}<span class="block text-xs text-red-600 dark:text-red-400">{sendTtlError}</span>{/if}
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => sendModal?.close()}>Cancel</button>
		<button type="submit" form="servicebus-send-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={sendBlocked}>
			{sending ? 'Sending…' : 'Send'}
		</button>
	{/snippet}
</Modal>
