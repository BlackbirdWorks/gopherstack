<script lang="ts">
	import { onMount } from 'svelte';
	import { Inbox, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { confirmedAttempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { shortOcid } from '#lib/ocirest.ts';
	import {
		formatBytes,
		formatSeconds,
		getQueue,
		getStats,
		listCompartments,
		listQueues,
		peekMessages,
		purgeQueue,
		readStoredQueueCompartment,
		writeStoredQueueCompartment,
		type PeekedMessage,
		type PurgeType,
		type QueueInfo,
		type QueueStats
	} from '#lib/ociqueue.ts';

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredQueueCompartment());
	let queues = $state<QueueInfo[]>([]);
	let stats = $state<Record<string, QueueStats>>({});
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let detail = $state<QueueInfo | null>(null);
	let showDlq = $state(false);
	let messages = $state<PeekedMessage[]>([]);
	let detailError = $state('');
	let detailLoading = $state(false);

	async function loadQueues() {
		if (!compartment) {
			queues = [];
			return;
		}
		try {
			queues = await listQueues(compartment);
			const entries = await Promise.all(
				queues.filter((q) => q.state === 'ACTIVE').map(async (q) => [q.id, await getStats(q.id).catch(() => null)] as const)
			);
			stats = Object.fromEntries(entries.filter((e): e is [string, QueueStats] => e[1] !== null));
			error = '';
			if (!queues.some((q) => q.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		if (selected) await loadDetail();
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadQueues();
		loading = false;
	}

	async function loadDetail() {
		if (!selected) return;
		detailLoading = true;
		detailError = '';
		try {
			[detail, messages] = await Promise.all([getQueue(selected), peekMessages(selected, showDlq)]);
		} catch (e) {
			detailError = describeGcpError(e);
		} finally {
			detailLoading = false;
		}
	}

	function pickCompartment() {
		writeStoredQueueCompartment(compartment);
		selected = '';
		loading = true;
		void loadQueues();
	}

	function select(q: QueueInfo) {
		selected = selected === q.id ? '' : q.id;
		detail = null;
		messages = [];
		showDlq = false;
		if (selected) void loadDetail();
	}

	function toggleDlq() {
		showDlq = !showDlq;
		void loadDetail();
	}

	async function purge(q: QueueInfo, type: PurgeType) {
		const what = type === 'NORMAL' ? 'all messages' : type === 'DLQ' ? 'the dead letter queue' : 'all messages and the dead letter queue';
		const ok = await confirmedAttempt({ title: 'Purge queue', message: `Permanently delete ${what} of "${q.name}"?`, confirmLabel: 'Purge' }, () => purgeQueue(q.id, type), `Purge requested for ${q.name}`, `Failed to purge ${q.name}`);
		if (ok) await loadQueues();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="ociqueue-dashboard">
	<PageHeader icon={Inbox} title="Queue" description="Oracle Cloud queues: depth, in-flight messages, dead letter queues and message peek" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="ociqueue-error">
			<p class="font-medium">Could not load queues</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="ociqueue-empty">No queues yet. Create one with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="ociqueue-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="ociqueue-list-error">{error}</p>{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="ociqueue-queues">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Queue</th><th class={cell}>State</th><th class={cell}>Visible</th><th class={cell}>In flight</th><th class={cell}>Dead letter</th><th class={cell}>Size</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each queues as q (q.id)}
						{@const s = stats[q.id]}
						<tr class={selected === q.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="ociqueue-queue">
							<td class="{cell} font-mono text-xs" title={q.id}><button class="underline decoration-dotted" onclick={() => select(q)} aria-expanded={selected === q.id} data-testid="ociqueue-select">{q.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(q.state)}">{q.state || '—'}</span></td>
							<td class="{cell} text-xs" data-testid="ociqueue-visible">{s ? s.queue.visible : '—'}</td>
							<td class="{cell} text-xs" data-testid="ociqueue-inflight">{s ? s.queue.inFlight : '—'}</td>
							<td class="{cell} text-xs" data-testid="ociqueue-dlq">{s ? s.dlq.visible + s.dlq.inFlight : '—'}</td>
							<td class="{cell} text-xs">{s ? formatBytes(s.queue.sizeBytes + s.dlq.sizeBytes) : '—'}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								<button class={GCP_MINI} onclick={() => purge(q, 'NORMAL')} disabled={q.state !== 'ACTIVE'} data-testid="ociqueue-purge">Purge</button>
								<button class={GCP_MINI} onclick={() => purge(q, 'DLQ')} disabled={q.state !== 'ACTIVE'} data-testid="ociqueue-purge-dlq">Purge DLQ</button>
							</td>
						</tr>
					{:else}
						<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="ociqueue-none">No queues in this compartment</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if selected}
			<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="ociqueue-detail">
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="ociqueue-detail-error">{detailError}</p>{/if}
				{#if detail}
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.name}</h2>
					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs" data-testid="ociqueue-config">
						<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{detail.id}</dd>
						<dt class="text-gray-500">Retention</dt><dd>{formatSeconds(detail.retention)}</dd>
						<dt class="text-gray-500">Visibility timeout</dt><dd>{formatSeconds(detail.visibility)}</dd>
						<dt class="text-gray-500">Long-poll timeout</dt><dd>{formatSeconds(detail.timeout)}</dd>
						<dt class="text-gray-500">Dead letter after</dt><dd>{detail.dlqDeliveryCount ? `${detail.dlqDeliveryCount} deliveries` : 'disabled'}</dd>
					</dl>
				{/if}
				<div class="flex items-center gap-2">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">{showDlq ? 'Dead letter messages' : 'Messages'} <span class="font-normal text-gray-400">(peek, up to 20; nothing is received or delivered)</span></h3>
					<button class={GCP_MINI} onclick={toggleDlq} data-testid="ociqueue-toggle-dlq">{showDlq ? 'Show queue' : 'Show dead letter queue'}</button>
				</div>
				{#if detailLoading && messages.length === 0}
					<RefreshCw class="h-4 w-4 animate-spin text-gray-400" />
				{:else if messages.length === 0}
					<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="ociqueue-messages-empty">No messages</p>
				{:else}
					<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
						<table class="w-full text-xs" data-testid="ociqueue-messages">
							<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>ID</th><th class={cell}>Content</th><th class={cell}>State</th><th class={cell}>Deliveries</th><th class={cell}>Channel</th><th class={cell}>Created</th><th class={cell}>Expires</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each messages as m (m.id)}
									<tr data-testid="ociqueue-message">
										<td class={cell}>{m.id}</td>
										<td class="{cell} font-mono break-all" data-testid="ociqueue-message-content">{m.content}</td>
										<td class={cell}>{m.inFlight ? 'in flight' : 'visible'}</td>
										<td class={cell}>{m.deliveryCount}</td>
										<td class="{cell} font-mono">{m.channel ? shortOcid(m.channel) : '—'}</td>
										<td class={cell}>{m.created}</td>
										<td class={cell}>{m.expireAfter}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		{/if}
	{/if}
</div>
