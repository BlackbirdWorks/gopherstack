<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Radio, Send, Inbox } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { attempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		listNamespaces,
		listPartitions,
		peekEvents,
		sendEvent,
		validateSend,
		type EhEvent,
		type EhNamespace,
		type EhPartition
	} from '#lib/azureeventhubs.ts';

	let namespaces = $state<EhNamespace[]>([]);
	let nsName = $state('');
	let hubName = $state('');
	let partitions = $state<EhPartition[]>([]);
	let peeked = $state('');
	let events = $state<EhEvent[]>([]);
	let loading = $state(true);
	let loadError = $state('');
	let sending = $state(false);
	let fBody = $state('{"hello":"gopherstack"}');
	let fKey = $state('');
	let fPartition = $state('');

	const ns = $derived(namespaces.find((n) => n.name === nsName));
	const hub = $derived(ns?.hubs.find((h) => h.name === hubName));
	const sendError = $derived(validateSend({ body: fBody, partitionKey: fKey, partition: fPartition }, hub?.partitionIds.length ?? 0));

	async function load() {
		loading = true;
		loadError = '';
		try {
			namespaces = await listNamespaces();
			if (!namespaces.some((n) => n.name === nsName)) nsName = namespaces[0]?.name ?? '';
			const hubs = namespaces.find((n) => n.name === nsName)?.hubs ?? [];
			if (!hubs.some((h) => h.name === hubName)) hubName = hubs[0]?.name ?? '';
			await loadPartitions();
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Event Hubs: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadPartitions() {
		peeked = '';
		events = [];
		partitions = nsName && hubName ? await listPartitions(nsName, hubName) : [];
	}

	async function pickNamespace(name: string) {
		nsName = name;
		hubName = namespaces.find((n) => n.name === name)?.hubs[0]?.name ?? '';
		await attempt(loadPartitions, '', 'Failed to load partitions');
	}

	async function pickHub(name: string) {
		hubName = name;
		await attempt(loadPartitions, '', 'Failed to load partitions');
	}

	async function peek(id: string) {
		await attempt(async () => {
			events = await peekEvents(nsName, hubName, id);
			peeked = id;
		}, '', 'Failed to read events');
	}

	async function send(e: Event) {
		e.preventDefault();
		if (sendError) return void toast.error(sendError);
		sending = true;
		let landed = -1;
		const ok = await attempt(async () => {
			landed = await sendEvent(nsName, hubName, { body: fBody, partitionKey: fKey, partition: fPartition });
		}, '', 'Failed to send event');
		sending = false;
		if (!ok) return;
		toast.success(`Event sent to partition ${landed}`);
		await attempt(async () => {
			partitions = await listPartitions(nsName, hubName);
			events = await peekEvents(nsName, hubName, String(landed));
			peeked = String(landed);
		}, '', 'Failed to refresh');
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="eventhubs-dashboard">
	<PageHeader icon={Radio} title="Event Hubs" description="Azure Event Hubs: namespaces, hubs, partitions and events" color="blue" onRefresh={load} />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="eventhubs-error">
			<p class="font-medium">Could not reach Azure Event Hubs</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if namespaces.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="eventhubs-empty">
			<Inbox class="h-10 w-10 mb-3 opacity-30" />
			<p>No Event Hubs namespaces. Create one with the Azure CLI, Terraform or ARM.</p>
		</div>
	{:else}
		<div class="flex flex-wrap gap-4">
			<label class="text-sm">Namespace
				<select class={GCP_INPUT} value={nsName} onchange={(e) => pickNamespace(e.currentTarget.value)} data-testid="eventhubs-namespace">
					{#each namespaces as n (n.name)}<option value={n.name}>{n.name}</option>{/each}
				</select>
			</label>
			<label class="text-sm">Event hub
				<select class={GCP_INPUT} value={hubName} onchange={(e) => pickHub(e.currentTarget.value)} data-testid="eventhubs-hub" disabled={!ns?.hubs.length}>
					{#each ns?.hubs ?? [] as h (h.name)}<option value={h.name}>{h.name}</option>{/each}
				</select>
			</label>
		</div>

		{#if !hub}
			<p class="text-gray-500" data-testid="eventhubs-no-hubs">This namespace has no event hubs.</p>
		{:else}
			<p class="text-sm text-gray-500 dark:text-gray-400">
				{hub.partitionIds.length} partitions, retention {hub.retentionHours} h, consumer groups: {hub.consumerGroups.join(', ') || 'none'}
			</p>

			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="eventhubs-partitions">
					<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Partition</th><th class="px-4 py-2">Sequence range</th><th class="px-4 py-2">Last enqueued</th><th class="px-4 py-2"></th></tr></thead>
					<tbody>
						{#each partitions as p (p.id)}
							<tr class="border-t border-gray-100 dark:border-gray-800">
								<td class="px-4 py-2 font-mono">{p.id}</td>
								<td class="px-4 py-2">{p.empty ? 'empty' : `${p.beginSequence} - ${p.lastSequence}`}</td>
								<td class="px-4 py-2">{p.empty ? '-' : p.lastEnqueuedTime}</td>
								<td class="px-4 py-2 text-right"><button class={GCP_MINI} onclick={() => peek(p.id)} disabled={p.empty} data-testid="eventhubs-peek-{p.id}">Peek events</button></td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if peeked}
				<section data-testid="eventhubs-events">
					<h2 class="mb-2 font-semibold">Newest events in partition {peeked}</h2>
					{#each events as ev (ev.sequence)}
						<div class="mb-2 rounded-lg border border-gray-200 p-3 text-sm dark:border-gray-700">
							<p class="text-xs text-gray-500">#{ev.sequence} offset {ev.offset} {ev.enqueued}{ev.partitionKey ? ` key=${ev.partitionKey}` : ''}</p>
							<pre class="mt-1 whitespace-pre-wrap break-all font-mono">{ev.body}</pre>
						</div>
					{:else}
						<p class="text-gray-500">No events.</p>
					{/each}
				</section>
			{/if}

			<form class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={send} data-testid="eventhubs-send-form">
				<h2 class="font-semibold">Send event</h2>
				<label class="block text-sm">Body
					<textarea class="{GCP_INPUT} font-mono" rows="3" bind:value={fBody} data-testid="eventhubs-body"></textarea>
				</label>
				<div class="flex flex-wrap gap-4">
					<label class="text-sm">Partition key<input class={GCP_INPUT} bind:value={fKey} placeholder="optional" data-testid="eventhubs-key" /></label>
					<label class="text-sm">Partition<input class={GCP_INPUT} bind:value={fPartition} placeholder="auto" inputmode="numeric" data-testid="eventhubs-partition" /></label>
				</div>
				{#if sendError}<p class="text-sm text-red-600" data-testid="eventhubs-send-error">{sendError}</p>{/if}
				<button type="submit" class={GCP_PRIMARY} disabled={sending || Boolean(sendError)} data-testid="eventhubs-send"><Send class="mr-1 inline h-4 w-4" />Send</button>
			</form>
		{/if}
	{/if}
</div>
