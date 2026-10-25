<script lang="ts">
	import { onMount } from 'svelte';
	import { Radio, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		DEFAULT_READ,
		listPartitions,
		listPools,
		listStreams,
		readMessages,
		showBytes,
		type MessageWindow,
		type PartitionRange,
		type PoolInfo,
		type StreamInfo
	} from '#lib/ocistreaming.ts';
	import { shortOcid } from '#lib/ocivault.ts';

	let pools = $state<PoolInfo[]>([]);
	let streams = $state<StreamInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let poolId = $state('');
	let streamId = $state('');
	let partitions = $state<PartitionRange[]>([]);
	let partition = $state('0');
	let limit = $state(DEFAULT_READ);
	let win = $state<MessageWindow | null>(null);
	let reading = $state(false);
	let readError = $state('');

	async function load() {
		try {
			[pools, streams] = await Promise.all([listPools(), listStreams()]);
			error = '';
			if (!streams.some((s) => s.id === streamId)) streamId = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function read() {
		if (!streamId) return;
		reading = true;
		readError = '';
		try {
			win = await readMessages(streamId, { partition, limit });
		} catch (e) {
			readError = describeGcpError(e);
			win = null;
		} finally {
			reading = false;
		}
	}

	async function selectStream(row: ResourceRow) {
		streamId = streamId === row.id ? '' : row.id;
		win = null;
		partitions = [];
		partition = '0';
		if (!streamId) return;
		try {
			partitions = await listPartitions(streamId);
			readError = '';
			await read();
		} catch (e) {
			readError = describeGcpError(e);
		}
	}

	const poolName = (id: string) => pools.find((p) => p.id === id)?.name ?? shortOcid(id);
	const poolRows = $derived(pools.map((p) => ({ id: p.id, cells: [p.name, p.state, p.isPrivate ? 'private' : 'public', p.timeCreated], raw: p.raw })));
	const visible = $derived(streams.filter((s) => !poolId || s.poolId === poolId));
	const streamRows = $derived(visible.map((s) => ({ id: s.id, cells: [s.name, s.state, poolName(s.poolId), String(s.partitions), s.retentionInHours ? `${s.retentionInHours} h` : '—'], raw: s.raw })));
	const cell = 'px-4 py-2';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="oci-streaming-dashboard">
	<PageHeader icon={Radio} title="Streaming" description="Oracle Cloud Streaming: stream pools, streams and recent messages per partition" color="red" onRefresh={load} />
	<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Stream pools</h2>
	<GcpResourceTable testId="oci-pool" noun="stream pools" columns={['Pool', 'State', 'Endpoint', 'Created']} rows={poolRows} {loading} {error} selected={poolId} onRetry={load} onSelect={(row) => (poolId = poolId === row.id ? '' : row.id)} />
	<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Streams{poolId ? ` in ${poolName(poolId)}` : ''}</h2>
	<GcpResourceTable testId="oci-stream" noun="streams" columns={['Stream', 'State', 'Pool', 'Partitions', 'Retention']} rows={streamRows} {loading} {error} selected={streamId} onRetry={load} onSelect={selectStream} />

	{#if streamId}
		<div class="space-y-3" data-testid="oci-stream-messages">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Recent messages</h2>
			<div class="flex flex-wrap items-end gap-3 text-xs">
				<label class="space-y-1">Partition
					<select class={GCP_INPUT} bind:value={partition} onchange={read} data-testid="oci-stream-partition">
						{#each partitions as p (p.partition)}<option value={p.partition}>{p.partition} (offsets {p.base}–{Math.max(p.base, p.end - 1)}, {p.end - p.base} retained)</option>{/each}
					</select>
				</label>
				<label class="space-y-1">Count
					<input class={GCP_INPUT} type="number" min="1" max="200" bind:value={limit} data-testid="oci-stream-limit" />
				</label>
				<button class={GCP_MINI} onclick={read} disabled={reading} data-testid="oci-stream-read">Read</button>
			</div>
			{#if readError}<p class="text-sm text-red-600" role="alert" data-testid="oci-stream-error">{readError}</p>{/if}
			{#if reading && !win}
				<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
			{:else if win && win.messages.length === 0}
				<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="oci-stream-empty">No messages in partition {partition}</p>
			{:else if win}
				<table class="w-full text-sm" data-testid="oci-stream-table">
					<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Offset</th><th class={cell}>Timestamp</th><th class={cell}>Key</th><th class={cell}>Value</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each win.messages as m (m.offset)}
							<tr data-testid="oci-stream-message"><td class="{cell} font-mono text-xs">{m.offset}</td><td class="{cell} text-xs">{m.timestamp}</td><td class="{cell} font-mono text-xs break-all">{showBytes(m.key) || '—'}</td><td class="{cell} font-mono text-xs break-all">{showBytes(m.value)}</td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{/if}
</div>
