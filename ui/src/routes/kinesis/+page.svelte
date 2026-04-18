<script lang="ts">
	import { onMount } from 'svelte';
	import { getKinesisClient } from '$lib/aws-client';
	import {
		ListStreamsCommand,
		DescribeStreamSummaryCommand,
		CreateStreamCommand,
		DeleteStreamCommand,
		PutRecordCommand,
		GetShardIteratorCommand,
		GetRecordsCommand,
		ListShardsCommand,
		type StreamDescriptionSummary
	} from '@aws-sdk/client-kinesis';
	import { toast } from 'svelte-sonner';
	import { Waves, Search, RefreshCw, Plus, Trash2, Send, Download } from 'lucide-svelte';

	const kinesis = getKinesisClient();

	let loading = $state(false);
	let streams = $state<string[]>([]);
	let searchQuery = $state('');
	let selectedStream = $state<string | null>(null);
	let streamDetail = $state<StreamDescriptionSummary | null>(null);
	let loadingDetail = $state(false);

	// Create modal
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newStreamName = $state('');
	let newShardCount = $state(1);
	let newOnDemand = $state(false);

	// Put record modal
	let showPutModal = $state(false);
	let putting = $state(false);
	let putPartitionKey = $state('');
	let putData = $state('');

	// Records
	let records = $state<Array<{ sequenceNumber: string; data: string; partitionKey: string }>>([]);
	let gettingRecords = $state(false);

	const filteredStreams = $derived(
		streams.filter((s) => s.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function statusColor(status: string | undefined): string {
		if (status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'CREATING') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'DELETING') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	async function loadStreams() {
		loading = true;
		try {
			const res = await kinesis.send(new ListStreamsCommand({ Limit: 100 }));
			streams = res.StreamNames ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load streams: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectStream(name: string) {
		selectedStream = name;
		streamDetail = null;
		records = [];
		loadingDetail = true;
		try {
			const res = await kinesis.send(new DescribeStreamSummaryCommand({ StreamName: name }));
			streamDetail = res.StreamDescriptionSummary ?? null;
		} catch (err: unknown) {
			toast.error(`Failed to describe stream: ${(err as Error).message}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function createStream() {
		if (!newStreamName.trim()) return;
		creating = true;
		try {
			await kinesis.send(new CreateStreamCommand({
				StreamName: newStreamName.trim(),
				ShardCount: newOnDemand ? undefined : newShardCount,
				StreamModeDetails: newOnDemand ? { StreamMode: 'ON_DEMAND' } : { StreamMode: 'PROVISIONED' }
			}));
			toast.success(`Stream "${newStreamName.trim()}" creating`);
			showCreateModal = false;
			newStreamName = '';
			newShardCount = 1;
			newOnDemand = false;
			await loadStreams();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteStream(name: string) {
		if (!confirm(`Delete stream "${name}"?`)) return;
		try {
			await kinesis.send(new DeleteStreamCommand({ StreamName: name }));
			toast.success(`Stream "${name}" deleting`);
			if (selectedStream === name) { selectedStream = null; streamDetail = null; }
			await loadStreams();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function putRecord() {
		if (!selectedStream || !putPartitionKey.trim() || !putData.trim()) return;
		putting = true;
		try {
			const encoded = new TextEncoder().encode(putData);
			await kinesis.send(new PutRecordCommand({
				StreamName: selectedStream,
				PartitionKey: putPartitionKey.trim(),
				Data: encoded
			}));
			toast.success('Record put successfully');
			showPutModal = false;
			putPartitionKey = '';
			putData = '';
		} catch (err: unknown) {
			toast.error(`Put record failed: ${(err as Error).message}`);
		} finally {
			putting = false;
		}
	}

	async function getRecords() {
		if (!selectedStream) return;
		gettingRecords = true;
		try {
			const shardId = (await kinesis.send(new ListShardsCommand({ StreamName: selectedStream }))).Shards?.[0]?.ShardId ?? '';
			const iterRes = await kinesis.send(new GetShardIteratorCommand({
				StreamName: selectedStream,
				ShardId: shardId,
				ShardIteratorType: 'TRIM_HORIZON'
			}));
			const iterator = iterRes.ShardIterator;
			if (!iterator) return;
			const recRes = await kinesis.send(new GetRecordsCommand({
				ShardIterator: iterator,
				Limit: 10
			}));
			records = (recRes.Records ?? []).map((r) => ({
				sequenceNumber: r.SequenceNumber ?? '',
				partitionKey: r.PartitionKey ?? '',
				data: r.Data ? new TextDecoder().decode(r.Data) : ''
			}));
			if (records.length === 0) toast.info('No records in this shard');
		} catch (err: unknown) {
			toast.error(`Get records failed: ${(err as Error).message}`);
		} finally {
			gettingRecords = false;
		}
	}

	onMount(() => { loadStreams(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Waves class="w-6 h-6 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Kinesis Data Streams</h1>
				<p class="text-slate-600 dark:text-slate-300">Real-time data streaming</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadStreams()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button onclick={() => { showCreateModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />Create Stream
			</button>
		</div>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input type="text" bind:value={searchQuery} placeholder="Search streams..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Waves class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{streams.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Total Streams</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Send class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{streamDetail?.OpenShardCount ?? (selectedStream ? '…' : '—')}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Open Shards</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg">
				<Download class="w-5 h-5 text-indigo-600 dark:text-indigo-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{records.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Records Fetched</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<RefreshCw class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{streamDetail?.StreamStatus ?? (selectedStream ? '…' : '—')}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Selected Status</p>
			</div>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading streams...</p>
				</div>
			{:else if filteredStreams.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<Waves class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No streams found</p>
				</div>
			{:else}
				{#each filteredStreams as stream}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectStream(stream)}
						onkeypress={(e) => { if (e.key === 'Enter') selectStream(stream); }}
						class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-indigo-400 transition-colors cursor-pointer {selectedStream === stream ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
					>
						<div class="flex items-center justify-between">
							<p class="font-medium text-slate-900 dark:text-white truncate">{stream}</p>
							<button onclick={(e) => { e.stopPropagation(); deleteStream(stream); }} class="p-1 text-slate-400 hover:text-red-500 ml-2">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>
				{/each}
			{/if}
		</div>

		<!-- Stream Detail -->
		<div class="lg:col-span-2">
			{#if selectedStream}
				<div class="space-y-4">
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
						<div class="flex items-start justify-between mb-4">
							<div>
								<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedStream}</h2>
								{#if streamDetail}
									<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {statusColor(streamDetail.StreamStatus)}">{streamDetail.StreamStatus}</span>
								{/if}
							</div>
							<div class="flex gap-2">
								<button onclick={() => { showPutModal = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
									<Send class="w-4 h-4" />Put Record
								</button>
								<button onclick={() => getRecords()} disabled={gettingRecords} class="px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-1.5 text-sm disabled:opacity-50">
									<Download class="w-4 h-4" />{gettingRecords ? '...' : 'Get Records'}
								</button>
							</div>
						</div>
						{#if loadingDetail}
							<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
						{:else if streamDetail}
							<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
								{#each [
									['Stream ARN', streamDetail.StreamARN?.split('/').pop() ?? 'N/A'],
									['Shard Count', String(streamDetail.OpenShardCount ?? 0)],
									['Retention (hours)', String(streamDetail.RetentionPeriodHours ?? 24)],
									['Encryption', streamDetail.EncryptionType ?? 'NONE'],
									['Stream Mode', streamDetail.StreamModeDetails?.StreamMode ?? 'PROVISIONED'],
									['Consumer Count', String(streamDetail.ConsumerCount ?? 0)]
								] as [label, value]}
									<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
										<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
										<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate">{value}</p>
									</div>
								{/each}
							</div>
						{/if}
					</div>

					<!-- Records panel -->
					{#if records.length > 0}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
							<h3 class="font-semibold text-slate-900 dark:text-white mb-3">Records ({records.length})</h3>
							<div class="space-y-2">
								{#each records as rec}
									<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
										<div class="flex items-center gap-2 mb-1">
											<span class="text-xs text-slate-500 dark:text-slate-400 font-mono">Partition: {rec.partitionKey}</span>
											<span class="text-xs text-slate-400">·</span>
											<span class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate">{rec.sequenceNumber}</span>
										</div>
										<pre class="text-xs text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-all">{rec.data}</pre>
									</div>
								{/each}
							</div>
						</div>
					{/if}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Waves class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a stream to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Stream Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Data Stream</h2>
			<form onsubmit={(e) => { e.preventDefault(); createStream(); }} class="space-y-4">
				<div>
					<label for="kinesis-stream-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Stream Name</label>
					<input id="kinesis-stream-name" type="text" bind:value={newStreamName} placeholder="e.g. user-events" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<label class="flex items-center gap-2 cursor-pointer">
					<input type="checkbox" bind:checked={newOnDemand} class="rounded" />
					<span class="text-sm text-slate-700 dark:text-slate-300">On-demand capacity (auto-scaling)</span>
				</label>
				{#if !newOnDemand}
					<div>
						<label for="kinesis-shard-count" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Shard Count (1–500)</label>
						<input id="kinesis-shard-count" type="number" bind:value={newShardCount} min="1" max="500" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
				{/if}
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creating} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creating ? 'Creating...' : 'Create Stream'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Put Record Modal -->
{#if showPutModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Put Record</h2>
			<form onsubmit={(e) => { e.preventDefault(); putRecord(); }} class="space-y-4">
				<div>
					<label for="kinesis-partition-key" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Partition Key</label>
					<input id="kinesis-partition-key" type="text" bind:value={putPartitionKey} placeholder="e.g. user-123" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div>
					<label for="kinesis-data" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Data (text, will be base64 encoded)</label>
					<textarea id="kinesis-data" bind:value={putData} rows={4} placeholder="Enter JSON or text data to stream..." class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none" required></textarea>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showPutModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={putting} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
						<Send class="w-4 h-4" />{putting ? 'Putting...' : 'Put Record'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
