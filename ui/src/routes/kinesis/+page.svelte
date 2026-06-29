<script lang="ts">
import { onMount } from 'svelte';
import { getKinesisClient } from '$lib/aws-client';
import {
	ListStreamsCommand,
	DescribeStreamCommand,
	CreateStreamCommand,
	DeleteStreamCommand,
	ListShardsCommand,
	GetShardIteratorCommand,
	GetRecordsCommand,
	PutRecordCommand,
	MergeShardsCommand,
	SplitShardCommand,
	type Shard,
	type _Record as KinesisRecord
} from '@aws-sdk/client-kinesis';
import { toast } from 'svelte-sonner';
import { confirmDestructive } from '$lib/confirm-dialog';
import {
	Activity, Search, RefreshCw, Plus, Trash2, Send, Inbox,
	Layers, SplitSquareHorizontal, Merge, Box
} from 'lucide-svelte';

const kinesis = getKinesisClient();

// State
let loading = $state(false);
let streams = $state<string[]>([]);
let searchQuery = $state('');

let selectedStream = $state<{ name: string; arn: string; status: string; retention: number } | null>(null);
let streamShards = $state<Shard[]>([]);
let loadingShards = $state(false);

let activeTab = $state<'shards' | 'put_record'>('shards');

// Create stream
let showCreateModal = $state(false);
let creating = $state(false);
let newStreamName = $state('');
let newShardCount = $state(1);

// Put Record
let putRecordPartitionKey = $state('');
let putRecordData = $state('');
let puttingRecord = $state(false);

// Get Records
let viewingShardId = $state<string | null>(null);
let shardRecords = $state<KinesisRecord[]>([]);
let loadingRecords = $state(false);

// Shard operations
let merging = $state(false);
let splitting = $state(false);

const filteredStreams = $derived(
	streams.filter(name => name.toLowerCase().includes(searchQuery.toLowerCase()))
);

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

async function createStream() {
	if (!newStreamName.trim() || newShardCount < 1) return;
	creating = true;
	try {
		await kinesis.send(new CreateStreamCommand({
			StreamName: newStreamName.trim(),
			ShardCount: newShardCount
		}));
		toast.success(`Stream "${newStreamName}" created`);
		showCreateModal = false;
		newStreamName = '';
		newShardCount = 1;
		await loadStreams();
	} catch (err: unknown) {
		toast.error(`Create failed: ${(err as Error).message}`);
	} finally {
		creating = false;
	}
}

async function deleteStream(name: string) {
	if (!await confirmDestructive({ title: 'Delete Stream', message: `Delete stream "${name}"?` })) return;
	try {
		await kinesis.send(new DeleteStreamCommand({ StreamName: name }));
		toast.success(`Stream "${name}" deleted`);
		if (selectedStream?.name === name) {
			selectedStream = null;
			streamShards = [];
		}
		await loadStreams();
	} catch (err: unknown) {
		toast.error(`Delete failed: ${(err as Error).message}`);
	}
}

async function selectStream(name: string) {
	try {
		const res = await kinesis.send(new DescribeStreamCommand({ StreamName: name }));
		const desc = res.StreamDescription;
		if (!desc) return;
		selectedStream = {
			name: desc.StreamName ?? name,
			arn: desc.StreamARN ?? '',
			status: desc.StreamStatus ?? 'UNKNOWN',
			retention: desc.RetentionPeriodHours ?? 24
		};
		activeTab = 'shards';
		viewingShardId = null;
		await loadShards();
	} catch (err: unknown) {
		toast.error(`Failed to load stream details: ${(err as Error).message}`);
	}
}

async function loadShards() {
	if (!selectedStream) return;
	loadingShards = true;
	try {
		const res = await kinesis.send(new ListShardsCommand({ StreamName: selectedStream.name }));
		streamShards = res.Shards ?? [];
	} catch (err: unknown) {
		toast.error(`Failed to load shards: ${(err as Error).message}`);
	} finally {
		loadingShards = false;
	}
}

async function putRecord() {
	if (!selectedStream || !putRecordPartitionKey.trim() || !putRecordData) return;
	puttingRecord = true;
	try {
		const res = await kinesis.send(new PutRecordCommand({
			StreamName: selectedStream.name,
			PartitionKey: putRecordPartitionKey.trim(),
			Data: new TextEncoder().encode(putRecordData)
		}));
		toast.success(`Record put to shard ${res.ShardId}`);
		putRecordData = '';
	} catch (err: unknown) {
		toast.error(`PutRecord failed: ${(err as Error).message}`);
	} finally {
		puttingRecord = false;
	}
}

async function viewRecords(shardId: string) {
	if (!selectedStream) return;
	viewingShardId = shardId;
	loadingRecords = true;
	try {
		const iterRes = await kinesis.send(new GetShardIteratorCommand({
			StreamName: selectedStream.name,
			ShardId: shardId,
			ShardIteratorType: 'TRIM_HORIZON'
		}));
		const iterator = iterRes.ShardIterator;
		if (iterator) {
			const recRes = await kinesis.send(new GetRecordsCommand({
				ShardIterator: iterator,
				Limit: 100
			}));
			shardRecords = recRes.Records ?? [];
		} else {
			shardRecords = [];
		}
	} catch (err: unknown) {
		toast.error(`Failed to fetch records: ${(err as Error).message}`);
		shardRecords = [];
	} finally {
		loadingRecords = false;
	}
}

async function mergeShards(shardToMerge: string, adjacentShardToMerge: string) {
	if (!selectedStream) return;
	merging = true;
	try {
		await kinesis.send(new MergeShardsCommand({
			StreamName: selectedStream.name,
			ShardToMerge: shardToMerge,
			AdjacentShardToMerge: adjacentShardToMerge
		}));
		toast.success('Shards merged successfully');
		await loadShards();
	} catch (err: unknown) {
		toast.error(`Merge failed: ${(err as Error).message}`);
	} finally {
		merging = false;
	}
}

async function splitShard(shardToSplit: string, newStartingHashKey: string) {
	if (!selectedStream) return;
	splitting = true;
	try {
		await kinesis.send(new SplitShardCommand({
			StreamName: selectedStream.name,
			ShardToSplit: shardToSplit,
			NewStartingHashKey: newStartingHashKey
		}));
		toast.success('Shard split successfully');
		await loadShards();
	} catch (err: unknown) {
		toast.error(`Split failed: ${(err as Error).message}`);
	} finally {
		splitting = false;
	}
}

function parseRecordData(data: Uint8Array | undefined): string {
	if (!data) return '';
	return new TextDecoder().decode(data);
}

onMount(() => {
	loadStreams();
});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Activity class="w-6 h-6 text-blue-600 dark:text-blue-400" />
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
		<input
			type="text"
			bind:value={searchQuery}
			placeholder="Search streams..."
			class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
		/>
	</div>

	<!-- Stat Cards -->
	<div class="grid grid-cols-2 lg:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
			<p class="text-sm text-slate-500 dark:text-slate-400">Total Streams</p>
			<p class="text-2xl font-bold text-slate-900 dark:text-white">{streams.length}</p>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
			<p class="text-sm text-slate-500 dark:text-slate-400">Open Shards</p>
			<p class="text-2xl font-bold text-slate-900 dark:text-white">{streamShards.length}</p>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
			<p class="text-sm text-slate-500 dark:text-slate-400">Consumers</p>
			<p class="text-2xl font-bold text-slate-900 dark:text-white">0</p>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
			<p class="text-sm text-slate-500 dark:text-slate-400">Shards Used/Limit</p>
			<p class="text-2xl font-bold text-slate-900 dark:text-white">{streamShards.length}/500</p>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Stream List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading streams...</p>
				</div>
			{:else if filteredStreams.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<Activity class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No streams found</p>
				</div>
			{:else}
				{#each filteredStreams as streamName}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectStream(streamName)}
						onkeypress={(e) => { if (e.key === 'Enter') selectStream(streamName); }}
						class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-4 hover:border-indigo-400 transition-colors cursor-pointer {selectedStream?.name === streamName ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
					>
						<div class="flex items-center justify-between">
							<div class="min-w-0 flex-1">
								<p class="font-medium text-slate-900 dark:text-white truncate">{streamName}</p>
							</div>
							<div class="flex items-center gap-1 ml-2 flex-shrink-0">
								<button onclick={(e) => { e.stopPropagation(); deleteStream(streamName); }} class="p-1 text-slate-400 hover:text-red-500">
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
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
								<h2 class="text-xl font-bold text-slate-900 dark:text-white flex items-center gap-2">
									{selectedStream.name}
									<span class="px-2 py-0.5 text-xs rounded-full {selectedStream.status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300'}">{selectedStream.status}</span>
								</h2>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-1 font-mono">{selectedStream.arn}</p>
							</div>
						</div>
						<div class="grid grid-cols-2 gap-4 text-sm mt-4 pt-4 border-t border-slate-100 dark:border-slate-700">
							<div>
								<span class="text-slate-500 dark:text-slate-400 block">Retention Period</span>
								<span class="font-medium text-slate-900 dark:text-white">{selectedStream.retention} hours</span>
							</div>
							<div>
								<span class="text-slate-500 dark:text-slate-400 block">Status</span>
								<span class="font-medium text-slate-900 dark:text-white">{selectedStream.status}</span>
							</div>
						</div>
					</div>

					<div class="border-b border-slate-200 dark:border-slate-700">
						<div class="flex gap-4">
							<button
								onclick={() => activeTab = 'shards'}
								class="px-4 py-2 font-medium text-sm border-b-2 transition-colors {activeTab === 'shards' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
							>
								Shards & Records
							</button>
							<button
								onclick={() => activeTab = 'put_record'}
								class="px-4 py-2 font-medium text-sm border-b-2 transition-colors {activeTab === 'put_record' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
							>
								Put Record
							</button>
						</div>
					</div>

					{#if activeTab === 'shards'}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
							{#if loadingShards}
								<div class="p-8 text-center text-slate-500 dark:text-slate-400">Loading shards...</div>
							{:else if streamShards.length === 0}
								<div class="p-8 text-center text-slate-500 dark:text-slate-400">No shards found</div>
							{:else}
								<div class="divide-y divide-slate-100 dark:divide-slate-700">
									{#each streamShards as shard}
										<div class="p-4">
											<div class="flex items-center justify-between">
												<div>
													<h3 class="font-medium text-slate-900 dark:text-white flex items-center gap-2">
														<Layers class="w-4 h-4 text-indigo-500" />
														{shard.ShardId}
													</h3>
													<div class="mt-1 flex gap-4 text-xs font-mono text-slate-500 dark:text-slate-400">
														<span>Start: {shard.HashKeyRange?.StartingHashKey}</span>
														<span>End: {shard.HashKeyRange?.EndingHashKey}</span>
													</div>
												</div>
												<div class="flex items-center gap-2">
													<button
														onclick={() => viewRecords(shard.ShardId!)}
														class="px-3 py-1.5 text-xs font-medium text-slate-700 bg-slate-100 hover:bg-slate-200 dark:text-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600 rounded flex items-center gap-1"
													>
														<Inbox class="w-3 h-3" /> View Records
													</button>
													<button
														onclick={() => {
															const nextShard = streamShards.find(s => s.ShardId !== shard.ShardId);
															if (nextShard) mergeShards(shard.ShardId!, nextShard.ShardId!);
															else toast.error('No adjacent shard available');
														}}
														disabled={merging}
														class="px-3 py-1.5 text-xs font-medium text-indigo-700 bg-indigo-50 hover:bg-indigo-100 dark:text-indigo-300 dark:bg-indigo-900/30 dark:hover:bg-indigo-900/50 rounded flex items-center gap-1"
													>
														<Merge class="w-3 h-3" /> Merge
													</button>
													<button
														onclick={() => splitShard(shard.ShardId!, shard.HashKeyRange!.StartingHashKey!)}
														disabled={splitting}
														class="px-3 py-1.5 text-xs font-medium text-indigo-700 bg-indigo-50 hover:bg-indigo-100 dark:text-indigo-300 dark:bg-indigo-900/30 dark:hover:bg-indigo-900/50 rounded flex items-center gap-1"
													>
														<SplitSquareHorizontal class="w-3 h-3" /> Split
													</button>
												</div>
											</div>

											{#if viewingShardId === shard.ShardId}
												<div class="mt-4 pl-6 border-l-2 border-indigo-200 dark:border-indigo-800">
													{#if loadingRecords}
														<div class="text-sm text-slate-500 dark:text-slate-400 py-2">Loading records...</div>
													{:else if shardRecords.length === 0}
														<div class="text-sm text-slate-500 dark:text-slate-400 py-2">No records found.</div>
													{:else}
														<div class="space-y-2">
															{#each shardRecords as record}
																<div class="bg-slate-50 dark:bg-slate-900 p-3 rounded border border-slate-200 dark:border-slate-700">
																	<div class="flex justify-between items-center mb-2">
																		<span class="text-xs font-mono text-slate-500 dark:text-slate-400">Seq: {record.SequenceNumber}</span>
																		<span class="text-xs font-mono bg-slate-200 dark:bg-slate-700 px-2 py-0.5 rounded">PK: {record.PartitionKey}</span>
																	</div>
																	<pre class="text-sm text-slate-900 dark:text-slate-300 whitespace-pre-wrap font-mono overflow-hidden">{parseRecordData(record.Data)}</pre>
																</div>
															{/each}
														</div>
													{/if}
												</div>
											{/if}
										</div>
									{/each}
								</div>
							{/if}
						</div>
					{:else if activeTab === 'put_record'}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
							<div class="space-y-1">
								<label for="kinesis-partition-key" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Partition Key</label>
								<input
									id="kinesis-partition-key"
									type="text"
									bind:value={putRecordPartitionKey}
									placeholder="e.g. user-123"
									class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:text-white"
								/>
							</div>
							<div class="space-y-1">
								<label for="kinesis-data-payload" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Data payload</label>
								<textarea
									id="kinesis-data-payload"
									bind:value={putRecordData}
									rows="5"
									placeholder="Enter JSON or plain text data..."
									class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:text-white font-mono text-sm"
								></textarea>
							</div>
							<button
								onclick={putRecord}
								disabled={puttingRecord || !putRecordData || !putRecordPartitionKey}
								class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2"
							>
								{#if puttingRecord}
									<RefreshCw class="w-4 h-4 animate-spin" /> Putting...
								{:else}
									<Send class="w-4 h-4" /> Put Record
								{/if}
							</button>
						</div>
					{/if}
				</div>
			{:else}
				<div class="h-full flex items-center justify-center bg-slate-50 dark:bg-slate-800/50 rounded-lg border border-slate-200 dark:border-slate-700 border-dashed">
					<div class="text-center p-8">
						<Box class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">Select a stream to view details</p>
					</div>
				</div>
			{/if}
		</div>
	</div>
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4 bg-slate-900/50 backdrop-blur-sm">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md overflow-hidden border border-slate-200 dark:border-slate-700">
			<div class="px-6 py-4 border-b border-slate-100 dark:border-slate-700">
				<h3 class="text-lg font-bold text-slate-900 dark:text-white">Create Data Stream</h3>
			</div>
			<div class="p-6 space-y-4">
				<div class="space-y-1">
					<label for="kinesis-stream-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Stream Name</label>
					<input
						id="kinesis-stream-name"
						type="text"
						bind:value={newStreamName}
						placeholder="e.g. user-events"
						class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:text-white"
					/>
				</div>
				<div class="space-y-1">
					<label for="kinesis-shard-count" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Initial Shard Count</label>
					<input
						id="kinesis-shard-count"
						type="number"
						bind:value={newShardCount}
						min="1"
						max="100"
						class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:text-white"
					/>
				</div>
			</div>
			<div class="px-6 py-4 bg-slate-50 dark:bg-slate-900/50 border-t border-slate-100 dark:border-slate-700 flex justify-end gap-3">
				<button onclick={() => showCreateModal = false} class="px-4 py-2 text-sm font-medium text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-800 rounded-lg">
					Cancel
				</button>
				<button type="submit" onclick={createStream} disabled={creating || !newStreamName.trim()} class="px-4 py-2 text-sm font-medium bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
					{creating ? 'Creating...' : 'Create Stream'}
				</button>
			</div>
		</div>
	</div>
{/if}
