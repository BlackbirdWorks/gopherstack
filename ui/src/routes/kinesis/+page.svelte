<script lang="ts">
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
import { multiRegionList } from '$lib/multi-region';
import RegionChip from '$lib/components/RegionChip.svelte';
import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
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
	PutRecordsCommand,
	MergeShardsCommand,
	SplitShardCommand,
	UpdateShardCountCommand,
	IncreaseStreamRetentionPeriodCommand,
	DecreaseStreamRetentionPeriodCommand,
	StartStreamEncryptionCommand,
	StopStreamEncryptionCommand,
	UpdateStreamModeCommand,
	RegisterStreamConsumerCommand,
	DeregisterStreamConsumerCommand,
	ListStreamConsumersCommand,
	type Shard,
	type Consumer,
	type _Record as KinesisRecord
} from '@aws-sdk/client-kinesis';
import { toast } from 'svelte-sonner';
import { confirmDestructive } from '$lib/confirm-dialog';
import {
	Activity, Search, RefreshCw, Plus, Trash2, Send, Inbox,
	Layers, SplitSquareHorizontal, Merge, Box, Users, Settings, Lock, Unlock
} from 'lucide-svelte';

const kinesis = regionalClient(getKinesisClient);

interface StreamDetail {
	name: string;
	arn: string;
	status: string;
	retention: number;
	streamMode: string;
	encryptionType: string;
	keyId: string;
	region: string;
}

// Row and detail actions must use the region a stream's own row was fanned
// out from, not the page's shared `kinesis()` client -- in All mode the
// same stream name can legitimately exist in two different regions.

// State
let loading = $state(false);
let streams = $state<{ name: string; region: string }[]>([]);
let searchQuery = $state('');

let selectedStream = $state<StreamDetail | null>(null);
let streamShards = $state<Shard[]>([]);
let loadingShards = $state(false);

type Tab = 'shards' | 'put_record' | 'manage' | 'consumers';
let activeTab = $state<Tab>('shards');

// Create stream
let showCreateModal = $state(false);
let creating = $state(false);
let newStreamName = $state('');
let newShardCount = $state(1);
let newStreamMode = $state<'PROVISIONED' | 'ON_DEMAND'>('PROVISIONED');

// Put Record / Put Records (batch)
let putRecordPartitionKey = $state('');
let putRecordData = $state('');
let puttingRecord = $state(false);
let batchInput = $state('');
let puttingBatch = $state(false);

// Get Records — iterator + paging
let viewingShardId = $state<string | null>(null);
let shardRecords = $state<KinesisRecord[]>([]);
let loadingRecords = $state(false);
let loadingMore = $state(false);
let iteratorType = $state<'TRIM_HORIZON' | 'LATEST'>('TRIM_HORIZON');
let nextShardIterator = $state<string | null>(null);

// Shard operations
let merging = $state(false);
let splitting = $state(false);

// Reshard
let reshardTarget = $state(2);
let resharding = $state(false);

// Retention
let retentionTarget = $state(24);
let updatingRetention = $state(false);

// Encryption
let encryptionKeyId = $state('alias/aws/kinesis');
let updatingEncryption = $state(false);

// Stream mode
let modeTarget = $state<'PROVISIONED' | 'ON_DEMAND'>('PROVISIONED');
let updatingMode = $state(false);

// Consumers
let consumers = $state<Consumer[]>([]);
let loadingConsumers = $state(false);
let newConsumerName = $state('');
let registeringConsumer = $state(false);

const filteredStreams = $derived(
	streams.filter(s => s.name.toLowerCase().includes(searchQuery.toLowerCase()))
);

// Fans ListStreams out across every region with data in All mode
// (single-region mode collapses to exactly one call).
async function loadStreams() {
	loading = true;
	try {
		const result = await multiRegionList(
			(region) => getKinesisClient(region).send(new ListStreamsCommand({ Limit: 100 })),
			(r) => r.StreamNames ?? []
		);
		streams = result.items.map(({ region, item }) => ({ name: item, region }));
		if (result.errors.length > 0) toast.error(`Failed to load streams from ${result.errors.length} region(s)`);
	} catch (err: unknown) {
		toast.error(`Failed to load streams: ${(err as Error).message}`);
	} finally {
		loading = false;
	}
}

async function createStream() {
	if (!newStreamName.trim()) return;
	creating = true;
	try {
		await kinesis().send(new CreateStreamCommand({
			StreamName: newStreamName.trim(),
			// ON_DEMAND streams must not specify a shard count.
			...(newStreamMode === 'PROVISIONED' ? { ShardCount: newShardCount } : {}),
			StreamModeDetails: { StreamMode: newStreamMode }
		}));
		toast.success(`Stream "${newStreamName}" created`);
		showCreateModal = false;
		newStreamName = '';
		newShardCount = 1;
		newStreamMode = 'PROVISIONED';
		await loadStreams();
	} catch (err: unknown) {
		toast.error(`Create failed: ${(err as Error).message}`);
	} finally {
		creating = false;
	}
}

async function deleteStream(name: string, region: string) {
	if (!await confirmDestructive({ title: 'Delete Stream', message: `Delete stream "${name}"?` })) return;
	try {
		await getKinesisClient(region).send(new DeleteStreamCommand({ StreamName: name }));
		toast.success(`Stream "${name}" deleted`);
		if (selectedStream?.name === name) {
			selectedStream = null;
			streamShards = [];
			consumers = [];
		}
		await loadStreams();
	} catch (err: unknown) {
		toast.error(`Delete failed: ${(err as Error).message}`);
	}
}

async function selectStream(name: string, region: string) {
	try {
		const res = await getKinesisClient(region).send(new DescribeStreamCommand({ StreamName: name }));
		const desc = res.StreamDescription;
		if (!desc) return;
		selectedStream = {
			name: desc.StreamName ?? name,
			arn: desc.StreamARN ?? '',
			status: desc.StreamStatus ?? 'UNKNOWN',
			retention: desc.RetentionPeriodHours ?? 24,
			streamMode: desc.StreamModeDetails?.StreamMode ?? 'PROVISIONED',
			encryptionType: desc.EncryptionType ?? 'NONE',
			keyId: desc.KeyId ?? '',
			region
		};
		activeTab = 'shards';
		viewingShardId = null;
		nextShardIterator = null;
		retentionTarget = selectedStream.retention;
		modeTarget = selectedStream.streamMode === 'ON_DEMAND' ? 'ON_DEMAND' : 'PROVISIONED';
		if (selectedStream.keyId) encryptionKeyId = selectedStream.keyId;
		await Promise.all([loadShards(), loadConsumers()]);
		reshardTarget = Math.max(1, streamShards.length);
	} catch (err: unknown) {
		toast.error(`Failed to load stream details: ${(err as Error).message}`);
	}
}

async function loadShards() {
	if (!selectedStream) return;
	loadingShards = true;
	try {
		const res = await getKinesisClient(selectedStream.region).send(new ListShardsCommand({ StreamName: selectedStream.name }));
		streamShards = res.Shards ?? [];
	} catch (err: unknown) {
		toast.error(`Failed to load shards: ${(err as Error).message}`);
	} finally {
		loadingShards = false;
	}
}

async function loadConsumers() {
	if (!selectedStream?.arn) {
		consumers = [];
		return;
	}
	loadingConsumers = true;
	try {
		const res = await getKinesisClient(selectedStream.region).send(new ListStreamConsumersCommand({ StreamARN: selectedStream.arn }));
		consumers = res.Consumers ?? [];
	} catch (err: unknown) {
		// A stream with no consumers should simply show an empty list.
		consumers = [];
	} finally {
		loadingConsumers = false;
	}
}

async function putRecord() {
	if (!selectedStream || !putRecordPartitionKey.trim() || !putRecordData) return;
	puttingRecord = true;
	try {
		const res = await getKinesisClient(selectedStream.region).send(new PutRecordCommand({
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

// putRecordsBatch parses lines of "partitionKey<TAB or comma>data" and sends
// them as a single PutRecords request.
async function putRecordsBatch() {
	if (!selectedStream || !batchInput.trim()) return;
	const entries = batchInput
		.split('\n')
		.map(line => line.trim())
		.filter(line => line.length > 0)
		.map(line => {
			const sep = line.includes('\t') ? '\t' : ',';
			const idx = line.indexOf(sep);
			const pk = idx >= 0 ? line.slice(0, idx).trim() : line;
			const data = idx >= 0 ? line.slice(idx + 1) : line;
			return { PartitionKey: pk || 'pk', Data: new TextEncoder().encode(data) };
		});
	if (entries.length === 0) return;
	puttingBatch = true;
	try {
		const res = await getKinesisClient(selectedStream.region).send(new PutRecordsCommand({
			StreamName: selectedStream.name,
			Records: entries
		}));
		const failed = res.FailedRecordCount ?? 0;
		if (failed > 0) {
			toast.warning(`PutRecords: ${entries.length - failed} succeeded, ${failed} failed`);
		} else {
			toast.success(`PutRecords: ${entries.length} records written`);
		}
		batchInput = '';
	} catch (err: unknown) {
		toast.error(`PutRecords failed: ${(err as Error).message}`);
	} finally {
		puttingBatch = false;
	}
}

async function viewRecords(shardId: string) {
	if (!selectedStream) return;
	viewingShardId = shardId;
	loadingRecords = true;
	shardRecords = [];
	nextShardIterator = null;
	try {
		const iterRes = await getKinesisClient(selectedStream.region).send(new GetShardIteratorCommand({
			StreamName: selectedStream.name,
			ShardId: shardId,
			ShardIteratorType: iteratorType
		}));
		const iterator = iterRes.ShardIterator;
		if (iterator) {
			const recRes = await getKinesisClient(selectedStream.region).send(new GetRecordsCommand({
				ShardIterator: iterator,
				Limit: 100
			}));
			shardRecords = recRes.Records ?? [];
			nextShardIterator = recRes.NextShardIterator ?? null;
		}
	} catch (err: unknown) {
		toast.error(`Failed to fetch records: ${(err as Error).message}`);
		shardRecords = [];
	} finally {
		loadingRecords = false;
	}
}

// loadMoreRecords pages forward using the NextShardIterator returned by the
// previous GetRecords call, appending any new records.
async function loadMoreRecords() {
	if (!nextShardIterator || !selectedStream) return;
	loadingMore = true;
	try {
		const recRes = await getKinesisClient(selectedStream.region).send(new GetRecordsCommand({
			ShardIterator: nextShardIterator,
			Limit: 100
		}));
		const more = recRes.Records ?? [];
		if (more.length > 0) shardRecords = [...shardRecords, ...more];
		nextShardIterator = recRes.NextShardIterator ?? null;
		if (more.length === 0) toast.info('No further records at this position');
	} catch (err: unknown) {
		toast.error(`Failed to page records: ${(err as Error).message}`);
	} finally {
		loadingMore = false;
	}
}

// adjacentShardId returns the ShardId of an OPEN shard whose hash-key range is
// directly adjacent to the given shard (required by MergeShards).
function adjacentShardId(shard: Shard): string | null {
	const range = shard.HashKeyRange;
	if (!range?.StartingHashKey || !range?.EndingHashKey) return null;
	try {
		const start = BigInt(range.StartingHashKey);
		const end = BigInt(range.EndingHashKey);
		for (const s of streamShards) {
			if (s.ShardId === shard.ShardId) continue;
			const r = s.HashKeyRange;
			if (!r?.StartingHashKey || !r?.EndingHashKey) continue;
			const sStart = BigInt(r.StartingHashKey);
			const sEnd = BigInt(r.EndingHashKey);
			if (sStart === end + 1n || start === sEnd + 1n) return s.ShardId ?? null;
		}
	} catch {
		return null;
	}
	return null;
}

// midpointHashKey returns a hash key strictly interior to the shard's range,
// as required by SplitShard (the range must be at least 2 keys wide).
function midpointHashKey(shard: Shard): string | null {
	const range = shard.HashKeyRange;
	if (!range?.StartingHashKey || !range?.EndingHashKey) return null;
	try {
		const start = BigInt(range.StartingHashKey);
		const end = BigInt(range.EndingHashKey);
		if (end - start < 2n) return null;
		return ((start + end) / 2n).toString();
	} catch {
		return null;
	}
}

async function mergeShardClicked(shard: Shard) {
	const adjacent = adjacentShardId(shard);
	if (!adjacent) {
		toast.error('No adjacent open shard available to merge with');
		return;
	}
	if (!selectedStream) return;
	merging = true;
	try {
		await getKinesisClient(selectedStream.region).send(new MergeShardsCommand({
			StreamName: selectedStream.name,
			ShardToMerge: shard.ShardId,
			AdjacentShardToMerge: adjacent
		}));
		toast.success('Shards merged successfully');
		await loadShards();
	} catch (err: unknown) {
		toast.error(`Merge failed: ${(err as Error).message}`);
	} finally {
		merging = false;
	}
}

async function splitShardClicked(shard: Shard) {
	const mid = midpointHashKey(shard);
	if (!mid) {
		toast.error('Shard range is too narrow to split');
		return;
	}
	if (!selectedStream) return;
	splitting = true;
	try {
		await getKinesisClient(selectedStream.region).send(new SplitShardCommand({
			StreamName: selectedStream.name,
			ShardToSplit: shard.ShardId,
			NewStartingHashKey: mid
		}));
		toast.success('Shard split successfully');
		await loadShards();
	} catch (err: unknown) {
		toast.error(`Split failed: ${(err as Error).message}`);
	} finally {
		splitting = false;
	}
}

async function reshard() {
	if (!selectedStream || reshardTarget < 1) return;
	resharding = true;
	try {
		await getKinesisClient(selectedStream.region).send(new UpdateShardCountCommand({
			StreamName: selectedStream.name,
			TargetShardCount: reshardTarget,
			ScalingType: 'UNIFORM_SCALING'
		}));
		toast.success(`Resharding to ${reshardTarget} shards`);
		await loadShards();
	} catch (err: unknown) {
		toast.error(`UpdateShardCount failed: ${(err as Error).message}`);
	} finally {
		resharding = false;
	}
}

async function updateRetention(increase: boolean) {
	if (!selectedStream) return;
	updatingRetention = true;
	try {
		const cmd = increase
			? new IncreaseStreamRetentionPeriodCommand({
				StreamName: selectedStream.name,
				RetentionPeriodHours: retentionTarget
			})
			: new DecreaseStreamRetentionPeriodCommand({
				StreamName: selectedStream.name,
				RetentionPeriodHours: retentionTarget
			});
		await getKinesisClient(selectedStream.region).send(cmd);
		toast.success(`Retention set to ${retentionTarget} hours`);
		if (selectedStream) selectedStream = { ...selectedStream, retention: retentionTarget };
	} catch (err: unknown) {
		toast.error(`Retention update failed: ${(err as Error).message}`);
	} finally {
		updatingRetention = false;
	}
}

async function toggleEncryption(enable: boolean) {
	if (!selectedStream) return;
	updatingEncryption = true;
	try {
		const cmd = enable
			? new StartStreamEncryptionCommand({
				StreamName: selectedStream.name,
				EncryptionType: 'KMS',
				KeyId: encryptionKeyId.trim() || 'alias/aws/kinesis'
			})
			: new StopStreamEncryptionCommand({
				StreamName: selectedStream.name,
				EncryptionType: 'KMS',
				KeyId: selectedStream.keyId || encryptionKeyId.trim() || 'alias/aws/kinesis'
			});
		await getKinesisClient(selectedStream.region).send(cmd);
		toast.success(enable ? 'Encryption enabled' : 'Encryption disabled');
		if (selectedStream) {
			selectedStream = {
				...selectedStream,
				encryptionType: enable ? 'KMS' : 'NONE',
				keyId: enable ? encryptionKeyId.trim() : ''
			};
		}
	} catch (err: unknown) {
		toast.error(`Encryption update failed: ${(err as Error).message}`);
	} finally {
		updatingEncryption = false;
	}
}

async function updateStreamMode() {
	if (!selectedStream?.arn) return;
	updatingMode = true;
	try {
		await getKinesisClient(selectedStream.region).send(new UpdateStreamModeCommand({
			StreamARN: selectedStream.arn,
			StreamModeDetails: { StreamMode: modeTarget }
		}));
		toast.success(`Stream mode set to ${modeTarget}`);
		if (selectedStream) selectedStream = { ...selectedStream, streamMode: modeTarget };
	} catch (err: unknown) {
		toast.error(`UpdateStreamMode failed: ${(err as Error).message}`);
	} finally {
		updatingMode = false;
	}
}

async function registerConsumer() {
	if (!selectedStream?.arn || !newConsumerName.trim()) return;
	registeringConsumer = true;
	try {
		await getKinesisClient(selectedStream.region).send(new RegisterStreamConsumerCommand({
			StreamARN: selectedStream.arn,
			ConsumerName: newConsumerName.trim()
		}));
		toast.success(`Consumer "${newConsumerName}" registered`);
		newConsumerName = '';
		await loadConsumers();
	} catch (err: unknown) {
		toast.error(`RegisterStreamConsumer failed: ${(err as Error).message}`);
	} finally {
		registeringConsumer = false;
	}
}

async function deregisterConsumer(consumer: Consumer) {
	if (!selectedStream?.arn || !consumer.ConsumerName) return;
	try {
		await getKinesisClient(selectedStream.region).send(new DeregisterStreamConsumerCommand({
			StreamARN: selectedStream.arn,
			ConsumerName: consumer.ConsumerName,
			ConsumerARN: consumer.ConsumerARN
		}));
		toast.success(`Consumer "${consumer.ConsumerName}" deregistered`);
		await loadConsumers();
	} catch (err: unknown) {
		toast.error(`DeregisterStreamConsumer failed: ${(err as Error).message}`);
	}
}

function parseRecordData(data: Uint8Array | undefined): string {
	if (!data) return '';
	return new TextDecoder().decode(data);
}

// A stream name/ARN is only unique within a region, so a stream selected
// (and its shards/records/consumers) in the old region must not survive a
// region switch -- clear that state and fall back to the list.
onRegionChange(() => {
	selectedStream = null;
	streamShards = [];
	consumers = [];
	activeTab = 'shards';
	viewingShardId = null;
	shardRecords = [];
	nextShardIterator = null;
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
			<WriteRegionHint />
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
			<p class="text-2xl font-bold text-slate-900 dark:text-white">{consumers.length}</p>
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
				{#each filteredStreams as stream}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectStream(stream.name, stream.region)}
						onkeypress={(e) => { if (e.key === 'Enter') selectStream(stream.name, stream.region); }}
						class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-4 hover:border-indigo-400 transition-colors cursor-pointer {selectedStream?.name === stream.name && selectedStream?.region === stream.region ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
					>
						<div class="flex items-center justify-between">
							<div class="min-w-0 flex-1">
								<div class="flex items-center gap-2">
									<p class="font-medium text-slate-900 dark:text-white truncate">{stream.name}</p>
									<RegionChip region={stream.region} />
								</div>
							</div>
							<div class="flex items-center gap-1 ml-2 flex-shrink-0">
								<button onclick={(e) => { e.stopPropagation(); deleteStream(stream.name, stream.region); }} class="p-1 text-slate-400 hover:text-red-500">
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
									<RegionChip region={selectedStream.region} />
									<span class="px-2 py-0.5 text-xs rounded-full {selectedStream.status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-800 text-slate-700 dark:text-slate-300'}">{selectedStream.status}</span>
								</h2>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-1 font-mono">{selectedStream.arn}</p>
							</div>
						</div>
						<div class="grid grid-cols-2 lg:grid-cols-4 gap-4 text-sm mt-4 pt-4 border-t border-slate-100 dark:border-slate-700">
							<div>
								<span class="text-slate-500 dark:text-slate-400 block">Retention</span>
								<span class="font-medium text-slate-900 dark:text-white">{selectedStream.retention}h</span>
							</div>
							<div>
								<span class="text-slate-500 dark:text-slate-400 block">Mode</span>
								<span class="font-medium text-slate-900 dark:text-white">{selectedStream.streamMode}</span>
							</div>
							<div>
								<span class="text-slate-500 dark:text-slate-400 block">Encryption</span>
								<span class="font-medium text-slate-900 dark:text-white">{selectedStream.encryptionType}</span>
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
								class="px-4 py-2 font-medium text-sm border-b-2 transition-colors flex items-center gap-1 {activeTab === 'shards' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
							>
								<Layers class="w-4 h-4" /> Shards &amp; Records
							</button>
							<button
								onclick={() => activeTab = 'put_record'}
								class="px-4 py-2 font-medium text-sm border-b-2 transition-colors flex items-center gap-1 {activeTab === 'put_record' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
							>
								<Send class="w-4 h-4" /> Put Records
							</button>
							<button
								onclick={() => activeTab = 'manage'}
								class="px-4 py-2 font-medium text-sm border-b-2 transition-colors flex items-center gap-1 {activeTab === 'manage' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
							>
								<Settings class="w-4 h-4" /> Manage
							</button>
							<button
								onclick={() => activeTab = 'consumers'}
								class="px-4 py-2 font-medium text-sm border-b-2 transition-colors flex items-center gap-1 {activeTab === 'consumers' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
							>
								<Users class="w-4 h-4" /> Consumers
							</button>
						</div>
					</div>

					{#if activeTab === 'shards'}
						<div class="flex items-center gap-2 text-sm">
							<label for="iter-type" class="text-slate-500 dark:text-slate-400">Iterator:</label>
							<select
								id="iter-type"
								bind:value={iteratorType}
								class="px-2 py-1 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded text-slate-900 dark:text-white"
							>
								<option value="TRIM_HORIZON">TRIM_HORIZON</option>
								<option value="LATEST">LATEST</option>
							</select>
						</div>
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
														onclick={() => mergeShardClicked(shard)}
														disabled={merging}
														class="px-3 py-1.5 text-xs font-medium text-indigo-700 bg-indigo-50 hover:bg-indigo-100 dark:text-indigo-300 dark:bg-indigo-900/30 dark:hover:bg-indigo-900/50 rounded flex items-center gap-1 disabled:opacity-50"
													>
														<Merge class="w-3 h-3" /> Merge
													</button>
													<button
														onclick={() => splitShardClicked(shard)}
														disabled={splitting}
														class="px-3 py-1.5 text-xs font-medium text-indigo-700 bg-indigo-50 hover:bg-indigo-100 dark:text-indigo-300 dark:bg-indigo-900/30 dark:hover:bg-indigo-900/50 rounded flex items-center gap-1 disabled:opacity-50"
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
													{#if nextShardIterator}
														<button
															onclick={loadMoreRecords}
															disabled={loadingMore}
															class="mt-3 px-3 py-1.5 text-xs font-medium text-indigo-700 bg-indigo-50 hover:bg-indigo-100 dark:text-indigo-300 dark:bg-indigo-900/30 dark:hover:bg-indigo-900/50 rounded flex items-center gap-1 disabled:opacity-50"
														>
															<RefreshCw class="w-3 h-3 {loadingMore ? 'animate-spin' : ''}" /> Load more (next iterator)
														</button>
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
									rows="4"
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

							<div class="pt-4 border-t border-slate-100 dark:border-slate-700 space-y-1">
								<label for="kinesis-batch" class="block text-sm font-medium text-slate-700 dark:text-slate-300">
									Batch (PutRecords) — one record per line, <code>partitionKey,data</code>
								</label>
								<textarea
									id="kinesis-batch"
									bind:value={batchInput}
									rows="4"
									placeholder={"user-1,hello\nuser-2,world"}
									class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:text-white font-mono text-sm"
								></textarea>
								<button
									onclick={putRecordsBatch}
									disabled={puttingBatch || !batchInput.trim()}
									class="mt-2 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2"
								>
									{#if puttingBatch}
										<RefreshCw class="w-4 h-4 animate-spin" /> Sending batch...
									{:else}
										<Send class="w-4 h-4" /> Put Records (batch)
									{/if}
								</button>
							</div>
						</div>
					{:else if activeTab === 'manage'}
						<div class="space-y-4">
							<!-- Reshard -->
							<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-3">
								<h3 class="font-semibold text-slate-900 dark:text-white flex items-center gap-2"><Layers class="w-4 h-4 text-indigo-500" /> Reshard (UpdateShardCount)</h3>
								<p class="text-xs text-slate-500 dark:text-slate-400">Target must be within 50%–200% of the current shard count.</p>
								<div class="flex items-end gap-3">
									<div class="space-y-1">
										<label for="reshard-target" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Target shard count</label>
										<input id="reshard-target" type="number" min="1" max="500" bind:value={reshardTarget} class="w-32 px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg dark:text-white" />
									</div>
									<button onclick={reshard} disabled={resharding || selectedStream.streamMode === 'ON_DEMAND'} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
										{resharding ? 'Resharding...' : 'Apply'}
									</button>
								</div>
							</div>

							<!-- Retention -->
							<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-3">
								<h3 class="font-semibold text-slate-900 dark:text-white flex items-center gap-2"><RefreshCw class="w-4 h-4 text-indigo-500" /> Retention Period</h3>
								<div class="flex items-end gap-3">
									<div class="space-y-1">
										<label for="retention-target" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Hours (24–8760)</label>
										<input id="retention-target" type="number" min="24" max="8760" bind:value={retentionTarget} class="w-32 px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg dark:text-white" />
									</div>
									<button onclick={() => updateRetention(true)} disabled={updatingRetention} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">Increase</button>
									<button onclick={() => updateRetention(false)} disabled={updatingRetention} class="px-4 py-2 bg-slate-600 text-white rounded-lg hover:bg-slate-700 disabled:opacity-50">Decrease</button>
								</div>
							</div>

							<!-- Encryption -->
							<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-3">
								<h3 class="font-semibold text-slate-900 dark:text-white flex items-center gap-2"><Lock class="w-4 h-4 text-indigo-500" /> Server-Side Encryption</h3>
								<div class="flex items-end gap-3">
									<div class="space-y-1 flex-1">
										<label for="enc-key" class="block text-sm font-medium text-slate-700 dark:text-slate-300">KMS Key ID</label>
										<input id="enc-key" type="text" bind:value={encryptionKeyId} class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg dark:text-white font-mono text-sm" />
									</div>
									<button onclick={() => toggleEncryption(true)} disabled={updatingEncryption} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-1"><Lock class="w-4 h-4" /> Enable</button>
									<button onclick={() => toggleEncryption(false)} disabled={updatingEncryption} class="px-4 py-2 bg-slate-600 text-white rounded-lg hover:bg-slate-700 disabled:opacity-50 flex items-center gap-1"><Unlock class="w-4 h-4" /> Disable</button>
								</div>
							</div>

							<!-- Stream mode -->
							<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-3">
								<h3 class="font-semibold text-slate-900 dark:text-white flex items-center gap-2"><Settings class="w-4 h-4 text-indigo-500" /> Stream Mode</h3>
								<div class="flex items-end gap-3">
									<div class="space-y-1">
										<label for="mode-target" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Mode</label>
										<select id="mode-target" bind:value={modeTarget} class="px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg dark:text-white">
											<option value="PROVISIONED">PROVISIONED</option>
											<option value="ON_DEMAND">ON_DEMAND</option>
										</select>
									</div>
									<button onclick={updateStreamMode} disabled={updatingMode} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">Update Mode</button>
								</div>
							</div>
						</div>
					{:else if activeTab === 'consumers'}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
							<div class="flex items-end gap-3">
								<div class="space-y-1 flex-1">
									<label for="consumer-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Register enhanced fan-out consumer</label>
									<input id="consumer-name" type="text" bind:value={newConsumerName} placeholder="e.g. analytics-app" class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg dark:text-white" />
								</div>
								<button onclick={registerConsumer} disabled={registeringConsumer || !newConsumerName.trim()} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-1"><Plus class="w-4 h-4" /> Register</button>
							</div>
							<div class="pt-4 border-t border-slate-100 dark:border-slate-700">
								{#if loadingConsumers}
									<div class="text-sm text-slate-500 dark:text-slate-400">Loading consumers...</div>
								{:else if consumers.length === 0}
									<div class="text-sm text-slate-500 dark:text-slate-400">No consumers registered.</div>
								{:else}
									<div class="divide-y divide-slate-100 dark:divide-slate-700">
										{#each consumers as consumer}
											<div class="flex items-center justify-between py-3">
												<div class="min-w-0">
													<p class="font-medium text-slate-900 dark:text-white truncate">{consumer.ConsumerName}</p>
													<p class="text-xs font-mono text-slate-500 dark:text-slate-400 truncate">{consumer.ConsumerARN}</p>
													<span class="text-xs text-slate-500 dark:text-slate-400">{consumer.ConsumerStatus}</span>
												</div>
												<button onclick={() => deregisterConsumer(consumer)} class="p-1 text-slate-400 hover:text-red-500 flex-shrink-0" title="Deregister">
													<Trash2 class="w-4 h-4" />
												</button>
											</div>
										{/each}
									</div>
								{/if}
							</div>
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
					<label for="kinesis-stream-mode" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Stream Mode</label>
					<select
						id="kinesis-stream-mode"
						bind:value={newStreamMode}
						class="w-full px-3 py-2 bg-white dark:bg-slate-900 border border-slate-300 dark:border-slate-600 rounded-lg focus:ring-2 focus:ring-indigo-500 dark:text-white"
					>
						<option value="PROVISIONED">PROVISIONED</option>
						<option value="ON_DEMAND">ON_DEMAND</option>
					</select>
				</div>
				{#if newStreamMode === 'PROVISIONED'}
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
				{/if}
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
