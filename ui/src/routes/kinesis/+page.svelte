<script lang="ts">
import { confirmDestructive } from '$lib/confirm-dialog';
import { onMount, onDestroy } from 'svelte';
import { getKinesisClient } from '$lib/aws-client';
import {
ListStreamsCommand,
DescribeStreamSummaryCommand,
DescribeStreamCommand,
CreateStreamCommand,
DeleteStreamCommand,
PutRecordCommand,
PutRecordsCommand,
RegisterStreamConsumerCommand,
ListStreamConsumersCommand,
DeregisterStreamConsumerCommand,
GetShardIteratorCommand,
GetRecordsCommand,
ListShardsCommand,
AddTagsToStreamCommand,
RemoveTagsFromStreamCommand,
ListTagsForStreamCommand,
IncreaseStreamRetentionPeriodCommand,
DecreaseStreamRetentionPeriodCommand,
UpdateShardCountCommand,
UpdateStreamModeCommand,
StartStreamEncryptionCommand,
StopStreamEncryptionCommand,
EnableEnhancedMonitoringCommand,
DisableEnhancedMonitoringCommand,
MergeShardsCommand,
SplitShardCommand,
DescribeLimitsCommand,
type StreamDescriptionSummary,
type Shard,
type MetricsName,
ShardIteratorType,
StreamMode
} from '@aws-sdk/client-kinesis';
import { toast } from 'svelte-sonner';
import { Waves, Search, RefreshCw, Plus, Trash2, Send, Download, Tag, Shield, Activity, Layers, Settings, ChevronRight, AlertTriangle } from 'lucide-svelte';

const kinesis = getKinesisClient();

// Converts a Uint8Array to a base64 string.
function toBase64(arr: Uint8Array): string {
  return btoa(arr.reduce((s, b) => s + String.fromCodePoint(b), ''));
}

// ─── Streams list ───────────────────────────────────────────────
let loading = $state(false);
let streams = $state<string[]>([]);
let searchQuery = $state('');

// ─── Selected stream ────────────────────────────────────────────
let selectedStream = $state<string | null>(null);
let streamDetail = $state<StreamDescriptionSummary | null>(null);
let loadingDetail = $state(false);
let allShards = $state<Shard[]>([]);

// ─── Active tab ─────────────────────────────────────────────────
type DetailTab = 'overview' | 'shards' | 'records' | 'consumers' | 'tags' | 'settings';
let activeTab = $state<DetailTab>('overview');

// ─── Create modal ───────────────────────────────────────────────
let showCreateModal = $state(false);
let creating = $state(false);
let newStreamName = $state('');
let newShardCount = $state(1);
let newOnDemand = $state(false);

// ─── Put single record ──────────────────────────────────────────
let showPutModal = $state(false);
let putting = $state(false);
let putPartitionKey = $state('');
let putData = $state('');

// ─── Put batch records ──────────────────────────────────────────
let showPutBatchModal = $state(false);
let puttingBatch = $state(false);
let batchRecordsJson = $state('[{"PartitionKey":"pk1","Data":"hello"},{"PartitionKey":"pk2","Data":"world"}]');

// ─── Get records ────────────────────────────────────────────────
let records = $state<Array<{ sequenceNumber: string; data: string; rawData: string; partitionKey: string; arrivedAt: string }>>([]);
let gettingRecords = $state(false);
let selectedShardForRead = $state('');
let selectedIteratorType = $state('TRIM_HORIZON');
let recordDisplayMode = $state<'text' | 'base64'>('text');
let shardExhausted = $state(false);
let shardRecordCounts = $state<Record<string, number>>({});

// ─── Auto-refresh ────────────────────────────────────────────────
let autoRefresh = $state(false);
let autoRefreshInterval = $state<ReturnType<typeof setInterval> | null>(null);

// ─── Demo data ───────────────────────────────────────────────────
let loadingDemo = $state(false);

// ─── Consumers ──────────────────────────────────────────────────
let consumers = $state<Array<{ consumerName: string; consumerARN: string; consumerStatus: string }>>([]);
let loadingConsumers = $state(false);
let registeringConsumer = $state(false);
let deregisteringConsumerArn = $state<string | null>(null);
let newConsumerName = $state('');

// ─── Tags ────────────────────────────────────────────────────────
let tags = $state<Array<{ key: string; value: string }>>([]);
let loadingTags = $state(false);
let newTagKey = $state('');
let newTagValue = $state('');
let addingTag = $state(false);

// ─── Settings ────────────────────────────────────────────────────
// Retention
let retentionHours = $state(24);
let updatingRetention = $state(false);
// Shard count
let targetShardCount = $state(1);
let updatingShardCount = $state(false);
// Stream mode
let updatingStreamMode = $state(false);
// Encryption
let encryptionKeyId = $state('alias/aws/kinesis');
let updatingEncryption = $state(false);
// Enhanced monitoring
const ALL_METRICS = ['IncomingBytes', 'IncomingRecords', 'OutgoingBytes', 'OutgoingRecords', 'WriteProvisionedThroughputExceeded', 'ReadProvisionedThroughputExceeded', 'IteratorAgeMilliseconds'];
let updatingMonitoring = $state(false);

// ─── Shard operations ────────────────────────────────────────────
let shardToSplit = $state('');
let splitHashKey = $state('');
let splittingShard = $state(false);
let shardToMerge1 = $state('');
let shardToMerge2 = $state('');
let mergingShards = $state(false);

// ─── Account limits ──────────────────────────────────────────────
let accountLimits = $state<{ openShardCount: number; shardLimit: number } | null>(null);

const filteredStreams = $derived(
streams.filter((s) => s.toLowerCase().includes(searchQuery.toLowerCase()))
);

function statusColor(status: string | undefined): string {
if (status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
if (status === 'CREATING') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
if (status === 'DELETING') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
}

function shortArn(arn: string | undefined): string {
if (!arn) return 'N/A';
return arn.split('/').pop() ?? arn;
}

async function loadStreams() {
loading = true;
try {
const res = await kinesis.send(new ListStreamsCommand({ Limit: 100 }));
streams = res.StreamNames ?? [];
// Refresh account limits while we're at it.
loadAccountLimits();
} catch (err: unknown) {
toast.error(`Failed to load streams: ${(err as Error).message}`);
} finally {
loading = false;
}
}

async function loadAccountLimits() {
try {
const res = await kinesis.send(new DescribeLimitsCommand({}));
accountLimits = { openShardCount: res.OpenShardCount ?? 0, shardLimit: res.ShardLimit ?? 500 };
} catch {
// Non-critical; ignore if it fails.
}
}

async function selectStream(name: string) {
selectedStream = name;
streamDetail = null;
records = [];
consumers = [];
tags = [];
allShards = [];
activeTab = 'overview';
loadingDetail = true;
try {
const res = await kinesis.send(new DescribeStreamSummaryCommand({ StreamName: name }));
streamDetail = res.StreamDescriptionSummary ?? null;
if (streamDetail?.StreamARN) {
await Promise.all([
loadConsumers(streamDetail.StreamARN),
loadTags(name),
loadAllShards(name)
]);
}
// Pre-populate editable settings from current values
retentionHours = streamDetail?.RetentionPeriodHours ?? 24;
targetShardCount = streamDetail?.OpenShardCount ?? 1;
} catch (err: unknown) {
toast.error(`Failed to describe stream: ${(err as Error).message}`);
} finally {
loadingDetail = false;
}
}

async function refreshSelectedStreamSummary() {
if (!selectedStream) return;
const res = await kinesis.send(new DescribeStreamSummaryCommand({ StreamName: selectedStream }));
streamDetail = res.StreamDescriptionSummary ?? null;
retentionHours = streamDetail?.RetentionPeriodHours ?? 24;
targetShardCount = streamDetail?.OpenShardCount ?? 1;
}

async function loadAllShards(streamName: string) {
try {
const res = await kinesis.send(new ListShardsCommand({ StreamName: streamName }));
allShards = res.Shards ?? [];
if (allShards.length > 0 && !selectedShardForRead) {
selectedShardForRead = allShards[0].ShardId ?? '';
}
} catch {
allShards = [];
}
}

async function loadConsumers(streamARN: string) {
loadingConsumers = true;
try {
const res = await kinesis.send(new ListStreamConsumersCommand({ StreamARN: streamARN }));
consumers = (res.Consumers ?? []).map((consumer) => ({
consumerName: consumer.ConsumerName ?? '',
consumerARN: consumer.ConsumerARN ?? '',
consumerStatus: consumer.ConsumerStatus ?? ''
}));
} catch (err: unknown) {
toast.error(`Failed to list consumers: ${(err as Error).message}`);
} finally {
loadingConsumers = false;
}
}

async function loadTags(streamName: string) {
loadingTags = true;
try {
const res = await kinesis.send(new ListTagsForStreamCommand({ StreamName: streamName }));
tags = (res.Tags ?? []).map((t) => ({ key: t.Key ?? '', value: t.Value ?? '' }));
} catch {
tags = [];
} finally {
loadingTags = false;
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
if (!await confirmDestructive({ title: 'Delete Stream', message: `Delete Kinesis stream "${name}"? All shards and retained data will be permanently removed.` })) return;
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
const res = await kinesis.send(new PutRecordCommand({
StreamName: selectedStream,
PartitionKey: putPartitionKey.trim(),
Data: encoded
}));
toast.success(`Record put → shard ${res.ShardId}, seq ${res.SequenceNumber?.slice(-8)}…`);
showPutModal = false;
putPartitionKey = '';
putData = '';
} catch (err: unknown) {
toast.error(`Put record failed: ${(err as Error).message}`);
} finally {
putting = false;
}
}

async function putRecordsBatch() {
if (!selectedStream) return;
puttingBatch = true;
try {
const parsed: Array<{ PartitionKey: string; Data: string }> = JSON.parse(batchRecordsJson);
const batchEntries = parsed.map((r) => ({
PartitionKey: r.PartitionKey,
Data: new TextEncoder().encode(r.Data)
}));
const res = await kinesis.send(new PutRecordsCommand({
StreamName: selectedStream,
Records: batchEntries
}));
const failed = res.FailedRecordCount ?? 0;
if (failed > 0) {
toast.warning(`Batch put: ${batchEntries.length - failed} succeeded, ${failed} failed`);
} else {
toast.success(`Batch put: ${batchEntries.length} records written`);
}
showPutBatchModal = false;
} catch (err: unknown) {
toast.error(`Batch put failed: ${(err as Error).message}`);
} finally {
puttingBatch = false;
}
}

async function registerConsumer() {
if (!streamDetail?.StreamARN || !newConsumerName.trim()) return;
registeringConsumer = true;
try {
await kinesis.send(new RegisterStreamConsumerCommand({
StreamARN: streamDetail.StreamARN,
ConsumerName: newConsumerName.trim()
}));
newConsumerName = '';
await Promise.all([
loadConsumers(streamDetail.StreamARN),
refreshSelectedStreamSummary()
]);
toast.success('Consumer registered');
} catch (err: unknown) {
toast.error(`Register consumer failed: ${(err as Error).message}`);
} finally {
registeringConsumer = false;
}
}

async function deregisterConsumer(consumerARN: string) {
if (!streamDetail?.StreamARN) return;
if (!await confirmDestructive({ title: 'Deregister Consumer', message: 'Deregister this stream consumer?' })) return;
deregisteringConsumerArn = consumerARN;
try {
await kinesis.send(new DeregisterStreamConsumerCommand({ ConsumerARN: consumerARN }));
await Promise.all([
loadConsumers(streamDetail.StreamARN),
refreshSelectedStreamSummary()
]);
toast.success('Consumer deregistered');
} catch (err: unknown) {
toast.error(`Deregister consumer failed: ${(err as Error).message}`);
} finally {
deregisteringConsumerArn = null;
}
}

async function getRecords() {
if (!selectedStream || !selectedShardForRead) return;
gettingRecords = true;
shardExhausted = false;
try {
const iterRes = await kinesis.send(new GetShardIteratorCommand({
StreamName: selectedStream,
ShardId: selectedShardForRead,
ShardIteratorType: selectedIteratorType as ShardIteratorType
}));
const iterator = iterRes.ShardIterator;
if (!iterator) { toast.info('No iterator returned'); return; }
const recRes = await kinesis.send(new GetRecordsCommand({
ShardIterator: iterator,
Limit: 50
}));
records = (recRes.Records ?? []).map((r) => {
    let data = '';
    let rawData = '';
    if (r.Data) {
        rawData = toBase64(r.Data);
        try { data = new TextDecoder().decode(r.Data); } catch { data = rawData; }
    }
    return { sequenceNumber: r.SequenceNumber ?? '', partitionKey: r.PartitionKey ?? '', arrivedAt: r.ApproximateArrivalTimestamp ? new Date(r.ApproximateArrivalTimestamp).toLocaleTimeString() : '', data, rawData };
});
shardExhausted = !recRes.NextShardIterator;
shardRecordCounts = { ...shardRecordCounts, [selectedShardForRead]: records.length };
if (records.length === 0) toast.info('No records in this shard');
else toast.success(`Fetched ${recRes.Records?.length ?? 0} record(s)`);
} catch (err: unknown) {
toast.error(`Get records failed: ${(err as Error).message}`);
} finally {
gettingRecords = false;
}
}

async function addTag() {
if (!selectedStream || !newTagKey.trim()) return;
addingTag = true;
try {
await kinesis.send(new AddTagsToStreamCommand({
StreamName: selectedStream,
Tags: { [newTagKey.trim()]: newTagValue.trim() }
}));
newTagKey = '';
newTagValue = '';
await loadTags(selectedStream);
toast.success('Tag added');
} catch (err: unknown) {
toast.error(`Add tag failed: ${(err as Error).message}`);
} finally {
addingTag = false;
}
}

async function removeTag(key: string) {
if (!selectedStream) return;
try {
await kinesis.send(new RemoveTagsFromStreamCommand({
StreamName: selectedStream,
TagKeys: [key]
}));
await loadTags(selectedStream);
toast.success(`Tag "${key}" removed`);
} catch (err: unknown) {
toast.error(`Remove tag failed: ${(err as Error).message}`);
}
}

async function updateRetention() {
if (!selectedStream) return;
updatingRetention = true;
try {
const current = streamDetail?.RetentionPeriodHours ?? 24;
if (retentionHours > current) {
await kinesis.send(new IncreaseStreamRetentionPeriodCommand({
StreamName: selectedStream,
RetentionPeriodHours: retentionHours
}));
} else if (retentionHours < current) {
await kinesis.send(new DecreaseStreamRetentionPeriodCommand({
StreamName: selectedStream,
RetentionPeriodHours: retentionHours
}));
}
await refreshSelectedStreamSummary();
toast.success(`Retention updated to ${retentionHours}h`);
} catch (err: unknown) {
toast.error(`Update retention failed: ${(err as Error).message}`);
} finally {
updatingRetention = false;
}
}

async function updateShardCount() {
if (!selectedStream || targetShardCount < 1) return;
updatingShardCount = true;
try {
await kinesis.send(new UpdateShardCountCommand({
StreamName: selectedStream,
TargetShardCount: targetShardCount,
ScalingType: 'UNIFORM_SCALING'
}));
await Promise.all([refreshSelectedStreamSummary(), loadAllShards(selectedStream)]);
toast.success(`Shard count updated to ${targetShardCount}`);
} catch (err: unknown) {
toast.error(`Update shard count failed: ${(err as Error).message}`);
} finally {
updatingShardCount = false;
}
}

async function toggleStreamMode() {
if (!streamDetail?.StreamARN) return;
updatingStreamMode = true;
const currentMode = streamDetail.StreamModeDetails?.StreamMode ?? 'PROVISIONED';
const newMode = currentMode === 'ON_DEMAND' ? 'PROVISIONED' : 'ON_DEMAND';
try {
await kinesis.send(new UpdateStreamModeCommand({
StreamARN: streamDetail.StreamARN,
StreamModeDetails: { StreamMode: newMode as StreamMode }
}));
await refreshSelectedStreamSummary();
toast.success(`Stream mode changed to ${newMode}`);
} catch (err: unknown) {
toast.error(`Update stream mode failed: ${(err as Error).message}`);
} finally {
updatingStreamMode = false;
}
}

async function startEncryption() {
if (!selectedStream || !encryptionKeyId.trim()) return;
updatingEncryption = true;
try {
await kinesis.send(new StartStreamEncryptionCommand({
StreamName: selectedStream,
EncryptionType: 'KMS',
KeyId: encryptionKeyId.trim()
}));
await refreshSelectedStreamSummary();
toast.success('Encryption enabled');
} catch (err: unknown) {
toast.error(`Start encryption failed: ${(err as Error).message}`);
} finally {
updatingEncryption = false;
}
}

async function stopEncryption() {
if (!selectedStream) return;
if (!await confirmDestructive({ title: 'Stop Encryption', message: 'Disable server-side encryption on this stream?' })) return;
updatingEncryption = true;
try {
await kinesis.send(new StopStreamEncryptionCommand({
StreamName: selectedStream,
EncryptionType: 'KMS',
KeyId: streamDetail?.KeyId ?? ''
}));
await refreshSelectedStreamSummary();
toast.success('Encryption disabled');
} catch (err: unknown) {
toast.error(`Stop encryption failed: ${(err as Error).message}`);
} finally {
updatingEncryption = false;
}
}

async function enableMonitoring(metric: string) {
if (!selectedStream) return;
updatingMonitoring = true;
try {
await kinesis.send(new EnableEnhancedMonitoringCommand({
StreamName: selectedStream,
ShardLevelMetrics: [metric as MetricsName]
}));
await refreshSelectedStreamSummary();
toast.success(`Monitoring enabled: ${metric}`);
} catch (err: unknown) {
toast.error(`Enable monitoring failed: ${(err as Error).message}`);
} finally {
updatingMonitoring = false;
}
}

async function disableMonitoring(metric: string) {
if (!selectedStream) return;
updatingMonitoring = true;
try {
await kinesis.send(new DisableEnhancedMonitoringCommand({
StreamName: selectedStream,
ShardLevelMetrics: [metric as MetricsName]
}));
await refreshSelectedStreamSummary();
toast.success(`Monitoring disabled: ${metric}`);
} catch (err: unknown) {
toast.error(`Disable monitoring failed: ${(err as Error).message}`);
} finally {
updatingMonitoring = false;
}
}

async function splitShard() {
if (!selectedStream || !shardToSplit || !splitHashKey.trim()) return;
splittingShard = true;
try {
await kinesis.send(new SplitShardCommand({
StreamName: selectedStream,
ShardToSplit: shardToSplit,
NewStartingHashKey: splitHashKey.trim()
}));
await Promise.all([refreshSelectedStreamSummary(), loadAllShards(selectedStream)]);
shardToSplit = '';
splitHashKey = '';
toast.success('Shard split initiated');
} catch (err: unknown) {
toast.error(`Split shard failed: ${(err as Error).message}`);
} finally {
splittingShard = false;
}
}

async function mergeShards() {
if (!selectedStream || !shardToMerge1 || !shardToMerge2) return;
if (!await confirmDestructive({ title: 'Merge Shards', message: `Merge shards ${shardToMerge1} and ${shardToMerge2}?` })) return;
mergingShards = true;
try {
await kinesis.send(new MergeShardsCommand({
StreamName: selectedStream,
ShardToMerge: shardToMerge1,
AdjacentShardToMerge: shardToMerge2
}));
await Promise.all([refreshSelectedStreamSummary(), loadAllShards(selectedStream)]);
shardToMerge1 = '';
shardToMerge2 = '';
toast.success('Shards merged');
} catch (err: unknown) {
toast.error(`Merge shards failed: ${(err as Error).message}`);
} finally {
mergingShards = false;
}
}

const monitoringEnabled = $derived(
new Set<string>((streamDetail?.EnhancedMonitoring ?? []).flatMap(e => e.ShardLevelMetrics ?? [] as string[]))
);

function toggleAutoRefresh() {
autoRefresh = !autoRefresh;
if (autoRefresh) {
    autoRefreshInterval = setInterval(() => loadStreams(), 10000);
} else if (autoRefreshInterval !== null) {
    clearInterval(autoRefreshInterval);
    autoRefreshInterval = null;
}
}

async function loadDemoData() {
loadingDemo = true;
try {
    await kinesis.send(new CreateStreamCommand({ StreamName: 'demo-events', ShardCount: 1, StreamModeDetails: { StreamMode: 'PROVISIONED' } }));
    const demoRecords = [
        { PartitionKey: 'user-1', Data: new TextEncoder().encode(JSON.stringify({ event: 'page_view', userId: 'user-1', page: '/home' })) },
        { PartitionKey: 'user-2', Data: new TextEncoder().encode(JSON.stringify({ event: 'click', userId: 'user-2', element: 'buy-button' })) },
        { PartitionKey: 'user-1', Data: new TextEncoder().encode(JSON.stringify({ event: 'purchase', userId: 'user-1', amount: 49.99 })) },
    ];
    await kinesis.send(new PutRecordsCommand({ StreamName: 'demo-events', Records: demoRecords }));
    toast.success('Demo stream "demo-events" created with 3 records');
    await loadStreams();
} catch (err: unknown) {
    toast.error(`Demo load failed: ${(err as Error).message}`);
} finally {
    loadingDemo = false;
}
}

onMount(() => { loadStreams(); });
onDestroy(() => { if (autoRefreshInterval !== null) clearInterval(autoRefreshInterval); });
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
<button onclick={() => toggleAutoRefresh()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white {autoRefresh ? 'text-green-600 dark:text-green-400' : ''}" title="Auto-refresh (10s)">
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
<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Waves class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
<div>
<p class="text-2xl font-bold text-slate-900 dark:text-white">{streams.length}</p>
<p class="text-sm text-slate-500 dark:text-slate-400">Total Streams</p>
</div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Layers class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
<div>
<p class="text-2xl font-bold text-slate-900 dark:text-white">{streamDetail?.OpenShardCount ?? (selectedStream ? '…' : '—')}</p>
<p class="text-sm text-slate-500 dark:text-slate-400">Open Shards</p>
</div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg"><Download class="w-5 h-5 text-indigo-600 dark:text-indigo-400" /></div>
<div>
<p class="text-2xl font-bold text-slate-900 dark:text-white">{consumers.length > 0 ? consumers.length : (selectedStream ? consumers.length : '—')}</p>
<p class="text-sm text-slate-500 dark:text-slate-400">Consumers</p>
</div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg"><Activity class="w-5 h-5 text-amber-600 dark:text-amber-400" /></div>
<div>
<p class="text-2xl font-bold text-slate-900 dark:text-white">{accountLimits ? `${accountLimits.openShardCount}/${accountLimits.shardLimit}` : '—'}</p>
<p class="text-sm text-slate-500 dark:text-slate-400">Shards Used/Limit</p>
</div>
</div>
</div>

<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
<!-- Stream list -->
<div class="lg:col-span-1 space-y-2">
{#if loading}
<div class="text-center py-12">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
<p class="text-slate-500 dark:text-slate-400">Loading streams...</p>
</div>
{:else if filteredStreams.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
<Waves class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
<p class="text-slate-500 dark:text-slate-400 mb-3">No streams found</p>
{#if streams.length === 0 && !loading}
<button onclick={() => loadDemoData()} disabled={loadingDemo} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 text-sm">
    {loadingDemo ? 'Creating…' : 'Load Demo Data'}
</button>
{/if}
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
<!-- Header + action buttons -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<div class="flex items-start justify-between mb-4">
<div class="min-w-0">
<h2 class="text-xl font-bold text-slate-900 dark:text-white truncate">{selectedStream}</h2>
<p class="text-xs text-slate-400 truncate mt-0.5" title={streamDetail?.StreamARN ?? ''}>{streamDetail?.StreamARN ?? '…'}</p>
{#if streamDetail}
<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {statusColor(streamDetail.StreamStatus)}">{streamDetail.StreamStatus}</span>
{/if}
</div>
<div class="flex gap-2 flex-shrink-0 ml-4">
<button onclick={() => { showPutModal = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
<Send class="w-4 h-4" />Put
</button>
<button onclick={() => { showPutBatchModal = true; }} class="px-3 py-1.5 bg-indigo-500 text-white rounded-lg hover:bg-indigo-600 flex items-center gap-1.5 text-sm">
<Layers class="w-4 h-4" />Batch
</button>
<button onclick={() => { activeTab = 'records'; getRecords(); }} disabled={gettingRecords} class="px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-1.5 text-sm disabled:opacity-50">
<Download class="w-4 h-4" />{gettingRecords ? '…' : 'Read'}
</button>
</div>
</div>

<!-- Tabs -->
<div class="flex gap-1 border-b border-slate-200 dark:border-slate-700 -mx-5 px-5 overflow-x-auto">
{#each [
						{ id: 'overview', label: 'Overview', count: null },
						{ id: 'shards', label: 'Shards', count: allShards.length },
						{ id: 'records', label: 'Records', count: records.length },
						{ id: 'consumers', label: 'Consumers', count: consumers.length },
						{ id: 'tags', label: 'Tags', count: tags.length },
						{ id: 'settings', label: 'Settings', count: null }
					] as tab}
<button
onclick={() => { activeTab = tab.id as DetailTab; }}
class="px-3 py-2 text-sm font-medium whitespace-nowrap border-b-2 transition-colors {activeTab === tab.id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
>
{tab.label}{#if tab.count !== null && tab.count > 0} <span class="ml-1 px-1.5 py-0.5 text-xs rounded-full bg-slate-100 dark:bg-slate-700">{tab.count}</span>{/if}
</button>
{/each}
</div>
</div>

<!-- TAB: Overview -->
{#if activeTab === 'overview'}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
{#if loadingDetail}
<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
{:else if streamDetail}
<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
{#each [
{ label: 'Status', value: streamDetail.StreamStatus ?? '—' },
{ label: 'Shard Count', value: String(streamDetail.OpenShardCount ?? 0) },
{ label: 'Retention (hours)', value: String(streamDetail.RetentionPeriodHours ?? 24) },
{ label: 'Encryption', value: streamDetail.EncryptionType ?? 'NONE' },
{ label: 'Stream Mode', value: streamDetail.StreamModeDetails?.StreamMode ?? 'PROVISIONED' },
{ label: 'Consumers', value: String(consumers.length) },
{ label: 'Created', value: streamDetail.StreamCreationTimestamp ? new Date((streamDetail.StreamCreationTimestamp as unknown as number) * 1000).toLocaleString() : '—' },
] as kv}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
<p class="text-xs text-slate-500 dark:text-slate-400">{kv.label}</p>
<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate">{kv.value}</p>
</div>
{/each}
</div>
<!-- Shard-level metrics -->
{#if (streamDetail.EnhancedMonitoring ?? []).flatMap(e => e.ShardLevelMetrics ?? []).length > 0}
<div class="mt-3 p-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg">
<p class="text-xs text-slate-500 dark:text-slate-400 mb-2 flex items-center gap-1"><Activity class="w-3 h-3" /> Shard-Level Metrics</p>
<div class="flex flex-wrap gap-1">
{#each (streamDetail.EnhancedMonitoring ?? []).flatMap(e => e.ShardLevelMetrics ?? []) as metric}
<span class="px-1.5 py-0.5 text-xs rounded bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 font-mono">{metric}</span>
{/each}
</div>
</div>
{/if}
<!-- ARN row -->
<div class="mt-3 flex items-center gap-2 p-2 bg-slate-50 dark:bg-slate-700/50 rounded-lg">
<span class="text-xs text-slate-500 dark:text-slate-400 flex-shrink-0">ARN:</span>
<span class="text-xs font-mono text-slate-700 dark:text-slate-300 truncate flex-1">{streamDetail.StreamARN}</span>
<button onclick={() => navigator.clipboard.writeText(streamDetail?.StreamARN ?? '')} class="p-1 text-slate-400 hover:text-indigo-500 flex-shrink-0" title="Copy ARN">
    <svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z"/></svg>
</button>
</div>
{#if streamDetail.KeyId}
<div class="mt-3 p-3 bg-green-50 dark:bg-green-900/20 rounded-lg">
<p class="text-xs text-green-700 dark:text-green-300 flex items-center gap-1.5"><Shield class="w-3.5 h-3.5" />KMS Key: {streamDetail.KeyId}</p>
</div>
{/if}
{/if}
</div>
{/if}

<!-- TAB: Shards -->
{#if activeTab === 'shards'}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<div class="flex items-center justify-between mb-4">
<h3 class="font-semibold text-slate-900 dark:text-white">Shards ({allShards.length})</h3>
<button onclick={() => loadAllShards(selectedStream!)} class="p-1.5 text-slate-400 hover:text-slate-700 dark:hover:text-white">
<RefreshCw class="w-4 h-4" />
</button>
</div>
{#if allShards.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No shards found</p>
{:else}
<div class="space-y-2 max-h-72 overflow-y-auto">
{#each allShards as shard}
<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
<div class="flex items-center gap-2 mb-1">
<p class="text-sm font-mono font-semibold text-slate-900 dark:text-white">{shard.ShardId}</p>
{#if shard.SequenceNumberRange?.EndingSequenceNumber}
<span class="text-xs px-1.5 py-0.5 rounded-full bg-slate-200 dark:bg-slate-600 text-slate-500 dark:text-slate-400">CLOSED</span>
{:else}
<span class="text-xs px-1.5 py-0.5 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">ACTIVE</span>
{/if}
</div>
<p class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate">
{shard.HashKeyRange?.StartingHashKey?.slice(0,16)}…–{shard.HashKeyRange?.EndingHashKey?.slice(-16)}
</p>
{#if shard.ParentShardId}
<p class="text-xs text-indigo-500 mt-0.5">Parent: {shard.ParentShardId}</p>
{/if}
{#if shardRecordCounts[shard.ShardId ?? '']}
<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{shardRecordCounts[shard.ShardId ?? '']} records fetched</p>
{/if}
</div>
{/each}
</div>
{/if}

<!-- Shard operations -->
<div class="mt-6 space-y-4 border-t border-slate-200 dark:border-slate-700 pt-4">
<h4 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Split Shard</h4>
<div class="grid grid-cols-2 gap-2">
<select bind:value={shardToSplit} class="px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
<option value="">Select shard</option>
{#each allShards as s}<option value={s.ShardId}>{s.ShardId}</option>{/each}
</select>
<input bind:value={splitHashKey} placeholder="New start hash key" class="px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
</div>
<button onclick={() => splitShard()} disabled={splittingShard || !shardToSplit || !splitHashKey.trim()} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 disabled:opacity-50 text-sm">
{splittingShard ? 'Splitting…' : 'Split Shard'}
</button>

<h4 class="text-sm font-semibold text-slate-700 dark:text-slate-300 mt-4">Merge Shards</h4>
<div class="grid grid-cols-2 gap-2">
<select bind:value={shardToMerge1} class="px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
<option value="">Shard 1</option>
{#each allShards as s}<option value={s.ShardId}>{s.ShardId}</option>{/each}
</select>
<select bind:value={shardToMerge2} class="px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
<option value="">Adjacent Shard</option>
{#each allShards as s}<option value={s.ShardId}>{s.ShardId}</option>{/each}
</select>
</div>
<button onclick={() => mergeShards()} disabled={mergingShards || !shardToMerge1 || !shardToMerge2 || shardToMerge1 === shardToMerge2} class="px-4 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 text-sm">
{mergingShards ? 'Merging…' : 'Merge Shards'}
</button>
</div>
</div>
{/if}

<!-- TAB: Records -->
{#if activeTab === 'records'}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<div class="flex flex-wrap gap-2 mb-4">
<select bind:value={selectedShardForRead} class="flex-1 min-w-32 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
{#each allShards as s}<option value={s.ShardId}>{s.ShardId}</option>{/each}
</select>
<select bind:value={selectedIteratorType} class="flex-1 min-w-40 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
<option value="TRIM_HORIZON">TRIM_HORIZON</option>
<option value="LATEST">LATEST</option>
<option value="AT_TIMESTAMP">AT_TIMESTAMP</option>
</select>
<button onclick={() => getRecords()} disabled={gettingRecords} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 text-sm flex items-center gap-1.5">
<Download class="w-4 h-4" />{gettingRecords ? '…' : 'Fetch'}
</button>
</div>
{#if records.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400 text-center py-8">No records fetched yet</p>
{:else}
<div class="space-y-2 max-h-80 overflow-y-auto">
{#each records as rec}
<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
<div class="flex items-center gap-2 mb-1">
<span class="text-xs text-slate-500 dark:text-slate-400 font-mono">pk: {rec.partitionKey}</span>
<span class="text-xs text-slate-400">·</span>
<span class="text-xs text-slate-400 font-mono truncate">{rec.sequenceNumber.slice(-12)}</span>
{#if rec.arrivedAt}<span class="text-xs text-slate-400 ml-auto">{rec.arrivedAt}</span>{/if}
</div>
<pre class="text-xs text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-all">{rec.data}</pre>
</div>
{/each}
</div>
{/if}
</div>
{/if}

<!-- TAB: Consumers -->
{#if activeTab === 'consumers'}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<form onsubmit={(e) => { e.preventDefault(); registerConsumer(); }} class="flex gap-2 mb-4">
<input type="text" bind:value={newConsumerName} placeholder="Consumer name" class="flex-1 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
<button type="submit" disabled={registeringConsumer || !newConsumerName.trim()} class="px-3 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 text-sm">
{registeringConsumer ? 'Adding…' : 'Add'}
</button>
</form>
{#if loadingConsumers}
<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
{:else if consumers.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No enhanced fan-out consumers</p>
{:else}
<div class="space-y-2">
{#each consumers as consumer}
<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3 flex items-center justify-between gap-3">
<div class="min-w-0">
<p class="text-sm font-medium text-slate-900 dark:text-white truncate">{consumer.consumerName}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 truncate">{consumer.consumerStatus} · {shortArn(consumer.consumerARN)}</p>
</div>
<button onclick={() => deregisterConsumer(consumer.consumerARN)} disabled={deregisteringConsumerArn === consumer.consumerARN} class="px-2 py-1 text-xs bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300 rounded hover:bg-red-200 dark:hover:bg-red-900/50 disabled:opacity-50 flex-shrink-0">
{deregisteringConsumerArn === consumer.consumerARN ? '…' : 'Remove'}
</button>
</div>
{/each}
</div>
{/if}
</div>
{/if}

<!-- TAB: Tags -->
{#if activeTab === 'tags'}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<form onsubmit={(e) => { e.preventDefault(); addTag(); }} class="flex gap-2 mb-4">
<input type="text" bind:value={newTagKey} placeholder="Key" class="flex-1 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
<input type="text" bind:value={newTagValue} placeholder="Value" class="flex-1 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
<button type="submit" disabled={addingTag || !newTagKey.trim()} class="px-3 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 text-sm flex items-center gap-1">
<Tag class="w-3.5 h-3.5" />{addingTag ? '…' : 'Add'}
</button>
</form>
{#if loadingTags}
<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
{:else if tags.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No tags</p>
{:else}
<div class="space-y-2">
{#each tags as tag}
<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2 gap-2">
<span class="text-xs font-mono text-slate-700 dark:text-slate-300 truncate"><span class="font-semibold">{tag.key}</span> = {tag.value}</span>
<button onclick={() => removeTag(tag.key)} class="text-xs text-red-500 hover:text-red-700 flex-shrink-0">Remove</button>
</div>
{/each}
</div>
{/if}
</div>
{/if}

<!-- TAB: Settings -->
{#if activeTab === 'settings'}
<div class="space-y-4">
<!-- Retention -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<h3 class="font-semibold text-slate-900 dark:text-white mb-3">Retention Period</h3>
<div class="flex items-center gap-3">
<input type="number" bind:value={retentionHours} min="24" max="8760" class="w-32 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
<span class="text-sm text-slate-500 dark:text-slate-400">hours (24–8760)</span>
<button onclick={() => updateRetention()} disabled={updatingRetention} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 text-sm">
{updatingRetention ? 'Updating…' : 'Update'}
</button>
</div>
</div>

<!-- Shard count (PROVISIONED only) -->
{#if (streamDetail?.StreamModeDetails?.StreamMode ?? 'PROVISIONED') === 'PROVISIONED'}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<h3 class="font-semibold text-slate-900 dark:text-white mb-3">Shard Count</h3>
<div class="flex items-center gap-3">
<input type="number" bind:value={targetShardCount} min="1" max="500" class="w-32 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
<button onclick={() => updateShardCount()} disabled={updatingShardCount} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 text-sm">
{updatingShardCount ? 'Scaling…' : 'Scale'}
</button>
</div>
</div>
{/if}

<!-- Stream Mode -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<h3 class="font-semibold text-slate-900 dark:text-white mb-3">Stream Mode</h3>
<div class="flex items-center gap-4">
<span class="text-sm text-slate-700 dark:text-slate-300">Current: <strong>{streamDetail?.StreamModeDetails?.StreamMode ?? 'PROVISIONED'}</strong></span>
<button onclick={() => toggleStreamMode()} disabled={updatingStreamMode} class="px-4 py-2 bg-slate-600 text-white rounded-lg hover:bg-slate-700 disabled:opacity-50 text-sm">
{updatingStreamMode ? 'Switching…' : `Switch to ${(streamDetail?.StreamModeDetails?.StreamMode ?? 'PROVISIONED') === 'ON_DEMAND' ? 'PROVISIONED' : 'ON_DEMAND'}`}
</button>
</div>
</div>

<!-- Encryption -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<h3 class="font-semibold text-slate-900 dark:text-white mb-3 flex items-center gap-2"><Shield class="w-4 h-4 text-green-500" />Encryption</h3>
{#if streamDetail?.EncryptionType === 'KMS'}
<div class="flex items-center gap-3">
<span class="text-sm text-green-700 dark:text-green-300 flex items-center gap-1"><Shield class="w-3.5 h-3.5" />KMS enabled — key: {streamDetail.KeyId}</span>
<button onclick={() => stopEncryption()} disabled={updatingEncryption} class="px-3 py-1.5 bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50 text-sm">
{updatingEncryption ? '…' : 'Stop Encryption'}
</button>
</div>
{:else}
<div class="flex items-center gap-3">
<input type="text" bind:value={encryptionKeyId} placeholder="KMS key ID or alias" class="flex-1 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500" />
<button onclick={() => startEncryption()} disabled={updatingEncryption || !encryptionKeyId.trim()} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 text-sm flex-shrink-0">
{updatingEncryption ? '…' : 'Enable KMS'}
</button>
</div>
{/if}
</div>

<!-- Enhanced Monitoring -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<h3 class="font-semibold text-slate-900 dark:text-white mb-3 flex items-center gap-2"><Activity class="w-4 h-4 text-amber-500" />Enhanced Monitoring</h3>
<div class="space-y-2">
{#each ALL_METRICS as metric}
{@const enabled = monitoringEnabled.has(metric)}
<div class="flex items-center justify-between">
<span class="text-sm text-slate-700 dark:text-slate-300 font-mono">{metric}</span>
<button
onclick={() => enabled ? disableMonitoring(metric) : enableMonitoring(metric)}
disabled={updatingMonitoring}
class="px-3 py-1 text-xs rounded-full disabled:opacity-50 {enabled ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300 hover:bg-green-200' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-400 hover:bg-slate-200 dark:hover:bg-slate-600'}"
>
{enabled ? '✓ Enabled' : 'Disabled'}
</button>
</div>
{/each}
</div>
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

<!-- Put Single Record Modal -->
{#if showPutModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Put Record → {selectedStream}</h2>
<form onsubmit={(e) => { e.preventDefault(); putRecord(); }} class="space-y-4">
<div>
<label for="kinesis-partition-key" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Partition Key</label>
<input id="kinesis-partition-key" type="text" bind:value={putPartitionKey} placeholder="e.g. user-123" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
</div>
<div>
<label for="kinesis-data" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Data</label>
<textarea id="kinesis-data" bind:value={putData} rows={4} placeholder="event:click, userId:abc" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none" required></textarea>
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

<!-- Put Batch Records Modal -->
{#if showPutBatchModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-xl">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Put Records (Batch) → {selectedStream}</h2>
<p class="text-sm text-slate-500 dark:text-slate-400 mb-4">JSON array of <code class="bg-slate-100 dark:bg-slate-700 px-1 rounded">{"{ PartitionKey, Data }"}</code> objects</p>
<textarea bind:value={batchRecordsJson} rows={8} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white font-mono text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 resize-none"></textarea>
<div class="flex justify-end gap-3 pt-4">
<button type="button" onclick={() => { showPutBatchModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
<button onclick={() => putRecordsBatch()} disabled={puttingBatch} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
<Layers class="w-4 h-4" />{puttingBatch ? 'Putting...' : 'Put Records'}
</button>
</div>
</div>
</div>
{/if}
