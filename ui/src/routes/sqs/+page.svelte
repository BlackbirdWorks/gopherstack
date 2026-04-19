<script lang="ts">
import { onMount } from 'svelte';
import { getSQSClient } from '$lib/aws-client';
import {
ListQueuesCommand,
GetQueueAttributesCommand,
SetQueueAttributesCommand,
CreateQueueCommand,
DeleteQueueCommand,
SendMessageCommand,
ReceiveMessageCommand,
DeleteMessageCommand,
PurgeQueueCommand,
TagQueueCommand,
UntagQueueCommand,
ListQueueTagsCommand,
type Message
} from '@aws-sdk/client-sqs';
import { toast } from 'svelte-sonner';
import {
MessageSquare, Search, RefreshCw, Plus, Trash2, Send, Inbox,
Flame, ChevronDown, ChevronUp, Copy, Settings, Tag, X, Eye
} from 'lucide-svelte';

const sqs = getSQSClient();

// ──────────────── State ────────────────
let loading = $state(false);
let queues = $state<Array<{ url: string; attrs: Record<string, string> }>>([]);
let searchQuery = $state('');
let selectedQueue = $state<{ url: string; attrs: Record<string, string> } | null>(null);
let activeTab = $state<'messages' | 'attributes' | 'tags'>('messages');

// Create queue
let showCreateModal = $state(false);
let creating = $state(false);
let newQueueName = $state('');
let newQueueFifo = $state(false);
let newContentBasedDedup = $state(false);
let newVisibilityTimeout = $state(30);
let newRetentionPeriod = $state(345600);
let newMaxMsgSize = $state(262144);
let newDelaySeconds = $state(0);

// Send message
let showSendModal = $state(false);
let sending = $state(false);
let msgBody = $state('');
let msgGroupId = $state('');
let msgDedupId = $state('');
let msgDelay = $state(0);
let msgAttrRows = $state<Array<{ key: string; value: string; dataType: string }>>([]);

// Receive messages
let messages = $state<Message[]>([]);
let receivingMessages = $state(false);
let expandedMsg = $state<string | null>(null);
let deletingReceipt = $state<string | null>(null);

// Edit attributes
let editAttrs = $state<Record<string, string>>({});
let savingAttrs = $state(false);

// Tags
let queueTags = $state<Record<string, string>>({});
let tagRows = $state<Array<{ key: string; value: string }>>([]);
let savingTags = $state(false);
let loadingTags = $state(false);

// ──────────────── Derived ────────────────
const filteredQueues = $derived(
queues.filter((q) => queueName(q.url).toLowerCase().includes(searchQuery.toLowerCase()))
);

// ──────────────── Helpers ────────────────
function queueName(url: string): string {
return url.split('/').pop() ?? url;
}

function isFifo(url: string): boolean {
return url.endsWith('.fifo');
}

function hasDLQ(attrs: Record<string, string>): boolean {
return Boolean(attrs.RedrivePolicy);
}

function formatCount(n: string | undefined): string {
return n ? parseInt(n, 10).toLocaleString() : '0';
}

function formatDuration(secs: string | undefined): string {
if (!secs) return '—';
const n = parseInt(secs, 10);
if (n < 60) return `${n}s`;
if (n < 3600) return `${Math.floor(n / 60)}m ${n % 60}s`;
if (n < 86400) return `${Math.floor(n / 3600)}h`;
return `${Math.floor(n / 86400)}d`;
}

// ──────────────── Queue List ────────────────
async function loadQueues() {
loading = true;
try {
const res = await sqs.send(new ListQueuesCommand({ MaxResults: 100 }));
const urls = res.QueueUrls ?? [];
const enriched = await Promise.all(
urls.map(async (url) => {
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
// Refresh selected queue attrs
if (selectedQueue) {
const updated = enriched.find((q) => q.url === selectedQueue!.url);
if (updated) selectedQueue = updated;
}
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
VisibilityTimeout: String(newVisibilityTimeout),
MessageRetentionPeriod: String(newRetentionPeriod),
MaximumMessageSize: String(newMaxMsgSize),
DelaySeconds: String(newDelaySeconds)
};
if (newQueueFifo) {
attrs.FifoQueue = 'true';
attrs.ContentBasedDeduplication = newContentBasedDedup ? 'true' : 'false';
}
await sqs.send(new CreateQueueCommand({ QueueName: name, Attributes: attrs }));
toast.success(`Queue "${name}" created`);
showCreateModal = false;
resetCreateForm();
await loadQueues();
} catch (err: unknown) {
toast.error(`Create failed: ${(err as Error).message}`);
} finally {
creating = false;
}
}

function resetCreateForm() {
newQueueName = '';
newQueueFifo = false;
newContentBasedDedup = false;
newVisibilityTimeout = 30;
newRetentionPeriod = 345600;
newMaxMsgSize = 262144;
newDelaySeconds = 0;
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

function selectQueue(q: typeof queues[0]) {
	selectedQueue = q;
	messages = [];
	activeTab = 'messages';
	editAttrs = {
		VisibilityTimeout: q.attrs.VisibilityTimeout ?? '30',
		MessageRetentionPeriod: q.attrs.MessageRetentionPeriod ?? '345600',
		MaximumMessageSize: q.attrs.MaximumMessageSize ?? '262144',
		DelaySeconds: q.attrs.DelaySeconds ?? '0',
		ReceiveMessageWaitTimeSeconds: q.attrs.ReceiveMessageWaitTimeSeconds ?? '0'
	};
}

// ──────────────── Send Message ────────────────
async function sendMessage() {
if (!selectedQueue || !msgBody.trim()) return;
sending = true;
try {
const messageAttributes: Record<string, { DataType: string; StringValue?: string }> = {};
for (const row of msgAttrRows) {
if (row.key.trim()) {
messageAttributes[row.key.trim()] = { DataType: row.dataType, StringValue: row.value };
}
}
await sqs.send(new SendMessageCommand({
QueueUrl: selectedQueue.url,
MessageBody: msgBody,
MessageGroupId: isFifo(selectedQueue.url) ? (msgGroupId || 'default') : undefined,
MessageDeduplicationId: (isFifo(selectedQueue.url) && msgDedupId) ? msgDedupId : undefined,
DelaySeconds: msgDelay > 0 ? msgDelay : undefined,
MessageAttributes: Object.keys(messageAttributes).length > 0 ? messageAttributes : undefined
}));
toast.success('Message sent');
showSendModal = false;
resetSendForm();
const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: selectedQueue.url, AttributeNames: ['All'] }));
selectedQueue = { ...selectedQueue, attrs: attrs.Attributes ?? {} };
} catch (err: unknown) {
toast.error(`Send failed: ${(err as Error).message}`);
} finally {
sending = false;
}
}

function resetSendForm() {
msgBody = '';
msgGroupId = '';
msgDedupId = '';
msgDelay = 0;
msgAttrRows = [];
}

function addMsgAttrRow() {
msgAttrRows = [...msgAttrRows, { key: '', value: '', dataType: 'String' }];
}

function removeMsgAttrRow(i: number) {
msgAttrRows = msgAttrRows.filter((_, idx) => idx !== i);
}

// ──────────────── Receive Messages ────────────────
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
if (messages.length === 0) toast.info('No messages available');
} catch (err: unknown) {
toast.error(`Receive failed: ${(err as Error).message}`);
} finally {
receivingMessages = false;
}
}

async function deleteMessage(msg: Message) {
if (!selectedQueue || !msg.ReceiptHandle) return;
deletingReceipt = msg.ReceiptHandle;
try {
await sqs.send(new DeleteMessageCommand({
QueueUrl: selectedQueue.url,
ReceiptHandle: msg.ReceiptHandle
}));
messages = messages.filter((m) => m.MessageId !== msg.MessageId);
toast.success('Message deleted');
} catch (err: unknown) {
toast.error(`Delete failed: ${(err as Error).message}`);
} finally {
deletingReceipt = null;
}
}

// ──────────────── Purge ────────────────
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

// ──────────────── Edit Attributes ────────────────
async function saveAttributes() {
if (!selectedQueue) return;
savingAttrs = true;
try {
await sqs.send(new SetQueueAttributesCommand({
QueueUrl: selectedQueue.url,
Attributes: editAttrs
}));
toast.success('Attributes updated');
const attrs = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: selectedQueue.url, AttributeNames: ['All'] }));
selectedQueue = { ...selectedQueue, attrs: attrs.Attributes ?? {} };
editAttrs = {
VisibilityTimeout: selectedQueue.attrs.VisibilityTimeout ?? '30',
MessageRetentionPeriod: selectedQueue.attrs.MessageRetentionPeriod ?? '345600',
MaximumMessageSize: selectedQueue.attrs.MaximumMessageSize ?? '262144',
DelaySeconds: selectedQueue.attrs.DelaySeconds ?? '0',
ReceiveMessageWaitTimeSeconds: selectedQueue.attrs.ReceiveMessageWaitTimeSeconds ?? '0'
};
} catch (err: unknown) {
toast.error(`Save failed: ${(err as Error).message}`);
} finally {
savingAttrs = false;
}
}

// ──────────────── Tags ────────────────
async function loadTags() {
if (!selectedQueue) return;
loadingTags = true;
try {
const res = await sqs.send(new ListQueueTagsCommand({ QueueUrl: selectedQueue.url }));
queueTags = res.Tags ?? {};
tagRows = Object.entries(queueTags).map(([key, value]) => ({ key, value }));
} catch (err: unknown) {
toast.error(`Load tags failed: ${(err as Error).message}`);
} finally {
loadingTags = false;
}
}

async function saveTags() {
if (!selectedQueue) return;
savingTags = true;
try {
// Remove old tags
const oldKeys = Object.keys(queueTags);
if (oldKeys.length > 0) {
await sqs.send(new UntagQueueCommand({ QueueUrl: selectedQueue.url, TagKeys: oldKeys }));
}
// Apply new tags
const newTags: Record<string, string> = {};
for (const row of tagRows) {
if (row.key.trim()) newTags[row.key.trim()] = row.value;
}
if (Object.keys(newTags).length > 0) {
await sqs.send(new TagQueueCommand({ QueueUrl: selectedQueue.url, Tags: newTags }));
}
queueTags = newTags;
toast.success('Tags saved');
} catch (err: unknown) {
toast.error(`Save tags failed: ${(err as Error).message}`);
} finally {
savingTags = false;
}
}

function addTagRow() {
tagRows = [...tagRows, { key: '', value: '' }];
}

function removeTagRow(i: number) {
tagRows = tagRows.filter((_, idx) => idx !== i);
}

async function onTabChange(tab: 'messages' | 'attributes' | 'tags') {
activeTab = tab;
if (tab === 'tags' && selectedQueue) await loadTags();
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
<Plus class="w-4 h-4" />Create Queue
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
~{formatCount(q.attrs.ApproximateNumberOfMessages)} msgs
</p>
</div>
<div class="flex items-center gap-1 ml-2 flex-shrink-0">
{#if isFifo(q.url)}
<span class="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">FIFO</span>
{/if}
{#if hasDLQ(q.attrs)}
<span class="px-2 py-0.5 text-xs rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300">DLQ</span>
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
<!-- Queue Header Card -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<div class="flex items-start justify-between mb-4">
<div>
<h2 class="text-xl font-bold text-slate-900 dark:text-white flex items-center gap-2">
{queueName(selectedQueue.url)}
{#if isFifo(selectedQueue.url)}
<span class="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">FIFO</span>
{/if}
{#if hasDLQ(selectedQueue.attrs)}
<span class="px-2 py-0.5 text-xs rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300">DLQ wired</span>
{/if}
</h2>
<button onclick={() => copyUrl(selectedQueue?.url ?? '')} class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500 mt-1 font-mono">
<Copy class="w-3 h-3" />
{selectedQueue.url}
</button>
{#if selectedQueue.attrs.QueueArn}
<p class="text-xs text-slate-400 dark:text-slate-500 mt-0.5 font-mono">{selectedQueue.attrs.QueueArn}</p>
{/if}
</div>
<div class="flex gap-2 flex-wrap justify-end">
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
<!-- Stats grid -->
<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
{#each [
['Available', formatCount(selectedQueue.attrs.ApproximateNumberOfMessages)],
['In Flight', formatCount(selectedQueue.attrs.ApproximateNumberOfMessagesNotVisible)],
['Delayed', formatCount(selectedQueue.attrs.ApproximateNumberOfMessagesDelayed)],
['Visibility', `${selectedQueue.attrs.VisibilityTimeout ?? 30}s`],
['Retention', formatDuration(selectedQueue.attrs.MessageRetentionPeriod)],
['Wait', `${selectedQueue.attrs.ReceiveMessageWaitTimeSeconds ?? 0}s`],
['Delay', `${selectedQueue.attrs.DelaySeconds ?? 0}s`],
['Max Size', selectedQueue.attrs.MaximumMessageSize ? `${(parseInt(selectedQueue.attrs.MaximumMessageSize) / 1024).toFixed(0)} KB` : '256 KB']
] as [label, value]}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
<p class="font-semibold text-slate-900 dark:text-white mt-0.5 text-sm">{value}</p>
</div>
{/each}
</div>
</div>

<!-- Tabs -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
<div class="flex border-b border-slate-200 dark:border-slate-700">
	<button
		onclick={() => onTabChange('messages')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors {activeTab === 'messages' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<Eye class="w-4 h-4" /> Messages
	</button>
	<button
		onclick={() => onTabChange('attributes')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors {activeTab === 'attributes' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<Settings class="w-4 h-4" /> Attributes
	</button>
	<button
		onclick={() => onTabChange('tags')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors {activeTab === 'tags' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<Tag class="w-4 h-4" /> Tags
	</button>
</div>

<!-- Messages Tab -->
{#if activeTab === 'messages'}
<div class="p-4">
{#if messages.length === 0}
<p class="text-center text-slate-400 dark:text-slate-500 py-8">No messages received. Click "Receive" to fetch messages.</p>
{:else}
<div class="space-y-2">
<p class="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Received Messages ({messages.length})</p>
{#each messages as msg}
<div class="border border-slate-200 dark:border-slate-600 rounded-lg overflow-hidden">
<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-700/30">
<button
onclick={() => { expandedMsg = expandedMsg === msg.MessageId ? null : (msg.MessageId ?? null); }}
class="flex items-center gap-2 text-left flex-1 min-w-0"
>
<span class="font-mono text-xs text-slate-600 dark:text-slate-400 truncate">{msg.MessageId}</span>
{#if expandedMsg === msg.MessageId}
<ChevronUp class="w-4 h-4 text-slate-400 flex-shrink-0" />
{:else}
<ChevronDown class="w-4 h-4 text-slate-400 flex-shrink-0" />
{/if}
</button>
<button
onclick={() => deleteMessage(msg)}
disabled={deletingReceipt === msg.ReceiptHandle}
class="p-1 text-slate-400 hover:text-red-500 disabled:opacity-50 ml-2"
title="Delete message"
>
<Trash2 class="w-4 h-4" />
</button>
</div>
{#if expandedMsg === msg.MessageId}
<div class="border-t border-slate-200 dark:border-slate-600 p-3 space-y-2">
<pre class="text-xs text-slate-700 dark:text-slate-300 whitespace-pre-wrap break-all bg-slate-50 dark:bg-slate-700/30 rounded p-2">{msg.Body}</pre>
{#if msg.MessageAttributes && Object.keys(msg.MessageAttributes).length > 0}
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 mb-1">Message Attributes</p>
<div class="space-y-1">
{#each Object.entries(msg.MessageAttributes) as [key, attr]}
<div class="flex items-center gap-2 text-xs">
<span class="font-mono text-indigo-600 dark:text-indigo-400">{key}</span>
<span class="text-slate-400">({attr.DataType})</span>
<span class="text-slate-700 dark:text-slate-300">{attr.StringValue ?? ''}</span>
</div>
{/each}
</div>
</div>
{/if}
{#if msg.Attributes && Object.keys(msg.Attributes).length > 0}
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 mb-1">System Attributes</p>
<div class="grid grid-cols-2 gap-1">
{#each Object.entries(msg.Attributes) as [key, value]}
<div class="text-xs">
<span class="text-slate-500">{key}:</span>
<span class="font-mono text-slate-700 dark:text-slate-300 ml-1">{value}</span>
</div>
{/each}
</div>
</div>
{/if}
<p class="text-xs text-slate-400 font-mono truncate">Receipt: {msg.ReceiptHandle}</p>
</div>
{/if}
</div>
{/each}
</div>
{/if}
</div>
{/if}

<!-- Attributes Tab -->
{#if activeTab === 'attributes'}
<div class="p-4">
<form onsubmit={(e) => { e.preventDefault(); saveAttributes(); }} class="space-y-4">
<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
{#each [
['VisibilityTimeout', 'Visibility Timeout (s)', '0', '43200'],
['MessageRetentionPeriod', 'Message Retention (s)', '60', '1209600'],
['MaximumMessageSize', 'Max Message Size (bytes)', '1024', '262144'],
['DelaySeconds', 'Delay Seconds', '0', '900'],
['ReceiveMessageWaitTimeSeconds', 'Receive Wait Time (s)', '0', '20']
] as [attr, label, min, max]}
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">{label}</label>
<input
type="number"
bind:value={editAttrs[attr]}
min={min}
max={max}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
{/each}
</div>
<div class="flex justify-end pt-2">
<button type="submit" disabled={savingAttrs} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
{savingAttrs ? 'Saving...' : 'Save Attributes'}
</button>
</div>
</form>
</div>
{/if}

<!-- Tags Tab -->
{#if activeTab === 'tags'}
<div class="p-4">
{#if loadingTags}
<p class="text-center text-slate-400 py-4">Loading tags...</p>
{:else}
<div class="space-y-3">
{#each tagRows as row, i}
<div class="flex items-center gap-2">
<input
type="text"
bind:value={row.key}
placeholder="Key"
class="flex-1 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm"
/>
<input
type="text"
bind:value={row.value}
placeholder="Value"
class="flex-1 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm"
/>
<button onclick={() => removeTagRow(i)} class="p-2 text-slate-400 hover:text-red-500">
<X class="w-4 h-4" />
</button>
</div>
{/each}
<div class="flex items-center justify-between pt-2">
<button type="button" onclick={addTagRow} class="text-sm text-indigo-600 dark:text-indigo-400 hover:underline flex items-center gap-1">
<Plus class="w-4 h-4" /> Add Tag
</button>
<button onclick={saveTags} disabled={savingTags} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 text-sm">
{savingTags ? 'Saving...' : 'Save Tags'}
</button>
</div>
</div>
{/if}
</div>
{/if}
</div>
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
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Queue</h2>
<form onsubmit={(e) => { e.preventDefault(); createQueue(); }} class="space-y-4">
<div>
<label for="sqs-queue-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Queue Name</label>
<input
id="sqs-queue-name"
type="text"
bind:value={newQueueName}
placeholder="e.g. order-processing"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
required
/>
</div>
<label class="flex items-center gap-2 cursor-pointer">
<input type="checkbox" bind:checked={newQueueFifo} class="rounded" />
<span class="text-sm text-slate-700 dark:text-slate-300">FIFO queue (.fifo suffix appended automatically)</span>
</label>
{#if newQueueFifo}
<label class="flex items-center gap-2 cursor-pointer ml-4">
<input type="checkbox" bind:checked={newContentBasedDedup} class="rounded" />
<span class="text-sm text-slate-700 dark:text-slate-300">Content-based deduplication</span>
</label>
{/if}
<div class="grid grid-cols-2 gap-4">
<div>
<label for="sqs-visibility" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Visibility Timeout (s)</label>
<input id="sqs-visibility" type="number" bind:value={newVisibilityTimeout} min="0" max="43200"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
</div>
<div>
<label for="sqs-delay" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Delay Seconds</label>
<input id="sqs-delay" type="number" bind:value={newDelaySeconds} min="0" max="900"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
</div>
<div>
<label for="sqs-retention" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Retention Period (s)</label>
<input id="sqs-retention" type="number" bind:value={newRetentionPeriod} min="60" max="1209600"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
</div>
<div>
<label for="sqs-maxsize" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Max Msg Size (bytes)</label>
<input id="sqs-maxsize" type="number" bind:value={newMaxMsgSize} min="1024" max="262144"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
</div>
</div>
<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showCreateModal = false; resetCreateForm(); }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
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
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-xl max-h-[90vh] overflow-y-auto">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Send Message</h2>
<form onsubmit={(e) => { e.preventDefault(); sendMessage(); }} class="space-y-4">
<div>
<label for="sqs-msg-body" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message Body</label>
<textarea
id="sqs-msg-body"
bind:value={msgBody}
rows={4}
placeholder="Enter message body..."
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
required
></textarea>
</div>
{#if selectedQueue && isFifo(selectedQueue.url)}
<div class="grid grid-cols-2 gap-4">
<div>
<label for="sqs-msg-group" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message Group ID <span class="text-red-500">*</span></label>
<input
id="sqs-msg-group"
type="text"
bind:value={msgGroupId}
placeholder="e.g. order-group-1"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
<div>
<label for="sqs-msg-dedup" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Deduplication ID <span class="text-slate-400 font-normal">(optional)</span></label>
<input
id="sqs-msg-dedup"
type="text"
bind:value={msgDedupId}
placeholder="Leave blank for content-based"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
</div>
{/if}
<div>
<label for="sqs-send-delay" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Delay Seconds (0–900)</label>
<input
id="sqs-send-delay"
type="number"
bind:value={msgDelay}
min="0" max="900"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>

<!-- Message Attributes -->
<div>
<div class="flex items-center justify-between mb-2">
<span class="text-sm font-medium text-slate-700 dark:text-slate-300">Message Attributes</span>
<button type="button" onclick={addMsgAttrRow} class="text-xs text-indigo-600 dark:text-indigo-400 hover:underline flex items-center gap-1">
<Plus class="w-3 h-3" /> Add
</button>
</div>
{#each msgAttrRows as row, i}
<div class="flex items-center gap-2 mb-2">
<input type="text" bind:value={row.key} placeholder="Name"
class="flex-1 px-2 py-1.5 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-1 focus:ring-indigo-500" />
<select bind:value={row.dataType}
class="px-2 py-1.5 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-1 focus:ring-indigo-500">
<option value="String">String</option>
<option value="Number">Number</option>
</select>
<input type="text" bind:value={row.value} placeholder="Value"
class="flex-1 px-2 py-1.5 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-1 focus:ring-indigo-500" />
<button type="button" onclick={() => removeMsgAttrRow(i)} class="p-1 text-slate-400 hover:text-red-500">
<X class="w-4 h-4" />
</button>
</div>
{/each}
</div>

<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showSendModal = false; resetSendForm(); }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button type="submit" disabled={sending} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
<Send class="w-4 h-4" />
{sending ? 'Sending...' : 'Send Message'}
</button>
</div>
</form>
</div>
</div>
{/if}
