<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
import { multiRegionList } from '$lib/multi-region';
import RegionChip from '$lib/components/RegionChip.svelte';
import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
import { getSQSClient } from '$lib/aws-client';
import {
ListQueuesCommand,
GetQueueAttributesCommand,
SetQueueAttributesCommand,
CreateQueueCommand,
DeleteQueueCommand,
SendMessageCommand,
SendMessageBatchCommand,
ReceiveMessageCommand,
DeleteMessageCommand,
PurgeQueueCommand,
TagQueueCommand,
UntagQueueCommand,
ListQueueTagsCommand,
ChangeMessageVisibilityCommand,
AddPermissionCommand,
RemovePermissionCommand,
ListDeadLetterSourceQueuesCommand,
StartMessageMoveTaskCommand,
CancelMessageMoveTaskCommand,
ListMessageMoveTasksCommand,
type Message,
type ListMessageMoveTasksCommandOutput
} from '@aws-sdk/client-sqs';
import { toast } from 'svelte-sonner';
import {
MessageSquare, Search, RefreshCw, Plus, Trash2, Send, Inbox,
Flame, ChevronDown, ChevronUp, Copy, Settings, Tag, X, Eye,
ArrowRightLeft, BookOpen, AlertCircle
} from 'lucide-svelte';

const sqs = regionalClient(getSQSClient);

// Every queue carries the region its ListQueues call was made against.
// Detail/action calls must build a client for THAT region -- in All mode
// the same queue name can legitimately exist in two different regions.
type QueueRow = { url: string; attrs: Record<string, string>; region: string };

// ──────────────── State ────────────────
let loading = $state(false);
let queues = $state<QueueRow[]>([]);
let searchQuery = $state('');
let filterType = $state<'all' | 'standard' | 'fifo'>('all');
let selectedQueue = $state<QueueRow | null>(null);
let activeTab = $state<'messages' | 'attributes' | 'tags' | 'move' | 'permissions' | 'docs'>('messages');

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
let newWaitTimeSeconds = $state(0);

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
let receiveMaxMessages = $state(10);
let messageFilter = $state('');

// Batch send
let showBatchSendModal = $state(false);
let batchMessages = $state<Array<{ id: string; body: string; delay: number }>>([
{ id: '1', body: '', delay: 0 },
{ id: '2', body: '', delay: 0 },
]);
let sendingBatch = $state(false);

// Edit attributes
let editAttrs = $state<Record<string, string>>({});
let savingAttrs = $state(false);
let editRedriveTargetArn = $state('');
let editRedriveMaxReceiveCount = $state('3');

// Tags
let queueTags = $state<Record<string, string>>({});
let tagRows = $state<Array<{ key: string; value: string }>>([]);
let savingTags = $state(false);
let loadingTags = $state(false);

// Dead-letter source queues
let dlqSourceQueues = $state<string[]>([]);
let loadingDlqSources = $state(false);

// Message move tasks
let moveTasks = $state<NonNullable<ListMessageMoveTasksCommandOutput['Results']>>([]);
let loadingMoveTasks = $state(false);
let moveTaskDestArn = $state('');
let startingMoveTask = $state(false);

// ──────────────── Derived ────────────────
const filteredQueues = $derived(
queues.filter((q) => {
const nameMatch = queueName(q.url).toLowerCase().includes(searchQuery.toLowerCase());
const typeMatch = filterType === 'all' || (filterType === 'fifo' ? isFifo(q.url) : !isFifo(q.url));
return nameMatch && typeMatch;
})
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

function formatTimestamp(ms: string | undefined): string {
if (!ms) return '—';
const ts = parseInt(ms, 10);
if (!ts) return '—';
return new Date(ts).toLocaleString();
}

// ──────────────── Queue List ────────────────
async function listQueuesInRegion(region: string): Promise<QueueRow[]> {
const client = getSQSClient(region);
const res = await client.send(new ListQueuesCommand({ MaxResults: 100 }));
const urls = res.QueueUrls ?? [];
return Promise.all(
urls.map(async (url) => {
try {
const attrs = await client.send(new GetQueueAttributesCommand({
QueueUrl: url,
AttributeNames: ['All']
}));
return { url, attrs: attrs.Attributes ?? {}, region };
} catch {
return { url, attrs: {}, region };
}
})
);
}

async function loadQueues() {
loading = true;
try {
const result = await multiRegionList(listQueuesInRegion, (items) => items);
queues = result.items.map(({ item }) => item);
if (result.errors.length > 0) toast.error(`Failed to load queues from ${result.errors.length} region(s)`);
// Refresh selected queue attrs
if (selectedQueue) {
const updated = queues.find((q) => q.url === selectedQueue!.url && q.region === selectedQueue!.region);
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
DelaySeconds: String(newDelaySeconds),
ReceiveMessageWaitTimeSeconds: String(newWaitTimeSeconds)
};
if (newQueueFifo) {
attrs.FifoQueue = 'true';
attrs.ContentBasedDeduplication = newContentBasedDedup ? 'true' : 'false';
}
await sqs().send(new CreateQueueCommand({ QueueName: name, Attributes: attrs }));
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
newWaitTimeSeconds = 0;
}

async function deleteQueue(q: QueueRow) {
const name = queueName(q.url);
if (!await confirmDestructive({ title: 'Delete Queue', message: `Delete queue "${name}"? All messages will be lost and the URL will be unavailable for 60 seconds.` })) return;
try {
await getSQSClient(q.region).send(new DeleteQueueCommand({ QueueUrl: q.url }));
toast.success(`Queue "${name}" deleted`);
if (selectedQueue && selectedQueue.url === q.url && selectedQueue.region === q.region) { selectedQueue = null; messages = []; }
await loadQueues();
} catch (err: unknown) {
toast.error(`Delete failed: ${(err as Error).message}`);
}
}

function selectQueue(q: typeof queues[0]) {
	selectedQueue = q;
	messages = [];
	activeTab = 'messages';
	dlqSourceQueues = [];
	moveTasks = [];
	editAttrs = {
		VisibilityTimeout: q.attrs.VisibilityTimeout ?? '30',
		MessageRetentionPeriod: q.attrs.MessageRetentionPeriod ?? '345600',
		MaximumMessageSize: q.attrs.MaximumMessageSize ?? '262144',
		DelaySeconds: q.attrs.DelaySeconds ?? '0',
		ReceiveMessageWaitTimeSeconds: q.attrs.ReceiveMessageWaitTimeSeconds ?? '0'
	};
	// Parse existing redrive policy
	if (q.attrs.RedrivePolicy) {
		try {
			const rp = JSON.parse(q.attrs.RedrivePolicy);
			editRedriveTargetArn = rp.deadLetterTargetArn ?? '';
			editRedriveMaxReceiveCount = String(rp.maxReceiveCount ?? 3);
		} catch {
			editRedriveTargetArn = '';
			editRedriveMaxReceiveCount = '3';
		}
	} else {
		editRedriveTargetArn = '';
		editRedriveMaxReceiveCount = '3';
	}
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
await getSQSClient(selectedQueue.region).send(new SendMessageCommand({
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
await refreshSelectedQueueStats();
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

async function sendBatch() {
if (!selectedQueue) return;
const nonEmpty = batchMessages.filter(m => m.body.trim());
if (nonEmpty.length === 0) { toast.error('Add at least one message body'); return; }
sendingBatch = true;
try {
await getSQSClient(selectedQueue.region).send(new SendMessageBatchCommand({
QueueUrl: selectedQueue.url,
Entries: nonEmpty.map(m => ({
Id: m.id,
MessageBody: m.body,
DelaySeconds: m.delay > 0 ? m.delay : undefined
}))
}));
toast.success(`${nonEmpty.length} message(s) sent`);
showBatchSendModal = false;
batchMessages = [{ id: '1', body: '', delay: 0 }, { id: '2', body: '', delay: 0 }];
await refreshSelectedQueueStats();
} catch (err: unknown) {
toast.error(`Batch send failed: ${(err as Error).message}`);
} finally {
sendingBatch = false;
}
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
const res = await getSQSClient(selectedQueue.region).send(new ReceiveMessageCommand({
QueueUrl: selectedQueue.url,
MaxNumberOfMessages: receiveMaxMessages,
WaitTimeSeconds: 1,
AttributeNames: ['All'],
MessageAttributeNames: ['All']
}));
messages = res.Messages ?? [];
if (messages.length === 0) toast.info('No messages available');
await refreshSelectedQueueStats();
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
await getSQSClient(selectedQueue.region).send(new DeleteMessageCommand({
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

let visibilityModal = $state<{ msg: Message | null; seconds: number }>({ msg: null, seconds: 30 });

function openVisibilityModal(msg: Message) {
	visibilityModal = { msg, seconds: 30 };
}

async function applyVisibilityChange() {
	const msg = visibilityModal.msg;
	if (!selectedQueue || !msg || !msg.ReceiptHandle) return;
	const seconds = Number(visibilityModal.seconds);
	if (!Number.isFinite(seconds) || seconds < 0 || seconds > 43200) {
		toast.error('Visibility must be between 0 and 43200 seconds');
		return;
	}
	try {
		await getSQSClient(selectedQueue.region).send(new ChangeMessageVisibilityCommand({
			QueueUrl: selectedQueue.url,
			ReceiptHandle: msg.ReceiptHandle,
			VisibilityTimeout: seconds,
		}));
		toast.success(seconds === 0 ? 'Message released for redelivery' : `Visibility set to ${seconds}s`);
		if (seconds === 0) messages = messages.filter((m) => m.MessageId !== msg.MessageId);
	} catch (err: unknown) {
		toast.error(`ChangeVisibility failed: ${(err as Error).message}`);
	} finally {
		visibilityModal = { msg: null, seconds: 30 };
	}
}

// ──────────────── Purge ────────────────
async function purgeQueue(url: string) {
if (!selectedQueue) return;
if (!await confirmDestructive({ title: 'Purge Queue', message: `Delete all messages from "${queueName(url)}"? This cannot be undone.`, confirmLabel: 'Purge' })) return;
try {
await getSQSClient(selectedQueue.region).send(new PurgeQueueCommand({ QueueUrl: url }));
toast.success('Queue purged');
messages = [];
await refreshSelectedQueueStats();
} catch (err: unknown) {
toast.error(`Purge failed: ${(err as Error).message}`);
}
}

// ──────────────── Refresh stats ────────────────
async function refreshSelectedQueueStats() {
if (!selectedQueue) return;
try {
const attrs = await getSQSClient(selectedQueue.region).send(new GetQueueAttributesCommand({ QueueUrl: selectedQueue.url, AttributeNames: ['All'] }));
selectedQueue = { ...selectedQueue, attrs: attrs.Attributes ?? {} };
// keep editAttrs in sync
editAttrs = {
VisibilityTimeout: selectedQueue.attrs.VisibilityTimeout ?? '30',
MessageRetentionPeriod: selectedQueue.attrs.MessageRetentionPeriod ?? '345600',
MaximumMessageSize: selectedQueue.attrs.MaximumMessageSize ?? '262144',
DelaySeconds: selectedQueue.attrs.DelaySeconds ?? '0',
ReceiveMessageWaitTimeSeconds: selectedQueue.attrs.ReceiveMessageWaitTimeSeconds ?? '0'
};
} catch {
// best-effort
}
}

// ──────────────── Edit Attributes ────────────────
async function saveAttributes() {
if (!selectedQueue) return;
savingAttrs = true;
try {
const attrsToSave: Record<string, string> = { ...editAttrs };
// Build/clear RedrivePolicy
if (editRedriveTargetArn.trim()) {
attrsToSave.RedrivePolicy = JSON.stringify({
deadLetterTargetArn: editRedriveTargetArn.trim(),
maxReceiveCount: parseInt(editRedriveMaxReceiveCount, 10) || 3
});
}
await getSQSClient(selectedQueue.region).send(new SetQueueAttributesCommand({
QueueUrl: selectedQueue.url,
Attributes: attrsToSave
}));
toast.success('Attributes updated');
await refreshSelectedQueueStats();
if (selectedQueue.attrs.RedrivePolicy) {
try {
const rp = JSON.parse(selectedQueue.attrs.RedrivePolicy);
editRedriveTargetArn = rp.deadLetterTargetArn ?? '';
editRedriveMaxReceiveCount = String(rp.maxReceiveCount ?? 3);
} catch (_) {
// ignore parse errors
}
}
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
const res = await getSQSClient(selectedQueue.region).send(new ListQueueTagsCommand({ QueueUrl: selectedQueue.url }));
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
await getSQSClient(selectedQueue.region).send(new UntagQueueCommand({ QueueUrl: selectedQueue.url, TagKeys: oldKeys }));
}
// Apply new tags
const newTags: Record<string, string> = {};
for (const row of tagRows) {
if (row.key.trim()) newTags[row.key.trim()] = row.value;
}
if (Object.keys(newTags).length > 0) {
await getSQSClient(selectedQueue.region).send(new TagQueueCommand({ QueueUrl: selectedQueue.url, Tags: newTags }));
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

// ──────────────── Permissions (AddPermission / RemovePermission) ────────────────
type PermissionRow = { label: string; principal: string; actions: string };
let permissions = $state<PermissionRow[]>([]);
let permissionsLoading = $state(false);
let newPermission = $state<PermissionRow>({ label: '', principal: '*', actions: 'SendMessage,ReceiveMessage' });

// Permissions are stored on the QueueArn — there is no list operation in AWS,
// so we re-derive from the Policy attribute exposed via GetQueueAttributes.
async function loadPermissions() {
	if (!selectedQueue) return;
	permissionsLoading = true;
	try {
		const res = await getSQSClient(selectedQueue.region).send(new GetQueueAttributesCommand({
			QueueUrl: selectedQueue.url,
			AttributeNames: ['Policy'],
		}));
		const policyJson = res.Attributes?.Policy;
		permissions = [];
		if (policyJson) {
			try {
				const parsed = JSON.parse(policyJson) as { Statement?: Array<{ Sid?: string; Principal?: unknown; Action?: unknown }> };
				for (const s of parsed.Statement ?? []) {
					permissions.push({
						label: s.Sid ?? '',
						principal: typeof s.Principal === 'string' ? s.Principal : JSON.stringify(s.Principal ?? '*'),
						actions: Array.isArray(s.Action) ? (s.Action as string[]).join(', ') : String(s.Action ?? ''),
					});
				}
			} catch {
				// policy not JSON-parseable
			}
		}
	} catch (err: unknown) {
		toast.error(`Load permissions failed: ${(err as Error).message}`);
	} finally {
		permissionsLoading = false;
	}
}

async function addPermission() {
	if (!selectedQueue || !newPermission.label.trim()) return;
	try {
		const accountIDs = newPermission.principal.split(',').map((s) => s.trim()).filter(Boolean);
		const actions = newPermission.actions.split(',').map((s) => s.trim()).filter(Boolean);
		if (accountIDs.length === 0 || actions.length === 0) {
			toast.error('Principal and Actions are required');
			return;
		}
		await getSQSClient(selectedQueue.region).send(new AddPermissionCommand({
			QueueUrl: selectedQueue.url,
			Label: newPermission.label.trim(),
			AWSAccountIds: accountIDs,
			Actions: actions,
		}));
		toast.success(`Permission "${newPermission.label}" added`);
		newPermission = { label: '', principal: '*', actions: 'SendMessage,ReceiveMessage' };
		await loadPermissions();
	} catch (err: unknown) {
		toast.error(`AddPermission failed: ${(err as Error).message}`);
	}
}

async function removePermission(label: string) {
	if (!selectedQueue || !label) return;
	if (!await confirmDestructive({ title: 'Remove Permission', message: `Remove permission "${label}"?`, confirmLabel: 'Remove' })) return;
	try {
		await getSQSClient(selectedQueue.region).send(new RemovePermissionCommand({ QueueUrl: selectedQueue.url, Label: label }));
		toast.success('Permission removed');
		await loadPermissions();
	} catch (err: unknown) {
		toast.error(`RemovePermission failed: ${(err as Error).message}`);
	}
}

async function onTabChange(tab: 'messages' | 'attributes' | 'tags' | 'move' | 'permissions' | 'docs') {
activeTab = tab;
if (tab === 'tags' && selectedQueue) await loadTags();
if (tab === 'permissions' && selectedQueue) await loadPermissions();
if (tab === 'move' && selectedQueue) await loadMoveTasks();
}

async function copyUrl(url: string) {
await navigator.clipboard.writeText(url);
toast.success('URL copied');
}

async function copyArn(arn: string) {
await navigator.clipboard.writeText(arn);
toast.success('ARN copied');
}

function clearMessages() {
messages = [];
expandedMsg = null;
}

// ──────────────── Dead-Letter Source Queues ────────────────
async function loadDlqSourceQueues() {
if (!selectedQueue) return;
loadingDlqSources = true;
try {
const res = await getSQSClient(selectedQueue.region).send(new ListDeadLetterSourceQueuesCommand({ QueueUrl: selectedQueue.url }));
dlqSourceQueues = res.queueUrls ?? [];
} catch {
dlqSourceQueues = [];
} finally {
loadingDlqSources = false;
}
}

// ──────────────── Message Move Tasks ────────────────
async function loadMoveTasks() {
if (!selectedQueue) return;
loadingMoveTasks = true;
try {
const arn = selectedQueue.attrs.QueueArn;
if (!arn) { moveTasks = []; return; }
const res = await getSQSClient(selectedQueue.region).send(new ListMessageMoveTasksCommand({ SourceArn: arn }));
moveTasks = res.Results ?? [];
} catch {
moveTasks = [];
} finally {
loadingMoveTasks = false;
}
}

async function startMoveTask() {
if (!selectedQueue) return;
const sourceArn = selectedQueue.attrs.QueueArn;
if (!sourceArn) { toast.error('Queue ARN not available'); return; }
startingMoveTask = true;
try {
await getSQSClient(selectedQueue.region).send(new StartMessageMoveTaskCommand({
SourceArn: sourceArn,
DestinationArn: moveTaskDestArn.trim() || undefined
}));
toast.success('Message move task started');
moveTaskDestArn = '';
await loadMoveTasks();
} catch (err: unknown) {
toast.error(`Failed to start move task: ${(err as Error).message}`);
} finally {
startingMoveTask = false;
}
}

async function cancelMoveTask(taskHandle: string) {
if (!selectedQueue) return;
try {
await getSQSClient(selectedQueue.region).send(new CancelMessageMoveTaskCommand({ TaskHandle: taskHandle }));
toast.success('Move task cancelled');
await loadMoveTasks();
} catch (err: unknown) {
toast.error(`Cancel failed: ${(err as Error).message}`);
}
}

// Queues and every queue-scoped detail (messages, edited attributes, tags,
// dead-letter sources, move tasks, permissions) are region-scoped, so a
// region switch must clear them (not just reload) rather than keep showing
// the old region's selected queue.
onRegionChange(() => {
	queues = [];
	selectedQueue = null;
	messages = [];
	expandedMsg = null;
	editAttrs = {};
	editRedriveTargetArn = '';
	editRedriveMaxReceiveCount = '3';
	queueTags = {};
	tagRows = [];
	dlqSourceQueues = [];
	moveTasks = [];
	moveTaskDestArn = '';
	permissions = [];
	visibilityModal = { msg: null, seconds: 30 };
	showCreateModal = false;
	showSendModal = false;
	showBatchSendModal = false;
	void loadQueues();
});
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
<WriteRegionHint />
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

<!-- Type filter + queue count -->
<div class="flex items-center justify-between">
<div class="flex items-center gap-1">
{#each [['all', 'All'], ['standard', 'Standard'], ['fifo', 'FIFO']] as [type, label]}
<button
onclick={() => { filterType = type as typeof filterType; }}
class="px-3 py-1 text-xs rounded-full border transition-colors {filterType === type ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 dark:border-slate-600 text-slate-600 dark:text-slate-400 hover:border-indigo-400'}"
>{label}</button>
{/each}
</div>
<span class="text-xs text-slate-500 dark:text-slate-400">{filteredQueues.length} / {queues.length} queue{queues.length !== 1 ? 's' : ''}</span>
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
class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-4 hover:border-indigo-400 transition-colors cursor-pointer {selectedQueue && selectedQueue.url === q.url && selectedQueue.region === q.region ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
>
<div class="flex items-center justify-between">
<div class="min-w-0 flex-1">
<p class="font-medium text-slate-900 dark:text-white truncate">{queueName(q.url)}</p>
<div class="flex items-center gap-2 mt-0.5">
<RegionChip region={q.region} />
<p class="text-xs text-slate-500 dark:text-slate-400">
~{formatCount(q.attrs.ApproximateNumberOfMessages)} messages
</p>
</div>
</div>
<div class="flex items-center gap-1 ml-2 flex-shrink-0">
{#if isFifo(q.url)}
<span class="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">FIFO</span>
{/if}
{#if hasDLQ(q.attrs)}
<span class="px-2 py-0.5 text-xs rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300">DLQ</span>
{/if}
{#if q.attrs.SqsManagedSseEnabled === 'true'}
<span class="px-2 py-0.5 text-xs rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300" title="Server-side encryption enabled">SSE</span>
{/if}
<button onclick={(e) => { e.stopPropagation(); deleteQueue(q); }} class="p-1 text-slate-400 hover:text-red-500">
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
<RegionChip region={selectedQueue.region} />
{#if isFifo(selectedQueue.url)}
<span class="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">FIFO</span>
{/if}
{#if hasDLQ(selectedQueue.attrs)}
<span class="px-2 py-0.5 text-xs rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300">DLQ wired</span>
{/if}
{#if selectedQueue.attrs.SqsManagedSseEnabled === 'true'}
<span class="px-2 py-0.5 text-xs rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300" title="Server-side encryption">SSE</span>
{/if}
</h2>
<button onclick={() => copyUrl(selectedQueue?.url ?? '')} class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500 mt-1 font-mono">
<Copy class="w-3 h-3" />
{selectedQueue.url}
</button>
{#if selectedQueue.attrs.QueueArn}
<button onclick={() => copyArn(selectedQueue?.attrs.QueueArn ?? '')} class="flex items-center gap-1 text-xs text-slate-400 dark:text-slate-500 hover:text-indigo-500 mt-0.5 font-mono">
<Copy class="w-3 h-3" />
{selectedQueue.attrs.QueueArn}
</button>
{/if}
</div>
<div class="flex gap-2 flex-wrap justify-end">
<button onclick={() => { showSendModal = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
<Send class="w-4 h-4" />Send
</button>
<button onclick={() => { showBatchSendModal = true; }} class="px-3 py-1.5 bg-indigo-500 text-white rounded-lg hover:bg-indigo-600 flex items-center gap-1.5 text-sm">
<Send class="w-4 h-4" />Batch Send
</button>
<button onclick={() => receiveMessages()} disabled={receivingMessages} class="px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-1.5 text-sm disabled:opacity-50">
<Inbox class="w-4 h-4" />{receivingMessages ? '...' : 'Receive'}
</button>
<button onclick={() => onTabChange('move')} class="px-3 py-1.5 bg-orange-600 text-white rounded-lg hover:bg-orange-700 flex items-center gap-1.5 text-sm" title="Move messages — redrive from DLQ or migrate to another queue">
<ArrowRightLeft class="w-4 h-4" />Redrive
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
<div class="flex border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
	<button
		onclick={() => onTabChange('messages')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'messages' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<Eye class="w-4 h-4" /> Messages
	</button>
	<button
		onclick={() => onTabChange('attributes')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'attributes' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<Settings class="w-4 h-4" /> Attributes
	</button>
	<button
		onclick={() => onTabChange('tags')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'tags' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<Tag class="w-4 h-4" /> Tags
	</button>
	<button
		onclick={() => onTabChange('move')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'move' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<ArrowRightLeft class="w-4 h-4" /> Move Tasks
	</button>
	<button
		onclick={() => onTabChange('permissions')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'permissions' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<Settings class="w-4 h-4" /> Permissions
	</button>
	<button
		onclick={() => onTabChange('docs')}
		class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'docs' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
	>
		<BookOpen class="w-4 h-4" /> Docs
	</button>
</div>

<!-- Messages Tab -->
{#if activeTab === 'messages'}
<div class="p-4">
<!-- Receive options bar -->
<div class="flex items-center gap-3 mb-4">
<div class="flex items-center gap-2">
<label for="receive-count" class="text-xs text-slate-500 dark:text-slate-400 whitespace-nowrap">Max messages:</label>
<select id="receive-count" bind:value={receiveMaxMessages}
class="px-2 py-1 text-xs bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white focus:outline-none focus:ring-1 focus:ring-indigo-500">
{#each [1,2,3,4,5,6,7,8,9,10] as n}
<option value={n}>{n}</option>
{/each}
</select>
</div>
{#if messages.length > 0}
<button onclick={clearMessages} class="ml-auto text-xs text-slate-500 dark:text-slate-400 hover:text-red-500 flex items-center gap-1">
<X class="w-3 h-3" /> Clear ({messages.length})
</button>
{/if}
</div>
{#if messages.length > 0}
<div class="flex items-center gap-2 mb-3">
<input type="text" bind:value={messageFilter} placeholder="Filter by body, attribute key or value…" class="flex-1 text-xs border border-slate-200 dark:border-slate-600 rounded-lg px-3 py-1.5 bg-white dark:bg-slate-700 text-slate-900 dark:text-white focus:outline-none focus:ring-1 focus:ring-indigo-500" />
{#if messageFilter}<button onclick={() => messageFilter = ''} class="text-xs text-slate-400 hover:text-slate-600"><X class="w-3 h-3" /></button>{/if}
</div>
{/if}
{#if messages.length === 0}
<p class="text-center text-slate-400 dark:text-slate-500 py-8">No messages received. Click "Receive" to fetch messages.</p>
{:else}
{@const filteredMessages = messageFilter ? messages.filter(m => {
					const lf = messageFilter.toLowerCase();
					if ((m.Body ?? '').toLowerCase().includes(lf)) return true;
					if (m.MessageAttributes) {
						return Object.entries(m.MessageAttributes).some(([k, v]) =>
							k.toLowerCase().includes(lf) || (v.StringValue ?? '').toLowerCase().includes(lf)
						);
					}
					return false;
				}) : messages}
<div class="space-y-2">
<p class="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Received Messages ({filteredMessages.length}{messageFilter ? ` of ${messages.length}` : ''})</p>
{#each filteredMessages as msg}
<div class="border border-slate-200 dark:border-slate-600 rounded-lg overflow-hidden">
<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-700/30">
<button
onclick={() => { expandedMsg = expandedMsg === msg.MessageId ? null : (msg.MessageId ?? null); }}
class="flex items-center gap-2 text-left flex-1 min-w-0"
>
<div class="min-w-0 flex-1">
<span class="font-mono text-xs text-slate-600 dark:text-slate-400 truncate block">{msg.MessageId}</span>
{#if msg.Attributes?.SentTimestamp}
<span class="text-xs text-slate-400 dark:text-slate-500">{formatTimestamp(msg.Attributes.SentTimestamp)}</span>
{/if}
</div>
{#if expandedMsg === msg.MessageId}
<ChevronUp class="w-4 h-4 text-slate-400 flex-shrink-0" />
{:else}
<ChevronDown class="w-4 h-4 text-slate-400 flex-shrink-0" />
{/if}
</button>
<button
onclick={() => openVisibilityModal(msg)}
class="p-1 text-slate-400 hover:text-blue-500 ml-2"
title="Change visibility timeout"
>
<RefreshCw class="w-4 h-4" />
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
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1" for="edit-attrs">{label}</label>
<input id="edit-attrs"
type="number"
bind:value={editAttrs[attr]}
min={min}
max={max}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
{/each}
</div>

<!-- Redrive Policy (DLQ) -->
<div class="border border-slate-200 dark:border-slate-700 rounded-lg p-4 space-y-3">
<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300 flex items-center gap-2">
<AlertCircle class="w-4 h-4 text-orange-500" /> Dead Letter Queue (Redrive Policy)
</h3>
<div>
<label class="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1" for="edit-redrive-target-arn">DLQ ARN <span class="text-slate-400 font-normal">(leave blank to remove)</span></label>
<input id="edit-redrive-target-arn"
type="text"
bind:value={editRedriveTargetArn}
placeholder="arn:aws:sqs:us-east-1:000000000000:my-dlq"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm font-mono"
/>
</div>
{#if editRedriveTargetArn.trim()}
<div>
<label for="sqs-max-receive-count" class="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Max Receive Count</label>
<input
id="sqs-max-receive-count"
type="number"
bind:value={editRedriveMaxReceiveCount}
min="1" max="1000"
class="w-40 px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
{/if}
{#if selectedQueue?.attrs.RedrivePolicy}
<div class="flex items-center gap-2 p-2 bg-orange-50 dark:bg-orange-900/20 rounded text-xs text-orange-700 dark:text-orange-300">
<AlertCircle class="w-3 h-3 flex-shrink-0" />
DLQ wired — messages exceeding maxReceiveCount are routed to the dead-letter queue
</div>
{/if}

<!-- DLQ Source Queues (shown when this queue is a DLQ) -->
{#if selectedQueue?.attrs.QueueArn}
<div class="mt-2">
<div class="flex items-center justify-between mb-1">
<p class="text-xs font-medium text-slate-600 dark:text-slate-400">Source Queues using this as DLQ</p>
<button type="button" onclick={loadDlqSourceQueues} class="text-xs text-indigo-500 hover:underline">Refresh</button>
</div>
{#if loadingDlqSources}
<p class="text-xs text-slate-400">Loading...</p>
{:else if dlqSourceQueues.length === 0}
<p class="text-xs text-slate-400 italic">None found (or not a DLQ target)</p>
{:else}
<div class="space-y-1 max-h-32 overflow-y-auto">
{#each dlqSourceQueues as srcUrl}
<p class="text-xs font-mono text-slate-600 dark:text-slate-400 truncate">{srcUrl}</p>
{/each}
</div>
{/if}
</div>
{/if}
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

<!-- Move Tasks Tab -->
{#if activeTab === 'move'}
<div class="p-4 space-y-4">
<div class="border border-slate-200 dark:border-slate-700 rounded-lg p-4 space-y-3">
<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Start Message Move Task</h3>
<p class="text-xs text-slate-500 dark:text-slate-400">Move messages from this queue (source) to another queue. Typically used to move messages out of a DLQ back to the original queue.</p>
<div>
<label for="sqs-move-dest-arn" class="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Destination Queue ARN <span class="text-slate-400 font-normal">(optional — omit to use default destination)</span></label>
<input
id="sqs-move-dest-arn"
type="text"
bind:value={moveTaskDestArn}
placeholder="arn:aws:sqs:us-east-1:000000000000:destination-queue"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm font-mono"
/>
</div>
<button onclick={startMoveTask} disabled={startingMoveTask} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2 text-sm">
<ArrowRightLeft class="w-4 h-4" />
{startingMoveTask ? 'Starting...' : 'Start Move Task'}
</button>
</div>

<div>
<div class="flex items-center justify-between mb-3">
<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Move Tasks</h3>
<button onclick={loadMoveTasks} class="text-xs text-indigo-500 hover:underline flex items-center gap-1">
<RefreshCw class="w-3 h-3 {loadingMoveTasks ? 'animate-spin' : ''}" /> Refresh
</button>
</div>
{#if loadingMoveTasks}
<p class="text-sm text-slate-400 text-center py-4">Loading...</p>
{:else if moveTasks.length === 0}
<p class="text-sm text-slate-400 text-center py-4 italic">No move tasks found</p>
{:else}
<div class="space-y-2">
{#each moveTasks as task}
<div class="border border-slate-200 dark:border-slate-600 rounded-lg p-3">
<div class="flex items-start justify-between">
<div class="min-w-0 flex-1">
<div class="flex items-center gap-2 mb-1">
<span class="text-xs font-semibold px-2 py-0.5 rounded-full {task.Status === 'RUNNING' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : task.Status === 'COMPLETED' ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' : task.Status === 'CANCELLING' ? 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400'}">{task.Status ?? 'UNKNOWN'}</span>
{#if task.ApproximateNumberOfMessagesMoved != null}
<span class="text-xs text-slate-500">{task.ApproximateNumberOfMessagesMoved} moved</span>
{/if}
{#if task.ApproximateNumberOfMessagesToMove != null}
<span class="text-xs text-slate-500">of ~{task.ApproximateNumberOfMessagesToMove}</span>
{/if}
</div>
{#if task.DestinationArn}
<p class="text-xs font-mono text-slate-500 truncate">→ {task.DestinationArn}</p>
{/if}
{#if task.StartedTimestamp}
<p class="text-xs text-slate-400">Started: {new Date(task.StartedTimestamp * 1000).toLocaleString()}</p>
{/if}
</div>
{#if task.Status === 'RUNNING' || task.Status === 'CANCELLING'}
<button onclick={() => cancelMoveTask(task.TaskHandle ?? '')} class="ml-2 text-xs px-2 py-1 text-red-600 dark:text-red-400 border border-red-200 dark:border-red-800 rounded hover:bg-red-50 dark:hover:bg-red-900/20 flex-shrink-0">
Cancel
</button>
{/if}
</div>
</div>
{/each}
</div>
{/if}
</div>
</div>
{/if}

<!-- Permissions Tab -->
{#if activeTab === 'permissions'}
<div class="p-4 space-y-4">
<p class="text-xs text-slate-500 dark:text-slate-400">Cross-account access policy. AddPermission appends one Sid'd statement; RemovePermission deletes by Sid (label).</p>

{#if permissionsLoading}
<div class="text-sm text-slate-500 dark:text-slate-400">Loading…</div>
{:else if permissions.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400 italic">No permissions set on this queue.</p>
{:else}
<div class="space-y-2">
{#each permissions as p}
<div class="flex items-start gap-2 p-3 bg-slate-50 dark:bg-slate-700/30 rounded">
<div class="min-w-0 flex-1">
<div class="text-sm font-mono text-slate-900 dark:text-white truncate">{p.label || '(no label)'}</div>
<div class="text-xs text-slate-500 dark:text-slate-400 mt-1">Principal: <span class="font-mono">{p.principal}</span></div>
<div class="text-xs text-slate-500 dark:text-slate-400">Actions: <span class="font-mono">{p.actions}</span></div>
</div>
<button onclick={() => removePermission(p.label)} disabled={!p.label} class="p-1 text-slate-400 hover:text-red-500 disabled:opacity-30" title="Remove permission">
<Trash2 class="w-4 h-4" />
</button>
</div>
{/each}
</div>
{/if}

<div class="border-t border-slate-200 dark:border-slate-700 pt-4 space-y-2">
<h4 class="text-sm font-semibold text-slate-900 dark:text-white">Add Permission</h4>
<div class="grid grid-cols-1 md:grid-cols-3 gap-2">
<input type="text" placeholder="Label (Sid)" bind:value={newPermission.label} class="px-2 py-1.5 text-sm bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white focus:outline-none focus:ring-1 focus:ring-indigo-500" />
<input type="text" placeholder="AWS Account IDs (comma-separated)" bind:value={newPermission.principal} class="px-2 py-1.5 text-sm bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white focus:outline-none focus:ring-1 focus:ring-indigo-500" />
<input type="text" placeholder="Actions (comma-separated)" bind:value={newPermission.actions} class="px-2 py-1.5 text-sm bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white focus:outline-none focus:ring-1 focus:ring-indigo-500" />
</div>
<button onclick={addPermission} class="px-3 py-1.5 text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 rounded">Add Permission</button>
</div>
</div>
{/if}

<!-- Docs Tab -->
{#if activeTab === 'docs'}
<div class="p-4 space-y-4 max-h-96 overflow-y-auto">
<p class="text-xs text-slate-500 dark:text-slate-400">Supported SQS operations in GopherStack.</p>
{#each [
['CreateQueue', 'Create a new standard or FIFO queue. Queue names must be 1–80 characters, alphanumeric, hyphens, and underscores. FIFO queues must end with .fifo. Re-creating with identical attributes is idempotent; different attributes returns QueueNameExists.'],
['DeleteQueue', 'Delete a queue by URL. The operation is irreversible. Inflight messages are discarded.'],
['ListQueues', 'List up to 1000 queue URLs. Supports QueueNamePrefix filter and NextToken pagination.'],
['GetQueueUrl', 'Return the URL for a queue given its name.'],
['GetQueueAttributes', 'Retrieve attributes such as ApproximateNumberOfMessages, VisibilityTimeout, QueueArn, and RedrivePolicy.'],
['SetQueueAttributes', 'Update configurable attributes including VisibilityTimeout, MessageRetentionPeriod, DelaySeconds, MaximumMessageSize, ReceiveMessageWaitTimeSeconds, and RedrivePolicy.'],
['SendMessage', 'Publish a message (1 B – MaximumMessageSize). DelaySeconds overrides the queue-level delay for this message. FIFO queues require MessageGroupId.'],
['SendMessageBatch', 'Send up to 10 messages in a single call. Per-entry failures are reported in the Failed list.'],
['ReceiveMessage', 'Pull 1–10 messages. Received messages become invisible for VisibilityTimeout seconds. Supports long-polling via WaitTimeSeconds (0–20).'],
['DeleteMessage', 'Permanently delete a received message by its ReceiptHandle.'],
['DeleteMessageBatch', 'Delete up to 10 messages in one call.'],
['ChangeMessageVisibility', 'Extend or shorten a message\'s visibility timeout. Setting it to 0 makes the message immediately available again.'],
['ChangeMessageVisibilityBatch', 'Change visibility for up to 10 in-flight messages.'],
['PurgeQueue', 'Delete all messages from a queue without deleting the queue itself.'],
['TagQueue', 'Add or update resource tags on a queue.'],
['UntagQueue', 'Remove resource tags by key.'],
['ListQueueTags', 'List all resource tags on a queue.'],
['AddPermission', 'Add an IAM-style permission to the queue policy.'],
['RemovePermission', 'Remove a permission entry by label.'],
['ListDeadLetterSourceQueues', 'List all queues that point to this queue as their dead-letter queue.'],
['StartMessageMoveTask', 'Initiate a task to move messages from a source queue (e.g. DLQ) to a destination.'],
['ListMessageMoveTasks', 'List move tasks for a source queue ARN.'],
['CancelMessageMoveTask', 'Cancel a running move task by its TaskHandle.']
] as [op, desc]}
<div class="border border-slate-100 dark:border-slate-700 rounded-lg p-3">
<p class="text-sm font-semibold text-slate-800 dark:text-slate-200 font-mono">{op}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{desc}</p>
</div>
{/each}
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
{#if visibilityModal.msg}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-lg p-6 max-w-md w-full mx-4">
<h3 class="text-lg font-semibold text-slate-900 dark:text-white mb-3">Change Message Visibility</h3>
<p class="text-xs text-slate-500 dark:text-slate-400 mb-3">Seconds (0 returns the message immediately, max 43200).</p>
<input type="number" min="0" max="43200" bind:value={visibilityModal.seconds} class="w-full px-3 py-2 text-sm bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white focus:outline-none focus:ring-1 focus:ring-indigo-500" />
<div class="flex justify-end gap-2 mt-4">
<button onclick={() => { visibilityModal = { msg: null, seconds: 30 }; }} class="px-3 py-1.5 text-sm text-slate-600 dark:text-slate-400">Cancel</button>
<button onclick={applyVisibilityChange} class="px-3 py-1.5 text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700 rounded">Apply</button>
</div>
</div>
</div>
{/if}

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
<div>
<label for="sqs-wait-time" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Receive Wait Time (s)</label>
<input id="sqs-wait-time" type="number" bind:value={newWaitTimeSeconds} min="0" max="20"
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
<div class="flex items-center justify-between mb-1">
<label for="sqs-msg-body" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Message Body</label>
<span class="text-xs text-slate-400" class:text-red-500={new TextEncoder().encode(msgBody).length > 262144}>{new TextEncoder().encode(msgBody).length.toLocaleString()} / 262,144 bytes</span>
</div>
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

<!-- Batch Send Modal -->
{#if showBatchSendModal}
<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
<div role="none" onclick={() => { showBatchSendModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showBatchSendModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
<div class="relative w-full max-w-2xl bg-white dark:bg-slate-800 rounded-2xl shadow-2xl border border-white/20 dark:border-slate-700 overflow-hidden">
<div class="flex items-center justify-between px-6 py-4 border-b border-slate-200 dark:border-slate-700">
<h3 class="text-lg font-bold text-slate-900 dark:text-white">Batch Send Messages</h3>
<button onclick={() => showBatchSendModal = false} class="text-slate-400 hover:text-slate-600 text-xl">&times;</button>
</div>
<div class="p-6 space-y-4 max-h-[70vh] overflow-y-auto">
{#each batchMessages as msg, i}
<div class="p-3 border border-slate-200 dark:border-slate-600 rounded-lg space-y-2">
<div class="flex items-center justify-between">
<span class="text-xs font-bold text-slate-500 dark:text-slate-400">Message {i + 1}</span>
{#if batchMessages.length > 1}
<button onclick={() => batchMessages = batchMessages.filter((_, j) => j !== i)} class="text-red-400 hover:text-red-600 text-xs">Remove</button>
{/if}
</div>
<textarea bind:value={msg.body} rows="3" placeholder="Message body" class="w-full text-sm font-mono border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white focus:outline-none focus:ring-1 focus:ring-indigo-500"></textarea>
<div class="flex items-center gap-2">
<label class="text-xs text-slate-500" for="delay">Delay (s):</label>
<input id="delay" type="number" bind:value={msg.delay} min="0" max="900" class="w-20 text-sm border border-slate-300 dark:border-slate-600 rounded-lg p-1.5 bg-white dark:bg-slate-700 dark:text-white" />
</div>
</div>
{/each}
{#if batchMessages.length < 10}
<button onclick={() => batchMessages = [...batchMessages, { id: String(batchMessages.length + 1), body: '', delay: 0 }]} class="w-full text-sm text-indigo-600 hover:text-indigo-700 border-2 border-dashed border-indigo-300 dark:border-indigo-700 rounded-lg py-2">+ Add Message</button>
{/if}
<div class="flex justify-end gap-3 pt-2">
<button onclick={() => showBatchSendModal = false} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button onclick={sendBatch} disabled={sendingBatch} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
<Send class="w-4 h-4" />
{sendingBatch ? 'Sending…' : `Send ${batchMessages.filter(m => m.body.trim()).length} Message(s)`}
</button>
</div>
</div>
</div>
</div>
{/if}
