<script lang="ts">
import { confirmDestructive } from '$lib/confirm-dialog';
import { untrack } from 'svelte';
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
import { getSNSClient } from '$lib/aws-client';
import {
ListTopicsCommand,
GetTopicAttributesCommand,
SetTopicAttributesCommand,
ListTagsForResourceCommand,
TagResourceCommand,
UntagResourceCommand,
ListSubscriptionsByTopicCommand,
CreateTopicCommand,
DeleteTopicCommand,
SubscribeCommand,
PublishCommand,
UnsubscribeCommand,
ConfirmSubscriptionCommand,
GetSubscriptionAttributesCommand,
SetSubscriptionAttributesCommand,
ListPlatformApplicationsCommand,
CreatePlatformApplicationCommand,
DeletePlatformApplicationCommand,
type Topic,
type Subscription,
type PlatformApplication
} from '@aws-sdk/client-sns';
import { toast } from 'svelte-sonner';
import {
Bell, Search, RefreshCw, Plus, Trash2, Send, Users,
X, Copy, Tag, BarChart3, BookOpen, Smartphone, CheckCircle2,
Settings
} from 'lucide-svelte';

const sns = regionalClient(getSNSClient);

// ─── Page tabs ───────────────────────────────────────────────────────────────
let pageTab = $state<'topics' | 'platform-apps'>('topics');

// ─── Topics ──────────────────────────────────────────────────────────────────
let loading = $state(false);
let topics = $state<Array<Topic & { Attributes?: Record<string, string> }>>([]);
let searchQuery = $state('');
let selectedTopic = $state<(Topic & { Attributes?: Record<string, string> }) | null>(null);
let activeTab = $state<'subscriptions' | 'attributes' | 'tags' | 'metrics' | 'docs'>('subscriptions');

// Subscriptions
let subscriptions = $state<Subscription[]>([]);
let subscriptionDetails = $state<Record<string, Record<string, string>>>({});
let loadingSubscriptions = $state(false);

// Topic tags
let topicTags = $state<Record<string, string>>({});
let tagKey = $state('');
let tagValue = $state('');
let savingTag = $state(false);

// ─── Create Topic modal ───────────────────────────────────────────────────────
let showCreateModal = $state(false);
let creating = $state(false);
let newTopicName = $state('');
let newTopicFifo = $state(false);
let newTopicContentBasedDedup = $state(false);
let newTopicDisplayName = $state('');

// Topic name is valid if it matches AWS constraints: 1-256 chars, alphanumeric/hyphen/underscore.
const topicNameValid = $derived(
  newTopicName.trim() !== '' && /^[a-zA-Z0-9_-]{1,256}$/.test(newTopicName.trim())
);

// ─── Subscribe modal ──────────────────────────────────────────────────────────
let showSubscribeModal = $state(false);
let subscribing = $state(false);
let subProtocol = $state('sqs');
let subEndpoint = $state('');
let subFilterPolicy = $state('');
let subRawMessageDelivery = $state(false);

// ─── Filter Policy modal ──────────────────────────────────────────────────────
let showFilterPolicyModal = $state(false);
let savingFilterPolicy = $state(false);
let selectedSubscriptionArn = $state('');
let editFilterPolicy = $state('{}');
let editFilterPolicyScope = $state('MessageAttributes');

// ─── Redrive Policy modal ─────────────────────────────────────────────────────
let showRedriveModal = $state(false);
let savingRedrive = $state(false);
let editRedriveArn = $state('');
let redriveSubArn = $state('');

// ─── Publish modal ────────────────────────────────────────────────────────────
let showPublishModal = $state(false);
let publishing = $state(false);
let pubSubject = $state('');
let pubMessage = $state('');
let pubAttrRows = $state<Array<{ name: string; type: string; value: string }>>([]);
let pubAttrJsonMode = $state(false);
let pubAttrJson = $state('{}');
let pubDeduplicationId = $state('');
let pubGroupId = $state('');

// ─── Platform Applications ────────────────────────────────────────────────────
let platformApps = $state<PlatformApplication[]>([]);
let loadingApps = $state(false);
let showCreateAppModal = $state(false);
let creatingApp = $state(false);
let newAppName = $state('');
let newAppPlatform = $state('GCM');
let newAppCredential = $state('');
let newAppPrincipal = $state('');

// ─── Derived ─────────────────────────────────────────────────────────────────
const filteredTopics = $derived(
topics.filter((t) => {
const name = topicName(t.TopicArn ?? '');
return name.toLowerCase().includes(searchQuery.toLowerCase());
})
);

const confirmedCount = $derived(
subscriptions.filter((s) => s.SubscriptionArn !== 'PendingConfirmation').length
);

const pendingCount = $derived(
subscriptions.filter((s) => s.SubscriptionArn === 'PendingConfirmation').length
);

// ─── Helpers ─────────────────────────────────────────────────────────────────
function topicName(arn: string): string {
return arn.split(':').pop() ?? arn;
}

function isFifo(arn: string | undefined): boolean {
return arn?.endsWith('.fifo') ?? false;
}

function shortArn(arn: string): string {
const parts = arn.split(':');
return '\u2026:' + parts.slice(-2).join(':');
}

async function copyText(text: string, label = 'ARN') {
await navigator.clipboard.writeText(text);
toast.success(`${label} copied`);
}

// ─── Load Topics ─────────────────────────────────────────────────────────────
async function loadTopics() {
loading = true;
try {
const res = await sns().send(new ListTopicsCommand({}));
const raw = res.Topics ?? [];
topics = await Promise.all(
raw.slice(0, 50).map(async (t) => {
try {
const attrs = await sns().send(new GetTopicAttributesCommand({ TopicArn: t.TopicArn }));
return { ...t, Attributes: attrs.Attributes ?? {} };
} catch {
return { ...t, Attributes: {} };
}
})
);
} catch (err: unknown) {
toast.error(`Failed to load topics: ${(err as Error).message}`);
} finally {
loading = false;
}
}

// ─── Create Topic ─────────────────────────────────────────────────────────────
async function createTopic() {
if (!newTopicName.trim()) return;
creating = true;
try {
const name = newTopicFifo ? `${newTopicName.trim()}.fifo` : newTopicName.trim();
const attrs: Record<string, string> = {};
if (newTopicFifo) {
attrs['FifoTopic'] = 'true';
attrs['ContentBasedDeduplication'] = newTopicContentBasedDedup ? 'true' : 'false';
}
if (newTopicDisplayName.trim()) attrs['DisplayName'] = newTopicDisplayName.trim();
await sns().send(new CreateTopicCommand({
Name: name,
Attributes: Object.keys(attrs).length > 0 ? attrs : undefined
}));
toast.success(`Topic "${name}" created`);
showCreateModal = false;
newTopicName = '';
newTopicFifo = false;
newTopicContentBasedDedup = false;
newTopicDisplayName = '';
await loadTopics();
} catch (err: unknown) {
toast.error(`Create failed: ${(err as Error).message}`);
} finally {
creating = false;
}
}

// ─── Delete Topic ─────────────────────────────────────────────────────────────
async function deleteTopic(arn: string) {
const name = topicName(arn);
if (!await confirmDestructive({
title: 'Delete Topic',
message: `Delete topic "${name}"? All subscriptions will be removed.`
})) return;
try {
await sns().send(new DeleteTopicCommand({ TopicArn: arn }));
toast.success(`Topic "${name}" deleted`);
if (selectedTopic?.TopicArn === arn) selectedTopic = null;
await loadTopics();
} catch (err: unknown) {
toast.error(`Delete failed: ${(err as Error).message}`);
}
}

// ─── Select Topic ─────────────────────────────────────────────────────────────
async function selectTopic(topic: typeof topics[0]) {
selectedTopic = topic;
activeTab = 'subscriptions';
await Promise.all([
loadSubscriptions(topic.TopicArn ?? ''),
loadTopicTags(topic.TopicArn ?? '')
]);
}

// ─── Load Subscriptions ───────────────────────────────────────────────────────
async function loadSubscriptions(topicArn: string) {
loadingSubscriptions = true;
try {
const res = await sns().send(new ListSubscriptionsByTopicCommand({ TopicArn: topicArn }));
subscriptions = res.Subscriptions ?? [];
await loadSubscriptionDetails(subscriptions);
} catch (err: unknown) {
toast.error(`Failed to load subscriptions: ${(err as Error).message}`);
} finally {
loadingSubscriptions = false;
}
}

async function loadSubscriptionDetails(subs: Subscription[]) {
const loaded: Record<string, Record<string, string>> = {};
await Promise.all(
subs
.filter((s) => s.SubscriptionArn && s.SubscriptionArn !== 'PendingConfirmation')
.map(async (sub) => {
try {
const attrs = await sns().send(
new GetSubscriptionAttributesCommand({ SubscriptionArn: sub.SubscriptionArn })
);
loaded[sub.SubscriptionArn!] = attrs.Attributes ?? {};
} catch {
loaded[sub.SubscriptionArn!] = {};
}
})
);
subscriptionDetails = loaded;
}

// ─── Subscribe ────────────────────────────────────────────────────────────────
async function subscribe() {
if (!selectedTopic || !subEndpoint.trim()) return;
subscribing = true;
try {
const subAttrs: Record<string, string> = {};
if (subFilterPolicy.trim() && subFilterPolicy.trim() !== '{}') {
subAttrs['FilterPolicy'] = subFilterPolicy.trim();
}
if (subRawMessageDelivery) subAttrs['RawMessageDelivery'] = 'true';
await sns().send(new SubscribeCommand({
TopicArn: selectedTopic.TopicArn,
Protocol: subProtocol,
Endpoint: subEndpoint.trim(),
Attributes: Object.keys(subAttrs).length > 0 ? subAttrs : undefined
}));
toast.success('Subscription created');
showSubscribeModal = false;
subEndpoint = '';
subFilterPolicy = '';
subRawMessageDelivery = false;
await loadSubscriptions(selectedTopic.TopicArn ?? '');
} catch (err: unknown) {
toast.error(`Subscribe failed: ${(err as Error).message}`);
} finally {
subscribing = false;
}
}

// ─── Unsubscribe ──────────────────────────────────────────────────────────────
async function unsubscribe(arn: string) {
if (!await confirmDestructive({
title: 'Remove Subscription',
message: 'Remove this SNS subscription?',
confirmLabel: 'Remove'
})) return;
try {
await sns().send(new UnsubscribeCommand({ SubscriptionArn: arn }));
toast.success('Subscription removed');
if (selectedTopic) await loadSubscriptions(selectedTopic.TopicArn ?? '');
} catch (err: unknown) {
toast.error(`Unsubscribe failed: ${(err as Error).message}`);
}
}

// ─── Confirm Subscription ─────────────────────────────────────────────────────
async function confirmSubscription(topicArn: string) {
try {
await sns().send(new ConfirmSubscriptionCommand({
TopicArn: topicArn,
Token: 'mock-confirm-token'
}));
toast.success('Subscription confirmed');
await loadSubscriptions(topicArn);
} catch (err: unknown) {
toast.error(`Confirm failed: ${(err as Error).message}`);
}
}

// ─── Filter Policy ────────────────────────────────────────────────────────────
function openFilterPolicyEditor(sub: Subscription) {
selectedSubscriptionArn = sub.SubscriptionArn ?? '';
const attrs = subscriptionDetails[selectedSubscriptionArn] ?? {};
editFilterPolicy = attrs['FilterPolicy'] || '{}';
editFilterPolicyScope = attrs['FilterPolicyScope'] || 'MessageAttributes';
showFilterPolicyModal = true;
}

async function saveFilterPolicy() {
if (!selectedSubscriptionArn) return;
const value = editFilterPolicy.trim();
if (value && value !== '{}') {
try { JSON.parse(value); } catch {
toast.error('Filter policy must be valid JSON');
return;
}
}
savingFilterPolicy = true;
try {
await sns().send(new SetSubscriptionAttributesCommand({
SubscriptionArn: selectedSubscriptionArn,
AttributeName: 'FilterPolicy',
AttributeValue: value === '{}' ? '' : value
}));
if (editFilterPolicyScope) {
await sns().send(new SetSubscriptionAttributesCommand({
SubscriptionArn: selectedSubscriptionArn,
AttributeName: 'FilterPolicyScope',
AttributeValue: editFilterPolicyScope
}));
}
subscriptionDetails = {
...subscriptionDetails,
[selectedSubscriptionArn]: {
...subscriptionDetails[selectedSubscriptionArn],
FilterPolicy: value === '{}' ? '' : value,
FilterPolicyScope: editFilterPolicyScope
}
};
showFilterPolicyModal = false;
toast.success('Filter policy updated');
} catch (err: unknown) {
toast.error(`Failed: ${(err as Error).message}`);
} finally {
savingFilterPolicy = false;
}
}

// ─── RawMessageDelivery toggle ────────────────────────────────────────────────
async function toggleRawDelivery(subArn: string) {
const current = subscriptionDetails[subArn]?.['RawMessageDelivery'] === 'true';
try {
await sns().send(new SetSubscriptionAttributesCommand({
SubscriptionArn: subArn,
AttributeName: 'RawMessageDelivery',
AttributeValue: current ? 'false' : 'true'
}));
subscriptionDetails = {
...subscriptionDetails,
[subArn]: {
...subscriptionDetails[subArn],
RawMessageDelivery: current ? 'false' : 'true'
}
};
toast.success(`Raw delivery ${current ? 'disabled' : 'enabled'}`);
} catch (err: unknown) {
toast.error(`Failed: ${(err as Error).message}`);
}
}

// ─── Redrive Policy ───────────────────────────────────────────────────────────
function openRedriveModal(sub: Subscription) {
redriveSubArn = sub.SubscriptionArn ?? '';
const attrs = subscriptionDetails[redriveSubArn] ?? {};
try {
const rp = attrs['RedrivePolicy'] ? JSON.parse(attrs['RedrivePolicy']) : {};
editRedriveArn = rp.deadLetterTargetArn ?? '';
} catch {
editRedriveArn = '';
}
showRedriveModal = true;
}

async function saveRedrivePolicy() {
if (!redriveSubArn) return;
savingRedrive = true;
try {
const policy = editRedriveArn.trim()
? JSON.stringify({ deadLetterTargetArn: editRedriveArn.trim() })
: '';
await sns().send(new SetSubscriptionAttributesCommand({
SubscriptionArn: redriveSubArn,
AttributeName: 'RedrivePolicy',
AttributeValue: policy
}));
subscriptionDetails = {
...subscriptionDetails,
[redriveSubArn]: {
...subscriptionDetails[redriveSubArn],
RedrivePolicy: policy
}
};
showRedriveModal = false;
toast.success('Redrive policy updated');
} catch (err: unknown) {
toast.error(`Failed: ${(err as Error).message}`);
} finally {
savingRedrive = false;
}
}

// ─── Publish ──────────────────────────────────────────────────────────────────
function pubAttrsToSdk(): Record<string, { DataType: string; StringValue?: string; BinaryValue?: Uint8Array }> | undefined {
	let rows: Array<{ name: string; type: string; value: string }>;
	if (pubAttrJsonMode) {
		try {
			const parsed = JSON.parse(pubAttrJson);
			rows = Object.entries(parsed).map(([k, v]) => ({ name: k, type: 'String', value: String(v) }));
		} catch { return undefined; }
	} else {
		rows = pubAttrRows.filter(r => r.name.trim());
	}
	if (rows.length === 0) return undefined;
	return Object.fromEntries(rows.map(r => [r.name, { DataType: r.type || 'String', StringValue: r.value }]));
}

async function publish() {
if (!selectedTopic || !pubMessage.trim()) return;
publishing = true;
try {
await sns().send(new PublishCommand({
TopicArn: selectedTopic.TopicArn,
Subject: pubSubject || undefined,
Message: pubMessage,
MessageAttributes: pubAttrsToSdk(),
MessageDeduplicationId: pubDeduplicationId || undefined,
MessageGroupId: pubGroupId || undefined
}));
toast.success('Message published');
showPublishModal = false;
pubSubject = '';
pubMessage = '';
pubAttrRows = [];
pubAttrJsonMode = false;
pubAttrJson = '{}';
pubDeduplicationId = '';
pubGroupId = '';
} catch (err: unknown) {
toast.error(`Publish failed: ${(err as Error).message}`);
} finally {
publishing = false;
}
}

// ─── Topic Tags ───────────────────────────────────────────────────────────────
async function loadTopicTags(topicArn: string) {
try {
const res = await sns().send(new ListTagsForResourceCommand({ ResourceArn: topicArn }));
topicTags = Object.fromEntries((res.Tags ?? []).map((t) => [t.Key!, t.Value!]));
} catch {
topicTags = {};
}
}

async function addTag() {
if (!tagKey.trim() || !selectedTopic) return;
savingTag = true;
try {
await sns().send(new TagResourceCommand({
ResourceArn: selectedTopic.TopicArn,
Tags: [{ Key: tagKey.trim(), Value: tagValue.trim() }]
}));
topicTags = { ...topicTags, [tagKey.trim()]: tagValue.trim() };
tagKey = '';
tagValue = '';
toast.success('Tag added');
} catch (err: unknown) {
toast.error(`Failed: ${(err as Error).message}`);
} finally {
savingTag = false;
}
}

async function removeTag(key: string) {
if (!selectedTopic) return;
try {
await sns().send(new UntagResourceCommand({
ResourceArn: selectedTopic.TopicArn,
TagKeys: [key]
}));
const updated = { ...topicTags };
delete updated[key];
topicTags = updated;
toast.success('Tag removed');
} catch (err: unknown) {
toast.error(`Failed: ${(err as Error).message}`);
}
}

// ─── ContentBasedDeduplication toggle ────────────────────────────────────────
async function toggleContentBasedDedup() {
if (!selectedTopic?.TopicArn) return;
const current = selectedTopic.Attributes?.ContentBasedDeduplication === 'true';
try {
await sns().send(new SetTopicAttributesCommand({
TopicArn: selectedTopic.TopicArn,
AttributeName: 'ContentBasedDeduplication',
AttributeValue: current ? 'false' : 'true'
}));
selectedTopic = {
...selectedTopic,
Attributes: {
...selectedTopic.Attributes,
ContentBasedDeduplication: current ? 'false' : 'true'
}
};
toast.success(`Content-based deduplication ${current ? 'disabled' : 'enabled'}`);
} catch (err: unknown) {
toast.error(`Failed: ${(err as Error).message}`);
}
}

// ─── Platform Applications ────────────────────────────────────────────────────
async function loadPlatformApps() {
loadingApps = true;
try {
const res = await sns().send(new ListPlatformApplicationsCommand({}));
platformApps = res.PlatformApplications ?? [];
} catch (err: unknown) {
toast.error(`Failed to load platform apps: ${(err as Error).message}`);
} finally {
loadingApps = false;
}
}

async function createPlatformApp() {
if (!newAppName.trim() || !newAppCredential.trim()) return;
creatingApp = true;
try {
const attrs: Record<string, string> = { PlatformCredential: newAppCredential.trim() };
if (newAppPrincipal.trim()) attrs['PlatformPrincipal'] = newAppPrincipal.trim();
await sns().send(new CreatePlatformApplicationCommand({
Name: newAppName.trim(),
Platform: newAppPlatform,
Attributes: attrs
}));
toast.success(`Platform application "${newAppName.trim()}" created`);
showCreateAppModal = false;
newAppName = '';
newAppCredential = '';
newAppPrincipal = '';
await loadPlatformApps();
} catch (err: unknown) {
toast.error(`Create failed: ${(err as Error).message}`);
} finally {
creatingApp = false;
}
}

async function deletePlatformApp(arn: string) {
const name = arn.split('/').pop() ?? arn;
if (!await confirmDestructive({
title: 'Delete Platform Application',
message: `Delete "${name}"? All endpoints will be removed.`
})) return;
try {
await sns().send(new DeletePlatformApplicationCommand({ PlatformApplicationArn: arn }));
toast.success('Platform application deleted');
await loadPlatformApps();
} catch (err: unknown) {
toast.error(`Delete failed: ${(err as Error).message}`);
}
}

// ─── Page tab switch ──────────────────────────────────────────────────────────
async function switchPageTab(tab: typeof pageTab) {
pageTab = tab;
if (tab === 'platform-apps') await loadPlatformApps();
}

// Topics, subscriptions, tags, and platform applications are all
// region-scoped, so a region switch must clear them (not just reload) and
// then reload whichever page tab is currently active. `pageTab` is read via
// `untrack` because `switchPageTab` also writes it — without that, this
// effect would depend on `pageTab` too and re-clear/re-fetch everything on
// every tab click, not just on a region change.
onRegionChange(() => {
	topics = [];
	selectedTopic = null;
	subscriptions = [];
	subscriptionDetails = {};
	topicTags = {};
	platformApps = [];
	showCreateModal = false;
	showSubscribeModal = false;
	showFilterPolicyModal = false;
	showRedriveModal = false;
	showPublishModal = false;
	showCreateAppModal = false;
	void loadTopics();
	if (untrack(() => pageTab) === 'platform-apps') void loadPlatformApps();
});
</script>

<div class="space-y-6">
<!-- Header -->
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
<Bell class="w-6 h-6 text-orange-600 dark:text-orange-400" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">SNS Topics</h1>
<p class="text-slate-600 dark:text-slate-300">Simple Notification Service — {topics.length} topics</p>
</div>
</div>
<div class="flex items-center gap-2">
<button
onclick={() => loadTopics()}
class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
title="Refresh"
>
<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
</button>
<button
onclick={() => { showCreateModal = true; }}
class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"
>
<Plus class="w-4 h-4" />
Create Topic
</button>
</div>
</div>

<!-- Page Tabs -->
<div class="border-b border-slate-200 dark:border-slate-700">
<nav class="flex gap-0">
{#each ([['topics', Bell, 'Topics'], ['platform-apps', Smartphone, 'Platform Applications']] as const) as [id, Icon, label]}
<button
onclick={() => switchPageTab(id)}
class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors {pageTab === id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
>
<Icon class="w-4 h-4" />{label}
</button>
{/each}
</nav>
</div>

<!-- ═══════════════════════════ TOPICS TAB ════════════════════════════════ -->
{#if pageTab === 'topics'}
<!-- Search -->
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input
type="text"
bind:value={searchQuery}
placeholder="Search topics..."
class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>

<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
<!-- Topic List -->
<div class="lg:col-span-1 space-y-2">
{#if loading}
<div class="text-center py-12">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
<p class="text-slate-500 dark:text-slate-400">Loading topics...</p>
</div>
{:else if filteredTopics.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
<Bell class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
{#if searchQuery}
<p class="text-slate-500 dark:text-slate-400">No topics match &ldquo;{searchQuery}&rdquo;</p>
<button onclick={() => { searchQuery = ''; }} class="mt-2 text-xs text-indigo-500 hover:underline">Clear search</button>
{:else}
<p class="text-slate-500 dark:text-slate-400">No topics found</p>
<p class="text-xs text-slate-400 dark:text-slate-500 mt-1">Create your first topic to get started.</p>
{/if}
</div>
{:else}
{#each filteredTopics as topic}
<div
role="button"
tabindex="0"
onclick={() => selectTopic(topic)}
onkeypress={(e) => { if (e.key === 'Enter') selectTopic(topic); }}
class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-4 hover:border-indigo-400 transition-colors cursor-pointer {selectedTopic?.TopicArn === topic.TopicArn ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
>
<div class="flex items-center justify-between">
<div class="min-w-0 flex-1">
<p class="font-medium text-slate-900 dark:text-white truncate">{topicName(topic.TopicArn ?? '')}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
{topic.Attributes?.SubscriptionsConfirmed ?? 0} confirmed &middot; {topic.Attributes?.SubscriptionsPending ?? 0} pending
</p>
</div>
<div class="flex items-center gap-1 ml-2 flex-shrink-0">
{#if isFifo(topic.TopicArn)}
<span class="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">FIFO</span>
{/if}
<button
onclick={(e) => { e.stopPropagation(); deleteTopic(topic.TopicArn ?? ''); }}
class="p-1 text-slate-400 hover:text-red-500"
>
<Trash2 class="w-4 h-4" />
</button>
</div>
</div>
</div>
{/each}
{/if}
</div>

<!-- Topic Detail -->
<div class="lg:col-span-2">
{#if selectedTopic}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
<!-- Detail Header -->
<div class="p-5 border-b border-slate-200 dark:border-slate-700">
<div class="flex items-start justify-between">
<div>
<h2 class="text-xl font-bold text-slate-900 dark:text-white">{topicName(selectedTopic.TopicArn ?? '')}</h2>
{#if selectedTopic.Attributes?.DisplayName}
<p class="text-sm text-slate-500 dark:text-slate-400 mt-0.5">{selectedTopic.Attributes.DisplayName}</p>
{/if}
<button
onclick={() => copyText(selectedTopic?.TopicArn ?? '', 'Topic ARN')}
title={selectedTopic.TopicArn}
class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500 mt-1 font-mono"
>
<Copy class="w-3 h-3" />
{shortArn(selectedTopic.TopicArn ?? '')}
</button>
</div>
<div class="flex gap-2">
<button
onclick={() => { showSubscribeModal = true; }}
class="px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-1.5 text-sm"
>
<Users class="w-4 h-4" />
Subscribe
</button>
<button
onclick={() => { showPublishModal = true; }}
class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm"
>
<Send class="w-4 h-4" />
Publish
</button>
</div>
</div>
</div>

<!-- Tabs -->
<div class="border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
<nav class="flex">
{#each ([['subscriptions', Users, `Subscriptions (${subscriptions.length})`], ['attributes', Settings, 'Attributes'], ['tags', Tag, 'Tags'], ['metrics', BarChart3, 'Metrics'], ['docs', BookOpen, 'Docs']] as const) as [id, Icon, label]}
<button
onclick={() => { activeTab = id; }}
class="flex items-center gap-2 px-4 py-3 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
>
<Icon class="w-4 h-4" />{label}
</button>
{/each}
</nav>
</div>

<!-- ── Subscriptions Tab ── -->
{#if activeTab === 'subscriptions'}
<div class="p-5 space-y-3">
<div class="flex items-center justify-between mb-1">
<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Subscriptions</h3>
<div class="flex items-center gap-3">
<div class="flex gap-3 text-xs text-slate-500 dark:text-slate-400">
<span class="text-green-600 dark:text-green-400 font-medium">&#10003; {confirmedCount} confirmed</span>
{#if pendingCount > 0}
<span class="text-amber-600 dark:text-amber-400 font-medium">&#8987; {pendingCount} pending</span>
{/if}
</div>
<button
onclick={() => loadSubscriptions(selectedTopic?.TopicArn ?? '')}
class="p-1 text-slate-400 hover:text-slate-700 dark:hover:text-slate-200"
title="Refresh subscriptions"
>
<RefreshCw class="w-4 h-4" />
</button>
<button
onclick={() => { showSubscribeModal = true; }}
class="flex items-center gap-1 px-2 py-1 text-xs bg-green-600 hover:bg-green-700 text-white rounded"
title="Add subscription"
>
<Plus class="w-3 h-3" />Add
</button>
</div>
</div>

{#if loadingSubscriptions}
<div class="text-center py-6">
<div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500"></div>
</div>
{:else if subscriptions.length === 0}
<div class="py-6 text-center">
<p class="text-slate-500 dark:text-slate-400 text-sm">No subscriptions yet</p>
<button
onclick={() => { showSubscribeModal = true; }}
class="mt-2 text-xs text-indigo-500 hover:underline"
>Add first subscription &rarr;</button>
</div>
{:else}
{#each subscriptions as sub}
{@const subArn = sub.SubscriptionArn ?? ''}
{@const isPending = subArn === 'PendingConfirmation'}
{@const details = subscriptionDetails[subArn] ?? {}}
{@const hasFilter = !!details['FilterPolicy']}
{@const hasRedrive = !!details['RedrivePolicy']}
{@const rawDelivery = details['RawMessageDelivery'] === 'true'}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3 space-y-2">
<div class="flex items-start justify-between gap-2">
<div class="min-w-0 flex-1">
<div class="flex items-center gap-2 flex-wrap">
<span class="px-2 py-0.5 text-xs rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 font-mono">{sub.Protocol}</span>
{#if isPending}
<span class="px-2 py-0.5 text-xs rounded bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300">Pending</span>
{/if}
{#if rawDelivery}
<span class="px-2 py-0.5 text-xs rounded bg-slate-100 dark:bg-slate-600 text-slate-600 dark:text-slate-300">Raw</span>
{/if}
{#if hasFilter}
<span class="px-2 py-0.5 text-xs rounded bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300">
Filtered{details['FilterPolicyScope'] ? ` (${details['FilterPolicyScope']})` : ''}
</span>
{/if}
{#if hasRedrive}
<span class="px-2 py-0.5 text-xs rounded bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300">DLQ</span>
{/if}
</div>
<p class="text-sm text-slate-700 dark:text-slate-300 truncate mt-1">{sub.Endpoint}</p>
{#if !isPending}
<button
onclick={() => copyText(subArn, 'Subscription ARN')}
class="flex items-center gap-1 text-xs text-slate-400 hover:text-indigo-500 font-mono mt-0.5"
>
<Copy class="w-3 h-3" />{shortArn(subArn)}
</button>
{/if}
</div>
<div class="flex items-center gap-1 flex-shrink-0">
{#if isPending}
<button
onclick={() => confirmSubscription(selectedTopic?.TopicArn ?? '')}
class="px-2 py-1 text-xs bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 rounded hover:bg-amber-200 dark:hover:bg-amber-900/50 flex items-center gap-1"
title="Confirm subscription"
>
<CheckCircle2 class="w-3 h-3" /> Confirm
</button>
{:else}
<button
onclick={() => openFilterPolicyEditor(sub)}
class="px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700"
title="Edit filter policy"
>Filter</button>
<button
onclick={() => toggleRawDelivery(subArn)}
class="px-2 py-1 text-xs border rounded {rawDelivery ? 'border-green-300 dark:border-green-700 text-green-600 dark:text-green-400' : 'border-slate-200 dark:border-slate-600 text-slate-600 dark:text-slate-300'} hover:bg-slate-100 dark:hover:bg-slate-700"
title="Toggle raw message delivery"
>Raw</button>
<button
onclick={() => openRedriveModal(sub)}
class="px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700"
title="Edit redrive (DLQ) policy"
>DLQ</button>
{/if}
<button
onclick={() => unsubscribe(isPending ? (sub.TopicArn ?? '') : subArn)}
class="p-1 text-slate-400 hover:text-red-500"
>
<X class="w-4 h-4" />
</button>
</div>
</div>
</div>
{/each}
{/if}
</div>
{/if}

<!-- ── Attributes Tab ── -->
{#if activeTab === 'attributes'}
<div class="p-5 space-y-3">
<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
{#each Object.entries(selectedTopic.Attributes ?? {}).filter(([k]) => !['Policy', 'DeliveryPolicy', 'EffectiveDeliveryPolicy', 'TopicArn', 'FifoTopic', 'ContentBasedDeduplication'].includes(k) && k !== 'Owner') as [key, value]}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
<p class="text-xs font-medium text-slate-500 dark:text-slate-400">{key}</p>
<p class="text-sm text-slate-900 dark:text-white mt-0.5 break-all">{value || '—'}</p>
</div>
{/each}
</div>
{#if selectedTopic.Attributes?.EffectiveDeliveryPolicy || selectedTopic.Attributes?.DeliveryPolicy}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Effective Delivery Policy</p>
<pre class="text-xs font-mono text-slate-700 dark:text-slate-300 overflow-x-auto whitespace-pre-wrap break-all">{(() => { try { return JSON.stringify(JSON.parse(selectedTopic.Attributes?.EffectiveDeliveryPolicy ?? selectedTopic.Attributes?.DeliveryPolicy ?? ''), null, 2); } catch { return selectedTopic.Attributes?.EffectiveDeliveryPolicy ?? ''; } })()}</pre>
</div>
{/if}
{#if isFifo(selectedTopic.TopicArn)}
<div class="mt-2 p-3 bg-purple-50 dark:bg-purple-900/20 rounded-lg flex items-center justify-between">
<div>
<p class="text-sm font-medium text-purple-800 dark:text-purple-300">Content-based Deduplication</p>
<p class="text-xs text-purple-600 dark:text-purple-400 mt-0.5">Automatically deduplicates messages based on content hash.</p>
</div>
<button
onclick={toggleContentBasedDedup}
class="px-3 py-1.5 text-xs rounded-lg {selectedTopic.Attributes?.ContentBasedDeduplication === 'true' ? 'bg-purple-600 text-white' : 'border border-purple-300 dark:border-purple-700 text-purple-700 dark:text-purple-300'}"
>
{selectedTopic.Attributes?.ContentBasedDeduplication === 'true' ? 'Enabled' : 'Disabled'}
</button>
</div>
{/if}
</div>
{/if}

<!-- ── Tags Tab ── -->
{#if activeTab === 'tags'}
<div class="p-5 space-y-4">
{#if Object.keys(topicTags).length > 0}
<div class="space-y-2">
{#each Object.entries(topicTags) as [k, v]}
<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/50 rounded-lg px-3 py-2">
<div>
<span class="text-sm font-mono text-slate-700 dark:text-slate-300">{k}</span>
<span class="text-slate-400 mx-2">=</span>
<span class="text-sm text-slate-600 dark:text-slate-400">{v}</span>
</div>
<button onclick={() => removeTag(k)} class="text-slate-400 hover:text-red-500">
<X class="w-4 h-4" />
</button>
</div>
{/each}
</div>
{:else}
<p class="text-sm text-slate-500 dark:text-slate-400">No tags set</p>
{/if}
<form onsubmit={(e) => { e.preventDefault(); addTag(); }} class="flex gap-2 items-end">
<div class="flex-1">
<label for="tag-key" class="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Key</label>
<input id="tag-key" type="text" bind:value={tagKey} placeholder="Key"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
</div>
<div class="flex-1">
<label for="tag-val" class="block text-xs font-medium text-slate-600 dark:text-slate-400 mb-1">Value</label>
<input id="tag-val" type="text" bind:value={tagValue} placeholder="Value"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
</div>
<button type="submit" disabled={savingTag} class="px-3 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 text-sm disabled:opacity-50">
{savingTag ? '&#8230;' : 'Add'}
</button>
</form>
</div>
{/if}

<!-- ── Metrics Tab ── -->
{#if activeTab === 'metrics'}
<div class="p-5 space-y-4">
<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
{#each ([
['Confirmed', selectedTopic.Attributes?.SubscriptionsConfirmed ?? '0', 'text-green-600 dark:text-green-400'],
['Pending', selectedTopic.Attributes?.SubscriptionsPending ?? '0', 'text-amber-600 dark:text-amber-400'],
['Deleted', selectedTopic.Attributes?.SubscriptionsDeleted ?? '0', 'text-red-600 dark:text-red-400'],
['Type', isFifo(selectedTopic.TopicArn) ? 'FIFO' : 'Standard', 'text-purple-600 dark:text-purple-400']
] as const) as [label, value, color]}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3 text-center">
<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
<p class="text-xl font-bold mt-1 {color}">{value}</p>
</div>
{/each}
</div>
{#if subscriptions.length > 0}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-4">
<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">Protocol breakdown</p>
<div class="space-y-2">
{#each Object.entries(subscriptions.reduce<Record<string, number>>((acc, s) => { acc[s.Protocol ?? 'unknown'] = (acc[s.Protocol ?? 'unknown'] ?? 0) + 1; return acc; }, {})) as [proto, count]}
<div class="flex items-center gap-2">
<span class="text-xs font-mono text-slate-600 dark:text-slate-300 w-20">{proto}</span>
<div class="flex-1 bg-slate-200 dark:bg-slate-600 rounded-full h-2">
<div class="bg-indigo-500 h-2 rounded-full" style="width: {Math.round((count / subscriptions.length) * 100)}%"></div>
</div>
<span class="text-xs text-slate-500 dark:text-slate-400">{count}</span>
</div>
{/each}
</div>
</div>
{/if}
</div>
{/if}

<!-- ── Docs Tab ── -->
{#if activeTab === 'docs'}
<div class="p-4 space-y-3 max-h-96 overflow-y-auto">
<p class="text-xs text-slate-500 dark:text-slate-400">Supported SNS operations in GopherStack.</p>
{#each ([
['CreateTopic', 'Create a standard or FIFO topic. FIFO names must end with .fifo. Optional attributes: DisplayName, FifoTopic, ContentBasedDeduplication, KmsMasterKeyId.'],
['DeleteTopic', 'Delete a topic and remove all its subscriptions.'],
['ListTopics', 'List topics (100 per page, paginated via NextToken).'],
['GetTopicAttributes', 'Retrieve attributes including SubscriptionsConfirmed, SubscriptionsPending, Policy, DisplayName.'],
['SetTopicAttributes', 'Update DisplayName, DeliveryPolicy, ContentBasedDeduplication, KmsMasterKeyId, TracingConfig.'],
['Subscribe', 'Create a subscription. Protocols: email, email-json, http, https, sqs, lambda, sms, application, firehose. Optional: FilterPolicy, RawMessageDelivery, RedrivePolicy.'],
['ConfirmSubscription', 'Confirm a pending http/https subscription using its confirmation token.'],
['Unsubscribe', 'Delete a subscription by ARN.'],
['ListSubscriptions', 'List all subscriptions across all topics (100 per page).'],
['ListSubscriptionsByTopic', 'List subscriptions for a given topic ARN.'],
['GetSubscriptionAttributes', 'Get subscription attributes: FilterPolicy, RawMessageDelivery, RedrivePolicy, FilterPolicyScope.'],
['SetSubscriptionAttributes', 'Update FilterPolicy, RawMessageDelivery, RedrivePolicy, FilterPolicyScope, SubscriptionRoleArn.'],
['Publish', 'Publish a message. Supports Subject, MessageAttributes, MessageStructure=json, MessageDeduplicationId, MessageGroupId (FIFO).'],
['PublishBatch', 'Publish up to 10 messages in one request.'],
['TagResource', 'Add or update tags on a topic.'],
['UntagResource', 'Remove tags by key from a topic.'],
['ListTagsForResource', 'List all resource tags on a topic.'],
['CreatePlatformApplication', 'Register a mobile push platform (GCM/FCM, APNS, ADM, BAIDU, WNS).'],
['CreatePlatformEndpoint', 'Register a device token under a platform application.'],
['ListPlatformApplications', 'List all platform applications.'],
['GetPlatformApplicationAttributes', 'Get attributes of a platform application.'],
['SetPlatformApplicationAttributes', 'Update platform application attributes.'],
['DeletePlatformApplication', 'Delete a platform application and all its endpoints.'],
['ListEndpointsByPlatformApplication', 'List device endpoints registered under an application.'],
['GetEndpointAttributes', 'Get attributes (Token, Enabled, CustomUserData) of a device endpoint.'],
['SetEndpointAttributes', 'Update device endpoint attributes.'],
['DeleteEndpoint', 'Delete a device endpoint.'],
['AddPermission', 'Add a policy statement to a topic.'],
['RemovePermission', 'Remove a policy statement by label.'],
['GetDataProtectionPolicy', 'Retrieve the data protection policy for a topic.'],
['PutDataProtectionPolicy', 'Store a data protection policy on a topic.'],
['GetSMSAttributes', 'Get SMS delivery attributes (DefaultSMSType, MonthlySpendLimit, etc.).'],
['SetSMSAttributes', 'Update SMS delivery attributes.'],
['GetSMSSandboxAccountStatus', 'Check if the account is in SMS sandbox mode.'],
['CreateSMSSandboxPhoneNumber', 'Add a phone number to the SMS sandbox for testing.'],
['VerifySMSSandboxPhoneNumber', 'Verify a sandbox phone number with a one-time password.'],
['DeleteSMSSandboxPhoneNumber', 'Remove a phone number from the SMS sandbox.'],
['ListSMSSandboxPhoneNumbers', 'List sandbox phone numbers and their verification status.'],
['CheckIfPhoneNumberIsOptedOut', 'Check if a phone number has opted out of SMS.'],
['ListPhoneNumbersOptedOut', 'List opted-out phone numbers.'],
['OptInPhoneNumber', 'Re-opt-in a phone number to receive SNS SMS messages.'],
['ListOriginationNumbers', 'List SMS origination phone numbers for this account.']
] as const) as [op, desc]}
<div class="border border-slate-100 dark:border-slate-700 rounded-lg p-3">
<p class="text-sm font-semibold text-slate-800 dark:text-slate-200 font-mono">{op}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{desc}</p>
</div>
{/each}
</div>
{/if}
</div>
{:else}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Bell class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
<p class="text-slate-500 dark:text-slate-400">Select a topic to view details</p>
</div>
{/if}
</div>
</div>
{/if}

<!-- ════════════════════════ PLATFORM APPS TAB ════════════════════════════ -->
{#if pageTab === 'platform-apps'}
<div class="space-y-4">
<div class="flex items-center justify-between">
<p class="text-sm text-slate-500 dark:text-slate-400">
{platformApps.length} platform application{platformApps.length !== 1 ? 's' : ''}
</p>
<div class="flex gap-2">
<button
onclick={loadPlatformApps}
class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
title="Refresh"
>
<RefreshCw class="w-4 h-4 {loadingApps ? 'animate-spin' : ''}" />
</button>
<button
onclick={() => { showCreateAppModal = true; }}
class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2 text-sm"
>
<Plus class="w-4 h-4" />
Create Application
</button>
</div>
</div>

{#if loadingApps}
<div class="text-center py-12">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div>
</div>
{:else if platformApps.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Smartphone class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
<p class="text-slate-500 dark:text-slate-400">No platform applications</p>
<p class="text-xs text-slate-400 dark:text-slate-500 mt-1">Create one to enable mobile push notifications (APNs, FCM, ADM, Baidu)</p>
</div>
{:else}
<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
{#each platformApps as app}
{@const parts = (app.PlatformApplicationArn ?? '').split('/')}
{@const platform = parts[1] ?? ''}
{@const appName = parts[2] ?? app.PlatformApplicationArn ?? ''}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
<div class="flex items-start justify-between">
<div class="min-w-0">
<p class="font-medium text-slate-900 dark:text-white truncate">{appName}</p>
<div class="flex items-center gap-2 mt-1 flex-wrap">
<span class="inline-block px-2 py-0.5 text-xs rounded bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300">{platform}</span>
{#if app.Attributes?.Enabled === 'false'}
<span class="px-2 py-0.5 text-xs rounded bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300">Disabled</span>
{:else}
<span class="px-2 py-0.5 text-xs rounded bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">Enabled</span>
{/if}
</div>
</div>
<div class="flex items-center gap-1">
<button
onclick={() => copyText(app.PlatformApplicationArn ?? '', 'Application ARN')}
class="p-1 text-slate-400 hover:text-slate-700 dark:hover:text-slate-200"
title="Copy ARN"
>
<Copy class="w-4 h-4" />
</button>
<button
onclick={() => deletePlatformApp(app.PlatformApplicationArn ?? '')}
class="p-1 text-slate-400 hover:text-red-500"
>
<Trash2 class="w-4 h-4" />
</button>
</div>
</div>
<button
onclick={() => copyText(app.PlatformApplicationArn ?? '', 'Application ARN')}
class="mt-2 text-xs text-slate-400 dark:text-slate-500 hover:text-indigo-500 font-mono truncate block w-full text-left"
>
{shortArn(app.PlatformApplicationArn ?? '')}
</button>
</div>
{/each}
</div>
{/if}
</div>
{/if}
</div>

<!-- ═══════════════════════════════ MODALS ═══════════════════════════════════ -->

<!-- Create Topic Modal -->
{#if showCreateModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Topic</h2>
<form onsubmit={(e) => { e.preventDefault(); createTopic(); }} class="space-y-4">
<div>
<label for="sns-topic-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Topic Name</label>
<input
id="sns-topic-name"
type="text"
bind:value={newTopicName}
placeholder="e.g. my-notifications"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border {newTopicName.trim() && !topicNameValid ? 'border-red-400 focus:ring-red-400' : 'border-slate-200 dark:border-slate-600 focus:ring-indigo-500'} rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2"
required
/>
{#if newTopicName.trim() && !topicNameValid}
<p class="text-xs text-red-500 mt-1">Name must be 1–256 characters: letters, numbers, hyphens, underscores only.</p>
{:else if newTopicFifo && newTopicName.trim()}
<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">Final name: {newTopicName.trim()}.fifo</p>
{/if}
</div>
<div>
<label for="sns-display-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Display Name <span class="font-normal text-slate-400">(optional)</span></label>
<input
id="sns-display-name"
type="text"
bind:value={newTopicDisplayName}
placeholder="e.g. My Alerts"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
<label class="flex items-center gap-2 cursor-pointer">
<input type="checkbox" bind:checked={newTopicFifo} class="rounded" />
<span class="text-sm text-slate-700 dark:text-slate-300">FIFO topic (ordered, deduplicated)</span>
</label>
{#if newTopicFifo}
<label class="flex items-center gap-2 cursor-pointer ml-5">
<input type="checkbox" bind:checked={newTopicContentBasedDedup} class="rounded" />
<span class="text-sm text-slate-700 dark:text-slate-300">Content-based deduplication</span>
</label>
{/if}
<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showCreateModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button type="submit" disabled={creating || !topicNameValid} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
{creating ? 'Creating...' : 'Create Topic'}
</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Subscribe Modal -->
{#if showSubscribeModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Subscribe to Topic</h2>
<form onsubmit={(e) => { e.preventDefault(); subscribe(); }} class="space-y-4">
<div>
<label for="sns-protocol" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Protocol</label>
<select
id="sns-protocol"
bind:value={subProtocol}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
>
{#each ['email', 'email-json', 'sqs', 'lambda', 'http', 'https', 'sms', 'application', 'firehose'] as proto}
<option value={proto}>{proto}</option>
{/each}
</select>
</div>
<div>
<label for="sns-endpoint" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Endpoint</label>
<input
id="sns-endpoint"
type="text"
bind:value={subEndpoint}
placeholder={subProtocol === 'email' || subProtocol === 'email-json' ? 'user@example.com' : subProtocol === 'sqs' ? 'arn:aws:sqs:us-east-1:000000000000:my-queue' : subProtocol === 'lambda' ? 'arn:aws:lambda:us-east-1:000000000000:function:my-fn' : subProtocol === 'http' ? 'http://example.com/notify' : subProtocol === 'https' ? 'https://example.com/notify' : subProtocol === 'sms' ? '+15555555555' : subProtocol === 'firehose' ? 'arn:aws:firehose:us-east-1:000000000000:deliverystream/my-stream' : 'Enter endpoint'}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
required
/>
{#if subProtocol === 'http' || subProtocol === 'https'}
<p class="text-xs text-amber-600 dark:text-amber-400 mt-1">⚠ HTTP/HTTPS subscriptions require manual confirmation via ConfirmSubscription.</p>
{/if}
</div>
<div>
<label for="sns-filter-policy" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Filter Policy <span class="font-normal text-slate-400">(JSON, optional)</span></label>
<textarea
id="sns-filter-policy"
bind:value={subFilterPolicy}
rows={3}
placeholder={`{"eventType": ["order"]}`}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
></textarea>
</div>
<label class="flex items-center gap-2 cursor-pointer">
<input type="checkbox" bind:checked={subRawMessageDelivery} class="rounded" />
<span class="text-sm text-slate-700 dark:text-slate-300">Raw message delivery (skip SNS envelope)</span>
</label>
<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showSubscribeModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button type="submit" disabled={subscribing} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50">
{subscribing ? 'Subscribing...' : 'Subscribe'}
</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Filter Policy Modal -->
{#if showFilterPolicyModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Edit Filter Policy</h2>
<form onsubmit={(e) => { e.preventDefault(); saveFilterPolicy(); }} class="space-y-4">
<div>
<label for="sns-filter-scope" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Scope</label>
<select
id="sns-filter-scope"
bind:value={editFilterPolicyScope}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
>
<option value="MessageAttributes">MessageAttributes</option>
<option value="MessageBody">MessageBody</option>
</select>
</div>
<div>
<label for="sns-edit-filter-policy" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Filter Policy JSON</label>
<textarea
id="sns-edit-filter-policy"
bind:value={editFilterPolicy}
rows={7}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
></textarea>
<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">Supports: string/number exact, prefix, anything-but (string, array, prefix), exists, numeric operators (&lt;, &lt;=, =, &lt;&gt;, &gt;=, &gt;).</p>
</div>
<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showFilterPolicyModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button type="submit" disabled={savingFilterPolicy} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
{savingFilterPolicy ? 'Saving...' : 'Save'}
</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Redrive Policy Modal -->
{#if showRedriveModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Redrive (DLQ) Policy</h2>
<form onsubmit={(e) => { e.preventDefault(); saveRedrivePolicy(); }} class="space-y-4">
<div>
<label for="dlq-arn" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
Dead-letter Queue ARN <span class="font-normal text-slate-400">(leave blank to remove)</span>
</label>
<input
id="dlq-arn"
type="text"
bind:value={editRedriveArn}
placeholder="arn:aws:sqs:us-east-1:000000000000:my-dlq"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showRedriveModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button type="submit" disabled={savingRedrive} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
{savingRedrive ? 'Saving...' : 'Save'}
</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Publish Modal -->
{#if showPublishModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Publish Message</h2>
<form onsubmit={(e) => { e.preventDefault(); publish(); }} class="space-y-4">
<div>
<label for="sns-subject" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Subject <span class="font-normal text-slate-400">(optional)</span></label>
<input
id="sns-subject"
type="text"
bind:value={pubSubject}
placeholder="e.g. Alert notification"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
<div>
<div class="flex items-center justify-between mb-1">
<label for="sns-message" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Message</label>
<span class="text-xs {pubMessage.length > 262144 ? 'text-red-500 font-medium' : 'text-slate-400'}">{pubMessage.length.toLocaleString()} / 262,144</span>
</div>
<textarea
id="sns-message"
bind:value={pubMessage}
rows={4}
placeholder="Message body..."
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
required
></textarea>
</div>
<div>
	<div class="flex items-center justify-between mb-1">
		<div class="block text-sm font-medium text-slate-700 dark:text-slate-300">Message Attributes</div>
		<button type="button" onclick={() => {
			if (!pubAttrJsonMode) {
				pubAttrJson = pubAttrRows.filter(r => r.name.trim()).length
					? JSON.stringify(Object.fromEntries(pubAttrRows.filter(r => r.name.trim()).map(r => [r.name, r.value])), null, 2)
					: '{}';
			} else {
				try {
					const parsed = JSON.parse(pubAttrJson);
					pubAttrRows = Object.entries(parsed).map(([k, v]) => ({ name: k, type: 'String', value: String(v) }));
				} catch { /* keep rows */ }
			}
			pubAttrJsonMode = !pubAttrJsonMode;
		}} class="text-xs text-indigo-600 dark:text-indigo-400 hover:underline">
			{pubAttrJsonMode ? 'Switch to fields' : 'Switch to JSON'}
		</button>
	</div>
	{#if pubAttrJsonMode}
		<textarea
			bind:value={pubAttrJson}
			rows={3}
			placeholder={`{"key": "value"}`}
			class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
		></textarea>
	{:else}
		<div class="space-y-1">
			{#each pubAttrRows as row, i}
				<div class="flex gap-1 items-center">
					<input type="text" bind:value={row.name} placeholder="Name" class="flex-1 min-w-0 px-2 py-1 text-xs bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white" />
					<select bind:value={row.type} class="px-1 py-1 text-xs bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white">
						<option value="String">String</option>
						<option value="Number">Number</option>
						<option value="Binary">Binary</option>
					</select>
					<input type="text" bind:value={row.value} placeholder="Value" class="flex-1 min-w-0 px-2 py-1 text-xs bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded text-slate-900 dark:text-white" />
					<button type="button" onclick={() => { pubAttrRows = pubAttrRows.filter((_, j) => j !== i); }} class="text-red-400 hover:text-red-600 text-xs px-1">✕</button>
				</div>
			{/each}
			<button type="button" onclick={() => { pubAttrRows = [...pubAttrRows, { name: '', type: 'String', value: '' }]; }} class="text-xs text-indigo-600 dark:text-indigo-400 hover:underline mt-1">+ Add attribute</button>
		</div>
	{/if}
</div>
{#if isFifo(selectedTopic?.TopicArn)}
<div class="grid grid-cols-2 gap-4">
<div>
<label for="sns-group-id" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message Group ID</label>
<input
id="sns-group-id"
type="text"
bind:value={pubGroupId}
placeholder="e.g. order-group-1"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
<div>
<label for="sns-dedup-id" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Deduplication ID <span class="font-normal text-slate-400">(optional)</span></label>
<input
id="sns-dedup-id"
type="text"
bind:value={pubDeduplicationId}
placeholder="e.g. unique-msg-id-123"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
/>
</div>
</div>
{/if}
<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showPublishModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button type="submit" disabled={publishing} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
<Send class="w-4 h-4" />
{publishing ? 'Publishing...' : 'Publish'}
</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Create Platform Application Modal -->
{#if showCreateAppModal}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Platform Application</h2>
<form onsubmit={(e) => { e.preventDefault(); createPlatformApp(); }} class="space-y-4">
<div>
<label for="app-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Application Name</label>
<input
id="app-name"
type="text"
bind:value={newAppName}
placeholder="e.g. MyMobileApp"
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
required
/>
</div>
<div>
<label for="app-platform" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Platform</label>
<select
id="app-platform"
bind:value={newAppPlatform}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
>
<option value="GCM">GCM / FCM (Android)</option>
<option value="APNS">APNS (iOS Production)</option>
<option value="APNS_SANDBOX">APNS Sandbox (iOS Dev)</option>
<option value="ADM">ADM (Amazon Kindle)</option>
<option value="BAIDU">BAIDU (China)</option>
<option value="WNS">WNS (Windows)</option>
<option value="MPNS">MPNS (Windows Phone)</option>
</select>
</div>
<div>
<label for="app-credential" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">
{newAppPlatform === 'GCM' ? 'Server API Key (PlatformCredential)' : newAppPlatform === 'APNS' || newAppPlatform === 'APNS_SANDBOX' ? 'Private Key (PlatformCredential)' : newAppPlatform === 'WNS' ? 'Client Secret (PlatformCredential)' : 'Platform Credential'}
<span class="font-normal text-slate-400">(server key / certificate)</span>
</label>
<textarea
id="app-credential"
bind:value={newAppCredential}
rows={3}
placeholder={newAppPlatform === 'GCM' ? 'AIza...' : newAppPlatform === 'APNS' || newAppPlatform === 'APNS_SANDBOX' ? '-----BEGIN PRIVATE KEY-----\n...' : 'Server API key or PEM certificate content...'}
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-xs resize-none"
required
></textarea>
</div>
{#if newAppPlatform === 'APNS' || newAppPlatform === 'APNS_SANDBOX'}
<div>
<label for="app-principal" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Platform Principal <span class="font-normal text-slate-400">(SSL certificate)</span></label>
<textarea
id="app-principal"
bind:value={newAppPrincipal}
rows={2}
placeholder="SSL certificate..."
class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-xs resize-none"
></textarea>
</div>
{/if}
<div class="flex justify-end gap-3 pt-2">
<button type="button" onclick={() => { showCreateAppModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
<button type="submit" disabled={creatingApp} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
{creatingApp ? 'Creating...' : 'Create'}
</button>
</div>
</form>
</div>
</div>
{/if}
