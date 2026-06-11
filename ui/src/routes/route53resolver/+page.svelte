<script lang="ts">
import { confirmDestructive } from '$lib/confirm-dialog';
import { onMount } from 'svelte';
import { getRoute53ResolverClient } from '$lib/aws-client';
import {
ListResolverEndpointsCommand,
ListResolverRulesCommand,
ListFirewallRuleGroupsCommand,
ListFirewallDomainListsCommand,
ListResolverQueryLogConfigsCommand,
ListFirewallRulesCommand,
ListFirewallDomainsCommand,
CreateResolverEndpointCommand,
DeleteResolverEndpointCommand,
CreateResolverRuleCommand,
DeleteResolverRuleCommand,
CreateFirewallRuleGroupCommand,
DeleteFirewallRuleGroupCommand,
UpdateFirewallRuleCommand,
CreateFirewallDomainListCommand,
DeleteFirewallDomainListCommand,
CreateResolverQueryLogConfigCommand,
DeleteResolverQueryLogConfigCommand,
type ResolverEndpoint,
type ResolverRule,
type FirewallRuleGroup,
type FirewallDomainList,
type ResolverQueryLogConfig,
type FirewallRule,
ResolverEndpointDirection,
RuleTypeOption
} from '@aws-sdk/client-route53resolver';
import { toast } from 'svelte-sonner';
import {
Globe,
RefreshCw,
Search,
ArrowRightLeft,
BookOpen,
Plus,
Trash2,
Shield,
ListChecks,
FileText,
BarChart3,
ChevronDown,
ChevronRight,
ChevronUp,
Copy,
X
} from 'lucide-svelte';

const r53r = getRoute53ResolverClient();

// ── page-level tab ──────────────────────────────────────────────────────────
let activeTab = $state<'endpoints' | 'rules' | 'firewall' | 'querylog' | 'docs'>('endpoints');
let firewallSubTab = $state<'groups' | 'domains'>('groups');

// ── loading / search ────────────────────────────────────────────────────────
let loading = $state(false);
let searchQuery = $state('');

// ── data ────────────────────────────────────────────────────────────────────
let endpoints = $state<ResolverEndpoint[]>([]);
let rules = $state<ResolverRule[]>([]);
let fwGroups = $state<FirewallRuleGroup[]>([]);
let fwDomainLists = $state<FirewallDomainList[]>([]);
let queryLogConfigs = $state<ResolverQueryLogConfig[]>([]);

// ── expanded rows ────────────────────────────────────────────────────────────
let expandedFwGroup = $state<string | null>(null);
let expandedFwGroupRules = $state<Record<string, FirewallRule[]>>({}); 
let expandedDomainList = $state<string | null>(null);
let expandedDomainListDomains = $state<Record<string, string[]>>({});

// ── derived filtered lists ───────────────────────────────────────────────────
const q = $derived(searchQuery.toLowerCase());
const filteredEndpoints = $derived(endpoints.filter((e) => (e.Name ?? '').toLowerCase().includes(q)));
const filteredRules = $derived(rules.filter((r) => (r.Name ?? '').toLowerCase().includes(q) || (r.DomainName ?? '').toLowerCase().includes(q)));
const filteredFwGroups = $derived(fwGroups.filter((g) => (g.Name ?? '').toLowerCase().includes(q)));
const filteredDomainLists = $derived(fwDomainLists.filter((d) => (d.Name ?? '').toLowerCase().includes(q)));
const filteredQueryLogs = $derived(queryLogConfigs.filter((c) => (c.Name ?? '').toLowerCase().includes(q)));

// ── stat cards ───────────────────────────────────────────────────────────────
const inboundCount = $derived(endpoints.filter((e) => e.Direction === 'INBOUND').length);
const outboundCount = $derived(endpoints.filter((e) => e.Direction === 'OUTBOUND').length);

// ── create-endpoint modal ────────────────────────────────────────────────────
let showCreateEndpoint = $state(false);
let ceCreating = $state(false);
let ceName = $state('');
let ceDirection = $state<ResolverEndpointDirection>(ResolverEndpointDirection.Inbound);
let ceVpcId = $state('');
let ceSgIds = $state('');
let ceSubnetId = $state('');
let ceIp = $state('');

// ── create-rule modal ────────────────────────────────────────────────────────
let showCreateRule = $state(false);
let crCreating = $state(false);
let crName = $state('');
let crDomainName = $state('');
let crRuleType = $state<RuleTypeOption>(RuleTypeOption.Forward);
let crEndpointId = $state('');
let crTargetIp = $state('');

// ── create-fw-group modal ────────────────────────────────────────────────────
let showCreateFwGroup = $state(false);
let fwgCreating = $state(false);
let fwgName = $state('');

// ── create-domain-list modal ─────────────────────────────────────────────────
let showCreateDomainList = $state(false);
let dlCreating = $state(false);
let dlName = $state('');

// ── create-query-log modal ────────────────────────────────────────────────────
let showCreateQueryLog = $state(false);
let qlCreating = $state(false);
let qlName = $state('');
let qlDestArn = $state('');

// ── load data ────────────────────────────────────────────────────────────────
async function loadData() {
loading = true;
try {
const [epRes, ruleRes, fwgRes, dlRes, qlRes] = await Promise.all([
r53r.send(new ListResolverEndpointsCommand({})),
r53r.send(new ListResolverRulesCommand({})),
r53r.send(new ListFirewallRuleGroupsCommand({})),
r53r.send(new ListFirewallDomainListsCommand({})),
r53r.send(new ListResolverQueryLogConfigsCommand({}))
]);
endpoints = epRes.ResolverEndpoints ?? [];
rules = ruleRes.ResolverRules ?? [];
fwGroups = fwgRes.FirewallRuleGroups ?? [];
fwDomainLists = dlRes.FirewallDomainLists ?? [];
queryLogConfigs = qlRes.ResolverQueryLogConfigs ?? [];
} catch (e) {
toast.error('Failed to load Route 53 Resolver data: ' + String(e));
} finally {
loading = false;
}
}

onMount(loadData);

function copyToClipboard(text: string) {
navigator.clipboard.writeText(text).then(() => toast.success('Copied!'));
}

// ── endpoints ─────────────────────────────────────────────────────────────────
async function createEndpoint() {
if (!ceName.trim()) return toast.error('Name is required');
ceCreating = true;
try {
const sgIds = ceSgIds.split(',').map((s) => s.trim()).filter(Boolean);
const ipAddresses = ceSubnetId.trim()
? [{ SubnetId: ceSubnetId.trim(), Ip: ceIp.trim() || undefined }]
: [];
await r53r.send(new CreateResolverEndpointCommand({
CreatorRequestId: crypto.randomUUID(),
Name: ceName.trim(),
Direction: ceDirection,
SecurityGroupIds: sgIds.length > 0 ? sgIds : ['sg-demo'],
IpAddresses: ipAddresses.length > 0 ? ipAddresses : [{ SubnetId: ceVpcId.trim() || 'subnet-demo' }]
}));
toast.success(`Endpoint "${ceName}" created`);
showCreateEndpoint = false;
ceName = '';
ceVpcId = '';
ceSgIds = '';
ceSubnetId = '';
ceIp = '';
await loadData();
} catch (e) {
toast.error('Failed to create endpoint: ' + String(e));
} finally {
ceCreating = false;
}
}

async function deleteEndpoint(id: string, name: string) {
if (!(await confirmDestructive(`Delete endpoint "${name}"?`))) return;
try {
await r53r.send(new DeleteResolverEndpointCommand({ ResolverEndpointId: id }));
toast.success(`Endpoint "${name}" deleted`);
await loadData();
} catch (e) {
toast.error('Delete failed: ' + String(e));
}
}

// ── rules ─────────────────────────────────────────────────────────────────────
async function createRule() {
if (!crName.trim() || !crDomainName.trim()) return toast.error('Name and DomainName are required');
crCreating = true;
try {
const targetIps = crTargetIp.trim() ? [{ Ip: crTargetIp.trim(), Port: 53 }] : undefined;
await r53r.send(new CreateResolverRuleCommand({
CreatorRequestId: crypto.randomUUID(),
Name: crName.trim(),
DomainName: crDomainName.trim(),
RuleType: crRuleType,
ResolverEndpointId: crEndpointId.trim() || undefined,
TargetIps: targetIps
}));
toast.success(`Rule "${crName}" created`);
showCreateRule = false;
crName = '';
crDomainName = '';
crEndpointId = '';
crTargetIp = '';
await loadData();
} catch (e) {
toast.error('Failed to create rule: ' + String(e));
} finally {
crCreating = false;
}
}

async function deleteRule(id: string, name: string) {
if (!(await confirmDestructive(`Delete rule "${name}"?`))) return;
try {
await r53r.send(new DeleteResolverRuleCommand({ ResolverRuleId: id }));
toast.success(`Rule "${name}" deleted`);
await loadData();
} catch (e) {
toast.error('Delete failed: ' + String(e));
}
}

// ── firewall groups ────────────────────────────────────────────────────────────
async function createFwGroup() {
if (!fwgName.trim()) return toast.error('Name is required');
fwgCreating = true;
try {
await r53r.send(new CreateFirewallRuleGroupCommand({ CreatorRequestId: crypto.randomUUID(), Name: fwgName.trim() }));
toast.success(`Firewall rule group "${fwgName}" created`);
showCreateFwGroup = false;
fwgName = '';
await loadData();
} catch (e) {
toast.error('Failed to create firewall rule group: ' + String(e));
} finally {
fwgCreating = false;
}
}

async function deleteFwGroup(id: string, name: string) {
if (!(await confirmDestructive(`Delete firewall rule group "${name}"? This also deletes its rules.`))) return;
try {
await r53r.send(new DeleteFirewallRuleGroupCommand({ FirewallRuleGroupId: id }));
toast.success(`Firewall rule group "${name}" deleted`);
await loadData();
} catch (e) {
toast.error('Delete failed: ' + String(e));
}
}

async function toggleFwGroup(id: string) {
if (expandedFwGroup === id) {
expandedFwGroup = null;
return;
}
expandedFwGroup = id;
if (!expandedFwGroupRules[id]) {
try {
const res = await r53r.send(new ListFirewallRulesCommand({ FirewallRuleGroupId: id }));
expandedFwGroupRules = { ...expandedFwGroupRules, [id]: res.FirewallRules ?? [] };
} catch {
expandedFwGroupRules = { ...expandedFwGroupRules, [id]: [] };
}
}
}

let reorderingRule = $state('');

// Reorder a firewall rule within its group by swapping its priority with the
// adjacent rule (rules are displayed sorted by priority).
async function moveFwRule(groupId: string, index: number, dir: -1 | 1) {
const list = [...(expandedFwGroupRules[groupId] ?? [])].toSorted(
(a, b) => (a.Priority ?? 0) - (b.Priority ?? 0)
);
const target = index + dir;
if (target < 0 || target >= list.length) return;
const a = list[index];
const b = list[target];
if (!a?.FirewallDomainListId || !b?.FirewallDomainListId) return;
const pa = a.Priority ?? 0;
const pb = b.Priority ?? 0;
reorderingRule = a.FirewallDomainListId ?? `${groupId}:${index}`;
try {
await r53r.send(
new UpdateFirewallRuleCommand({
FirewallRuleGroupId: groupId,
FirewallDomainListId: a.FirewallDomainListId,
Priority: pb
})
);
await r53r.send(
new UpdateFirewallRuleCommand({
FirewallRuleGroupId: groupId,
FirewallDomainListId: b.FirewallDomainListId,
Priority: pa
})
);
toast.success('Rule priority updated');
const res = await r53r.send(new ListFirewallRulesCommand({ FirewallRuleGroupId: groupId }));
expandedFwGroupRules = { ...expandedFwGroupRules, [groupId]: res.FirewallRules ?? [] };
} catch (e) {
toast.error('Failed to reorder rule: ' + String(e));
} finally {
reorderingRule = '';
}
}

// ── domain lists ───────────────────────────────────────────────────────────────
async function createDomainList() {
if (!dlName.trim()) return toast.error('Name is required');
dlCreating = true;
try {
await r53r.send(new CreateFirewallDomainListCommand({ CreatorRequestId: crypto.randomUUID(), Name: dlName.trim() }));
toast.success(`Domain list "${dlName}" created`);
showCreateDomainList = false;
dlName = '';
await loadData();
} catch (e) {
toast.error('Failed to create domain list: ' + String(e));
} finally {
dlCreating = false;
}
}

async function deleteDomainList(id: string, name: string) {
if (!(await confirmDestructive(`Delete firewall domain list "${name}"?`))) return;
try {
await r53r.send(new DeleteFirewallDomainListCommand({ FirewallDomainListId: id }));
toast.success(`Domain list "${name}" deleted`);
await loadData();
} catch (e) {
toast.error('Delete failed: ' + String(e));
}
}

async function toggleDomainList(id: string) {
if (expandedDomainList === id) {
expandedDomainList = null;
return;
}
expandedDomainList = id;
if (!expandedDomainListDomains[id]) {
try {
const res = await r53r.send(new ListFirewallDomainsCommand({ FirewallDomainListId: id }));
expandedDomainListDomains = { ...expandedDomainListDomains, [id]: res.Domains ?? [] };
} catch {
expandedDomainListDomains = { ...expandedDomainListDomains, [id]: [] };
}
}
}

// ── query log configs ──────────────────────────────────────────────────────────
async function createQueryLog() {
if (!qlName.trim() || !qlDestArn.trim()) return toast.error('Name and Destination ARN are required');
qlCreating = true;
try {
await r53r.send(new CreateResolverQueryLogConfigCommand({
CreatorRequestId: crypto.randomUUID(),
Name: qlName.trim(),
DestinationArn: qlDestArn.trim()
}));
toast.success(`Query log config "${qlName}" created`);
showCreateQueryLog = false;
qlName = '';
qlDestArn = '';
await loadData();
} catch (e) {
toast.error('Failed to create query log config: ' + String(e));
} finally {
qlCreating = false;
}
}

async function deleteQueryLog(id: string, name: string) {
if (!(await confirmDestructive(`Delete query log config "${name}"?`))) return;
try {
await r53r.send(new DeleteResolverQueryLogConfigCommand({ ResolverQueryLogConfigId: id }));
toast.success(`Query log config "${name}" deleted`);
await loadData();
} catch (e) {
toast.error('Delete failed: ' + String(e));
}
}

// Status badge helper
function statusBadge(status: string | undefined) {
if (status === 'OPERATIONAL' || status === 'COMPLETE' || status === 'CREATED' || status === 'ACTIVE') {
return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
}
if (status === 'DELETING' || status === 'FAILED') {
return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
}
return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
}
</script>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!--  Page layout                                                               -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<div class="p-6 space-y-6">

<!-- Header -->
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<Globe class="w-7 h-7 text-blue-500" />
<div>
<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Route 53 Resolver</h1>
<p class="text-sm text-gray-500 dark:text-gray-400">DNS resolution for hybrid cloud environments</p>
</div>
</div>
<button onclick={loadData} title="Refresh"
class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
<RefreshCw class="w-4 h-4" /> Refresh
</button>
</div>

<!-- Stats cards -->
<div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-3">
{#each [
{ icon: ArrowRightLeft, color: 'blue', count: endpoints.length, label: 'Endpoints' },
{ icon: Globe, color: 'green', count: inboundCount, label: 'Inbound' },
{ icon: Globe, color: 'orange', count: outboundCount, label: 'Outbound' },
{ icon: BookOpen, color: 'purple', count: rules.length, label: 'Rules' },
{ icon: Shield, color: 'red', count: fwGroups.length, label: 'FW Groups' },
{ icon: ListChecks, color: 'yellow', count: fwDomainLists.length, label: 'Domain Lists' },
{ icon: FileText, color: 'teal', count: queryLogConfigs.length, label: 'Query Logs' }
] as card}
{@const CardIcon = card.icon}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3 flex items-center gap-2">
<div class="p-1.5 bg-{card.color}-100 dark:bg-{card.color}-900/30 rounded-lg">
<CardIcon class="w-4 h-4 text-{card.color}-600 dark:text-{card.color}-400" />
</div>
<div>
<p class="text-xl font-bold text-gray-900 dark:text-white">{card.count}</p>
<p class="text-xs text-gray-500 dark:text-gray-400">{card.label}</p>
</div>
</div>
{/each}
</div>

<!-- Main card with tabs -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">

<!-- Tab bar + search -->
<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center">
<div class="flex flex-wrap gap-2">
{#snippet tabBtn(t: typeof activeTab, label: string)}
<button onclick={() => { activeTab = t; searchQuery = ''; }}
class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-sm font-medium
{activeTab === t
? 'bg-blue-600 text-white'
: 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
{label}
</button>
{/snippet}
{@render tabBtn('endpoints', 'Endpoints')}
{@render tabBtn('rules', 'Rules')}
{@render tabBtn('firewall', 'DNS Firewall')}
{@render tabBtn('querylog', 'Query Logs')}
{@render tabBtn('docs', 'Docs')}
</div>
<div class="flex items-center gap-2 w-full sm:w-auto">
<div class="relative flex-1 sm:flex-none">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
<input bind:value={searchQuery} placeholder="Search…"
class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-56" />
</div>
{#if activeTab === 'endpoints'}
<button onclick={() => (showCreateEndpoint = true)}
class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">
<Plus class="w-4 h-4" /> New
</button>
{:else if activeTab === 'rules'}
<button onclick={() => (showCreateRule = true)}
class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">
<Plus class="w-4 h-4" /> New
</button>
{:else if activeTab === 'firewall'}
{#if firewallSubTab === 'groups'}
<button onclick={() => (showCreateFwGroup = true)}
class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">
<Plus class="w-4 h-4" /> New Group
</button>
{:else}
<button onclick={() => (showCreateDomainList = true)}
class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">
<Plus class="w-4 h-4" /> New List
</button>
{/if}
{:else if activeTab === 'querylog'}
<button onclick={() => (showCreateQueryLog = true)}
class="flex items-center gap-1 px-3 py-2 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700">
<Plus class="w-4 h-4" /> New
</button>
{/if}
</div>
</div>

<div class="p-4">
{#if loading}
<div class="text-center py-10 text-gray-500 dark:text-gray-400">Loading…</div>

<!-- ── ENDPOINTS tab ───────────────────────────────────────────────── -->
{:else if activeTab === 'endpoints'}
{#if filteredEndpoints.length === 0}
<div class="text-center py-10 text-gray-500 dark:text-gray-400">
No resolver endpoints found.
<button onclick={() => (showCreateEndpoint = true)} class="ml-2 text-blue-500 hover:underline">Create one</button>
</div>
{:else}
<div class="overflow-x-auto">
<table class="w-full text-sm">
<thead>
<tr class="text-left text-gray-500 dark:text-gray-400 border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 pr-4 font-medium">Name</th>
<th class="pb-2 pr-4 font-medium">Direction</th>
<th class="pb-2 pr-4 font-medium">Type</th>
<th class="pb-2 pr-4 font-medium">IP Count</th>
<th class="pb-2 pr-4 font-medium">VPC</th>
<th class="pb-2 pr-4 font-medium">Status</th>
<th class="pb-2 font-medium">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredEndpoints as ep}
<tr class="hover:bg-gray-50 dark:hover:bg-slate-700/40">
<td class="py-2 pr-4">
<p class="font-medium text-gray-900 dark:text-white">{ep.Name ?? ep.Id}</p>
<p class="text-xs text-gray-400 font-mono">{ep.Id}</p>
</td>
<td class="py-2 pr-4">
<span class="px-2 py-0.5 rounded text-xs font-medium
{ep.Direction === 'INBOUND' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400'}">
{ep.Direction}
</span>
</td>
<td class="py-2 pr-4 text-gray-600 dark:text-gray-400">{ep.ResolverEndpointType ?? 'IPV4'}</td>
<td class="py-2 pr-4 text-gray-600 dark:text-gray-400">{ep.IpAddressCount ?? 0}</td>
<td class="py-2 pr-4 font-mono text-xs text-gray-500 dark:text-gray-400">{ep.HostVPCId ?? '—'}</td>
<td class="py-2 pr-4">
<span class="text-xs px-2 py-1 rounded-full {statusBadge(ep.Status)}">{ep.Status}</span>
</td>
<td class="py-2">
<div class="flex items-center gap-1">
<button onclick={() => copyToClipboard(ep.Id ?? '')} title="Copy ID"
class="p-1 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 rounded">
<Copy class="w-3.5 h-3.5" />
</button>
<button onclick={() => deleteEndpoint(ep.Id ?? '', ep.Name ?? ep.Id ?? '')} title="Delete"
class="p-1 text-red-400 hover:text-red-600 rounded">
<Trash2 class="w-3.5 h-3.5" />
</button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- ── RULES tab ───────────────────────────────────────────────────── -->
{:else if activeTab === 'rules'}
{#if filteredRules.length === 0}
<div class="text-center py-10 text-gray-500 dark:text-gray-400">
No resolver rules found.
<button onclick={() => (showCreateRule = true)} class="ml-2 text-blue-500 hover:underline">Create one</button>
</div>
{:else}
<div class="overflow-x-auto">
<table class="w-full text-sm">
<thead>
<tr class="text-left text-gray-500 dark:text-gray-400 border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 pr-4 font-medium">Name</th>
<th class="pb-2 pr-4 font-medium">Domain</th>
<th class="pb-2 pr-4 font-medium">Type</th>
<th class="pb-2 pr-4 font-medium">Share Status</th>
<th class="pb-2 pr-4 font-medium">Status</th>
<th class="pb-2 font-medium">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredRules as rule}
<tr class="hover:bg-gray-50 dark:hover:bg-slate-700/40">
<td class="py-2 pr-4">
<p class="font-medium text-gray-900 dark:text-white">{rule.Name ?? rule.Id}</p>
<p class="text-xs text-gray-400 font-mono">{rule.Id}</p>
</td>
<td class="py-2 pr-4 font-mono text-xs text-gray-600 dark:text-gray-400">{rule.DomainName}</td>
<td class="py-2 pr-4">
<span class="px-2 py-0.5 rounded text-xs font-medium bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-400">
{rule.RuleType}
</span>
</td>
<td class="py-2 pr-4 text-xs text-gray-500 dark:text-gray-400">{rule.ShareStatus ?? '—'}</td>
<td class="py-2 pr-4">
<span class="text-xs px-2 py-1 rounded-full {statusBadge(rule.Status)}">{rule.Status}</span>
</td>
<td class="py-2">
<div class="flex items-center gap-1">
<button onclick={() => copyToClipboard(rule.Id ?? '')} title="Copy ID"
class="p-1 text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 rounded">
<Copy class="w-3.5 h-3.5" />
</button>
<button onclick={() => deleteRule(rule.Id ?? '', rule.Name ?? rule.Id ?? '')} title="Delete"
class="p-1 text-red-400 hover:text-red-600 rounded">
<Trash2 class="w-3.5 h-3.5" />
</button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- ── DNS FIREWALL tab ────────────────────────────────────────────── -->
{:else if activeTab === 'firewall'}
<!-- Sub-tabs -->
<div class="flex gap-2 mb-4">
{#each [['groups', 'Rule Groups'], ['domains', 'Domain Lists']] as [sub, label]}
<button onclick={() => { firewallSubTab = sub as typeof firewallSubTab; searchQuery = ''; }}
class="px-3 py-1.5 rounded-lg text-sm font-medium
{firewallSubTab === sub
? 'bg-blue-600 text-white'
: 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
{label}
</button>
{/each}
</div>

{#if firewallSubTab === 'groups'}
{#if filteredFwGroups.length === 0}
<div class="text-center py-10 text-gray-500 dark:text-gray-400">No firewall rule groups. <button onclick={() => (showCreateFwGroup = true)} class="text-blue-500 hover:underline">Create one</button></div>
{:else}
<div class="space-y-2">
{#each filteredFwGroups as grp}
<div class="border border-slate-200 dark:border-slate-700 rounded-lg">
<div class="flex items-center justify-between p-3">
<button onclick={() => toggleFwGroup(grp.Id ?? '')} class="flex items-center gap-2 text-left flex-1 min-w-0">
{#if expandedFwGroup === grp.Id}
<ChevronDown class="w-4 h-4 text-gray-400 shrink-0" />
{:else}
<ChevronRight class="w-4 h-4 text-gray-400 shrink-0" />
{/if}
<Shield class="w-4 h-4 text-blue-500 shrink-0" />
<div class="min-w-0">
<p class="font-medium text-gray-900 dark:text-white truncate">{grp.Name}</p>
<p class="text-xs text-gray-400 font-mono">{grp.Id} · {grp.RuleCount ?? 0} rules</p>
</div>
</button>
<div class="flex items-center gap-2">
<span class="text-xs px-2 py-1 rounded-full {statusBadge(grp.Status)}">{grp.Status}</span>
<button onclick={() => copyToClipboard(grp.Id ?? '')} class="p-1 text-gray-400 hover:text-gray-600 rounded">
<Copy class="w-3.5 h-3.5" />
</button>
<button onclick={() => deleteFwGroup(grp.Id ?? '', grp.Name ?? '')} class="p-1 text-red-400 hover:text-red-600 rounded">
<Trash2 class="w-3.5 h-3.5" />
</button>
</div>
</div>
{#if expandedFwGroup === grp.Id}
<div class="border-t border-slate-200 dark:border-slate-700 p-3 bg-gray-50 dark:bg-slate-700/30">
{#if !expandedFwGroupRules[grp.Id ?? '']}
<p class="text-sm text-gray-500">Loading rules…</p>
{:else if expandedFwGroupRules[grp.Id ?? ''].length === 0}
<p class="text-sm text-gray-500">No rules in this group.</p>
{:else}
<table class="w-full text-xs">
<thead><tr class="text-gray-500 dark:text-gray-400"><th class="pb-1 pr-3 text-left">Name</th><th class="pb-1 pr-3 text-left">Action</th><th class="pb-1 pr-3 text-left">Priority</th><th class="pb-1 pr-3 text-left">Domain List ID</th><th class="pb-1 text-left">Order</th></tr></thead>
<tbody>
{#each [...expandedFwGroupRules[grp.Id ?? '']].sort((a, b) => (a.Priority ?? 0) - (b.Priority ?? 0)) as fwRule, ri (fwRule.FirewallDomainListId ?? ri)}
<tr class="border-t border-slate-100 dark:border-slate-700">
<td class="py-1 pr-3">{fwRule.Name}</td>
<td class="py-1 pr-3">
<span class="px-1.5 py-0.5 rounded text-xs
{fwRule.Action === 'ALLOW' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : fwRule.Action === 'BLOCK' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400'}">
{fwRule.Action}
</span>
</td>
<td class="py-1 pr-3 font-mono">{fwRule.Priority}</td>
<td class="py-1 pr-3 font-mono text-gray-400 truncate max-w-[180px]">{fwRule.FirewallDomainListId ?? '—'}</td>
<td class="py-1">
<div class="flex items-center gap-1">
<button
disabled={ri === 0 || reorderingRule !== ''}
onclick={() => moveFwRule(grp.Id ?? '', ri, -1)}
class="p-0.5 text-gray-400 hover:text-blue-600 disabled:opacity-30 disabled:hover:text-gray-400"
title="Move up (higher priority)"
><ChevronUp class="w-3.5 h-3.5" /></button>
<button
disabled={ri === expandedFwGroupRules[grp.Id ?? ''].length - 1 || reorderingRule !== ''}
onclick={() => moveFwRule(grp.Id ?? '', ri, 1)}
class="p-0.5 text-gray-400 hover:text-blue-600 disabled:opacity-30 disabled:hover:text-gray-400"
title="Move down (lower priority)"
><ChevronDown class="w-3.5 h-3.5" /></button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{/if}
</div>
{/each}
</div>
{/if}

{:else}
<!-- Domain Lists -->
{#if filteredDomainLists.length === 0}
<div class="text-center py-10 text-gray-500 dark:text-gray-400">No domain lists. <button onclick={() => (showCreateDomainList = true)} class="text-blue-500 hover:underline">Create one</button></div>
{:else}
<div class="space-y-2">
{#each filteredDomainLists as dl}
<div class="border border-slate-200 dark:border-slate-700 rounded-lg">
<div class="flex items-center justify-between p-3">
<button onclick={() => toggleDomainList(dl.Id ?? '')} class="flex items-center gap-2 text-left flex-1 min-w-0">
{#if expandedDomainList === dl.Id}
<ChevronDown class="w-4 h-4 text-gray-400 shrink-0" />
{:else}
<ChevronRight class="w-4 h-4 text-gray-400 shrink-0" />
{/if}
<ListChecks class="w-4 h-4 text-yellow-500 shrink-0" />
<div class="min-w-0">
<p class="font-medium text-gray-900 dark:text-white truncate">{dl.Name}</p>
<p class="text-xs text-gray-400 font-mono">{dl.Id} · {dl.DomainCount ?? 0} domains</p>
</div>
</button>
<div class="flex items-center gap-2">
<span class="text-xs px-2 py-1 rounded-full {statusBadge(dl.Status)}">{dl.Status}</span>
<button onclick={() => copyToClipboard(dl.Id ?? '')} class="p-1 text-gray-400 hover:text-gray-600 rounded">
<Copy class="w-3.5 h-3.5" />
</button>
<button onclick={() => deleteDomainList(dl.Id ?? '', dl.Name ?? '')} class="p-1 text-red-400 hover:text-red-600 rounded">
<Trash2 class="w-3.5 h-3.5" />
</button>
</div>
</div>
{#if expandedDomainList === dl.Id}
<div class="border-t border-slate-200 dark:border-slate-700 p-3 bg-gray-50 dark:bg-slate-700/30">
{#if !expandedDomainListDomains[dl.Id ?? '']}
<p class="text-sm text-gray-500">Loading domains…</p>
{:else if expandedDomainListDomains[dl.Id ?? ''].length === 0}
<p class="text-sm text-gray-500">No domains in this list.</p>
{:else}
<div class="flex flex-wrap gap-1">
{#each expandedDomainListDomains[dl.Id ?? ''] as domain}
<span class="px-2 py-0.5 rounded bg-white dark:bg-slate-600 border border-slate-200 dark:border-slate-500 text-xs font-mono">{domain}</span>
{/each}
</div>
{/if}
</div>
{/if}
</div>
{/each}
</div>
{/if}
{/if}

<!-- ── QUERY LOGS tab ──────────────────────────────────────────────── -->
{:else if activeTab === 'querylog'}
{#if filteredQueryLogs.length === 0}
<div class="text-center py-10 text-gray-500 dark:text-gray-400">
No query log configurations. <button onclick={() => (showCreateQueryLog = true)} class="text-blue-500 hover:underline">Create one</button>
</div>
{:else}
<div class="overflow-x-auto">
<table class="w-full text-sm">
<thead>
<tr class="text-left text-gray-500 dark:text-gray-400 border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 pr-4 font-medium">Name</th>
<th class="pb-2 pr-4 font-medium">Destination ARN</th>
<th class="pb-2 pr-4 font-medium">Status</th>
<th class="pb-2 font-medium">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredQueryLogs as cfg}
<tr class="hover:bg-gray-50 dark:hover:bg-slate-700/40">
<td class="py-2 pr-4">
<p class="font-medium text-gray-900 dark:text-white">{cfg.Name}</p>
<p class="text-xs text-gray-400 font-mono">{cfg.Id}</p>
</td>
<td class="py-2 pr-4 font-mono text-xs text-gray-500 dark:text-gray-400 max-w-[280px] truncate">{cfg.DestinationArn ?? '—'}</td>
<td class="py-2 pr-4">
<span class="text-xs px-2 py-1 rounded-full {statusBadge(cfg.Status)}">{cfg.Status}</span>
</td>
<td class="py-2">
<div class="flex items-center gap-1">
<button onclick={() => copyToClipboard(cfg.Id ?? '')} class="p-1 text-gray-400 hover:text-gray-600 rounded">
<Copy class="w-3.5 h-3.5" />
</button>
<button onclick={() => deleteQueryLog(cfg.Id ?? '', cfg.Name ?? '')} class="p-1 text-red-400 hover:text-red-600 rounded">
<Trash2 class="w-3.5 h-3.5" />
</button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- ── DOCS tab ────────────────────────────────────────────────────── -->
{:else if activeTab === 'docs'}
<div class="prose dark:prose-invert max-w-none text-sm">
<h3 class="text-base font-semibold text-gray-900 dark:text-white mb-2">Route 53 Resolver Operations</h3>
<p class="text-gray-600 dark:text-gray-400 mb-4">All AWS Route 53 Resolver SDK operations are emulated. Key resources:</p>
<div class="grid sm:grid-cols-2 gap-4">
{#each [
{ title: 'Resolver Endpoints', desc: 'Inbound (DNS queries from your network) and Outbound (DNS queries to your network) endpoints. Each requires a VPC, security groups, and at least one IP address in a subnet.' },
{ title: 'Resolver Rules', desc: 'Forward DNS queries for specified domains to IP addresses in your VPC or network. Supports FORWARD, SYSTEM, and RECURSIVE rule types. TargetIps are required for FORWARD rules.' },
{ title: 'DNS Firewall Rule Groups', desc: 'Named sets of firewall rules that filter DNS queries. Associate with VPCs at specific priorities. Rules specify ALLOW, BLOCK, or ALERT actions against domain lists.' },
{ title: 'Firewall Domain Lists', desc: 'Lists of domain names used in firewall rules. Domains can be added via UpdateFirewallDomains or ImportFirewallDomains (from S3 URL).' },
{ title: 'Resolver Query Log Configs', desc: 'Configure DNS query logging to CloudWatch Logs, S3, or Kinesis Data Firehose. Associate configs with VPCs to capture all resolver queries.' },
{ title: 'DNSSEC Config', desc: 'Enable DNSSEC validation on a per-VPC basis. Use GetResolverDnssecConfig and UpdateResolverDnssecConfig to manage validation status.' },
{ title: 'Resolver Config', desc: 'Manage autodefined reverse DNS records for VPCs. Use GetResolverConfig and UpdateResolverConfig to control AutodefinedReverse behavior.' },
{ title: 'Firewall Config', desc: 'Control DNS Firewall fail-open behavior per VPC. ENABLED = allow queries if firewall is unavailable; DISABLED = block queries.' }
] as item}
<div class="bg-gray-50 dark:bg-slate-700/50 rounded-lg p-3">
<h4 class="font-semibold text-gray-900 dark:text-white text-sm">{item.title}</h4>
<p class="text-gray-600 dark:text-gray-400 text-xs mt-1">{item.desc}</p>
</div>
{/each}
</div>
</div>
{/if}
</div>
</div>
</div>

<!-- ═══════════════════════════════════════════════════════════════════════════ -->
<!--  Modals                                                                    -->
<!-- ═══════════════════════════════════════════════════════════════════════════ -->

<!-- Create Endpoint modal -->
{#if showCreateEndpoint}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md mx-4 p-6">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Resolver Endpoint</h2>
<button onclick={() => (showCreateEndpoint = false)} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name *</label>
<input bind:value={ceName} placeholder="my-inbound-endpoint"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Direction *</label>
<select bind:value={ceDirection} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
<option value="INBOUND">INBOUND</option>
<option value="OUTBOUND">OUTBOUND</option>
</select>
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">VPC ID</label>
<input bind:value={ceVpcId} placeholder="vpc-12345678 (default: vpc-demo)"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Security Group IDs (comma-separated)</label>
<input bind:value={ceSgIds} placeholder="sg-12345678 (default: sg-demo)"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div class="grid grid-cols-2 gap-2">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Subnet ID</label>
<input bind:value={ceSubnetId} placeholder="subnet-12345678"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">IP Address</label>
<input bind:value={ceIp} placeholder="10.0.0.5"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
</div>
</div>
<div class="flex justify-end gap-2 mt-5">
<button onclick={() => (showCreateEndpoint = false)} class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:underline">Cancel</button>
<button onclick={createEndpoint} disabled={ceCreating}
class="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50">
{ceCreating ? 'Creating…' : 'Create Endpoint'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Rule modal -->
{#if showCreateRule}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md mx-4 p-6">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Resolver Rule</h2>
<button onclick={() => (showCreateRule = false)} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name *</label>
<input bind:value={crName} placeholder="my-forward-rule"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Domain Name *</label>
<input bind:value={crDomainName} placeholder="example.internal."
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Rule Type *</label>
<select bind:value={crRuleType} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
<option value="FORWARD">FORWARD</option>
<option value="SYSTEM">SYSTEM</option>
<option value="RECURSIVE">RECURSIVE</option>
</select>
</div>
{#if crRuleType === 'FORWARD'}
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Resolver Endpoint ID</label>
<input bind:value={crEndpointId} placeholder="rslvr-out-12345678"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Target IP (port 53)</label>
<input bind:value={crTargetIp} placeholder="10.0.0.2"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
{/if}
</div>
<div class="flex justify-end gap-2 mt-5">
<button onclick={() => (showCreateRule = false)} class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:underline">Cancel</button>
<button onclick={createRule} disabled={crCreating}
class="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50">
{crCreating ? 'Creating…' : 'Create Rule'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Firewall Rule Group modal -->
{#if showCreateFwGroup}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-sm mx-4 p-6">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Firewall Rule Group</h2>
<button onclick={() => (showCreateFwGroup = false)} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name *</label>
<input bind:value={fwgName} placeholder="my-fw-rule-group"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div class="flex justify-end gap-2 mt-5">
<button onclick={() => (showCreateFwGroup = false)} class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:underline">Cancel</button>
<button onclick={createFwGroup} disabled={fwgCreating}
class="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50">
{fwgCreating ? 'Creating…' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Domain List modal -->
{#if showCreateDomainList}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-sm mx-4 p-6">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Firewall Domain List</h2>
<button onclick={() => (showCreateDomainList = false)} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name *</label>
<input bind:value={dlName} placeholder="my-domain-blocklist"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div class="flex justify-end gap-2 mt-5">
<button onclick={() => (showCreateDomainList = false)} class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:underline">Cancel</button>
<button onclick={createDomainList} disabled={dlCreating}
class="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50">
{dlCreating ? 'Creating…' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Query Log Config modal -->
{#if showCreateQueryLog}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md mx-4 p-6">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Query Log Configuration</h2>
<button onclick={() => (showCreateQueryLog = false)} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name *</label>
<input bind:value={qlName} placeholder="my-query-log-config"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Destination ARN * (CloudWatch Logs, S3, or Firehose)</label>
<input bind:value={qlDestArn} placeholder="arn:aws:logs:us-east-1:000000000000:log-group:/resolver/queries"
class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</div>
</div>
<div class="flex justify-end gap-2 mt-5">
<button onclick={() => (showCreateQueryLog = false)} class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:underline">Cancel</button>
<button onclick={createQueryLog} disabled={qlCreating}
class="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50">
{qlCreating ? 'Creating…' : 'Create'}
</button>
</div>
</div>
</div>
{/if}
