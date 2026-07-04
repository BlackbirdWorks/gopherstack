<script lang="ts">
import { onMount } from 'svelte';
import { confirmDestructive } from '$lib/confirm-dialog';
import { getDocDBClient } from '$lib/aws-client';
import {
DescribeDBClustersCommand,
DeleteDBClusterCommand,
CreateDBClusterCommand,
StopDBClusterCommand,
StartDBClusterCommand,
FailoverDBClusterCommand,
DescribeDBInstancesCommand,
CreateDBInstanceCommand,
DeleteDBInstanceCommand,
RebootDBInstanceCommand,
DescribeDBClusterSnapshotsCommand,
CreateDBClusterSnapshotCommand,
DeleteDBClusterSnapshotCommand,
DescribeDBClusterParameterGroupsCommand,
CreateDBClusterParameterGroupCommand,
DeleteDBClusterParameterGroupCommand,
DescribeDBClusterParametersCommand,
ModifyDBClusterParameterGroupCommand,
DescribeGlobalClustersCommand,
CreateGlobalClusterCommand,
DeleteGlobalClusterCommand,
DescribeEventSubscriptionsCommand,
CreateEventSubscriptionCommand,
DeleteEventSubscriptionCommand,
type DBCluster,
type DBInstance,
type DBClusterSnapshot,
type DBClusterParameterGroup,
type Parameter,
type GlobalCluster,
type EventSubscription
} from '@aws-sdk/client-docdb';
import { toast } from 'svelte-sonner';
import {
Database,
Server,
Camera,
Layers,
Bell,
Globe,
Plus,
Trash2,
RefreshCw,
Search,
ChevronRight,
Play,
Square,
Zap,
Settings,
X,
Copy,
} from 'lucide-svelte';

type TabName = 'clusters' | 'instances' | 'snapshots' | 'parametergroups' | 'globalclusters' | 'eventsubscriptions';

let cachedClient: ReturnType<typeof getDocDBClient> | undefined;
function client() {
	return (cachedClient ??= getDocDBClient());
}

let loading = $state(false);
let activeTab = $state<TabName>('clusters');
let searchQuery = $state('');
let selectedCluster = $state<DBCluster | null>(null);
let snapshotTypeFilter = $state('all');

// Data
let clusters = $state<DBCluster[]>([]);
let instances = $state<DBInstance[]>([]);
let snapshots = $state<DBClusterSnapshot[]>([]);
let paramGroups = $state<DBClusterParameterGroup[]>([]);
let globalClusters = $state<GlobalCluster[]>([]);
let eventSubs = $state<EventSubscription[]>([]);

// Modals
let showCreateCluster = $state(false);
let showCreateInstance = $state(false);
let showCreateSnapshot = $state(false);
let showCreateParamGroup = $state(false);
let showCreateGlobal = $state(false);
let showCreateEvent = $state(false);

// Forms
let clusterForm = $state({ DBClusterIdentifier: '', MasterUsername: '', MasterUserPassword: '', Engine: 'docdb', EngineVersion: '4.0.0', BackupRetentionPeriod: 1, StorageEncrypted: false, DeletionProtection: false });
let instanceForm = $state({ DBInstanceIdentifier: '', DBClusterIdentifier: '', DBInstanceClass: 'db.t3.medium', Engine: 'docdb' });
let snapshotForm = $state({ DBClusterSnapshotIdentifier: '', DBClusterIdentifier: '' });
let paramGroupForm = $state({ DBClusterParameterGroupName: '', DBParameterGroupFamily: 'docdb4.0', Description: '' });
let globalForm = $state({ GlobalClusterIdentifier: '', Engine: 'docdb', EngineVersion: '4.0.0' });
let eventForm = $state({ SubscriptionName: '', SnsTopicArn: '', SourceType: 'db-cluster' });

// Stats
const totalClusters = $derived(clusters.length);
const totalInstances = $derived(instances.length);
const availableClusters = $derived(clusters.filter(c => c.Status === 'available').length);
const stoppedClusters = $derived(clusters.filter(c => c.Status === 'stopped').length);

// Filtered
const q = $derived(searchQuery.toLowerCase());
const filteredClusters = $derived(clusters.filter(c => !q || (c.DBClusterIdentifier ?? '').toLowerCase().includes(q)));
const filteredInstances = $derived(instances.filter(i => !q || (i.DBInstanceIdentifier ?? '').toLowerCase().includes(q)));
const filteredSnapshots = $derived(snapshots.filter(s => {
  const matchQ = !q || (s.DBClusterSnapshotIdentifier ?? '').toLowerCase().includes(q);
  const matchType = snapshotTypeFilter === 'all' || s.SnapshotType === snapshotTypeFilter;
  return matchQ && matchType;
}));
const filteredParamGroups = $derived(paramGroups.filter(p => !q || (p.DBClusterParameterGroupName ?? '').toLowerCase().includes(q)));
const filteredGlobal = $derived(globalClusters.filter(g => !q || (g.GlobalClusterIdentifier ?? '').toLowerCase().includes(q)));
const filteredEvents = $derived(eventSubs.filter(e => !q || (e.CustSubscriptionId ?? '').toLowerCase().includes(q)));

const tabCounts = $derived<Record<TabName, number>>({
  clusters: clusters.length,
  instances: instances.length,
  snapshots: snapshots.length,
  parametergroups: paramGroups.length,
  globalclusters: globalClusters.length,
  eventsubscriptions: eventSubs.length
});

const clusterInstances = $derived(selectedCluster ? instances.filter(i => i.DBClusterIdentifier === selectedCluster!.DBClusterIdentifier) : []);

function statusClass(status: string | undefined) {
switch (status) {
case 'available': return 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400';
case 'stopped': return 'bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-400';
case 'creating': case 'modifying': return 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400';
case 'deleting': case 'failed': case 'failing-over': return 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400';
default: return 'bg-gray-100 text-gray-600 dark:bg-gray-800 dark:text-gray-400';
}
}

async function copyToClipboard(text: string, label: string) {
  try {
    await navigator.clipboard.writeText(text);
    toast.success(`${label} copied to clipboard`);
  } catch {
    toast.error('Failed to copy to clipboard');
  }
}

async function loadAll() {
loading = true;
try {
const [cr, ir, sr, pr, gr, er] = await Promise.all([
client().send(new DescribeDBClustersCommand({})),
client().send(new DescribeDBInstancesCommand({})),
client().send(new DescribeDBClusterSnapshotsCommand({})),
client().send(new DescribeDBClusterParameterGroupsCommand({})),
client().send(new DescribeGlobalClustersCommand({})),
client().send(new DescribeEventSubscriptionsCommand({}))
]);
clusters = cr.DBClusters ?? [];
instances = ir.DBInstances ?? [];
snapshots = sr.DBClusterSnapshots ?? [];
paramGroups = pr.DBClusterParameterGroups ?? [];
globalClusters = gr.GlobalClusters ?? [];
eventSubs = er.EventSubscriptionsList ?? [];
} catch (e) {
toast.error('Failed to load data: ' + String(e));
} finally {
loading = false;
}
}

async function seedDemo() {
if (clusters.length > 0 || instances.length > 0 || snapshots.length > 0 || paramGroups.length > 0) return;
try {
await Promise.allSettled([
client().send(new CreateDBClusterCommand({ DBClusterIdentifier: 'demo-cluster-1', Engine: 'docdb', MasterUsername: 'admin', MasterUserPassword: 'DemoPass123!' })),
client().send(new CreateDBClusterCommand({ DBClusterIdentifier: 'demo-cluster-2', Engine: 'docdb', MasterUsername: 'admin', MasterUserPassword: 'DemoPass123!', StorageEncrypted: true })),
]);
await Promise.allSettled([
client().send(new CreateDBInstanceCommand({ DBInstanceIdentifier: 'demo-instance-1', DBClusterIdentifier: 'demo-cluster-1', DBInstanceClass: 'db.t3.medium', Engine: 'docdb' })),
client().send(new CreateDBInstanceCommand({ DBInstanceIdentifier: 'demo-instance-2', DBClusterIdentifier: 'demo-cluster-2', DBInstanceClass: 'db.r5.large', Engine: 'docdb' })),
client().send(new CreateDBClusterParameterGroupCommand({ DBClusterParameterGroupName: 'demo-param-group', DBParameterGroupFamily: 'docdb4.0', Description: 'Demo parameter group' })),
]);
await loadAll();
} catch {
// demo seed errors are non-fatal
}
}

onMount(async () => {
await loadAll();
await seedDemo();
});

// Cluster actions
async function createCluster() {
loading = true;
try {
await client().send(new CreateDBClusterCommand({
DBClusterIdentifier: clusterForm.DBClusterIdentifier,
MasterUsername: clusterForm.MasterUsername || undefined,
MasterUserPassword: clusterForm.MasterUserPassword || undefined,
Engine: clusterForm.Engine,
BackupRetentionPeriod: clusterForm.BackupRetentionPeriod,
StorageEncrypted: clusterForm.StorageEncrypted,
DeletionProtection: clusterForm.DeletionProtection
}));
toast.success('Cluster creation initiated');
showCreateCluster = false;
clusterForm = { DBClusterIdentifier: '', MasterUsername: '', MasterUserPassword: '', Engine: 'docdb', EngineVersion: '4.0.0', BackupRetentionPeriod: 1, StorageEncrypted: false, DeletionProtection: false };
await loadAll();
} catch (e) { toast.error('Failed to create cluster: ' + String(e)); }
finally { loading = false; }
}

async function deleteCluster(id: string) {
if (!await confirmDestructive({ title: 'Delete Cluster', message: `Delete cluster ${id}? This action cannot be undone.` })) return;
loading = true;
try {
await client().send(new DeleteDBClusterCommand({ DBClusterIdentifier: id, SkipFinalSnapshot: true }));
toast.success(`Cluster ${id} deletion initiated`);
selectedCluster = null;
await loadAll();
} catch (e) { toast.error('Failed to delete cluster: ' + String(e)); }
finally { loading = false; }
}

async function stopCluster(id: string) {
loading = true;
try {
await client().send(new StopDBClusterCommand({ DBClusterIdentifier: id }));
toast.success(`Cluster ${id} stopping`);
await loadAll();
} catch (e) { toast.error('Failed to stop cluster: ' + String(e)); }
finally { loading = false; }
}

async function startCluster(id: string) {
loading = true;
try {
await client().send(new StartDBClusterCommand({ DBClusterIdentifier: id }));
toast.success(`Cluster ${id} starting`);
await loadAll();
} catch (e) { toast.error('Failed to start cluster: ' + String(e)); }
finally { loading = false; }
}

async function failoverCluster(id: string) {
loading = true;
try {
await client().send(new FailoverDBClusterCommand({ DBClusterIdentifier: id }));
toast.success(`Cluster ${id} failover initiated`);
await loadAll();
} catch (e) { toast.error('Failed to failover cluster: ' + String(e)); }
finally { loading = false; }
}

// Instance actions
async function createInstance() {
loading = true;
try {
await client().send(new CreateDBInstanceCommand({
DBInstanceIdentifier: instanceForm.DBInstanceIdentifier,
DBClusterIdentifier: instanceForm.DBClusterIdentifier,
DBInstanceClass: instanceForm.DBInstanceClass,
Engine: instanceForm.Engine
}));
toast.success('Instance creation initiated');
showCreateInstance = false;
instanceForm = { DBInstanceIdentifier: '', DBClusterIdentifier: '', DBInstanceClass: 'db.t3.medium', Engine: 'docdb' };
await loadAll();
} catch (e) { toast.error('Failed to create instance: ' + String(e)); }
finally { loading = false; }
}

async function deleteInstance(id: string) {
if (!await confirmDestructive({ title: 'Delete Instance', message: `Delete instance ${id}?` })) return;
loading = true;
try {
await client().send(new DeleteDBInstanceCommand({ DBInstanceIdentifier: id }));
toast.success(`Instance ${id} deletion initiated`);
await loadAll();
} catch (e) { toast.error('Failed to delete instance: ' + String(e)); }
finally { loading = false; }
}

async function rebootInstance(id: string) {
loading = true;
try {
await client().send(new RebootDBInstanceCommand({ DBInstanceIdentifier: id }));
toast.success(`Instance ${id} rebooting`);
await loadAll();
} catch (e) { toast.error('Failed to reboot instance: ' + String(e)); }
finally { loading = false; }
}

// Snapshot actions
async function createSnapshot() {
loading = true;
try {
await client().send(new CreateDBClusterSnapshotCommand({
DBClusterSnapshotIdentifier: snapshotForm.DBClusterSnapshotIdentifier,
DBClusterIdentifier: snapshotForm.DBClusterIdentifier
}));
toast.success('Snapshot creation initiated');
showCreateSnapshot = false;
snapshotForm = { DBClusterSnapshotIdentifier: '', DBClusterIdentifier: '' };
await loadAll();
} catch (e) { toast.error('Failed to create snapshot: ' + String(e)); }
finally { loading = false; }
}

async function deleteSnapshot(id: string) {
if (!await confirmDestructive({ title: 'Delete Snapshot', message: `Delete snapshot ${id}?` })) return;
loading = true;
try {
await client().send(new DeleteDBClusterSnapshotCommand({ DBClusterSnapshotIdentifier: id }));
toast.success(`Snapshot ${id} deleted`);
await loadAll();
} catch (e) { toast.error('Failed to delete snapshot: ' + String(e)); }
finally { loading = false; }
}

// Parameter Group actions
async function createParamGroup() {
loading = true;
try {
await client().send(new CreateDBClusterParameterGroupCommand({
DBClusterParameterGroupName: paramGroupForm.DBClusterParameterGroupName,
DBParameterGroupFamily: paramGroupForm.DBParameterGroupFamily,
Description: paramGroupForm.Description
}));
toast.success('Parameter group created');
showCreateParamGroup = false;
paramGroupForm = { DBClusterParameterGroupName: '', DBParameterGroupFamily: 'docdb4.0', Description: '' };
await loadAll();
} catch (e) { toast.error('Failed to create parameter group: ' + String(e)); }
finally { loading = false; }
}

async function deleteParamGroup(name: string) {
if (!await confirmDestructive({ title: 'Delete Parameter Group', message: `Delete parameter group ${name}?` })) return;
loading = true;
try {
await client().send(new DeleteDBClusterParameterGroupCommand({ DBClusterParameterGroupName: name }));
toast.success(`Parameter group ${name} deleted`);
await loadAll();
} catch (e) { toast.error('Failed to delete parameter group: ' + String(e)); }
finally { loading = false; }
}

// Parameter editor
let selectedParamGroup = $state<string | null>(null);
let groupParameters = $state<Parameter[]>([]);
let loadingParameters = $state(false);
let savingParameters = $state(false);
let editedValues = $state<Record<string, string>>({});
const modifiableParams = $derived(groupParameters.filter((p) => p.IsModifiable !== false));

async function openParameterEditor(name: string) {
	selectedParamGroup = name;
	loadingParameters = true;
	editedValues = {};
	try {
		const resp = await client().send(new DescribeDBClusterParametersCommand({ DBClusterParameterGroupName: name }));
		groupParameters = resp.Parameters ?? [];
	} catch (e) {
		toast.error('Failed to load parameters: ' + String(e));
	} finally {
		loadingParameters = false;
	}
}

async function saveParameters() {
	if (!selectedParamGroup) return;
	const changed = Object.entries(editedValues).filter(([k, v]) => {
		const orig = groupParameters.find((p) => p.ParameterName === k);
		return orig && (orig.ParameterValue ?? '') !== v;
	});
	if (changed.length === 0) {
		toast.info('No parameter changes to save');
		return;
	}
	savingParameters = true;
	try {
		await client().send(new ModifyDBClusterParameterGroupCommand({
			DBClusterParameterGroupName: selectedParamGroup,
			Parameters: changed.map(([name, value]) => ({ ParameterName: name, ParameterValue: value, ApplyMethod: 'pending-reboot' }))
		}));
		toast.success(`${changed.length} parameter(s) updated`);
		await openParameterEditor(selectedParamGroup);
	} catch (e) {
		toast.error('Failed to save parameters: ' + String(e));
	} finally {
		savingParameters = false;
	}
}

// Global Cluster actions
async function createGlobal() {
loading = true;
try {
await client().send(new CreateGlobalClusterCommand({
GlobalClusterIdentifier: globalForm.GlobalClusterIdentifier,
Engine: globalForm.Engine,
EngineVersion: globalForm.EngineVersion
}));
toast.success('Global cluster created');
showCreateGlobal = false;
globalForm = { GlobalClusterIdentifier: '', Engine: 'docdb', EngineVersion: '4.0.0' };
await loadAll();
} catch (e) { toast.error('Failed to create global cluster: ' + String(e)); }
finally { loading = false; }
}

async function deleteGlobal(id: string) {
if (!await confirmDestructive({ title: 'Delete Global Cluster', message: `Delete global cluster ${id}?` })) return;
loading = true;
try {
await client().send(new DeleteGlobalClusterCommand({ GlobalClusterIdentifier: id }));
toast.success(`Global cluster ${id} deleted`);
await loadAll();
} catch (e) { toast.error('Failed to delete global cluster: ' + String(e)); }
finally { loading = false; }
}

// Event Subscription actions
async function createEventSub() {
loading = true;
try {
await client().send(new CreateEventSubscriptionCommand({
SubscriptionName: eventForm.SubscriptionName,
SnsTopicArn: eventForm.SnsTopicArn,
SourceType: eventForm.SourceType === 'all' ? undefined : eventForm.SourceType,
Enabled: true
}));
toast.success('Event subscription created');
showCreateEvent = false;
eventForm = { SubscriptionName: '', SnsTopicArn: '', SourceType: 'db-cluster' };
await loadAll();
} catch (e) { toast.error('Failed to create event subscription: ' + String(e)); }
finally { loading = false; }
}

async function deleteEventSub(name: string) {
if (!await confirmDestructive({ title: 'Delete Event Subscription', message: `Delete subscription ${name}?` })) return;
loading = true;
try {
await client().send(new DeleteEventSubscriptionCommand({ SubscriptionName: name }));
toast.success(`Subscription ${name} deleted`);
await loadAll();
} catch (e) { toast.error('Failed to delete subscription: ' + String(e)); }
finally { loading = false; }
}

const tabs: { id: TabName; label: string }[] = [
{ id: 'clusters', label: 'Clusters' },
{ id: 'instances', label: 'Instances' },
{ id: 'snapshots', label: 'Snapshots' },
{ id: 'parametergroups', label: 'Parameter Groups' },
{ id: 'globalclusters', label: 'Global Clusters' },
{ id: 'eventsubscriptions', label: 'Event Subscriptions' }
];
</script>

<svelte:window onkeydown={(e) => {
  if (e.key === 'Escape') {
    showCreateCluster = false;
    showCreateInstance = false;
    showCreateSnapshot = false;
    showCreateParamGroup = false;
    showCreateGlobal = false;
    showCreateEvent = false;
    if (selectedCluster) selectedCluster = null;
  }
}} />

<div class="p-6 space-y-6">
<!-- Header -->
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<Database class="w-7 h-7 text-green-500" />
<div>
<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon DocumentDB</h1>
<p class="text-sm text-gray-500 dark:text-gray-400">MongoDB-compatible document database service</p>
</div>
</div>
<button onclick={loadAll} disabled={loading} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm disabled:opacity-50">
<RefreshCw class="w-4 h-4 {loading ? 'animate-spin' : ''}" /> {loading ? 'Refreshing...' : 'Refresh'}
</button>
</div>

<!-- Stat Cards -->
<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4">
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Database class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{totalClusters}</p><p class="text-sm text-gray-500 dark:text-gray-400">Total Clusters</p></div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Server class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{totalInstances}</p><p class="text-sm text-gray-500 dark:text-gray-400">Total Instances</p></div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg"><Database class="w-5 h-5 text-emerald-600 dark:text-emerald-400" /></div>
<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{availableClusters}</p><p class="text-sm text-gray-500 dark:text-gray-400">Available Clusters</p></div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-gray-100 dark:bg-gray-700 rounded-lg"><Square class="w-5 h-5 text-gray-600 dark:text-gray-400" /></div>
<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{stoppedClusters}</p><p class="text-sm text-gray-500 dark:text-gray-400">Stopped Clusters</p></div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Camera class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{snapshots.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Snapshots</p></div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Settings class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{paramGroups.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Param Groups</p></div>
</div>
</div>

<!-- Cluster Detail Panel -->
{#if selectedCluster}
<div class="flex items-center gap-2 text-sm mb-2">
<button onclick={() => { selectedCluster = null; }} class="text-green-600 dark:text-green-400 hover:underline">Clusters</button>
<ChevronRight class="w-4 h-4 text-gray-400" />
<span class="font-medium text-gray-700 dark:text-gray-300">{selectedCluster.DBClusterIdentifier}</span>
</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6 space-y-5">
<div class="flex items-center justify-between flex-wrap gap-2">
<div class="flex items-center gap-3">
  <h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selectedCluster.DBClusterIdentifier}</h2>
  <span class={`px-2 py-0.5 rounded-full text-xs font-medium ${statusClass(selectedCluster.Status)}`}>{selectedCluster.Status ?? 'unknown'}</span>
</div>
<div class="flex gap-2 flex-wrap">
{#if selectedCluster.Status === 'available'}
<button onclick={() => stopCluster(selectedCluster!.DBClusterIdentifier!)} class="flex items-center gap-1 px-3 py-1.5 text-sm rounded-lg bg-yellow-100 text-yellow-700 hover:bg-yellow-200 dark:bg-yellow-900/30 dark:text-yellow-400"><Square class="w-4 h-4" /> Stop</button>
<button onclick={() => failoverCluster(selectedCluster!.DBClusterIdentifier!)} class="flex items-center gap-1 px-3 py-1.5 text-sm rounded-lg bg-blue-100 text-blue-700 hover:bg-blue-200 dark:bg-blue-900/30 dark:text-blue-400"><Zap class="w-4 h-4" /> Failover</button>
{:else if selectedCluster.Status === 'stopped'}
<button onclick={() => startCluster(selectedCluster!.DBClusterIdentifier!)} class="flex items-center gap-1 px-3 py-1.5 text-sm rounded-lg bg-green-100 text-green-700 hover:bg-green-200 dark:bg-green-900/30 dark:text-green-400"><Play class="w-4 h-4" /> Start</button>
{/if}
<button onclick={() => { snapshotForm = { DBClusterSnapshotIdentifier: '', DBClusterIdentifier: selectedCluster!.DBClusterIdentifier ?? '' }; showCreateSnapshot = true; }} class="flex items-center gap-1 px-3 py-1.5 text-sm rounded-lg bg-purple-100 text-purple-700 hover:bg-purple-200 dark:bg-purple-900/30 dark:text-purple-400"><Camera class="w-4 h-4" /> Snapshot</button>
<button onclick={() => deleteCluster(selectedCluster!.DBClusterIdentifier!)} class="flex items-center gap-1 px-3 py-1.5 text-sm rounded-lg bg-red-100 text-red-700 hover:bg-red-200 dark:bg-red-900/30 dark:text-red-400"><Trash2 class="w-4 h-4" /> Delete</button>
</div>
</div>

<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
{#each [
['Engine Version', selectedCluster.EngineVersion ?? '—'],
['Port', String(selectedCluster.Port ?? 27017)],
['Backup Retention', `${selectedCluster.BackupRetentionPeriod ?? 1} day(s)`],
['Storage Encrypted', selectedCluster.StorageEncrypted ? 'Yes' : 'No'],
['Deletion Protection', selectedCluster.DeletionProtection ? 'Enabled' : 'Disabled'],
['DB Subnet Group', selectedCluster.DBSubnetGroup ?? '—'],
['Parameter Group', selectedCluster.DBClusterParameterGroup ?? '—'],
] as [label, value]}
<div class="bg-gray-50 dark:bg-gray-800 rounded-lg p-3">
  <p class="text-xs text-gray-500 dark:text-gray-400">{label}</p>
  <p class="text-sm font-semibold text-gray-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
</div>
{/each}
</div>

<div class="space-y-2">
  <h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Endpoints</h3>
  {#each [
    ['Writer Endpoint', selectedCluster.Endpoint ?? ''],
    ['Reader Endpoint', selectedCluster.ReaderEndpoint ?? ''],
  ] as [label, value]}
    {#if value}
    <div class="flex items-center justify-between bg-gray-50 dark:bg-gray-800 rounded-lg px-3 py-2">
      <div class="min-w-0">
        <p class="text-xs text-gray-500 dark:text-gray-400">{label}</p>
        <p class="text-sm font-mono text-gray-900 dark:text-white truncate" title={value}>{value}</p>
      </div>
      <button onclick={() => copyToClipboard(value, label)} title="Copy {label}" class="ml-2 p-1.5 text-gray-400 hover:text-green-500 dark:hover:text-green-400 flex-shrink-0"><Copy class="w-3.5 h-3.5" /></button>
    </div>
    {/if}
  {/each}
  {#if selectedCluster.DBClusterArn}
  <div class="flex items-center justify-between bg-gray-50 dark:bg-gray-800 rounded-lg px-3 py-2">
    <div class="min-w-0">
      <p class="text-xs text-gray-500 dark:text-gray-400">Cluster ARN</p>
      <p class="text-xs font-mono text-gray-700 dark:text-gray-300 truncate" title={selectedCluster.DBClusterArn}>{selectedCluster.DBClusterArn}</p>
    </div>
    <button onclick={() => copyToClipboard(selectedCluster!.DBClusterArn!, 'ARN')} title="Copy ARN" class="ml-2 p-1.5 text-gray-400 hover:text-green-500 dark:hover:text-green-400 flex-shrink-0"><Copy class="w-3.5 h-3.5" /></button>
  </div>
  {/if}
</div>

<div>
  <h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">
    Cluster Members
    {#if (selectedCluster.DBClusterMembers ?? []).length > 0}
    <span class="ml-1 text-xs text-gray-500 font-normal">({(selectedCluster.DBClusterMembers ?? []).length})</span>
    {/if}
  </h3>
  {#if (selectedCluster.DBClusterMembers ?? []).length === 0}
    <div class="bg-blue-50 dark:bg-blue-900/20 rounded-lg px-4 py-3 text-sm text-blue-700 dark:text-blue-300">
      No instances in this cluster.
      <button onclick={() => { instanceForm = { ...instanceForm, DBClusterIdentifier: selectedCluster!.DBClusterIdentifier ?? '' }; showCreateInstance = true; }} class="underline ml-1">Add an instance</button>
    </div>
  {:else}
    <div class="space-y-1">
      {#each selectedCluster.DBClusterMembers ?? [] as member}
        <div class="flex items-center justify-between bg-gray-50 dark:bg-gray-800 rounded-lg px-3 py-2">
          <span class="text-sm text-gray-900 dark:text-white font-medium">{member.DBInstanceIdentifier}</span>
          <span class="text-xs px-2 py-0.5 rounded-full {member.IsClusterWriter ? 'bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">
            {member.IsClusterWriter ? 'Writer' : 'Reader'}
          </span>
        </div>
      {/each}
    </div>
    <button onclick={() => { instanceForm = { ...instanceForm, DBClusterIdentifier: selectedCluster!.DBClusterIdentifier ?? '' }; showCreateInstance = true; }} class="mt-2 flex items-center gap-1 text-sm text-green-600 dark:text-green-400 hover:underline"><Plus class="w-3.5 h-3.5" /> Add Instance</button>
  {/if}
</div>
</div>
{:else}
<!-- Tabs -->
<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700 overflow-x-auto">
{#each tabs as tab}
<button
onclick={() => { activeTab = tab.id; searchQuery = ''; }}
class={`px-4 py-2 text-sm font-medium border-b-2 whitespace-nowrap transition-colors ${activeTab === tab.id ? 'border-green-500 text-green-600 dark:text-green-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'}`}
>{tab.label}{#if tabCounts[tab.id] > 0}<span class="ml-1.5 px-1.5 py-0.5 rounded-full text-xs bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-300">{tabCounts[tab.id]}</span>{/if}</button>
{/each}
</div>

<!-- Search + Create -->
<div class="flex gap-3">
<div class="relative flex-1">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white" />
</div>
{#if activeTab === 'clusters'}
<button onclick={() => showCreateCluster = true} class="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Cluster</button>
{:else if activeTab === 'instances'}
<button onclick={() => showCreateInstance = true} class="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Instance</button>
{:else if activeTab === 'snapshots'}
<button onclick={() => showCreateSnapshot = true} class="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Snapshot</button>
{:else if activeTab === 'parametergroups'}
<button onclick={() => showCreateParamGroup = true} class="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Group</button>
{:else if activeTab === 'globalclusters'}
<button onclick={() => showCreateGlobal = true} class="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Global</button>
{:else if activeTab === 'eventsubscriptions'}
<button onclick={() => showCreateEvent = true} class="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Subscription</button>
{/if}
</div>

{#if loading}
<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
{:else}
<!-- Clusters Tab -->
{#if activeTab === 'clusters'}
{#if filteredClusters.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
  <Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
  <p class="font-medium">No clusters found</p>
  {#if !searchQuery}
  <button onclick={() => showCreateCluster = true} class="mt-3 inline-flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Cluster</button>
  {/if}
</div>
{:else}
<div class="text-sm text-gray-500 dark:text-gray-400 mb-2">Showing {filteredClusters.length} of {clusters.length} clusters</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Identifier</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Engine Version</th>
<th class="px-4 py-3 text-left">Endpoint</th>
<th class="px-4 py-3 text-left">Features</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredClusters as cluster}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3">
<button onclick={() => { selectedCluster = cluster; }} class="text-green-600 dark:text-green-400 hover:underline font-medium">{cluster.DBClusterIdentifier ?? '-'}</button>
</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${statusClass(cluster.Status)}`}>{cluster.Status ?? '-'}</span></td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{cluster.EngineVersion ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300 font-mono text-xs truncate max-w-xs">{cluster.Endpoint ?? '-'}</td>
<td class="px-4 py-3">
  <div class="flex gap-1 flex-wrap">
    {#if cluster.StorageEncrypted}<span class="px-1.5 py-0.5 rounded text-xs bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400">Encrypted</span>{/if}
    {#if cluster.DeletionProtection}<span class="px-1.5 py-0.5 rounded text-xs bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400">Protected</span>{/if}
    {#if cluster.MultiAZ}<span class="px-1.5 py-0.5 rounded text-xs bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400">MultiAZ</span>{/if}
  </div>
</td>
<td class="px-4 py-3">
<div class="flex gap-1">
{#if cluster.Status === 'available'}
<button onclick={() => stopCluster(cluster.DBClusterIdentifier!)} title="Stop" class="p-1.5 rounded hover:bg-yellow-100 dark:hover:bg-yellow-900/30 text-yellow-600 dark:text-yellow-400"><Square class="w-3.5 h-3.5" /></button>
{:else if cluster.Status === 'stopped'}
<button onclick={() => startCluster(cluster.DBClusterIdentifier!)} title="Start" class="p-1.5 rounded hover:bg-green-100 dark:hover:bg-green-900/30 text-green-600 dark:text-green-400"><Play class="w-3.5 h-3.5" /></button>
{/if}
<button onclick={() => deleteCluster(cluster.DBClusterIdentifier!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400"><Trash2 class="w-3.5 h-3.5" /></button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- Instances Tab -->
{:else if activeTab === 'instances'}
{#if filteredInstances.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
  <Server class="w-12 h-12 mx-auto mb-3 opacity-40" />
  <p class="font-medium">No instances found</p>
  {#if !searchQuery}
  <button onclick={() => showCreateInstance = true} class="mt-3 inline-flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Instance</button>
  {/if}
</div>
{:else}
<div class="text-sm text-gray-500 dark:text-gray-400 mb-2">Showing {filteredInstances.length} of {instances.length} instances</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Identifier</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Class</th>
<th class="px-4 py-3 text-left">Cluster</th>
<th class="px-4 py-3 text-left">Engine Version</th>
<th class="px-4 py-3 text-left">Endpoint</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredInstances as inst}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{inst.DBInstanceIdentifier ?? '-'}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${statusClass(inst.DBInstanceStatus)}`}>{inst.DBInstanceStatus ?? '-'}</span></td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{inst.DBInstanceClass ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{inst.DBClusterIdentifier ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{inst.EngineVersion ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300 font-mono text-xs truncate max-w-[200px]">{inst.Endpoint?.Address ?? '-'}</td>
<td class="px-4 py-3">
<div class="flex gap-1">
<button onclick={() => rebootInstance(inst.DBInstanceIdentifier!)} title="Reboot" class="p-1.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900/30 text-blue-600 dark:text-blue-400"><RefreshCw class="w-3.5 h-3.5" /></button>
<button onclick={() => deleteInstance(inst.DBInstanceIdentifier!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400"><Trash2 class="w-3.5 h-3.5" /></button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- Snapshots Tab -->
{:else if activeTab === 'snapshots'}
<div class="flex items-center gap-2 mb-3">
  <select bind:value={snapshotTypeFilter} class="px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm text-gray-900 dark:text-white">
    <option value="all">All Types</option>
    <option value="manual">Manual</option>
    <option value="automated">Automated</option>
  </select>
</div>
{#if filteredSnapshots.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
  <Camera class="w-12 h-12 mx-auto mb-3 opacity-40" />
  <p class="font-medium">No snapshots found</p>
  {#if !searchQuery && snapshotTypeFilter === 'all'}
  <button onclick={() => showCreateSnapshot = true} class="mt-3 inline-flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Snapshot</button>
  {/if}
</div>
{:else}
<div class="text-sm text-gray-500 dark:text-gray-400 mb-2">Showing {filteredSnapshots.length} of {snapshots.length} snapshots</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Identifier</th>
<th class="px-4 py-3 text-left">Cluster</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Type</th>
<th class="px-4 py-3 text-left">Engine Version</th>
<th class="px-4 py-3 text-left">Progress</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredSnapshots as snap}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{snap.DBClusterSnapshotIdentifier ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{snap.DBClusterIdentifier ?? '-'}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${statusClass(snap.Status)}`}>{snap.Status ?? '-'}</span></td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{snap.SnapshotType ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{snap.EngineVersion ?? '-'}</td>
<td class="px-4 py-3">
  <div class="flex items-center gap-2">
    <div class="w-16 bg-gray-200 dark:bg-gray-700 rounded-full h-1.5">
      <div class="bg-green-500 h-1.5 rounded-full" style="width: {snap.PercentProgress ?? 0}%"></div>
    </div>
    <span class="text-xs text-gray-500">{snap.PercentProgress ?? 0}%</span>
  </div>
</td>
<td class="px-4 py-3">
<button onclick={() => deleteSnapshot(snap.DBClusterSnapshotIdentifier!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400"><Trash2 class="w-3.5 h-3.5" /></button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- Parameter Groups Tab -->
{:else if activeTab === 'parametergroups'}
{#if filteredParamGroups.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
  <Settings class="w-12 h-12 mx-auto mb-3 opacity-40" />
  <p class="font-medium">No parameter groups found</p>
  {#if !searchQuery}
  <button onclick={() => showCreateParamGroup = true} class="mt-3 inline-flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Group</button>
  {/if}
</div>
{:else}
<div class="text-sm text-gray-500 dark:text-gray-400 mb-2">Showing {filteredParamGroups.length} of {paramGroups.length} parameter groups</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Family</th>
<th class="px-4 py-3 text-left">Description</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredParamGroups as pg}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{pg.DBClusterParameterGroupName ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{pg.DBParameterGroupFamily ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{pg.Description ?? '-'}</td>
<td class="px-4 py-3">
<div class="flex gap-1">
<button onclick={() => openParameterEditor(pg.DBClusterParameterGroupName!)} title="Edit parameters" class="p-1.5 rounded hover:bg-green-100 dark:hover:bg-green-900/30 text-green-600 dark:text-green-400"><Settings class="w-3.5 h-3.5" /></button>
<button onclick={() => deleteParamGroup(pg.DBClusterParameterGroupName!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400"><Trash2 class="w-3.5 h-3.5" /></button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- Global Clusters Tab -->
{:else if activeTab === 'globalclusters'}
{#if filteredGlobal.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
  <Globe class="w-12 h-12 mx-auto mb-3 opacity-40" />
  <p class="font-medium">No global clusters found</p>
  {#if !searchQuery}
  <button onclick={() => showCreateGlobal = true} class="mt-3 inline-flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Global</button>
  {/if}
</div>
{:else}
<div class="text-sm text-gray-500 dark:text-gray-400 mb-2">Showing {filteredGlobal.length} of {globalClusters.length} global clusters</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Identifier</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Engine</th>
<th class="px-4 py-3 text-left">Engine Version</th>
<th class="px-4 py-3 text-left">Protection</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredGlobal as gc}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{gc.GlobalClusterIdentifier ?? '-'}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${statusClass(gc.Status)}`}>{gc.Status ?? '-'}</span></td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{gc.Engine ?? '-'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{gc.EngineVersion ?? '-'}</td>
<td class="px-4 py-3">
  <div class="flex gap-1">
    {#if gc.DeletionProtection}
    <span class="px-2 py-0.5 rounded text-xs font-medium bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400">Protected</span>
    {/if}
    {#if gc.StorageEncrypted}
    <span class="px-2 py-0.5 rounded text-xs font-medium bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400">Encrypted</span>
    {/if}
  </div>
</td>
<td class="px-4 py-3">
<button onclick={() => deleteGlobal(gc.GlobalClusterIdentifier!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400"><Trash2 class="w-3.5 h-3.5" /></button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}

<!-- Event Subscriptions Tab -->
{:else if activeTab === 'eventsubscriptions'}
{#if filteredEvents.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
  <Bell class="w-12 h-12 mx-auto mb-3 opacity-40" />
  <p class="font-medium">No event subscriptions found</p>
  {#if !searchQuery}
  <button onclick={() => showCreateEvent = true} class="mt-3 inline-flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg text-sm font-medium"><Plus class="w-4 h-4" /> Create Subscription</button>
  {/if}
</div>
{:else}
<div class="text-sm text-gray-500 dark:text-gray-400 mb-2">Showing {filteredEvents.length} of {eventSubs.length} subscriptions</div>
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Source Type</th>
<th class="px-4 py-3 text-left">SNS ARN</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredEvents as ev}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{ev.CustSubscriptionId ?? '-'}</td>
<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${statusClass(ev.Status)}`}>{ev.Status ?? '-'}</span></td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{ev.SourceType ?? 'all'}</td>
<td class="px-4 py-3 text-gray-600 dark:text-gray-300 font-mono text-xs truncate max-w-xs">{ev.SnsTopicArn ?? '-'}</td>
<td class="px-4 py-3">
<button onclick={() => deleteEventSub(ev.CustSubscriptionId!)} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500 dark:text-red-400"><Trash2 class="w-3.5 h-3.5" /></button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}
{/if}
{/if}
</div>

<!-- Create Cluster Modal -->
{#if showCreateCluster}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h3 class="text-lg font-semibold text-gray-900 dark:text-white">Create Cluster</h3>
<button onclick={() => showCreateCluster = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Cluster Identifier *</label>
<input bind:value={clusterForm.DBClusterIdentifier} type="text" required class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Master Username</label>
<input bind:value={clusterForm.MasterUsername} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Master Password</label>
<input bind:value={clusterForm.MasterUserPassword} type="password" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Engine</label>
<select bind:value={clusterForm.Engine} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
  <option value="docdb">docdb</option>
</select>
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Engine Version</label>
<select bind:value={clusterForm.EngineVersion} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
  <option value="4.0.0">4.0.0</option>
  <option value="5.0.0">5.0.0</option>
</select>
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Backup Retention Period (days)</label>
<input bind:value={clusterForm.BackupRetentionPeriod} type="number" min="1" max="35" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div class="flex gap-4">
<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300 cursor-pointer">
<input bind:checked={clusterForm.StorageEncrypted} type="checkbox" class="rounded" /> Storage Encrypted
</label>
<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300 cursor-pointer">
<input bind:checked={clusterForm.DeletionProtection} type="checkbox" class="rounded" /> Deletion Protection
</label>
</div>
</div>
<div class="flex justify-end gap-3 pt-2">
<button onclick={() => showCreateCluster = false} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createCluster} disabled={!clusterForm.DBClusterIdentifier} class="px-4 py-2 text-sm rounded-lg bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white font-medium">Create</button>
</div>
</div>
</div>
{/if}

<!-- Create Instance Modal -->
{#if showCreateInstance}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h3 class="text-lg font-semibold text-gray-900 dark:text-white">Create Instance</h3>
<button onclick={() => showCreateInstance = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Identifier *</label>
<input bind:value={instanceForm.DBInstanceIdentifier} type="text" required class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Cluster Identifier *</label>
<input bind:value={instanceForm.DBClusterIdentifier} type="text" required class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Class</label>
<select bind:value={instanceForm.DBInstanceClass} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
<option value="db.t3.medium">db.t3.medium</option>
<option value="db.r5.large">db.r5.large</option>
<option value="db.r5.xlarge">db.r5.xlarge</option>
<option value="db.r5.2xlarge">db.r5.2xlarge</option>
<option value="db.r5.4xlarge">db.r5.4xlarge</option>
<option value="db.r6g.large">db.r6g.large</option>
<option value="db.r6g.xlarge">db.r6g.xlarge</option>
</select>
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Engine</label>
<input bind:value={instanceForm.Engine} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
</div>
<div class="flex justify-end gap-3 pt-2">
<button onclick={() => showCreateInstance = false} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createInstance} disabled={!instanceForm.DBInstanceIdentifier || !instanceForm.DBClusterIdentifier} class="px-4 py-2 text-sm rounded-lg bg-green-600 hover:bg-green-700 disabled:opacity-50 text-white font-medium">Create</button>
</div>
</div>
</div>
{/if}

<!-- Create Snapshot Modal -->
{#if showCreateSnapshot}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h3 class="text-lg font-semibold text-gray-900 dark:text-white">Create Snapshot</h3>
<button onclick={() => showCreateSnapshot = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Snapshot Identifier</label>
<input bind:value={snapshotForm.DBClusterSnapshotIdentifier} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Cluster</label>
<select bind:value={snapshotForm.DBClusterIdentifier} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
<option value="">Select cluster...</option>
{#each clusters as c}
<option value={c.DBClusterIdentifier}>{c.DBClusterIdentifier}</option>
{/each}
</select>
</div>
</div>
<div class="flex justify-end gap-3 pt-2">
<button onclick={() => showCreateSnapshot = false} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createSnapshot} class="px-4 py-2 text-sm rounded-lg bg-green-600 hover:bg-green-700 text-white font-medium">Create</button>
</div>
</div>
</div>
{/if}

<!-- Create Parameter Group Modal -->
{#if showCreateParamGroup}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h3 class="text-lg font-semibold text-gray-900 dark:text-white">Create Parameter Group</h3>
<button onclick={() => showCreateParamGroup = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Group Name</label>
<input bind:value={paramGroupForm.DBClusterParameterGroupName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Family</label>
<select bind:value={paramGroupForm.DBParameterGroupFamily} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
  <option value="docdb4.0">docdb4.0</option>
  <option value="docdb5.0">docdb5.0</option>
</select>
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
<input bind:value={paramGroupForm.Description} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
</div>
<div class="flex justify-end gap-3 pt-2">
<button onclick={() => showCreateParamGroup = false} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createParamGroup} class="px-4 py-2 text-sm rounded-lg bg-green-600 hover:bg-green-700 text-white font-medium">Create</button>
</div>
</div>
</div>
{/if}

<!-- Parameter Editor Modal -->
{#if selectedParamGroup}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 w-full max-w-3xl max-h-[85vh] flex flex-col p-6 space-y-4">
<div class="flex items-center justify-between">
<h3 class="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2"><Settings class="w-5 h-5 text-green-600" /> Parameters — {selectedParamGroup}</h3>
<button onclick={() => { selectedParamGroup = null; groupParameters = []; }} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"><X class="w-5 h-5" /></button>
</div>
{#if loadingParameters}
<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-green-600 border-t-transparent rounded-full"></div></div>
{:else if modifiableParams.length === 0}
<div class="text-center py-12 text-gray-500">No modifiable parameters in this group.</div>
{:else}
<div class="overflow-y-auto flex-1 -mx-2 px-2">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs sticky top-0">
<tr>
<th class="px-3 py-2 text-left">Parameter</th>
<th class="px-3 py-2 text-left">Value</th>
<th class="px-3 py-2 text-left">Allowed</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each modifiableParams as p}
<tr>
<td class="px-3 py-2 font-mono text-xs text-gray-900 dark:text-white align-top">
{p.ParameterName}
<div class="text-[10px] text-gray-400 font-sans normal-case max-w-xs truncate" title={p.Description}>{p.Description ?? ''}</div>
</td>
<td class="px-3 py-2">
<input
value={editedValues[p.ParameterName ?? ''] ?? p.ParameterValue ?? ''}
oninput={(e) => { editedValues = { ...editedValues, [p.ParameterName ?? '']: (e.currentTarget as HTMLInputElement).value }; }}
type="text"
class="w-full px-2 py-1 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs text-gray-900 dark:text-white font-mono"
/>
</td>
<td class="px-3 py-2 text-xs text-gray-400 align-top max-w-[160px] truncate" title={p.AllowedValues}>{p.AllowedValues ?? '-'}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
<div class="flex justify-end gap-3 pt-2 border-t border-gray-100 dark:border-gray-800">
<button onclick={() => { selectedParamGroup = null; groupParameters = []; }} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
<button onclick={saveParameters} disabled={savingParameters} class="px-4 py-2 text-sm rounded-lg bg-green-600 hover:bg-green-700 text-white font-medium disabled:opacity-50">{savingParameters ? 'Saving...' : 'Save Changes'}</button>
</div>
</div>
</div>
{/if}

<!-- Create Global Cluster Modal -->
{#if showCreateGlobal}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h3 class="text-lg font-semibold text-gray-900 dark:text-white">Create Global Cluster</h3>
<button onclick={() => showCreateGlobal = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Global Cluster Identifier</label>
<input bind:value={globalForm.GlobalClusterIdentifier} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Engine</label>
<input bind:value={globalForm.Engine} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Engine Version</label>
<input bind:value={globalForm.EngineVersion} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
</div>
<div class="flex justify-end gap-3 pt-2">
<button onclick={() => showCreateGlobal = false} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createGlobal} class="px-4 py-2 text-sm rounded-lg bg-green-600 hover:bg-green-700 text-white font-medium">Create</button>
</div>
</div>
</div>
{/if}

<!-- Create Event Subscription Modal -->
{#if showCreateEvent}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h3 class="text-lg font-semibold text-gray-900 dark:text-white">Create Event Subscription</h3>
<button onclick={() => showCreateEvent = false} class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"><X class="w-5 h-5" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Subscription Name</label>
<input bind:value={eventForm.SubscriptionName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">SNS Topic ARN</label>
<input bind:value={eventForm.SnsTopicArn} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white" />
</div>
<div>
<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Source Type</label>
<select bind:value={eventForm.SourceType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm text-gray-900 dark:text-white">
<option value="db-cluster">db-cluster</option>
<option value="db-instance">db-instance</option>
<option value="db-cluster-snapshot">db-cluster-snapshot</option>
<option value="all">all</option>
</select>
</div>
</div>
<div class="flex justify-end gap-3 pt-2">
<button onclick={() => showCreateEvent = false} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createEventSub} class="px-4 py-2 text-sm rounded-lg bg-green-600 hover:bg-green-700 text-white font-medium">Create</button>
</div>
</div>
</div>
{/if}

<div class="mt-8 bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-6">
  <h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Amazon DocumentDB Operations</h2>
  <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
    {#each [
      { name: 'CreateDBCluster', desc: 'Creates a new Amazon DocumentDB cluster.' },
      { name: 'DescribeDBClusters', desc: 'Returns information about provisioned clusters.' },
      { name: 'DeleteDBCluster', desc: 'Deletes a previously provisioned cluster.' },
      { name: 'ModifyDBCluster', desc: 'Modifies a setting for a cluster.' },
      { name: 'StopDBCluster', desc: 'Stops a running cluster.' },
      { name: 'StartDBCluster', desc: 'Starts a stopped cluster.' },
      { name: 'FailoverDBCluster', desc: 'Forces a failover for a cluster.' },
      { name: 'CreateDBInstance', desc: 'Creates a new instance in a cluster.' },
      { name: 'DeleteDBInstance', desc: 'Deletes a previously provisioned instance.' },
      { name: 'RebootDBInstance', desc: 'Might result in a brief downtime during instance reboot.' },
      { name: 'CreateDBClusterSnapshot', desc: 'Creates a snapshot of a cluster.' },
      { name: 'DeleteDBClusterSnapshot', desc: 'Deletes a cluster snapshot.' },
      { name: 'CreateDBClusterParameterGroup', desc: 'Creates a new cluster parameter group.' },
      { name: 'DeleteDBClusterParameterGroup', desc: 'Deletes a specified cluster parameter group.' },
      { name: 'CreateGlobalCluster', desc: 'Creates a global cluster that can span multiple regions.' },
      { name: 'DeleteGlobalCluster', desc: 'Deletes a global cluster.' },
      { name: 'CreateEventSubscription', desc: 'Creates an event notification subscription.' },
      { name: 'DeleteEventSubscription', desc: 'Deletes an event notification subscription.' },
    ] as op}
    <div class="border border-gray-100 dark:border-gray-700 rounded-lg p-3">
      <div class="text-sm font-mono font-medium text-green-600 dark:text-green-400">{op.name}</div>
      <div class="text-xs text-gray-500 dark:text-gray-400 mt-1">{op.desc}</div>
    </div>
    {/each}
  </div>
</div>
