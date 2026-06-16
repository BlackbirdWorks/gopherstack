<script lang="ts">
import { onMount } from 'svelte';
import { getCloudFrontClient } from '$lib/aws-client';
import {
ListDistributionsCommand,
CreateDistributionCommand,
GetDistributionCommand,
CreateInvalidationCommand,
ListInvalidationsCommand,
ListCachePoliciesCommand,
CreateCachePolicyCommand,
DeleteCachePolicyCommand,
ListOriginAccessControlsCommand,
CreateOriginAccessControlCommand,
DeleteOriginAccessControlCommand,
ListResponseHeadersPoliciesCommand,
CreateResponseHeadersPolicyCommand,
DeleteResponseHeadersPolicyCommand,
ListFunctionsCommand,
CreateFunctionCommand,
PublishFunctionCommand,
DeleteFunctionCommand,
ListOriginRequestPoliciesCommand,
CreateOriginRequestPolicyCommand,
DeleteOriginRequestPolicyCommand,
type DistributionSummary,
type Distribution,
type InvalidationSummary,
type CachePolicySummary,
type OriginAccessControlSummary,
type ResponseHeadersPolicySummary,
type FunctionSummary,
type OriginRequestPolicySummary
} from '@aws-sdk/client-cloudfront';
import { toast } from 'svelte-sonner';
import { Globe, Search, RefreshCw, Plus, ChevronRight, Zap, Shield, FileCode, Layout } from 'lucide-svelte';

const cf = getCloudFrontClient();

// ---- Main navigation tabs ----
type MainTab = 'distributions' | 'cache-policies' | 'oac' | 'response-headers' | 'functions' | 'origin-request-policies';
let mainTab = $state<MainTab>('distributions');

const mainTabs = [
	{ key: 'distributions' as MainTab, label: 'Distributions', icon: Globe },
	{ key: 'cache-policies' as MainTab, label: 'Cache Policies', icon: Layout },
	{ key: 'oac' as MainTab, label: 'Origin Access Control', icon: Shield },
	{ key: 'response-headers' as MainTab, label: 'Response Headers', icon: FileCode },
	{ key: 'functions' as MainTab, label: 'Functions', icon: Zap },
	{ key: 'origin-request-policies' as MainTab, label: 'Origin Request Policies', icon: Globe }
];

// ---- Distributions ----
let loading = $state(false);
let distributions = $state<DistributionSummary[]>([]);
let selectedDist = $state<Distribution | null>(null);
let activeTab = $state<'overview' | 'origins' | 'behaviors' | 'invalidations'>('overview');
let searchQuery = $state('');

let invalidations = $state<InvalidationSummary[]>([]);
let loadingInvalidations = $state(false);
let showInvalidate = $state(false);
let creatingInvalidation = $state(false);
let invalidationPaths = $state('/\n');

const filteredDists = $derived(
distributions.filter(
(d) =>
!searchQuery ||
(d.DomainName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
(d.Comment ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
(d.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase())
)
);

const statusColor = (enabled: boolean | undefined, status: string | undefined) => {
if (status === 'Deployed' && enabled) return 'green';
if (status === 'InProgress') return 'yellow';
if (!enabled) return 'gray';
return 'blue';
};

async function loadDistributions() {
loading = true;
try {
const resp = await cf.send(new ListDistributionsCommand({}));
distributions = resp.DistributionList?.Items ?? [];
} catch (e) {
toast.error('Failed to load distributions: ' + String(e));
} finally {
loading = false;
}
}

async function selectDistribution(id: string) {
activeTab = 'overview';
invalidations = [];
try {
const resp = await cf.send(new GetDistributionCommand({ Id: id }));
selectedDist = resp.Distribution ?? null;
} catch (e) {
toast.error('Failed to load distribution details: ' + String(e));
}
}

async function handleTabChange(tab: 'overview' | 'origins' | 'behaviors' | 'invalidations') {
activeTab = tab;
if (tab === 'invalidations' && selectedDist && invalidations.length === 0) {
await loadInvalidations();
}
}

async function loadInvalidations() {
if (!selectedDist) return;
loadingInvalidations = true;
try {
const resp = await cf.send(
new ListInvalidationsCommand({ DistributionId: selectedDist.Id ?? '' })
);
invalidations = resp.InvalidationList?.Items ?? [];
} catch (e) {
toast.error('Failed to load invalidations: ' + String(e));
} finally {
loadingInvalidations = false;
}
}

async function createInvalidation() {
if (!selectedDist) return;
const paths = invalidationPaths
.split('\n')
.map((p) => p.trim())
.filter(Boolean);
if (paths.length === 0) return;
creatingInvalidation = true;
try {
await cf.send(
new CreateInvalidationCommand({
DistributionId: selectedDist.Id ?? '',
InvalidationBatch: {
CallerReference: `invalidate-${Date.now()}`,
Paths: { Quantity: paths.length, Items: paths }
}
})
);
toast.success(`Invalidation created for ${paths.length} paths`);
showInvalidate = false;
invalidationPaths = '/\n';
invalidations = [];
activeTab = 'invalidations';
await loadInvalidations();
} catch (e) {
toast.error('Failed to create invalidation: ' + String(e));
} finally {
creatingInvalidation = false;
}
}

function httpMethodsList(methods: string[] | undefined): string {
return (methods ?? []).join(', ') || '-';
}

// ---- Cache Policies ----
let cachePolicies = $state<CachePolicySummary[]>([]);
let loadingCP = $state(false);
let showCreateCP = $state(false);
let cpName = $state('');
let cpComment = $state('');
let cpDefaultTTL = $state(86400);
let cpMaxTTL = $state(31536000);
let cpMinTTL = $state(0);
let creatingCP = $state(false);

async function loadCachePolicies() {
loadingCP = true;
try {
const resp = await cf.send(new ListCachePoliciesCommand({}));
cachePolicies = resp.CachePolicyList?.Items ?? [];
} catch (e) {
toast.error('Failed to load cache policies: ' + String(e));
} finally {
loadingCP = false;
}
}

async function createCachePolicy() {
creatingCP = true;
try {
await cf.send(
new CreateCachePolicyCommand({
CachePolicyConfig: {
Name: cpName,
Comment: cpComment,
DefaultTTL: cpDefaultTTL,
MaxTTL: cpMaxTTL,
MinTTL: cpMinTTL,
ParametersInCacheKeyAndForwardedToOrigin: {
EnableAcceptEncodingGzip: false,
EnableAcceptEncodingBrotli: false,
HeadersConfig: { HeaderBehavior: 'none' },
CookiesConfig: { CookieBehavior: 'none' },
QueryStringsConfig: { QueryStringBehavior: 'none' }
}
}
})
);
toast.success('Cache policy created');
showCreateCP = false;
cpName = '';
cpComment = '';
cpDefaultTTL = 86400;
cpMaxTTL = 31536000;
cpMinTTL = 0;
await loadCachePolicies();
} catch (e) {
toast.error('Failed to create cache policy: ' + String(e));
} finally {
creatingCP = false;
}
}

async function deleteCachePolicy(id: string) {
try {
await cf.send(new DeleteCachePolicyCommand({ Id: id, IfMatch: '*' }));
toast.success('Cache policy deleted');
await loadCachePolicies();
} catch (e) {
toast.error('Failed to delete cache policy: ' + String(e));
}
}

// ---- Origin Access Control ----
let oacs = $state<OriginAccessControlSummary[]>([]);
let loadingOAC = $state(false);
let showCreateOAC = $state(false);
let oacName = $state('');
let oacDescription = $state('');
let creatingOAC = $state(false);

async function loadOACs() {
loadingOAC = true;
try {
const resp = await cf.send(new ListOriginAccessControlsCommand({}));
oacs = resp.OriginAccessControlList?.Items ?? [];
} catch (e) {
toast.error('Failed to load origin access controls: ' + String(e));
} finally {
loadingOAC = false;
}
}

async function createOAC() {
creatingOAC = true;
try {
await cf.send(
new CreateOriginAccessControlCommand({
OriginAccessControlConfig: {
Name: oacName,
Description: oacDescription,
OriginAccessControlOriginType: 's3',
SigningBehavior: 'always',
SigningProtocol: 'sigv4'
}
})
);
toast.success('Origin access control created');
showCreateOAC = false;
oacName = '';
oacDescription = '';
await loadOACs();
} catch (e) {
toast.error('Failed to create OAC: ' + String(e));
} finally {
creatingOAC = false;
}
}

async function deleteOAC(id: string) {
try {
await cf.send(new DeleteOriginAccessControlCommand({ Id: id, IfMatch: '*' }));
toast.success('Origin access control deleted');
await loadOACs();
} catch (e) {
toast.error('Failed to delete OAC: ' + String(e));
}
}

// ---- Response Headers Policies ----
let rhps = $state<ResponseHeadersPolicySummary[]>([]);
let loadingRHP = $state(false);
let showCreateRHP = $state(false);
let rhpName = $state('');
let rhpComment = $state('');
let creatingRHP = $state(false);

async function loadRHPs() {
loadingRHP = true;
try {
const resp = await cf.send(new ListResponseHeadersPoliciesCommand({}));
rhps = resp.ResponseHeadersPolicyList?.Items ?? [];
} catch (e) {
toast.error('Failed to load response headers policies: ' + String(e));
} finally {
loadingRHP = false;
}
}

async function createRHP() {
creatingRHP = true;
try {
await cf.send(
new CreateResponseHeadersPolicyCommand({
ResponseHeadersPolicyConfig: {
Name: rhpName,
Comment: rhpComment
}
})
);
toast.success('Response headers policy created');
showCreateRHP = false;
rhpName = '';
rhpComment = '';
await loadRHPs();
} catch (e) {
toast.error('Failed to create response headers policy: ' + String(e));
} finally {
creatingRHP = false;
}
}

async function deleteRHP(id: string) {
try {
await cf.send(new DeleteResponseHeadersPolicyCommand({ Id: id, IfMatch: '*' }));
toast.success('Response headers policy deleted');
await loadRHPs();
} catch (e) {
toast.error('Failed to delete response headers policy: ' + String(e));
}
}

// ---- CloudFront Functions ----
let cfFunctions = $state<FunctionSummary[]>([]);
let loadingFn = $state(false);
let showCreateFn = $state(false);
let fnName = $state('');
let fnComment = $state('');
let fnCode = $state('function handler(event) { return event.request; }');
let creatingFn = $state(false);

// ---- Origin Request Policies ----
let orps = $state<OriginRequestPolicySummary[]>([]);
let loadingORP = $state(false);
let showCreateORP = $state(false);
let orpName = $state('');
let orpComment = $state('');
let creatingORP = $state(false);

// ---- Create Distribution ----
let showCreateDist = $state(false);
let distComment = $state('');
let distEnabled = $state(true);
let creatingDist = $state(false);

// ---- OAC form extra fields ----
let oacOriginType = $state('s3');
let oacSigningBehavior = $state('always');

async function loadFunctions() {
loadingFn = true;
try {
const resp = await cf.send(new ListFunctionsCommand({}));
cfFunctions = resp.FunctionList?.Items ?? [];
} catch (e) {
toast.error('Failed to load functions: ' + String(e));
} finally {
loadingFn = false;
}
}

async function createCFFunction() {
creatingFn = true;
try {
await cf.send(
new CreateFunctionCommand({
Name: fnName,
FunctionConfig: {
Comment: fnComment,
Runtime: 'cloudfront-js-2.0'
},
FunctionCode: new TextEncoder().encode(fnCode)
})
);
toast.success('Function created');
showCreateFn = false;
fnName = '';
fnComment = '';
fnCode = 'function handler(event) { return event.request; }';
await loadFunctions();
} catch (e) {
toast.error('Failed to create function: ' + String(e));
} finally {
creatingFn = false;
}
}

async function publishFunction(name: string) {
try {
await cf.send(new PublishFunctionCommand({ Name: name, IfMatch: '*' }));
toast.success('Function published (LIVE)');
await loadFunctions();
} catch (e) {
toast.error('Failed to publish function: ' + String(e));
}
}

async function deleteCFFunction(name: string) {
try {
await cf.send(new DeleteFunctionCommand({ Name: name, IfMatch: '*' }));
toast.success('Function deleted');
await loadFunctions();
} catch (e) {
toast.error('Failed to delete function: ' + String(e));
}
}

async function loadORPs() {
loadingORP = true;
try {
const resp = await cf.send(new ListOriginRequestPoliciesCommand({}));
orps = resp.OriginRequestPolicyList?.Items ?? [];
} catch (e) {
toast.error('Failed to load origin request policies: ' + String(e));
} finally {
loadingORP = false;
}
}

async function createORP() {
creatingORP = true;
try {
await cf.send(
new CreateOriginRequestPolicyCommand({
OriginRequestPolicyConfig: {
Name: orpName,
Comment: orpComment,
HeadersConfig: { HeaderBehavior: 'none' },
CookiesConfig: { CookieBehavior: 'none' },
QueryStringsConfig: { QueryStringBehavior: 'none' }
}
})
);
toast.success('Origin request policy created');
showCreateORP = false;
orpName = '';
orpComment = '';
await loadORPs();
} catch (e) {
toast.error('Failed to create origin request policy: ' + String(e));
} finally {
creatingORP = false;
}
}

async function deleteORP(id: string) {
try {
await cf.send(new DeleteOriginRequestPolicyCommand({ Id: id, IfMatch: '*' }));
toast.success('Origin request policy deleted');
await loadORPs();
} catch (e) {
toast.error('Failed to delete origin request policy: ' + String(e));
}
}

async function createDistribution() {
creatingDist = true;
try {
await cf.send(
new CreateDistributionCommand({
DistributionConfig: {
CallerReference: `dist-${Date.now()}`,
Comment: distComment,
Enabled: distEnabled,
Origins: {
Quantity: 1,
Items: [
{
Id: 'default-origin',
DomainName: 'example.com',
CustomOriginConfig: {
HTTPPort: 80,
HTTPSPort: 443,
OriginProtocolPolicy: 'https-only'
}
}
]
},
DefaultCacheBehavior: {
TargetOriginId: 'default-origin',
ViewerProtocolPolicy: 'redirect-to-https',
ForwardedValues: {
QueryString: false,
Cookies: { Forward: 'none' }
},
MinTTL: 0
}
}
})
);
toast.success('Distribution created');
showCreateDist = false;
distComment = '';
distEnabled = true;
await loadDistributions();
} catch (e) {
toast.error('Failed to create distribution: ' + String(e));
} finally {
creatingDist = false;
}
}

function handleMainTabChange(tab: MainTab) {
mainTab = tab;
selectedDist = null;
invalidations = [];
if (tab === 'distributions' && distributions.length === 0) loadDistributions();
if (tab === 'cache-policies' && cachePolicies.length === 0) loadCachePolicies();
if (tab === 'oac' && oacs.length === 0) loadOACs();
if (tab === 'response-headers' && rhps.length === 0) loadRHPs();
if (tab === 'functions' && cfFunctions.length === 0) loadFunctions();
if (tab === 'origin-request-policies' && orps.length === 0) loadORPs();
}

onMount(() => {
loadDistributions();
});
</script>

<div class="p-6 space-y-6">
<!-- Header -->
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<Globe class="w-7 h-7 text-violet-500" />
<div>
<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon CloudFront</h1>
<p class="text-sm text-gray-500 dark:text-gray-400">Global content delivery network</p>
</div>
</div>
<button
onclick={() => {
if (mainTab === 'distributions') loadDistributions();
else if (mainTab === 'cache-policies') loadCachePolicies();
else if (mainTab === 'oac') loadOACs();
else if (mainTab === 'response-headers') loadRHPs();
else if (mainTab === 'functions') loadFunctions();
else if (mainTab === 'origin-request-policies') loadORPs();
}}
class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
>
<RefreshCw class="w-4 h-4" /> Refresh
</button>
</div>

<!-- Top navigation tabs -->
<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700 overflow-x-auto">
{#each mainTabs as tab}
{@const TabIcon = tab.icon}
<button
onclick={() => handleMainTabChange(tab.key)}
class={`flex items-center gap-2 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${mainTab === tab.key ? 'border-violet-500 text-violet-600 dark:text-violet-400' : 'border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
>
<TabIcon class="w-4 h-4" />
{tab.label}
</button>
{/each}
</div>

<!-- ========== DISTRIBUTIONS ========== -->
{#if mainTab === 'distributions'}
{#if selectedDist}
<!-- Distribution Detail -->
<div class="flex items-center justify-between">
<div class="flex items-center gap-2 text-sm">
<button
onclick={() => {
selectedDist = null;
invalidations = [];
}}
class="text-violet-600 hover:underline">Distributions</button
>
<ChevronRight class="w-4 h-4 text-gray-400" />
<span class="font-medium">{selectedDist.DomainName}</span>
<span
class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedDist.DistributionConfig?.Enabled, selectedDist.Status)}-100 text-${statusColor(selectedDist.DistributionConfig?.Enabled, selectedDist.Status)}-700`}
>
{selectedDist.Status}
</span>
</div>
<button
onclick={() => (showInvalidate = true)}
class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium"
>
<Zap class="w-4 h-4" /> Create Invalidation
</button>
</div>

<!-- Dist Tabs -->
<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
{#each [['overview', 'Overview'], ['origins', 'Origins'], ['behaviors', 'Cache Behaviors'], ['invalidations', 'Invalidations']] as [tab, label]}
<button
onclick={() =>
handleTabChange(tab as 'overview' | 'origins' | 'behaviors' | 'invalidations')}
class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-violet-500 text-violet-600 dark:text-violet-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
>
{label}
</button>
{/each}
</div>

{#if activeTab === 'overview'}
<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
{#each [
{ label: 'Distribution ID', value: selectedDist.Id },
{ label: 'Domain Name', value: selectedDist.DomainName },
{ label: 'Status', value: selectedDist.Status },
{
label: 'Enabled',
value: selectedDist.DistributionConfig?.Enabled ? 'Yes' : 'No'
},
{
label: 'Price Class',
value: selectedDist.DistributionConfig?.PriceClass ?? '-'
},
{
label: 'HTTP Version',
value: selectedDist.DistributionConfig?.HttpVersion ?? '-'
},
{
label: 'IPv6 Enabled',
value: selectedDist.DistributionConfig?.IsIPV6Enabled ? 'Yes' : 'No'
},
{ label: 'Comment', value: selectedDist.DistributionConfig?.Comment || 'None' }
] as row}
<div
class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4"
>
<div class="text-xs text-gray-500 font-medium">{row.label}</div>
<div class="text-sm text-gray-900 dark:text-white mt-1 truncate">{row.value}</div>
</div>
{/each}
</div>
{#if (selectedDist.DistributionConfig?.Aliases?.Items ?? []).length > 0}
<div
class="bg-violet-50 dark:bg-violet-900/20 rounded-xl border border-violet-200 dark:border-violet-800 p-4"
>
<div class="text-sm font-semibold text-violet-800 dark:text-violet-300 mb-2">
Alternate Domain Names (CNAMEs)
</div>
<div class="flex flex-wrap gap-2">
{#each selectedDist.DistributionConfig?.Aliases?.Items ?? [] as alias}
<span
class="px-3 py-1 bg-violet-100 dark:bg-violet-900/40 text-violet-700 dark:text-violet-300 rounded-full text-xs font-mono"
>{alias}</span
>
{/each}
</div>
</div>
{/if}
{/if}

{#if activeTab === 'origins'}
{#if (selectedDist.DistributionConfig?.Origins?.Items ?? []).length === 0}
<div class="text-center py-12 text-gray-500">No origins configured</div>
{:else}
<div class="space-y-3">
{#each selectedDist.DistributionConfig?.Origins?.Items ?? [] as origin}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4"
>
<div class="flex items-start justify-between">
<div>
<div class="font-semibold text-sm">{origin.Id}</div>
<div class="text-xs text-gray-500 mt-1 font-mono">{origin.DomainName}</div>
{#if origin.OriginPath}
<div class="text-xs text-gray-500">Path: {origin.OriginPath}</div>
{/if}
</div>
<span class="px-2 py-0.5 rounded text-xs font-medium bg-violet-100 text-violet-700">
{origin.S3OriginConfig ? 'S3' : origin.CustomOriginConfig ? 'Custom' : 'Origin'}
</span>
</div>
</div>
{/each}
</div>
{/if}
{/if}

{#if activeTab === 'behaviors'}
<div class="space-y-3">
{#if selectedDist.DistributionConfig?.DefaultCacheBehavior}
{@const dcb = selectedDist.DistributionConfig.DefaultCacheBehavior}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-violet-200 dark:border-violet-800 p-4"
>
<div class="flex items-center gap-2 mb-2">
<span class="px-2 py-0.5 rounded text-xs font-medium bg-violet-100 text-violet-700"
>Default</span
>
<span class="text-sm font-medium">/*</span>
</div>
<div class="grid grid-cols-2 gap-2 text-xs text-gray-500">
<span>Origin: {dcb.TargetOriginId}</span>
<span>Protocol: {dcb.ViewerProtocolPolicy}</span>
<span>Methods: {httpMethodsList(dcb.AllowedMethods?.Items)}</span>
<span>Compress: {dcb.Compress ? 'Yes' : 'No'}</span>
</div>
</div>
{/if}
{#each selectedDist.DistributionConfig?.CacheBehaviors?.Items ?? [] as behavior}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4"
>
<div class="text-sm font-medium mb-2 font-mono">{behavior.PathPattern}</div>
<div class="grid grid-cols-2 gap-2 text-xs text-gray-500">
<span>Origin: {behavior.TargetOriginId}</span>
<span>Protocol: {behavior.ViewerProtocolPolicy}</span>
</div>
</div>
{/each}
</div>
{/if}

{#if activeTab === 'invalidations'}
{#if loadingInvalidations}
<div class="flex justify-center py-8">
<div
class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"
></div>
</div>
{:else if invalidations.length === 0}
<div class="text-center py-12 text-gray-500">
<Zap class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No invalidations</p>
</div>
{:else}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden"
>
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
<tr>
<th class="px-4 py-3 text-left">ID</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Created</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each invalidations as inv}
<tr>
<td class="px-4 py-3 font-mono text-xs">{inv.Id}</td>
<td class="px-4 py-3"
><span
class={`px-2 py-0.5 rounded text-xs font-medium ${inv.Status === 'Completed' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}
>{inv.Status}</span
></td
>
<td class="px-4 py-3 text-xs text-gray-500"
>{inv.CreateTime?.toLocaleString() ?? '-'}</td
>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}
{:else}
<!-- Distribution List -->
<div class="flex items-center justify-between gap-2">
<div class="relative flex-1">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
<input
bind:value={searchQuery}
type="text"
placeholder="Search distributions..."
class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm"
/>
</div>
<button
onclick={() => (showCreateDist = true)}
class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium whitespace-nowrap"
>
<Plus class="w-4 h-4" /> Create
</button>
</div>
{#if loading}
<div class="flex justify-center py-12">
<div
class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"
></div>
</div>
{:else if filteredDists.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<Globe class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No distributions found</p>
<button
onclick={() => (showCreateDist = true)}
class="mt-4 inline-flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium"
>
<Plus class="w-4 h-4" /> Create Distribution
</button>
</div>
{:else}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden"
>
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
<tr>
<th class="px-4 py-3 text-left">ID</th>
<th class="px-4 py-3 text-left">Domain</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Comment</th>
<th class="px-4 py-3 text-left">Origins</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each filteredDists as dist}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
<td class="px-4 py-3">
<button
onclick={() => selectDistribution(dist.Id ?? '')}
class="text-violet-600 dark:text-violet-400 hover:underline font-mono text-xs"
>{dist.Id}</button
>
</td>
<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400"
>{dist.DomainName}</td
>
<td class="px-4 py-3">
<span
class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(dist.Enabled, dist.Status)}-100 text-${statusColor(dist.Enabled, dist.Status)}-700`}
>
{dist.Status}
</span>
</td>
<td class="px-4 py-3 text-xs text-gray-500">{dist.Comment || '-'}</td>
<td class="px-4 py-3 text-xs text-gray-500">{dist.Origins?.Quantity ?? 0}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}
{/if}

<!-- ========== CACHE POLICIES ========== -->
{#if mainTab === 'cache-policies'}
<div class="flex items-center justify-between">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Cache Policies</h2>
<button
onclick={() => (showCreateCP = true)}
class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium"
>
<Plus class="w-4 h-4" /> Create Policy
</button>
</div>
{#if loadingCP}
<div class="flex justify-center py-8">
<div
class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"
></div>
</div>
{:else if cachePolicies.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<Layout class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No cache policies</p>
</div>
{:else}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden"
>
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Comment</th>
<th class="px-4 py-3 text-left">Default TTL</th>
<th class="px-4 py-3 text-left">Max TTL</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each cachePolicies as cp}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
<td class="px-4 py-3 font-medium text-sm"
>{cp.CachePolicy?.CachePolicyConfig?.Name ?? '-'}</td
>
<td class="px-4 py-3 text-xs text-gray-500"
>{cp.CachePolicy?.CachePolicyConfig?.Comment || '-'}</td
>
<td class="px-4 py-3 text-xs"
>{cp.CachePolicy?.CachePolicyConfig?.DefaultTTL ?? '-'}s</td
>
<td class="px-4 py-3 text-xs">{cp.CachePolicy?.CachePolicyConfig?.MaxTTL ?? '-'}s</td
>
<td class="px-4 py-3">
<button
onclick={() => deleteCachePolicy(cp.CachePolicy?.Id ?? '')}
class="text-xs text-red-500 hover:text-red-700 dark:hover:text-red-400"
>
Delete
</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- ========== ORIGIN ACCESS CONTROL ========== -->
{#if mainTab === 'oac'}
<div class="flex items-center justify-between">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Origin Access Controls</h2>
<button
onclick={() => (showCreateOAC = true)}
class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium"
>
<Plus class="w-4 h-4" /> Create OAC
</button>
</div>
{#if loadingOAC}
<div class="flex justify-center py-8">
<div
class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"
></div>
</div>
{:else if oacs.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<Shield class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No origin access controls</p>
</div>
{:else}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden"
>
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Origin Type</th>
<th class="px-4 py-3 text-left">Signing</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each oacs as oac}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
<td class="px-4 py-3 font-medium text-sm">{oac.Name ?? '-'}</td>
<td class="px-4 py-3 text-xs text-gray-500"
>{oac.OriginAccessControlOriginType ?? '-'}</td
>
<td class="px-4 py-3 text-xs">{oac.SigningBehavior ?? '-'} / {oac.SigningProtocol ?? '-'}</td>
<td class="px-4 py-3">
<button
onclick={() => deleteOAC(oac.Id ?? '')}
class="text-xs text-red-500 hover:text-red-700 dark:hover:text-red-400"
>
Delete
</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- ========== RESPONSE HEADERS POLICIES ========== -->
{#if mainTab === 'response-headers'}
<div class="flex items-center justify-between">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Response Headers Policies</h2>
<button
onclick={() => (showCreateRHP = true)}
class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium"
>
<Plus class="w-4 h-4" /> Create Policy
</button>
</div>
{#if loadingRHP}
<div class="flex justify-center py-8">
<div
class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"
></div>
</div>
{:else if rhps.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<FileCode class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No response headers policies</p>
</div>
{:else}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden"
>
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Comment</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each rhps as rhp}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
<td class="px-4 py-3 font-medium text-sm"
>{rhp.ResponseHeadersPolicy?.ResponseHeadersPolicyConfig?.Name ?? '-'}</td
>
<td class="px-4 py-3 text-xs text-gray-500"
>{rhp.ResponseHeadersPolicy?.ResponseHeadersPolicyConfig?.Comment || '-'}</td
>
<td class="px-4 py-3">
<button
onclick={() => deleteRHP(rhp.ResponseHeadersPolicy?.Id ?? '')}
class="text-xs text-red-500 hover:text-red-700 dark:hover:text-red-400"
>
Delete
</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- ========== FUNCTIONS ========== -->
{#if mainTab === 'functions'}
<div class="flex items-center justify-between">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">CloudFront Functions</h2>
<button
onclick={() => (showCreateFn = true)}
class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium"
>
<Plus class="w-4 h-4" /> Create Function
</button>
</div>
{#if loadingFn}
<div class="flex justify-center py-8">
<div
class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"
></div>
</div>
{:else if cfFunctions.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<Zap class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No CloudFront functions</p>
</div>
{:else}
<div
class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden"
>
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">Runtime</th>
<th class="px-4 py-3 text-left">Status</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each cfFunctions as fn}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
<td class="px-4 py-3 font-medium text-sm font-mono">{fn.Name ?? '-'}</td>
<td class="px-4 py-3 text-xs text-gray-500">{fn.FunctionConfig?.Runtime ?? '-'}</td>
<td class="px-4 py-3">
<span
class={`px-2 py-0.5 rounded text-xs font-medium ${fn.FunctionMetadata?.Stage === 'LIVE' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}`}
>
{fn.FunctionMetadata?.Stage ?? 'UNPUBLISHED'}
</span>
</td>
<td class="px-4 py-3 flex items-center gap-3">
{#if fn.FunctionMetadata?.Stage !== 'LIVE'}
<button
onclick={() => publishFunction(fn.Name ?? '')}
class="text-xs text-violet-600 hover:text-violet-800 dark:hover:text-violet-400"
>
Publish
</button>
{/if}
<button
onclick={() => deleteCFFunction(fn.Name ?? '')}
class="text-xs text-red-500 hover:text-red-700 dark:hover:text-red-400"
>
Delete
</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}
</div>

<!-- ========== ORIGIN REQUEST POLICIES ========== -->
{#if mainTab === 'origin-request-policies'}
<div class="flex items-center justify-between">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Origin Request Policies</h2>
<button
onclick={() => (showCreateORP = true)}
class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium"
>
<Plus class="w-4 h-4" /> Create Policy
</button>
</div>
{#if loadingORP}
<div class="flex justify-center py-8">
<div class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"></div>
</div>
{:else if orps.length === 0}
<div class="text-center py-16 text-gray-500 dark:text-gray-400">
<Globe class="w-12 h-12 mx-auto mb-3 opacity-40" />
<p class="font-medium">No origin request policies</p>
</div>
{:else}
<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
<tr>
<th class="px-4 py-3 text-left">Name</th>
<th class="px-4 py-3 text-left">ID</th>
<th class="px-4 py-3 text-left">Comment</th>
<th class="px-4 py-3 text-left">Actions</th>
</tr>
</thead>
<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
{#each orps as orp}
<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
<td class="px-4 py-3 font-medium text-sm">{orp.OriginRequestPolicy?.OriginRequestPolicyConfig?.Name ?? '-'}</td>
<td class="px-4 py-3 font-mono text-xs text-gray-400">{orp.OriginRequestPolicy?.Id ?? '-'}</td>
<td class="px-4 py-3 text-xs text-gray-500">{orp.OriginRequestPolicy?.OriginRequestPolicyConfig?.Comment || '-'}</td>
<td class="px-4 py-3">
<button
onclick={() => deleteORP(orp.OriginRequestPolicy?.Id ?? '')}
class="text-xs text-red-500 hover:text-red-700 dark:hover:text-red-400"
>Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- Create Distribution Modal -->
{#if showCreateDist}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Distribution</h2>
<div class="space-y-3">
<div>
<label for="dist-comment" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Comment</label>
<input id="dist-comment" bind:value={distComment} type="text" placeholder="My distribution" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div class="flex items-center gap-3">
<input bind:checked={distEnabled} id="dist-enabled" type="checkbox" class="w-4 h-4 rounded" />
<label for="dist-enabled" class="text-sm font-medium text-gray-700 dark:text-gray-300">Enabled</label>
</div>
<p class="text-xs text-gray-500">Creates a basic distribution with a placeholder HTTP origin (example.com).</p>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => (showCreateDist = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createDistribution} disabled={creatingDist} class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
{creatingDist ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Origin Request Policy Modal -->
{#if showCreateORP}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Origin Request Policy</h2>
<div class="space-y-3">
<div>
<label for="orp-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name</label>
<input id="orp-name" bind:value={orpName} type="text" placeholder="my-origin-request-policy" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="orp-comment" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Comment</label>
<input id="orp-comment" bind:value={orpComment} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => (showCreateORP = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createORP} disabled={creatingORP || !orpName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
{creatingORP ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Invalidation Modal -->
{#if showInvalidate && selectedDist}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Invalidation</h2>
<p class="text-sm text-gray-500">Distribution: {selectedDist.DomainName}</p>
<div>
<label
for="invalidation-paths"
class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
>Paths (one per line)</label
>
<textarea
id="invalidation-paths"
bind:value={invalidationPaths}
rows={6}
placeholder="/images/*&#10;/static/app.js"
class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"
></textarea>
</div>
<div class="flex gap-3 pt-2">
<button
onclick={() => (showInvalidate = false)}
class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800"
>Cancel</button
>
<button
onclick={createInvalidation}
disabled={creatingInvalidation || !invalidationPaths.trim()}
class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50"
>
{creatingInvalidation ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Cache Policy Modal -->
{#if showCreateCP}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Cache Policy</h2>
<div class="space-y-3">
<div>
<label for="cp-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name</label>
<input id="cp-name" bind:value={cpName} type="text" placeholder="my-cache-policy" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="cp-comment" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Comment</label>
<input id="cp-comment" bind:value={cpComment} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div class="grid grid-cols-3 gap-3">
<div>
<label for="cp-min-ttl" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Min TTL (s)</label>
<input id="cp-min-ttl" bind:value={cpMinTTL} type="number" class="w-full px-2 py-1.5 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="cp-default-ttl" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Default TTL (s)</label>
<input id="cp-default-ttl" bind:value={cpDefaultTTL} type="number" class="w-full px-2 py-1.5 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="cp-max-ttl" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Max TTL (s)</label>
<input id="cp-max-ttl" bind:value={cpMaxTTL} type="number" class="w-full px-2 py-1.5 rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => (showCreateCP = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createCachePolicy} disabled={creatingCP || !cpName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
{creatingCP ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create OAC Modal -->
{#if showCreateOAC}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Origin Access Control</h2>
<div class="space-y-3">
<div>
<label for="oac-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name</label>
<input id="oac-name" bind:value={oacName} type="text" placeholder="my-oac" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="oac-description" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
<input id="oac-description" bind:value={oacDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<p class="text-xs text-gray-500">Origin type: S3 · Signing: always · Protocol: sigv4</p>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => (showCreateOAC = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createOAC} disabled={creatingOAC || !oacName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
{creatingOAC ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Response Headers Policy Modal -->
{#if showCreateRHP}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Response Headers Policy</h2>
<div class="space-y-3">
<div>
<label for="rhp-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name</label>
<input id="rhp-name" bind:value={rhpName} type="text" placeholder="my-rhp" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="rhp-comment" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Comment</label>
<input id="rhp-comment" bind:value={rhpComment} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => (showCreateRHP = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createRHP} disabled={creatingRHP || !rhpName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
{creatingRHP ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}

<!-- Create Function Modal -->
{#if showCreateFn}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-lg p-6 space-y-4">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create CloudFront Function</h2>
<div class="space-y-3">
<div>
<label for="fn-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Name</label>
<input id="fn-name" bind:value={fnName} type="text" placeholder="my-function" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
</div>
<div>
<label for="fn-comment" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Comment</label>
<input id="fn-comment" bind:value={fnComment} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
</div>
<div>
<label for="fn-code" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Function Code (JS)</label>
<textarea id="fn-code" bind:value={fnCode} rows={8} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => (showCreateFn = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
<button onclick={createCFFunction} disabled={creatingFn || !fnName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
{creatingFn ? 'Creating...' : 'Create'}
</button>
</div>
</div>
</div>
{/if}
