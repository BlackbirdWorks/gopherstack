<script lang="ts">
import { onMount } from 'svelte';
import { getKMSClient } from '$lib/aws-client';
import { ListKeysCommand, DescribeKeyCommand, DisableKeyCommand, EnableKeyCommand } from '@aws-sdk/client-kms';
import { toast } from 'svelte-sonner';
import { Key, Plus, RefreshCw, Search, Lock, Unlock } from 'lucide-svelte';

const kms = getKMSClient();

let keys = $state<any[]>([]);
let loading = $state(true);
let search = $state('');

onMount(async () => { await loadKeys(); });

async function loadKeys() {
try {
loading = true;
const listData = await kms.send(new ListKeysCommand({}));
const rawKeys = listData.Keys || [];
const details = await Promise.all(
rawKeys.slice(0, 50).map(async (k: any) => {
try {
const d = await kms.send(new DescribeKeyCommand({ KeyId: k.KeyId }));
return d.KeyMetadata;
} catch {
return { KeyId: k.KeyId, KeyArn: k.KeyArn, Description: '', KeyState: 'Unknown', KeyUsage: 'Unknown' };
}
})
);
keys = details.filter(Boolean);
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load keys');
} finally {
loading = false;
}
}

async function disableKey(keyId: string) {
try {
await kms.send(new DisableKeyCommand({ KeyId: keyId }));
toast.success(`Key ${keyId} disabled`);
await loadKeys();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to disable key');
}
}

async function enableKey(keyId: string) {
try {
await kms.send(new EnableKeyCommand({ KeyId: keyId }));
toast.success(`Key ${keyId} enabled`);
await loadKeys();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to enable key');
}
}

function getStateColor(state: string) {
if (state === 'Enabled') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
if (state === 'Disabled') return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
}

let filtered = $derived(keys.filter(k => {
const desc = (k.Description || '').toLowerCase();
const id = (k.KeyId || '').toLowerCase();
return !search || desc.includes(search.toLowerCase()) || id.includes(search.toLowerCase());
}));
</script>

<div class="space-y-6">
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
<Key class="w-6 h-6 text-amber-600 dark:text-amber-400" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">KMS Keys</h1>
<p class="text-slate-600 dark:text-slate-300">Key Management Service</p>
</div>
</div>
<div class="flex gap-2">
<button onclick={loadKeys} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
<RefreshCw class="w-4 h-4" />
</button>
<button onclick={() => toast.info('Create key via AWS console')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
<Plus class="w-4 h-4" />
Create Key
</button>
</div>
</div>

<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search keys by description or ID..." bind:value={search}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>

{#if loading}
<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div><p>Loading keys...</p></div>
{:else if filtered.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Key class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p class="text-slate-600 dark:text-slate-300">No KMS keys found</p>
</div>
{:else}
<div class="grid gap-4">
{#each filtered as key}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<div class="flex items-start justify-between mb-3">
<div>
<h3 class="font-semibold text-slate-900 dark:text-white">{key.Description || '(no description)'}</h3>
<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mt-0.5">{key.KeyId}</p>
</div>
<span class={`px-3 py-1 rounded-full text-xs font-medium ${getStateColor(key.KeyState)}`}>{key.KeyState}</span>
</div>
<div class="grid grid-cols-2 gap-3 text-sm mb-4">
<div>
<p class="text-xs text-slate-500 dark:text-slate-400 uppercase">Usage</p>
<p class="font-medium text-slate-900 dark:text-white">{key.KeyUsage}</p>
</div>
<div>
<p class="text-xs text-slate-500 dark:text-slate-400 uppercase">Spec</p>
<p class="font-medium text-slate-900 dark:text-white">{key.CustomerMasterKeySpec || key.KeySpec || 'SYMMETRIC_DEFAULT'}</p>
</div>
</div>
<div class="flex gap-2">
{#if key.KeyState === 'Enabled'}
<button onclick={() => disableKey(key.KeyId)} class="px-3 py-1.5 bg-yellow-600 text-white rounded text-sm hover:bg-yellow-700 flex items-center gap-1">
<Lock class="w-3 h-3" /> Disable
</button>
{:else if key.KeyState === 'Disabled'}
<button onclick={() => enableKey(key.KeyId)} class="px-3 py-1.5 bg-green-600 text-white rounded text-sm hover:bg-green-700 flex items-center gap-1">
<Unlock class="w-3 h-3" /> Enable
</button>
{/if}
</div>
</div>
{/each}
</div>
{/if}
</div>
