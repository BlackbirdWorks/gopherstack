<script lang="ts">
import { onMount } from 'svelte';
import { getSecretsManagerClient } from '$lib/aws-client';
import { ListSecretsCommand, DeleteSecretCommand } from '@aws-sdk/client-secrets-manager';
import { toast } from 'svelte-sonner';
import { LockKeyhole, Plus, RefreshCw, Search, Trash2, RotateCcw } from 'lucide-svelte';

const sm = getSecretsManagerClient();

let secrets = $state<any[]>([]);
let loading = $state(true);
let search = $state('');
let showCreateModal = $state(false);
let newSecretName = $state('');
let newSecretValue = $state('');

onMount(async () => { await loadSecrets(); });

async function loadSecrets() {
try {
loading = true;
const data = await sm.send(new ListSecretsCommand({}));
secrets = data.SecretList || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load secrets');
} finally {
loading = false;
}
}

async function deleteSecret(name: string) {
if (!confirm(`Delete secret "${name}"?`)) return;
try {
await sm.send(new DeleteSecretCommand({ SecretId: name }));
toast.success(`Secret "${name}" deleted`);
await loadSecrets();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete secret');
}
}

let filtered = $derived(secrets.filter(s =>
!search || s.Name?.toLowerCase().includes(search.toLowerCase()) || s.Description?.toLowerCase().includes(search.toLowerCase())
));

let rotationCount = $derived(secrets.filter(s => s.RotationEnabled).length);
</script>

<div class="space-y-6">
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
<LockKeyhole class="w-6 h-6 text-red-600 dark:text-red-400" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Secrets Manager</h1>
<p class="text-slate-600 dark:text-slate-300">Secure secrets storage and rotation</p>
</div>
</div>
<div class="flex gap-2">
<button onclick={loadSecrets} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
<RefreshCw class="w-4 h-4" />
</button>
<button onclick={() => showCreateModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
<Plus class="w-4 h-4" />
Create Secret
</button>
</div>
</div>

{#if !loading && secrets.length > 0}
<div class="flex gap-4 text-sm text-slate-600 dark:text-slate-400">
<span>{secrets.length} secret{secrets.length !== 1 ? 's' : ''} total</span>
{#if rotationCount > 0}
<span class="text-green-600 dark:text-green-400">· {rotationCount} with rotation enabled</span>
{/if}
</div>
{/if}

<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search secrets..." bind:value={search}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>

{#if loading}
<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div><p>Loading secrets...</p></div>
{:else if filtered.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<LockKeyhole class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p class="text-slate-600 dark:text-slate-300">No secrets found</p>
<button onclick={() => showCreateModal = true} class="mt-4 px-4 py-2 bg-indigo-600 text-white rounded-lg">Create Secret</button>
</div>
{:else}
<div class="grid gap-4">
{#each filtered as secret}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
<div class="flex items-start justify-between mb-3">
<div>
<h3 class="font-semibold text-slate-900 dark:text-white">{secret.Name}</h3>
{#if secret.Description}
<p class="text-sm text-slate-500 dark:text-slate-400 mt-0.5">{secret.Description}</p>
{/if}
</div>
<div class="flex gap-2">
{#if secret.RotationEnabled}
<span class="px-2 py-1 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 rounded text-xs font-medium flex items-center gap-1">
<RotateCcw class="w-3 h-3" /> Rotation On
</span>
{/if}
</div>
</div>
<div class="grid grid-cols-2 gap-3 text-sm mb-3">
<div>
<p class="text-xs text-slate-500 uppercase">Last Changed</p>
<p class="text-slate-900 dark:text-white">{secret.LastChangedDate ? new Date(secret.LastChangedDate).toLocaleDateString() : 'N/A'}</p>
</div>
<div>
<p class="text-xs text-slate-500 uppercase">Last Accessed</p>
<p class="text-slate-900 dark:text-white">{secret.LastAccessedDate ? new Date(secret.LastAccessedDate).toLocaleDateString() : 'N/A'}</p>
</div>
</div>
<div class="flex gap-2">
<button onclick={() => toast.info(`ARN: ${secret.ARN}`)} class="px-3 py-1.5 border border-slate-200 dark:border-slate-700 rounded text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
View ARN
</button>
<button onclick={() => deleteSecret(secret.Name)} class="px-3 py-1.5 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1">
<Trash2 class="w-3 h-3" /> Delete
</button>
</div>
</div>
{/each}
</div>
{/if}
</div>

{#if showCreateModal}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-slate-800 rounded-xl w-full max-w-md p-6">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Secret</h2>
<div class="space-y-4">
<div>
<label for="secret-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Secret Name</label>
<input id="secret-name" type="text" bind:value={newSecretName} placeholder="e.g. prod/db/password"
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
</div>
<div>
<label for="secret-value" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Secret Value</label>
<textarea id="secret-value" bind:value={newSecretValue} rows={3} placeholder="{'key: value'}"
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white font-mono text-sm"></textarea>
</div>
</div>
<div class="flex justify-end gap-3 mt-6">
<button onclick={() => showCreateModal = false} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-700 dark:text-slate-300">Cancel</button>
<button onclick={() => { toast.info('Use AWS SDK or console to create secrets'); showCreateModal = false; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg">Create</button>
</div>
</div>
</div>
{/if}
