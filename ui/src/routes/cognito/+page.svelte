<script lang="ts">
import { onMount } from 'svelte';
import { getCognitoIDPClient } from '$lib/aws-client';
import { ListUserPoolsCommand } from '@aws-sdk/client-cognito-identity-provider';
import { toast } from 'svelte-sonner';
import { Users, Plus, RefreshCw, Search } from 'lucide-svelte';

const cognito = getCognitoIDPClient();

let userPools = $state<any[]>([]);
let loading = $state(true);
let search = $state('');

onMount(async () => {
await loadUserPools();
});

async function loadUserPools() {
try {
loading = true;
const data = await cognito.send(new ListUserPoolsCommand({ MaxResults: 60 }));
userPools = data.UserPools || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load user pools');
} finally {
loading = false;
}
}

function formatDate(date?: Date | number): string {
if (!date) return 'N/A';
return new Date(date).toLocaleDateString();
}

let filtered = $derived(userPools.filter(p =>
!search || p.Name?.toLowerCase().includes(search.toLowerCase()) || p.Id?.toLowerCase().includes(search.toLowerCase())
));
</script>

<div class="space-y-6">
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
<Users class="w-6 h-6 text-orange-600 dark:text-orange-400" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito User Pools</h1>
<p class="text-slate-600 dark:text-slate-300">User identity and access management</p>
</div>
</div>
<div class="flex gap-2">
<button onclick={loadUserPools} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300">
<RefreshCw class="w-4 h-4" />
</button>
<button onclick={() => toast.info('Create user pool via AWS console')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
<Plus class="w-4 h-4" />
Create Pool
</button>
</div>
</div>

{#if !loading && userPools.length > 0}
<div class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-400">
<Users class="w-4 h-4" />
<span>{userPools.length} pool{userPools.length !== 1 ? 's' : ''} in this region</span>
</div>
{/if}

<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input
type="text"
placeholder="Search user pools..."
bind:value={search}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
/>
</div>

{#if loading}
<div class="text-center py-12 text-slate-500 dark:text-slate-400">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
<p>Loading user pools...</p>
</div>
{:else if filtered.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Users class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p class="text-slate-600 dark:text-slate-300">No Cognito user pools found</p>
</div>
{:else}
<div class="grid gap-4">
{#each filtered as pool}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<div class="flex items-start justify-between mb-4">
<div>
<h3 class="font-semibold text-slate-900 dark:text-white text-lg">{pool.Name}</h3>
<p class="text-sm text-slate-500 dark:text-slate-400 font-mono">{pool.Id}</p>
</div>
<span class="px-3 py-1 rounded-full text-sm font-medium bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">
{pool.Status || 'Active'}
</span>
</div>
<div class="grid grid-cols-3 gap-4 text-sm">
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Status</p>
<p class="text-slate-900 dark:text-white">{pool.Status || 'ACTIVE'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Created</p>
<p class="text-slate-900 dark:text-white">{formatDate(pool.CreationDate)}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Updated</p>
<p class="text-slate-900 dark:text-white">{formatDate(pool.LastModifiedDate)}</p>
</div>
</div>
<div class="flex gap-2 mt-4">
<button onclick={() => toast.info(`Pool: ${pool.Id}`)} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
View Details
</button>
</div>
</div>
{/each}
</div>
{/if}
</div>
