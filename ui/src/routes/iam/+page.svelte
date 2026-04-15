<script lang="ts">
import { onMount } from 'svelte';
import { getIAMClient } from '$lib/aws-client';
import { ListUsersCommand, ListRolesCommand, ListGroupsCommand } from '@aws-sdk/client-iam';
import { toast } from 'svelte-sonner';
import { Users, Shield, RefreshCw, Search } from 'lucide-svelte';

const iam = getIAMClient();

type Tab = 'users' | 'roles' | 'groups';
let tab = $state<Tab>('users');
let users = $state<any[]>([]);
let roles = $state<any[]>([]);
let groups = $state<any[]>([]);
let loading = $state(true);
let search = $state('');

onMount(async () => { await loadUsers(); });

async function loadUsers() {
try {
loading = true;
const data = await iam.send(new ListUsersCommand({}));
users = data.Users || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load users');
} finally {
loading = false;
}
}

async function loadRoles() {
try {
loading = true;
const data = await iam.send(new ListRolesCommand({}));
roles = data.Roles || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load roles');
} finally {
loading = false;
}
}

async function loadGroups() {
try {
loading = true;
const data = await iam.send(new ListGroupsCommand({}));
groups = data.Groups || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load groups');
} finally {
loading = false;
}
}

async function selectTab(t: Tab) {
tab = t;
search = '';
if (t === 'users' && users.length === 0) await loadUsers();
else if (t === 'roles' && roles.length === 0) await loadRoles();
else if (t === 'groups' && groups.length === 0) await loadGroups();
}

let items = $derived(tab === 'users' ? users : tab === 'roles' ? roles : groups);
let filtered = $derived(items.filter((i: any) => {
const name = i.UserName || i.RoleName || i.GroupName || '';
return !search || name.toLowerCase().includes(search.toLowerCase());
}));
function getName(i: any) { return i.UserName || i.RoleName || i.GroupName || ''; }
function getArn(i: any) { return i.Arn || ''; }
</script>

<div class="space-y-6">
<div class="flex items-center justify-between">
<div class="flex items-center gap-3">
<div class="p-2 bg-slate-100 dark:bg-slate-700 rounded-lg">
<Shield class="w-6 h-6 text-slate-600 dark:text-slate-300" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">IAM</h1>
<p class="text-slate-600 dark:text-slate-300">Identity and Access Management</p>
</div>
</div>
<button onclick={loadUsers} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
<RefreshCw class="w-4 h-4" />
</button>
</div>

<div class="flex gap-1 p-1 bg-slate-100 dark:bg-slate-800 rounded-lg w-fit">
{#each (['users', 'roles', 'groups'] as Tab[]) as t}
<button
onclick={() => selectTab(t)}
class="px-4 py-2 rounded-md text-sm font-medium capitalize transition-colors {tab === t
? 'bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow'
: 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
>
{t.charAt(0).toUpperCase() + t.slice(1)}
</button>
{/each}
</div>

<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search {tab}..." bind:value={search}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>

{#if loading}
<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div><p>Loading {tab}...</p></div>
{:else if filtered.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Users class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p class="text-slate-600 dark:text-slate-300">No {tab} found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700 border-b border-slate-200 dark:border-slate-600">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Name</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300 hidden md:table-cell">ARN</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-200 dark:divide-slate-700">
{#each filtered as item}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{getName(item)}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400 font-mono text-xs hidden md:table-cell truncate max-w-xs">{getArn(item)}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
</div>
