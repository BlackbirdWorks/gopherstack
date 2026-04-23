<script lang="ts">
import { onMount } from 'svelte';
import { toast } from 'svelte-sonner';

type UserPool = { ID?: string; Name?: string; ARN?: string };
type UserPoolClient = { ClientID?: string; ClientName?: string; UserPoolID?: string; CreatedAt?: string };
type User = { Username?: string; Status?: string; Enabled?: boolean; CreatedAt?: string };
type Group = { GroupName?: string; Description?: string; Precedence?: number };

let activeTab = $state<'pools' | 'users' | 'clients' | 'groups'>('pools');
let selectedPool = $state<UserPool | null>(null);
let userPools = $state<UserPool[]>([]);
let users = $state<User[]>([]);
let clients = $state<UserPoolClient[]>([]);
let groups = $state<Group[]>([]);
let loading = $state(false);

// Create pool modal
let showCreatePoolModal = $state(false);
let newPoolName = $state('');

// Create user modal
let showCreateUserModal = $state(false);
let newUsername = $state('');
let newTempPassword = $state('');

// Create client modal
let showCreateClientModal = $state(false);
let newClientName = $state('');

// Create group modal
let showCreateGroupModal = $state(false);
let newGroupName = $state('');
let newGroupDescription = $state('');

async function loadUserPools() {
loading = true;
try {
const res = await fetch('/dashboard/api/cognitoidp/user-pools');
userPools = ((await res.json()) as { userPools?: UserPool[] }).userPools ?? [];
} catch (err: unknown) {
toast.error(`Failed to load user pools: ${(err as Error).message}`);
} finally {
loading = false;
}
}

async function loadUsers() {
if (!selectedPool?.ID) return;
loading = true;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/users`);
users = ((await res.json()) as { users?: User[] }).users ?? [];
} catch (err: unknown) {
toast.error(`Failed to load users: ${(err as Error).message}`);
} finally {
loading = false;
}
}

async function loadClients() {
if (!selectedPool?.ID) return;
loading = true;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/clients`);
clients = ((await res.json()) as { clients?: UserPoolClient[] }).clients ?? [];
} catch (err: unknown) {
toast.error(`Failed to load clients: ${(err as Error).message}`);
} finally {
loading = false;
}
}

async function loadGroups() {
if (!selectedPool?.ID) return;
loading = true;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/groups`);
groups = ((await res.json()) as { groups?: Group[] }).groups ?? [];
} catch (err: unknown) {
toast.error(`Failed to load groups: ${(err as Error).message}`);
} finally {
loading = false;
}
}

function selectPool(pool: UserPool) {
selectedPool = pool;
activeTab = 'users';
void loadUsers();
}

function clearSelectedPool() {
selectedPool = null;
users = [];
clients = [];
groups = [];
activeTab = 'pools';
}

function onTabChange(tab: 'pools' | 'users' | 'clients' | 'groups') {
	activeTab = tab;
	if (tab === 'users') void loadUsers();
	else if (tab === 'clients') void loadClients();
	else if (tab === 'groups') void loadGroups();
}

async function createPool() {
try {
const res = await fetch('/dashboard/api/cognitoidp/user-pools', {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ name: newPoolName })
});
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
showCreatePoolModal = false;
newPoolName = '';
await loadUserPools();
toast.success('User pool created');
} catch (err: unknown) {
toast.error(`Failed to create user pool: ${(err as Error).message}`);
}
}

async function deletePool(poolID: string) {
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${poolID}`, { method: 'DELETE' });
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
if (selectedPool?.ID === poolID) clearSelectedPool();
await loadUserPools();
toast.success('User pool deleted');
} catch (err: unknown) {
toast.error(`Failed to delete user pool: ${(err as Error).message}`);
}
}

async function createUser() {
if (!selectedPool?.ID) return;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/users`, {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ username: newUsername, tempPassword: newTempPassword })
});
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
showCreateUserModal = false;
newUsername = '';
newTempPassword = '';
await loadUsers();
toast.success('User created');
} catch (err: unknown) {
toast.error(`Failed to create user: ${(err as Error).message}`);
}
}

async function deleteUser(username: string) {
if (!selectedPool?.ID) return;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/users/${username}`, { method: 'DELETE' });
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
await loadUsers();
toast.success('User deleted');
} catch (err: unknown) {
toast.error(`Failed to delete user: ${(err as Error).message}`);
}
}

async function toggleUser(username: string, enabled: boolean) {
if (!selectedPool?.ID) return;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/users/${username}`, {
method: 'PATCH',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ enabled })
});
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
await loadUsers();
} catch (err: unknown) {
toast.error(`Failed to update user: ${(err as Error).message}`);
}
}

async function createClient() {
if (!selectedPool?.ID) return;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/clients`, {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ clientName: newClientName })
});
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
showCreateClientModal = false;
newClientName = '';
await loadClients();
toast.success('Client created');
} catch (err: unknown) {
toast.error(`Failed to create client: ${(err as Error).message}`);
}
}

async function deleteClient(clientId: string) {
if (!selectedPool?.ID) return;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/clients/${clientId}`, { method: 'DELETE' });
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
await loadClients();
toast.success('Client deleted');
} catch (err: unknown) {
toast.error(`Failed to delete client: ${(err as Error).message}`);
}
}

async function createGroup() {
if (!selectedPool?.ID) return;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/groups`, {
method: 'POST',
headers: { 'Content-Type': 'application/json' },
body: JSON.stringify({ groupName: newGroupName, description: newGroupDescription })
});
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
showCreateGroupModal = false;
newGroupName = '';
newGroupDescription = '';
await loadGroups();
toast.success('Group created');
} catch (err: unknown) {
toast.error(`Failed to create group: ${(err as Error).message}`);
}
}

async function deleteGroup(groupName: string) {
if (!selectedPool?.ID) return;
try {
const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/groups/${groupName}`, { method: 'DELETE' });
if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
await loadGroups();
toast.success('Group deleted');
} catch (err: unknown) {
toast.error(`Failed to delete group: ${(err as Error).message}`);
}
}

onMount(() => {
void loadUserPools();
});
</script>

<div class="space-y-6 p-6">
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito User Pools</h1>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Manage Cognito user pools, users, clients, and groups</p>
{#if selectedPool}
<div class="mt-3 flex items-center gap-2 rounded-lg bg-indigo-50 px-3 py-2 text-sm text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300">
<span class="font-medium">Selected pool:</span>
<span>{selectedPool.Name}</span>
<span class="text-xs text-indigo-500 dark:text-indigo-400">({selectedPool.ID})</span>
<button type="button" onclick={clearSelectedPool} class="ml-auto text-xs text-indigo-500 hover:text-indigo-700 dark:text-indigo-400 dark:hover:text-indigo-200">✕ Clear</button>
</div>
{/if}
</div>

<div class="flex gap-2">
<button type="button" onclick={() => onTabChange('pools')} class="rounded-lg border px-4 py-2 text-sm {activeTab === 'pools' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Pools</button>
<button type="button" onclick={() => onTabChange('users')} disabled={!selectedPool} class="rounded-lg border px-4 py-2 text-sm disabled:opacity-40 {activeTab === 'users' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Users</button>
<button type="button" onclick={() => onTabChange('clients')} disabled={!selectedPool} class="rounded-lg border px-4 py-2 text-sm disabled:opacity-40 {activeTab === 'clients' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Clients</button>
<button type="button" onclick={() => onTabChange('groups')} disabled={!selectedPool} class="rounded-lg border px-4 py-2 text-sm disabled:opacity-40 {activeTab === 'groups' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Groups</button>
</div>

{#if activeTab === 'pools'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex justify-end">
<button type="button" onclick={() => (showCreatePoolModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create User Pool</button>
</div>
{#if loading}
<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
{:else if userPools.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No user pools found</p>
{:else}
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Name</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">ID</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">ARN</th>
<th class="pb-2"></th>
</tr>
</thead>
<tbody>
{#each userPools as pool}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 font-medium text-slate-900 dark:text-white">{pool.Name}</td>
<td class="py-3 text-xs text-slate-500 dark:text-slate-400">{pool.ID}</td>
<td class="py-3 text-xs text-slate-500 dark:text-slate-400 max-w-xs truncate">{pool.ARN}</td>
<td class="py-3 text-right">
<button type="button" onclick={() => selectPool(pool)} class="mr-2 rounded-lg bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600 dark:text-slate-300">View</button>
<button type="button" onclick={() => deletePool(pool.ID ?? '')} class="rounded-lg border border-red-300 px-3 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{:else if activeTab === 'users'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex justify-end">
<button type="button" onclick={() => (showCreateUserModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create User</button>
</div>
{#if loading}
<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
{:else if users.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No users found in this pool</p>
{:else}
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Username</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Status</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Enabled</th>
<th class="pb-2"></th>
</tr>
</thead>
<tbody>
{#each users as user}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 font-medium text-slate-900 dark:text-white">{user.Username}</td>
<td class="py-3">
<span class="rounded-full px-2 py-0.5 text-xs {user.Status === 'CONFIRMED' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : user.Status === 'UNCONFIRMED' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300'}">{user.Status}</span>
</td>
<td class="py-3">
<button type="button" onclick={() => toggleUser(user.Username ?? '', !user.Enabled)} class="rounded-full px-2 py-0.5 text-xs {user.Enabled ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400'}">{user.Enabled ? 'Enabled' : 'Disabled'}</button>
</td>
<td class="py-3 text-right">
<button type="button" onclick={() => deleteUser(user.Username ?? '')} class="rounded-lg border border-red-300 px-3 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{:else if activeTab === 'clients'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex justify-end">
<button type="button" onclick={() => (showCreateClientModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create Client</button>
</div>
{#if loading}
<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
{:else if clients.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No clients found in this pool</p>
{:else}
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Client Name</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Client ID</th>
<th class="pb-2"></th>
</tr>
</thead>
<tbody>
{#each clients as client}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 font-medium text-slate-900 dark:text-white">{client.ClientName}</td>
<td class="py-3 text-xs text-slate-500 dark:text-slate-400">{client.ClientID}</td>
<td class="py-3 text-right">
<button type="button" onclick={() => deleteClient(client.ClientID ?? '')} class="rounded-lg border border-red-300 px-3 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{:else if activeTab === 'groups'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex justify-end">
<button type="button" onclick={() => (showCreateGroupModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create Group</button>
</div>
{#if loading}
<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
{:else if groups.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No groups found in this pool</p>
{:else}
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Group Name</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Description</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Precedence</th>
<th class="pb-2"></th>
</tr>
</thead>
<tbody>
{#each groups as group}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 font-medium text-slate-900 dark:text-white">{group.GroupName}</td>
<td class="py-3 text-slate-600 dark:text-slate-400">{group.Description ?? ''}</td>
<td class="py-3 text-slate-600 dark:text-slate-400">{group.Precedence ?? 0}</td>
<td class="py-3 text-right">
<button type="button" onclick={() => deleteGroup(group.GroupName ?? '')} class="rounded-lg border border-red-300 px-3 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{/if}
</div>

{#if showCreatePoolModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Create User Pool</h2>
<form onsubmit={(e) => { e.preventDefault(); void createPool(); }} class="space-y-4">
<input name="name" bind:value={newPoolName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="User pool name" required />
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showCreatePoolModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Cancel</button>
<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create User Pool</button>
</div>
</form>
</div>
</div>
{/if}

{#if showCreateUserModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Create User</h2>
<form onsubmit={(e) => { e.preventDefault(); void createUser(); }} class="space-y-4">
<input name="username" bind:value={newUsername} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Username" required />
<input name="tempPassword" type="password" bind:value={newTempPassword} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Temporary password" required />
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showCreateUserModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Cancel</button>
<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
</div>
</form>
</div>
</div>
{/if}

{#if showCreateClientModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Create Client</h2>
<form onsubmit={(e) => { e.preventDefault(); void createClient(); }} class="space-y-4">
<input name="clientName" bind:value={newClientName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Client name" required />
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showCreateClientModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Cancel</button>
<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
</div>
</form>
</div>
</div>
{/if}

{#if showCreateGroupModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Create Group</h2>
<form onsubmit={(e) => { e.preventDefault(); void createGroup(); }} class="space-y-4">
<input name="groupName" bind:value={newGroupName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Group name" required />
<input name="description" bind:value={newGroupDescription} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Description (optional)" />
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showCreateGroupModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Cancel</button>
<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
</div>
</form>
</div>
</div>
{/if}
