<script lang="ts">
import { onMount } from 'svelte';
import { toast } from 'svelte-sonner';

type Attribute = { Name?: string; Value?: string };
type UserPool = { ID?: string; Name?: string; ARN?: string; MfaConfiguration?: string; CreatedAt?: number };
type UserPoolClient = { ClientID?: string; ClientName?: string; UserPoolID?: string; ClientSecret?: string; CreationDate?: number };
type User = { Username?: string; Status?: string; Enabled?: boolean; CreatedAt?: string; Attributes?: Attribute[] };
type Group = { GroupName?: string; Description?: string; Precedence?: number };
type PoolMetrics = { userCount?: number; clientCount?: number; groupCount?: number; activeTokenCount?: number };

let activeTab = $state<'pools' | 'users' | 'clients' | 'groups' | 'docs'>('pools');
let selectedPool = $state<UserPool | null>(null);
let userPools = $state<UserPool[]>([]);
let users = $state<User[]>([]);
let clients = $state<UserPoolClient[]>([]);
let groups = $state<Group[]>([]);
let loading = $state(false);
let poolMetrics = $state<PoolMetrics | null>(null);

// Create pool modal
let showCreatePoolModal = $state(false);
let newPoolName = $state('');

// Pool detail drawer
let showPoolDetail = $state(false);
let detailPool = $state<UserPool | null>(null);
let editMfaConfig = $state('');

// Create user modal
let showCreateUserModal = $state(false);
let newUsername = $state('');
let newTempPassword = $state('');

// User attributes drawer
let showUserAttrsDrawer = $state(false);
let drawerUser = $state<User | null>(null);

// Add to group modal
let showAddToGroupModal = $state(false);
let addToGroupUsername = $state('');
let addToGroupName = $state('');

// Group members drawer
let showGroupMembersDrawer = $state(false);
let drawerGroup = $state<Group | null>(null);
let groupMembers = $state<User[]>([]);

// Create client modal
let showCreateClientModal = $state(false);
let newClientName = $state('');
let revealSecretId = $state<string | null>(null);

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

async function loadPoolMetrics() {
	if (!selectedPool?.ID) return;
	try {
		const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/metrics`);
		if (res.ok) poolMetrics = (await res.json()) as PoolMetrics;
	} catch {
		// non-critical; metrics are informational only
	}
}

async function loadGroupMembers(groupName: string) {
	if (!selectedPool?.ID) return;
	try {
		const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/groups/${encodeURIComponent(groupName)}/members`);
		groupMembers = ((await res.json()) as { users?: User[] }).users ?? [];
	} catch (err: unknown) {
		toast.error(`Failed to load group members: ${(err as Error).message}`);
	}
}

function selectPool(pool: UserPool) {
	selectedPool = pool;
	poolMetrics = null;
	activeTab = 'users';
	void loadUsers();
	void loadPoolMetrics();
}

function clearSelectedPool() {
	selectedPool = null;
	poolMetrics = null;
	users = [];
	clients = [];
	groups = [];
	activeTab = 'pools';
}

function onTabChange(tab: 'pools' | 'users' | 'clients' | 'groups' | 'docs') {
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

function openPoolDetail(pool: UserPool) {
	detailPool = pool;
	editMfaConfig = pool.MfaConfiguration ?? 'OFF';
	showPoolDetail = true;
}

async function savePoolMfa() {
	if (!detailPool?.ID) return;
	try {
		const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${detailPool.ID}`, {
			method: 'PATCH',
			headers: { 'Content-Type': 'application/json' },
			body: JSON.stringify({ mfaConfiguration: editMfaConfig })
		});
		if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
		await loadUserPools();
		showPoolDetail = false;
		toast.success('MFA configuration updated');
	} catch (err: unknown) {
		toast.error(`Failed to update pool: ${(err as Error).message}`);
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

async function resetUserPassword(username: string) {
	if (!selectedPool?.ID) return;
	try {
		const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/users/${username}/reset`, { method: 'POST' });
		if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
		await loadUsers();
		toast.success('Password reset to FORCE_CHANGE_PASSWORD');
	} catch (err: unknown) {
		toast.error(`Failed to reset password: ${(err as Error).message}`);
	}
}

async function signOutUser(username: string) {
	if (!selectedPool?.ID) return;
	try {
		const res = await fetch(`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/users/${username}/sign-out`, { method: 'POST' });
		if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
		void loadPoolMetrics();
		toast.success('User signed out from all sessions');
	} catch (err: unknown) {
		toast.error(`Failed to sign out user: ${(err as Error).message}`);
	}
}

function openUserAttrs(user: User) {
	drawerUser = user;
	showUserAttrsDrawer = true;
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

async function openGroupMembers(group: Group) {
	drawerGroup = group;
	showGroupMembersDrawer = true;
	await loadGroupMembers(group.GroupName ?? '');
}

async function addUserToGroup() {
	if (!selectedPool?.ID || !addToGroupUsername || !addToGroupName) return;
	try {
		const res = await fetch(
			`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/groups/${encodeURIComponent(addToGroupName)}/members/${encodeURIComponent(addToGroupUsername)}`,
			{ method: 'PUT' }
		);
		if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
		showAddToGroupModal = false;
		addToGroupUsername = '';
		addToGroupName = '';
		if (showGroupMembersDrawer && drawerGroup?.GroupName) await loadGroupMembers(drawerGroup.GroupName);
		toast.success('User added to group');
	} catch (err: unknown) {
		toast.error(`Failed to add user to group: ${(err as Error).message}`);
	}
}

async function removeUserFromGroup(groupName: string, username: string) {
	if (!selectedPool?.ID) return;
	try {
		const res = await fetch(
			`/dashboard/api/cognitoidp/user-pools/${selectedPool.ID}/groups/${encodeURIComponent(groupName)}/members/${encodeURIComponent(username)}`,
			{ method: 'DELETE' }
		);
		if (!res.ok) throw new Error(((await res.json()) as { error?: string }).error ?? 'request failed');
		await loadGroupMembers(groupName);
		toast.success('User removed from group');
	} catch (err: unknown) {
		toast.error(`Failed to remove user from group: ${(err as Error).message}`);
	}
}

onMount(() => {
	void loadUserPools();
});
</script>

<div class="space-y-6 p-6">
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito User Pools</h1>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Manage Cognito user pools, users, app clients, and groups</p>
{#if selectedPool}
<div class="mt-3 flex items-center gap-2 rounded-lg bg-indigo-50 px-3 py-2 text-sm text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300">
<span class="font-medium">Selected pool:</span>
<span>{selectedPool.Name}</span>
<span class="text-xs text-indigo-500 dark:text-indigo-400">({selectedPool.ID})</span>
{#if selectedPool.MfaConfiguration}
<span class="ml-1 rounded-full bg-indigo-100 px-2 py-0.5 text-xs font-medium text-indigo-700 dark:bg-indigo-800 dark:text-indigo-200">MFA: {selectedPool.MfaConfiguration}</span>
{/if}
<button type="button" onclick={() => openPoolDetail(selectedPool!)} class="ml-2 rounded bg-indigo-100 px-2 py-0.5 text-xs text-indigo-700 hover:bg-indigo-200 dark:bg-indigo-800 dark:text-indigo-200">Edit</button>
<button type="button" onclick={clearSelectedPool} class="ml-auto text-xs text-indigo-500 hover:text-indigo-700 dark:text-indigo-400 dark:hover:text-indigo-200">&#x2715; Clear</button>
</div>
{#if poolMetrics}
<div class="mt-2 flex gap-4 text-xs text-slate-500 dark:text-slate-400">
<span>Users: <strong class="text-slate-700 dark:text-slate-200">{poolMetrics.userCount ?? 0}</strong></span>
<span>Clients: <strong class="text-slate-700 dark:text-slate-200">{poolMetrics.clientCount ?? 0}</strong></span>
<span>Groups: <strong class="text-slate-700 dark:text-slate-200">{poolMetrics.groupCount ?? 0}</strong></span>
<span>Active Tokens: <strong class="text-slate-700 dark:text-slate-200">{poolMetrics.activeTokenCount ?? 0}</strong></span>
</div>
{/if}
{/if}
</div>

<div class="flex flex-wrap gap-2">
<button type="button" onclick={() => onTabChange('pools')} class="rounded-lg border px-4 py-2 text-sm {activeTab === 'pools' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Pools</button>
<button type="button" onclick={() => onTabChange('users')} disabled={!selectedPool} class="rounded-lg border px-4 py-2 text-sm disabled:opacity-40 {activeTab === 'users' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Users</button>
<button type="button" onclick={() => onTabChange('clients')} disabled={!selectedPool} class="rounded-lg border px-4 py-2 text-sm disabled:opacity-40 {activeTab === 'clients' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Clients</button>
<button type="button" onclick={() => onTabChange('groups')} disabled={!selectedPool} class="rounded-lg border px-4 py-2 text-sm disabled:opacity-40 {activeTab === 'groups' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Groups</button>
<button type="button" onclick={() => onTabChange('docs')} class="rounded-lg border px-4 py-2 text-sm {activeTab === 'docs' ? 'bg-indigo-600 text-white border-indigo-600' : 'border-slate-300 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700'}">Docs</button>
</div>

{#if activeTab === 'pools'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex justify-end">
<button type="button" onclick={() => (showCreatePoolModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create User Pool</button>
</div>
{#if loading}
<p class="text-sm text-slate-500 dark:text-slate-400">Loading...</p>
{:else if userPools.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No user pools found. Create one to get started.</p>
{:else}
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 dark:border-slate-700">
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Name</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Pool ID</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">MFA</th>
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">ARN</th>
<th class="pb-2"></th>
</tr>
</thead>
<tbody>
{#each userPools as pool}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 font-medium text-slate-900 dark:text-white">{pool.Name}</td>
<td class="py-3 text-xs font-mono text-slate-500 dark:text-slate-400">{pool.ID}</td>
<td class="py-3 text-xs text-slate-500 dark:text-slate-400">{pool.MfaConfiguration ?? 'OFF'}</td>
<td class="py-3 max-w-xs truncate text-xs text-slate-400 dark:text-slate-500" title={pool.ARN}>{pool.ARN}</td>
<td class="py-3 text-right">
<button type="button" onclick={() => selectPool(pool)} class="mr-2 rounded-lg bg-indigo-50 px-3 py-1 text-xs text-indigo-600 hover:bg-indigo-100 dark:bg-indigo-900/30 dark:text-indigo-300 dark:hover:bg-indigo-900/50">Select</button>
<button type="button" onclick={() => openPoolDetail(pool)} class="mr-2 rounded-lg bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600 dark:text-slate-300">Edit</button>
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
<div class="mb-4 flex items-center justify-between">
<p class="text-sm text-slate-500 dark:text-slate-400">{users.length} user{users.length !== 1 ? 's' : ''}</p>
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
<td class="py-3 font-medium text-slate-900 dark:text-white">
<button type="button" onclick={() => openUserAttrs(user)} class="hover:text-indigo-600 dark:hover:text-indigo-400">{user.Username}</button>
</td>
<td class="py-3">
<span class="rounded-full px-2 py-0.5 text-xs {user.Status === 'CONFIRMED' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : user.Status === 'UNCONFIRMED' ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400' : user.Status === 'FORCE_CHANGE_PASSWORD' ? 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-400' : 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300'}">{user.Status}</span>
</td>
<td class="py-3">
<button type="button" onclick={() => toggleUser(user.Username ?? '', !user.Enabled)} class="rounded-full px-2 py-0.5 text-xs {user.Enabled ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-slate-100 text-slate-500 dark:bg-slate-700 dark:text-slate-400'}">{user.Enabled ? 'Enabled' : 'Disabled'}</button>
</td>
<td class="py-3 text-right space-x-1">
<button type="button" onclick={() => openUserAttrs(user)} class="rounded-lg bg-slate-100 px-2 py-1 text-xs hover:bg-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600 dark:text-slate-300">Attrs</button>
<button type="button" onclick={() => resetUserPassword(user.Username ?? '')} class="rounded-lg border border-orange-300 px-2 py-1 text-xs text-orange-600 hover:bg-orange-50 dark:border-orange-700 dark:text-orange-400 dark:hover:bg-orange-900/20">Reset Pwd</button>
<button type="button" onclick={() => signOutUser(user.Username ?? '')} class="rounded-lg border border-slate-300 px-2 py-1 text-xs text-slate-600 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-400 dark:hover:bg-slate-700">Sign Out</button>
<button type="button" onclick={() => deleteUser(user.Username ?? '')} class="rounded-lg border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{:else if activeTab === 'clients'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex items-center justify-between">
<p class="text-sm text-slate-500 dark:text-slate-400">{clients.length} client{clients.length !== 1 ? 's' : ''}</p>
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
<th class="pb-2 font-medium text-slate-600 dark:text-slate-400">Secret</th>
<th class="pb-2"></th>
</tr>
</thead>
<tbody>
{#each clients as client}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 font-medium text-slate-900 dark:text-white">{client.ClientName}</td>
<td class="py-3 text-xs font-mono text-slate-500 dark:text-slate-400">{client.ClientID}</td>
<td class="py-3 text-xs font-mono text-slate-500 dark:text-slate-400">
{#if client.ClientSecret}
{#if revealSecretId === client.ClientID}
<span>{client.ClientSecret}</span>
<button type="button" onclick={() => (revealSecretId = null)} class="ml-1 text-indigo-500 hover:underline">hide</button>
{:else}
<span>&#x2022;&#x2022;&#x2022;&#x2022;&#x2022;&#x2022;&#x2022;&#x2022;</span>
<button type="button" onclick={() => (revealSecretId = client.ClientID ?? null)} class="ml-1 text-indigo-500 hover:underline">reveal</button>
{/if}
{:else}
<span class="text-slate-400 dark:text-slate-600">none</span>
{/if}
</td>
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
<div class="mb-4 flex items-center justify-between">
<p class="text-sm text-slate-500 dark:text-slate-400">{groups.length} group{groups.length !== 1 ? 's' : ''}</p>
<div class="flex gap-2">
<button type="button" onclick={() => { addToGroupName = ''; addToGroupUsername = ''; showAddToGroupModal = true; }} class="rounded-lg border border-indigo-300 px-4 py-2 text-sm font-medium text-indigo-600 hover:bg-indigo-50 dark:border-indigo-700 dark:text-indigo-300">+ Add User to Group</button>
<button type="button" onclick={() => (showCreateGroupModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create Group</button>
</div>
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
<td class="py-3 text-right space-x-1">
<button type="button" onclick={() => openGroupMembers(group)} class="rounded-lg bg-slate-100 px-3 py-1 text-xs hover:bg-slate-200 dark:bg-slate-700 dark:hover:bg-slate-600 dark:text-slate-300">Members</button>
<button type="button" onclick={() => deleteGroup(group.GroupName ?? '')} class="rounded-lg border border-red-300 px-3 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{:else if activeTab === 'docs'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800 space-y-6">
<h2 class="text-xl font-semibold text-slate-900 dark:text-white">Supported Operations</h2>
<p class="text-sm text-slate-500 dark:text-slate-400">All operations use the standard AWS Cognito IDP API endpoint with a JSON body containing the target action.</p>
<div class="grid gap-4 sm:grid-cols-2">
{#each [
['CreateUserPool', 'Create a new user pool.'],
['DescribeUserPool', 'Describe a user pool.'],
['ListUserPools', 'List all user pools.'],
['DeleteUserPool', 'Delete a user pool and all its contents.'],
['UpdateUserPool', 'Update MFA configuration and other pool settings.'],
['GetUserPoolMfaConfig', 'Get the MFA configuration for a pool.'],
['SetUserPoolMfaConfig', 'Set the MFA configuration for a pool.'],
['CreateUserPoolClient', 'Create an app client for a pool.'],
['DescribeUserPoolClient', 'Describe an app client.'],
['ListUserPoolClients', 'List all app clients for a pool.'],
['DeleteUserPoolClient', 'Delete an app client.'],
['UpdateUserPoolClient', 'Update an app client name.'],
['AddUserPoolClientSecret', 'Generate a client secret for an app client.'],
['SignUp', 'Self-service sign-up for a new user.'],
['ConfirmSignUp', 'Confirm a user sign-up with a verification code.'],
['ResendConfirmationCode', 'Resend the confirmation code.'],
['InitiateAuth', 'Initiate user authentication (USER_PASSWORD_AUTH / REFRESH_TOKEN_AUTH).'],
['AdminInitiateAuth', 'Admin-initiate user authentication (ADMIN_USER_PASSWORD_AUTH / REFRESH_TOKEN_AUTH).'],
['AdminCreateUser', 'Admin-create a user with FORCE_CHANGE_PASSWORD status.'],
['AdminGetUser', 'Admin-get a user by username.'],
['AdminSetUserPassword', 'Admin-set a permanent or temporary password.'],
['AdminResetUserPassword', 'Reset a user back to FORCE_CHANGE_PASSWORD status.'],
['AdminConfirmSignUp', 'Admin-confirm a user without requiring a code.'],
['AdminDeleteUser', 'Admin-delete a user.'],
['AdminDisableUser', 'Disable a user account.'],
['AdminEnableUser', 'Re-enable a disabled user account.'],
['AdminUpdateUserAttributes', 'Update user attributes as admin.'],
['AdminDeleteUserAttributes', 'Delete specific user attributes as admin.'],
['AdminUserGlobalSignOut', 'Revoke all refresh tokens for a user.'],
['ListUsers', 'List users in a pool, with optional Filter parameter.'],
['GetUser', 'Get the authenticated user via access token.'],
['UpdateUserAttributes', 'Update attributes for the authenticated user.'],
['DeleteUser', 'Self-service delete the authenticated user.'],
['DeleteUserAttributes', 'Self-service delete attributes from the authenticated user.'],
['VerifyUserAttribute', 'Verify an email or phone_number attribute.'],
['ChangePassword', 'Change the authenticated user password.'],
['ForgotPassword', 'Initiate a password reset flow.'],
['ConfirmForgotPassword', 'Complete a password reset with the confirmation code.'],
['GlobalSignOut', 'Sign out the authenticated user from all sessions.'],
['RevokeToken', 'Revoke a refresh token.'],
['CreateGroup', 'Create a user pool group.'],
['GetGroup', 'Describe a single group.'],
['ListGroups', 'List all groups in a pool.'],
['DeleteGroup', 'Delete a group.'],
['UpdateGroup', 'Update group description and precedence.'],
['AdminAddUserToGroup', 'Add a user to a group.'],
['AdminRemoveUserFromGroup', 'Remove a user from a group.'],
['AdminListGroupsForUser', 'List groups a user belongs to.'],
['ListUsersInGroup', 'List users in a group.'],
['AddCustomAttributes', 'Add custom attribute definitions to a pool schema.'],
['AdminForgetDevice', 'Forget a device for a user (stub).'],
['AdminDisableProviderForUser', 'Disable a federated provider for a user (stub).'],
['GetSigningCertificate', 'Get the signing certificate stub.'],
] as Array<[string, string]> as operation}
<div class="rounded-lg border border-slate-100 p-3 dark:border-slate-700">
<p class="font-medium text-slate-900 dark:text-white text-xs">{operation[0]}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{operation[1]}</p>
</div>
{/each}
</div>
</div>
{/if}
</div>

<!-- Pool Detail / Edit Drawer -->
{#if showPoolDetail && detailPool}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-lg rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Pool Details</h2>
<div class="space-y-3 text-sm">
<div><span class="text-slate-500 dark:text-slate-400">Name:</span> <span class="font-medium text-slate-900 dark:text-white">{detailPool.Name}</span></div>
<div><span class="text-slate-500 dark:text-slate-400">Pool ID:</span> <span class="font-mono text-xs text-slate-600 dark:text-slate-300">{detailPool.ID}</span></div>
<div class="break-all"><span class="text-slate-500 dark:text-slate-400">ARN:</span> <span class="font-mono text-xs text-slate-600 dark:text-slate-300">{detailPool.ARN}</span></div>
<div>
<label class="block text-slate-500 dark:text-slate-400 mb-1" for="mfaConfig">MFA Configuration</label>
<select id="mfaConfig" bind:value={editMfaConfig} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white">
<option value="OFF">OFF</option>
<option value="OPTIONAL">OPTIONAL</option>
<option value="ON">ON</option>
</select>
</div>
</div>
<div class="mt-5 flex justify-end gap-3">
<button type="button" onclick={() => (showPoolDetail = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Cancel</button>
<button type="button" onclick={() => void savePoolMfa()} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Save</button>
</div>
</div>
</div>
{/if}

<!-- User Attributes Drawer -->
{#if showUserAttrsDrawer && drawerUser}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">User: {drawerUser.Username}</h2>
{#if drawerUser.Attributes && drawerUser.Attributes.length > 0}
<table class="w-full text-sm">
<thead><tr class="border-b border-slate-200 dark:border-slate-700"><th class="pb-2 text-left font-medium text-slate-600 dark:text-slate-400">Attribute</th><th class="pb-2 text-left font-medium text-slate-600 dark:text-slate-400">Value</th></tr></thead>
<tbody>
{#each drawerUser.Attributes as attr}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-2 text-xs font-mono text-slate-700 dark:text-slate-300">{attr.Name}</td>
<td class="py-2 text-xs text-slate-600 dark:text-slate-400 break-all">{attr.Value}</td>
</tr>
{/each}
</tbody>
</table>
{:else}
<p class="text-sm text-slate-500 dark:text-slate-400">No attributes set.</p>
{/if}
<div class="mt-4 flex justify-end">
<button type="button" onclick={() => (showUserAttrsDrawer = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Close</button>
</div>
</div>
</div>
{/if}

<!-- Group Members Drawer -->
{#if showGroupMembersDrawer && drawerGroup}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-1 text-lg font-semibold text-slate-900 dark:text-white">Group: {drawerGroup.GroupName}</h2>
<p class="mb-4 text-xs text-slate-500 dark:text-slate-400">{drawerGroup.Description ?? ''}</p>
{#if groupMembers.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No members in this group.</p>
{:else}
<table class="w-full text-sm">
<thead><tr class="border-b border-slate-200 dark:border-slate-700"><th class="pb-2 text-left font-medium text-slate-600 dark:text-slate-400">Username</th><th class="pb-2 text-left font-medium text-slate-600 dark:text-slate-400">Status</th><th class="pb-2"></th></tr></thead>
<tbody>
{#each groupMembers as member}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-2 font-medium text-slate-900 dark:text-white">{member.Username}</td>
<td class="py-2 text-xs text-slate-500 dark:text-slate-400">{member.Status}</td>
<td class="py-2 text-right">
<button type="button" onclick={() => void removeUserFromGroup(drawerGroup!.GroupName ?? '', member.Username ?? '')} class="rounded border border-red-300 px-2 py-0.5 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400">Remove</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
<div class="mt-4 flex justify-end">
<button type="button" onclick={() => (showGroupMembersDrawer = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Close</button>
</div>
</div>
</div>
{/if}

<!-- Add User to Group Modal -->
{#if showAddToGroupModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Add User to Group</h2>
<form onsubmit={(e) => { e.preventDefault(); void addUserToGroup(); }} class="space-y-4">
<input name="username" bind:value={addToGroupUsername} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Username" required />
<select bind:value={addToGroupName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" required>
<option value="">Select group…</option>
{#each groups as g}
<option value={g.GroupName}>{g.GroupName}</option>
{/each}
</select>
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showAddToGroupModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400">Cancel</button>
<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Add</button>
</div>
</form>
</div>
</div>
{/if}

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
<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Create App Client</h2>
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

