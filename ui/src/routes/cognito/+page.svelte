<script lang="ts">
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
import { getCognitoIDPClient } from '$lib/aws-client';
import {
	ListUserPoolsCommand,
	DescribeUserPoolCommand,
	ListGroupsCommand,
	ListIdentityProvidersCommand,
	ListUserPoolClientsCommand,
	type UserPoolDescriptionType,
	type GroupType,
	type ProviderDescription,
	type UserPoolClientDescription
} from '@aws-sdk/client-cognito-identity-provider';
import { toast } from 'svelte-sonner';
import { Users, Plus, RefreshCw, Search, Shield, Key, Settings, ChevronRight } from 'lucide-svelte';

const cognito = regionalClient(getCognitoIDPClient);

let userPools = $state<UserPoolDescriptionType[]>([]);
let loading = $state(true);
let search = $state('');
let activeTab = $state<'pools' | 'groups' | 'idps' | 'clients'>('pools');

let selectedPoolId = $state<string | null>(null);
let selectedPoolName = $state<string>('');
let groups = $state<GroupType[]>([]);
let idps = $state<ProviderDescription[]>([]);
let clients = $state<UserPoolClientDescription[]>([]);
let subLoading = $state(false);

onRegionChange(loadUserPools);

async function loadUserPools() {
	try {
		loading = true;
		const data = await cognito().send(new ListUserPoolsCommand({ MaxResults: 60 }));
		userPools = data.UserPools || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load user pools');
	} finally {
		loading = false;
	}
}

async function selectPool(id: string, name: string) {
	selectedPoolId = id;
	selectedPoolName = name;
	activeTab = 'groups';
	await loadPoolDetails(id);
}

async function loadPoolDetails(poolId: string) {
	subLoading = true;
	try {
		const [grpData, idpData, clientData] = await Promise.all([
			cognito().send(new ListGroupsCommand({ UserPoolId: poolId })),
			cognito().send(new ListIdentityProvidersCommand({ UserPoolId: poolId })),
			cognito().send(new ListUserPoolClientsCommand({ UserPoolId: poolId }))
		]);
		groups = grpData.Groups || [];
		idps = idpData.Providers || [];
		clients = clientData.UserPoolClients || [];
	} catch (e) {
		toast.error('Failed to load pool details');
	} finally {
		subLoading = false;
	}
}

function formatDate(date?: Date | number): string {
	if (!date) return 'N/A';
	return new Date(date).toLocaleDateString();
}

let filtered = $derived(userPools.filter(p =>
	!search || p.Name?.toLowerCase().includes(search.toLowerCase()) || p.Id?.toLowerCase().includes(search.toLowerCase())
));

let enabledCount = $derived(userPools.filter(p => p.Status !== 'Disabled').length);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Users class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito</h1>
				<p class="text-slate-600 dark:text-slate-300">User identity and access management</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button onclick={loadUserPools} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300" title="Refresh">
				<RefreshCw class="w-4 h-4" />
			</button>
			<button onclick={() => toast.info('Create user pool via AWS console')} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Create Pool
			</button>
		</div>
	</div>

	<!-- Stats Cards -->
	{#if !loading}
		<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Total Pools</p>
				<p class="text-3xl font-bold text-slate-900 dark:text-white">{userPools.length}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Active</p>
				<p class="text-3xl font-bold text-green-600 dark:text-green-400">{enabledCount}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Groups</p>
				<p class="text-3xl font-bold text-indigo-600 dark:text-indigo-400">{selectedPoolId ? groups.length : '—'}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">App Clients</p>
				<p class="text-3xl font-bold text-purple-600 dark:text-purple-400">{selectedPoolId ? clients.length : '—'}</p>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<nav class="flex gap-1 -mb-px">
			{#each [
				{ id: 'pools', label: 'User Pools', Icon: Users },
				{ id: 'groups', label: 'Groups', Icon: Shield },
				{ id: 'idps', label: 'Identity Providers', Icon: Key },
				{ id: 'clients', label: 'App Clients', Icon: Settings }
			] as tab}
				<button
					onclick={() => { activeTab = tab.id as typeof activeTab; }}
					class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200 hover:border-slate-300'}">
					<tab.Icon class="w-4 h-4" />
					{tab.label}
					{#if tab.id === 'pools' && userPools.length > 0}
						<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{userPools.length}</span>
					{/if}
				</button>
			{/each}
		</nav>
	</div>

	<!-- Tab: User Pools -->
	{#if activeTab === 'pools'}
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" placeholder="Search user pools..." bind:value={search}
				class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
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
			<p class="text-sm text-slate-500 dark:text-slate-400">{userPools.length} pool{userPools.length !== 1 ? 's' : ''} in this region</p>
			<div class="grid gap-3">
				{#each filtered as pool}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5 hover:border-indigo-300 dark:hover:border-indigo-600 transition-colors">
						<div class="flex items-start justify-between mb-3">
							<div class="flex-1 min-w-0">
								<h3 class="font-semibold text-slate-900 dark:text-white text-lg">{pool.Name}</h3>
								<p class="text-sm text-slate-500 dark:text-slate-400 font-mono mt-0.5 truncate">{pool.Id}</p>
							</div>
							<span class="ml-3 px-3 py-1 rounded-full text-xs font-medium {pool.Status === 'Disabled' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300' : 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300'}">
								{pool.Status || 'Active'}
							</span>
						</div>
						<div class="grid grid-cols-2 gap-4 text-sm mb-3">
							<div>
								<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Created</p>
								<p class="text-slate-900 dark:text-white">{formatDate(pool.CreationDate)}</p>
							</div>
							<div>
								<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Updated</p>
								<p class="text-slate-900 dark:text-white">{formatDate(pool.LastModifiedDate)}</p>
							</div>
						</div>
						<div class="flex gap-2">
							<button onclick={() => selectPool(pool.Id ?? '', pool.Name ?? '')} class="px-3 py-1.5 text-sm bg-indigo-50 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 rounded-lg hover:bg-indigo-100 dark:hover:bg-indigo-900/50 flex items-center gap-1">
								View Details <ChevronRight class="w-3 h-3" />
							</button>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- Tab: Groups -->
	{#if activeTab === 'groups'}
		{#if !selectedPoolId}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-10 text-center">
				<Shield class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3 opacity-60" />
				<p class="text-slate-600 dark:text-slate-300">Select a user pool from the <button onclick={() => activeTab = 'pools'} class="underline text-indigo-600 dark:text-indigo-400">User Pools</button> tab to view groups</p>
			</div>
		{:else if subLoading}
			<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
		{:else}
			<div class="flex items-center justify-between mb-3">
				<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Pool: <span class="text-indigo-600 dark:text-indigo-400">{selectedPoolName}</span></p>
				<button onclick={() => loadPoolDetails(selectedPoolId!)} class="text-sm text-slate-500 hover:text-slate-900 dark:hover:text-white">Refresh</button>
			</div>
			{#if groups.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-10 text-center">
					<Shield class="w-12 h-12 mx-auto text-slate-300 mb-3 opacity-60" />
					<p class="text-slate-500 dark:text-slate-400">No groups in this user pool</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-slate-50 dark:bg-slate-700">
							<tr>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Group Name</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Description</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Precedence</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Created</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each groups as g}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{g.GroupName}</td>
									<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{g.Description || '—'}</td>
									<td class="px-4 py-3 text-slate-600 dark:text-slate-300">{g.Precedence ?? '—'}</td>
									<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{formatDate(g.CreationDate)}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Tab: Identity Providers -->
	{#if activeTab === 'idps'}
		{#if !selectedPoolId}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-10 text-center">
				<Key class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3 opacity-60" />
				<p class="text-slate-600 dark:text-slate-300">Select a user pool from the <button onclick={() => activeTab = 'pools'} class="underline text-indigo-600 dark:text-indigo-400">User Pools</button> tab</p>
			</div>
		{:else if subLoading}
			<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
		{:else}
			<div class="flex items-center justify-between mb-3">
				<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Pool: <span class="text-indigo-600 dark:text-indigo-400">{selectedPoolName}</span></p>
			</div>
			{#if idps.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-10 text-center">
					<Key class="w-12 h-12 mx-auto text-slate-300 mb-3 opacity-60" />
					<p class="text-slate-500 dark:text-slate-400">No identity providers configured</p>
				</div>
			{:else}
				<div class="grid gap-3">
					{#each idps as idp}
						<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg p-4 flex items-center justify-between">
							<div>
								<p class="font-medium text-slate-900 dark:text-white">{idp.ProviderName}</p>
								<p class="text-sm text-slate-500 dark:text-slate-400">{idp.ProviderType}</p>
							</div>
							<span class="px-2 py-1 bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 rounded text-xs">{idp.ProviderType}</span>
						</div>
					{/each}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Tab: App Clients -->
	{#if activeTab === 'clients'}
		{#if !selectedPoolId}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-10 text-center">
				<Settings class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3 opacity-60" />
				<p class="text-slate-600 dark:text-slate-300">Select a user pool from the <button onclick={() => activeTab = 'pools'} class="underline text-indigo-600 dark:text-indigo-400">User Pools</button> tab</p>
			</div>
		{:else if subLoading}
			<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
		{:else}
			<div class="flex items-center justify-between mb-3">
				<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Pool: <span class="text-indigo-600 dark:text-indigo-400">{selectedPoolName}</span></p>
			</div>
			{#if clients.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-10 text-center">
					<Settings class="w-12 h-12 mx-auto text-slate-300 mb-3 opacity-60" />
					<p class="text-slate-500 dark:text-slate-400">No app clients found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-slate-50 dark:bg-slate-700">
							<tr>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Client Name</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Client ID</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each clients as c}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{c.ClientName}</td>
									<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{c.ClientId}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
