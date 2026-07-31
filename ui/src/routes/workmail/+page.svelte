<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getWorkMailClient } from '$lib/aws-client';
	import {
		ListGroupsCommand,
		ListOrganizationsCommand,
		ListResourcesCommand,
		ListUsersCommand,
		type Group,
		type OrganizationSummary,
		type Resource,
		type User
	} from '@aws-sdk/client-workmail';
	import { toast } from 'svelte-sonner';
	import { Mail, RefreshCw, Search } from 'lucide-svelte';

	const client = regionalClient(getWorkMailClient);

	const activeStatuses = new Set<string>(['ACTIVE', 'AVAILABLE', 'ENABLED', 'RUNNING', 'COMPLETE', 'COMPLETED', 'IDLE', 'Active', 'opt-in-not-required', 'ENABLED_BY_DEFAULT']);
	function statusClass(s: unknown): string {
		return activeStatuses.has(String(s)) ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let loading = $state(false);
	let activeTab = $state<'organizations' | 'users' | 'groups' | 'resources'>('organizations');
	let searchQuery = $state('');
	let organizationsData = $state<OrganizationSummary[]>([]);
	let usersData = $state<User[]>([]);
	let groupsData = $state<Group[]>([]);
	let resourcesData = $state<Resource[]>([]);
	let orgIdFilter = $state('');

	const filteredOrganizations = $derived(organizationsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredUsers = $derived(usersData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredGroups = $derived(groupsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredResources = $derived(resourcesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			// `activeTab`/`orgIdFilter` are read with `untrack` so they never
			// become dependencies of the `onRegionChange` effect below --
			// switchTab() and the org-id input's `onchange` already write
			// these and call loadData() directly, so letting the effect also
			// depend on them would double-fetch on every region change.
			const tab = untrack(() => activeTab);
			const orgId = untrack(() => orgIdFilter);
			if (tab === 'organizations') {
				const resp = await client().send(new ListOrganizationsCommand({}));
				organizationsData = resp.OrganizationSummaries ?? [];
			}
			if (tab === 'users') {
				if (orgId) {
					const resp = await client().send(new ListUsersCommand({ OrganizationId: orgId }));
					usersData = resp.Users ?? [];
				} else {
					usersData = [];
				}
			}
			if (tab === 'groups') {
				if (orgId) {
					const resp = await client().send(new ListGroupsCommand({ OrganizationId: orgId }));
					groupsData = resp.Groups ?? [];
				} else {
					groupsData = [];
				}
			}
			if (tab === 'resources') {
				if (orgId) {
					const resp = await client().send(new ListResourcesCommand({ OrganizationId: orgId }));
					resourcesData = resp.Resources ?? [];
				} else {
					resourcesData = [];
				}
			}
		} catch (e) {
			toast.error('Failed to load Amazon WorkMail data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<Mail class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon WorkMail</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Managed business email and calendaring</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<input bind:value={orgIdFilter} onchange={loadData} placeholder="Organization ID" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-40" />
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<p class="text-xs text-gray-500 dark:text-gray-400">Enter an organization ID above to list its users, groups and resources.</p>
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['organizations', 'Organizations'], ['users', 'Users'], ['groups', 'Groups'], ['resources', 'Resources']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-green-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'organizations'}
				{#if filteredOrganizations.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No organizations found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredOrganizations as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Mail class="w-5 h-5 text-green-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Alias ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.DefaultMailDomain ?? '-'} · ${a.OrganizationId ?? ''}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'users'}
				{#if filteredUsers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No users found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredUsers as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Mail class="w-5 h-5 text-green-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Email ?? '-'} · ${a.UserRole ?? ''}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'groups'}
				{#if filteredGroups.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No groups found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredGroups as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Mail class="w-5 h-5 text-green-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Email ?? '-'}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'resources'}
				{#if filteredResources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resources found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredResources as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<Mail class="w-5 h-5 text-green-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.Type ?? '-'} · ${a.Email ?? ''}`}</p>
									</div>
								</div>
								{#if a.State}
									<span class="text-xs px-2 py-1 rounded-full shrink-0 {statusClass(a.State)}">{a.State}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
