<script lang="ts">
import { onMount } from 'svelte';
import { getIAMClient } from '$lib/aws-client';
import {
	ListUsersCommand,
	ListRolesCommand,
	ListGroupsCommand
} from '@aws-sdk/client-iam';
import { toast } from 'svelte-sonner';
import { Users, Shield, RefreshCw, Search, UserCircle, ChevronRight } from 'lucide-svelte';

const iam = getIAMClient();

type Tab = 'users' | 'roles' | 'groups';
let tab = $state<Tab>('users');
let users = $state<any[]>([]);
let roles = $state<any[]>([]);
let groups = $state<any[]>([]);
let loading = $state(true);
let search = $state('');
let selectedItem = $state<any | null>(null);

onMount(async () => {
	await loadUsers();
});

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
	selectedItem = null;
	if (t === 'users' && users.length === 0) await loadUsers();
	else if (t === 'roles' && roles.length === 0) await loadRoles();
	else if (t === 'groups' && groups.length === 0) await loadGroups();
}

async function refresh() {
	selectedItem = null;
	if (tab === 'users') { users = []; await loadUsers(); }
	else if (tab === 'roles') { roles = []; await loadRoles(); }
	else { groups = []; await loadGroups(); }
}

let items = $derived(tab === 'users' ? users : tab === 'roles' ? roles : groups);
let filtered = $derived(items.filter((i: any) => {
	const name = getName(i);
	return !search || name.toLowerCase().includes(search.toLowerCase());
}));

function getName(i: any) { return i.UserName || i.RoleName || i.GroupName || ''; }
function getArn(i: any) { return i.Arn || ''; }
function getId(i: any) { return i.UserId || i.RoleId || i.GroupId || ''; }
function getCreatedDate(i: any) {
	const d = i.CreateDate || i.CreatedDate;
	return d ? new Date(d).toLocaleDateString() : '—';
}
function getPath(i: any) { return i.Path || '/'; }
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Shield class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">IAM</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm">Identity and Access Management</p>
			</div>
		</div>
		<button onclick={refresh} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-2">
			<RefreshCw class="w-4 h-4" />
			<span class="hidden sm:inline text-sm">Refresh</span>
		</button>
	</div>

	<!-- Stats cards -->
	<div class="grid grid-cols-3 gap-4">
		{#each [
			{ label: 'Total Users', count: users.length, icon: UserCircle, color: 'text-blue-500' },
			{ label: 'Total Roles', count: roles.length, icon: Shield, color: 'text-purple-500' },
			{ label: 'Total Groups', count: groups.length, icon: Users, color: 'text-green-500' }
		] as stat}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
				<div class="flex items-center gap-3">
					<stat.icon class="w-5 h-5 {stat.color}" />
					<div>
						<p class="text-2xl font-bold text-slate-900 dark:text-white">{stat.count}</p>
						<p class="text-xs text-slate-500 dark:text-slate-400">{stat.label}</p>
					</div>
				</div>
			</div>
		{/each}
	</div>

	<div class="flex flex-col sm:flex-row gap-3">
		<div class="flex gap-1 p-1 bg-slate-100 dark:bg-slate-800 rounded-lg">
			{#each (['users', 'roles', 'groups'] as Tab[]) as t}
				<button
					onclick={() => selectTab(t)}
					class="px-4 py-2 rounded-md text-sm font-medium transition-colors {tab === t
						? 'bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow'
						: 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
				>
					<span>{t.charAt(0).toUpperCase() + t.slice(1)}</span><span class="ml-1 text-xs opacity-60">({t === 'users' ? users.length : t === 'roles' ? roles.length : groups.length})</span>
				</button>
			{/each}
		</div>
		<div class="relative flex-1">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" placeholder="Search {tab}..." bind:value={search}
				class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
		</div>
	</div>

	<!-- Two-panel layout: list + details -->
	<div class="flex gap-4">
		<!-- List -->
		<div class="flex-1 min-w-0">
			{#if loading}
				<div class="text-center py-12 text-slate-500">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
					<p>Loading {tab}...</p>
				</div>
			{:else if filtered.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Users class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
					<p class="text-slate-500 dark:text-slate-400 font-medium">No {tab} found</p>
					{#if search}<p class="text-xs text-slate-400 mt-1">Try clearing the search filter</p>{/if}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
					<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 text-xs text-slate-500 dark:text-slate-400">
						{filtered.length} of {items.length} {tab}
					</div>
					<div class="divide-y divide-slate-200 dark:divide-slate-700 max-h-[500px] overflow-y-auto">
						{#each filtered as item}
							<button
								onclick={() => selectedItem = selectedItem === item ? null : item}
								class="w-full text-left px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-700/50 flex items-center justify-between gap-2 transition-colors
									{selectedItem === item ? 'bg-indigo-50 dark:bg-indigo-900/20' : ''}"
							>
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white text-sm truncate">{getName(item)}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 truncate font-mono">{getArn(item)}</p>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-400 shrink-0 {selectedItem === item ? 'rotate-90' : ''} transition-transform" />
							</button>
						{/each}
					</div>
				</div>
			{/if}
		</div>

		<!-- Details panel -->
		{#if selectedItem}
			<div class="w-80 shrink-0">
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5 space-y-4 sticky top-4">
					<div class="flex items-start justify-between">
						<div>
							<h3 class="font-bold text-slate-900 dark:text-white">{getName(selectedItem)}</h3>
							<span class="inline-block mt-1 px-2 py-0.5 bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 text-xs rounded capitalize">{tab.slice(0, -1)}</span>
						</div>
						<button onclick={() => selectedItem = null} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 text-lg leading-none">&times;</button>
					</div>
					<div class="space-y-3 text-sm">
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">ARN</p>
							<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all">{getArn(selectedItem)}</p>
						</div>
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">ID</p>
							<p class="text-slate-700 dark:text-slate-300 font-mono text-xs">{getId(selectedItem)}</p>
						</div>
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Path</p>
							<p class="text-slate-700 dark:text-slate-300 text-xs">{getPath(selectedItem)}</p>
						</div>
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Created</p>
							<p class="text-slate-700 dark:text-slate-300 text-xs">{getCreatedDate(selectedItem)}</p>
						</div>
					</div>
				</div>
			</div>
		{/if}
	</div>
</div>

