<script lang="ts">
import { onMount } from 'svelte';
import { getIAMClient } from '$lib/aws-client';
import {
	ListUsersCommand,
	ListRolesCommand,
	ListGroupsCommand,
	ListPoliciesCommand
} from '@aws-sdk/client-iam';
import { toast } from 'svelte-sonner';
import { Users, Shield, RefreshCw, Search, UserCircle, ChevronRight, FileText, Copy } from 'lucide-svelte';

const iam = getIAMClient();

interface IamItem {
	UserName?: string;
	RoleName?: string;
	GroupName?: string;
	Arn?: string;
	UserId?: string;
	RoleId?: string;
	GroupId?: string;
	Path?: string;
	CreateDate?: Date;
}

interface IamPolicy {
	PolicyName?: string;
	Arn?: string;
	Description?: string;
	AttachmentCount?: number;
	CreateDate?: Date;
}

type Tab = 'users' | 'roles' | 'groups' | 'policies';
let tab = $state<Tab>('users');
let users = $state<IamItem[]>([]);
let roles = $state<IamItem[]>([]);
let groups = $state<IamItem[]>([]);
let policies = $state<IamPolicy[]>([]);
let loading = $state(true);
let search = $state('');
let selectedItem = $state<IamItem | null>(null);
let policyScope = $state<'Local' | 'AWS' | 'All'>('Local');

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

async function loadPolicies() {
	try {
		loading = true;
		const data = await iam.send(new ListPoliciesCommand({ Scope: policyScope }));
		policies = data.Policies || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load policies');
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
	else if (t === 'policies') await loadPolicies();
}

async function refresh() {
	selectedItem = null;
	if (tab === 'users') { users = []; await loadUsers(); }
	else if (tab === 'roles') { roles = []; await loadRoles(); }
	else if (tab === 'groups') { groups = []; await loadGroups(); }
	else { policies = []; await loadPolicies(); }
}

async function copyArn(arn: string) {
	await navigator.clipboard.writeText(arn);
	toast.success('ARN copied');
}

let items = $derived(tab === 'users' ? users : tab === 'roles' ? roles : tab === 'groups' ? groups : []);
let filteredItems = $derived(items.filter((i) => {
	const name = getName(i);
	return !search || name.toLowerCase().includes(search.toLowerCase());
}));
let filteredPolicies = $derived(policies.filter((p) =>
	!search || p.PolicyName?.toLowerCase().includes(search.toLowerCase())
));

function getName(i: IamItem | null): string { return i ? (i.UserName || i.RoleName || i.GroupName || '') : ''; }
function getArn(i: IamItem | null): string { return i?.Arn || ''; }
function getId(i: IamItem | null): string { return i ? (i.UserId || i.RoleId || i.GroupId || '') : ''; }
function getCreatedDate(i: IamItem | null): string {
	const d = i?.CreateDate;
	return d ? new Date(d).toLocaleDateString() : '—';
}
function getPath(i: IamItem | null): string { return i?.Path || '/'; }
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
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
			<div class="flex items-center gap-3">
				<UserCircle class="w-5 h-5 text-blue-500" />
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{users.length}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Total Users</p>
				</div>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
			<div class="flex items-center gap-3">
				<Shield class="w-5 h-5 text-purple-500" />
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{roles.length}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Total Roles</p>
				</div>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
			<div class="flex items-center gap-3">
				<Users class="w-5 h-5 text-green-500" />
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{groups.length}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Total Groups</p>
				</div>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
			<div class="flex items-center gap-3">
				<FileText class="w-5 h-5 text-amber-500" />
				<div>
					<p class="text-2xl font-bold text-slate-900 dark:text-white">{policies.length}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400">Policies</p>
				</div>
			</div>
		</div>
	</div>

	<div class="flex flex-col sm:flex-row gap-3">
		<div class="flex gap-1 p-1 bg-slate-100 dark:bg-slate-800 rounded-lg flex-wrap">
			{#each (['users', 'roles', 'groups', 'policies'] as Tab[]) as t}
				<button
					id="{t}-tab"
					onclick={() => selectTab(t)}
					class="px-3 py-2 rounded-md text-sm font-medium transition-colors {tab === t
						? 'bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow'
						: 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
				>
					<span>{t.charAt(0).toUpperCase() + t.slice(1)}</span>
					<span class="ml-1 text-xs opacity-60">({t === 'users' ? users.length : t === 'roles' ? roles.length : t === 'groups' ? groups.length : policies.length})</span>
				</button>
			{/each}
		</div>
		<div class="relative flex-1">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" placeholder="Search {tab}..." bind:value={search}
				class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
		</div>
		{#if tab === 'policies'}
			<select bind:value={policyScope} onchange={() => loadPolicies()} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
				<option value="Local">Customer Managed</option>
				<option value="AWS">AWS Managed</option>
				<option value="All">All Policies</option>
			</select>
		{/if}
	</div>

	<!-- Policies tab content -->
	{#if tab === 'policies'}
		{#if loading}
			<div class="text-center py-12 text-slate-500">
				<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
				<p>Loading policies...</p>
			</div>
		{:else if filteredPolicies.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
				<FileText class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
				<p id="empty-state-text" class="text-slate-500 dark:text-slate-400 font-medium">No policies found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
				<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 text-xs text-slate-500 dark:text-slate-400">
					{filteredPolicies.length} of {policies.length} policies
				</div>
				<div class="divide-y divide-slate-100 dark:divide-slate-700 max-h-[500px] overflow-y-auto">
					{#each filteredPolicies as policy}
						<div class="px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-700/50 flex items-center justify-between gap-2">
							<div class="min-w-0">
								<p class="font-medium text-slate-900 dark:text-white text-sm truncate">{policy.PolicyName}</p>
								<p class="text-xs text-slate-500 dark:text-slate-400 truncate font-mono">{policy.Arn}</p>
								{#if policy.Description}
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5 truncate">{policy.Description}</p>
								{/if}
							</div>
							<div class="flex items-center gap-2 shrink-0">
								<span class="text-xs px-2 py-0.5 bg-slate-100 dark:bg-slate-700 rounded text-slate-600 dark:text-slate-300">{policy.AttachmentCount ?? 0} attached</span>
								<button onclick={() => copyArn(policy.Arn ?? '')} class="p-1 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">
									<Copy class="w-3.5 h-3.5" />
								</button>
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
	{:else}
	<!-- Two-panel layout: list + details -->
	<div class="flex gap-4">
		<!-- List -->
		<div class="flex-1 min-w-0">
			{#if loading}
				<div class="text-center py-12 text-slate-500">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
					<p>Loading {tab}...</p>
				</div>
			{:else if filteredItems.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Users class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
					<p id="empty-state-text" class="text-slate-500 dark:text-slate-400 font-medium">No {tab} found</p>
					{#if search}<p class="text-xs text-slate-400 mt-1">Try clearing the search filter</p>{/if}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
					<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 text-xs text-slate-500 dark:text-slate-400">
						{filteredItems.length} of {items.length} {tab}
					</div>
					<div class="divide-y divide-slate-200 dark:divide-slate-700 max-h-[500px] overflow-y-auto">
						{#each filteredItems as item}
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
							<div class="flex items-start gap-1">
								<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all flex-1">{getArn(selectedItem)}</p>
								<button onclick={() => copyArn(getArn(selectedItem))} class="shrink-0 p-0.5 text-slate-400 hover:text-slate-600">
									<Copy class="w-3.5 h-3.5" />
								</button>
							</div>
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
	{/if}
</div>