<script lang="ts">
	import { onMount } from 'svelte';
	import { IAMClient, ListUsersCommand, ListRolesCommand, ListGroupsCommand } from '@aws-sdk/client-iam';
	import { toast } from 'svelte-sonner';
	import { Users, Plus, Shield, Lock } from 'lucide-svelte';

	let users = $state<any[]>([]);
	let roles = $state<any[]>([]);
	let groups = $state<any[]>([]);
	let activeTab = $state<'users' | 'roles' | 'groups'>('users');
	let loading = $state(true);
	let error = $state<string | null>(null);

	const iam = new IAMClient({});

	onMount(async () => {
		await loadData();
	});

	async function loadData() {
		try {
			loading = true;
			const [usersData, rolesData, groupsData] = await Promise.all([
				iam.send(new ListUsersCommand({})),
				iam.send(new ListRolesCommand({})),
				iam.send(new ListGroupsCommand({}))
			]);
			users = usersData.Users || [];
			roles = rolesData.Roles || [];
			groups = groupsData.Groups || [];
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load IAM data';
			toast.error(error);
		} finally {
			loading = false;
		}
	}
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
				<Shield class="w-6 h-6 text-red-600 dark:text-red-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Identity & Access Management</h1>
				<p class="text-slate-600 dark:text-slate-300">Users, Roles, and Groups</p>
			</div>
		</div>
	</div>

	{#if loading}
		<div class="text-center py-8 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p>Loading IAM data...</p>
		</div>
	{:else if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
			{error}
		</div>
	{:else}
		<div class="flex gap-4 border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
			<button onclick={() => activeTab = 'users'} class="px-4 py-2 font-medium {activeTab === 'users' ? 'border-b-2 border-indigo-600 text-indigo-600 dark:text-indigo-400' : 'text-slate-600 dark:text-slate-400'}">
				Users ({users.length})
			</button>
			<button onclick={() => activeTab = 'roles'} class="px-4 py-2 font-medium {activeTab === 'roles' ? 'border-b-2 border-indigo-600 text-indigo-600 dark:text-indigo-400' : 'text-slate-600 dark:text-slate-400'}">
				Roles ({roles.length})
			</button>
			<button onclick={() => activeTab = 'groups'} class="px-4 py-2 font-medium {activeTab === 'groups' ? 'border-b-2 border-indigo-600 text-indigo-600 dark:text-indigo-400' : 'text-slate-600 dark:text-slate-400'}">
				Groups ({groups.length})
			</button>
		</div>

		{#if activeTab === 'users'}
			<div class="grid gap-4">
				{#each users as user}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
						<div class="flex items-start justify-between">
							<div>
								<h3 class="font-semibold text-slate-900 dark:text-white">{user.UserName}</h3>
								<p class="text-xs text-slate-500 dark:text-slate-500 font-mono mt-1">{user.Arn}</p>
								<p class="text-sm text-slate-600 dark:text-slate-400 mt-2">Created: {new Date(user.CreateDate).toLocaleDateString()}</p>
							</div>
							<span class="px-2 py-1 bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 rounded text-xs font-medium">User</span>
						</div>
					</div>
				{/each}
			</div>
		{:else if activeTab === 'roles'}
			<div class="grid gap-4">
				{#each roles as role}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
						<div class="flex items-start justify-between">
							<div>
								<h3 class="font-semibold text-slate-900 dark:text-white">{role.RoleName}</h3>
								<p class="text-xs text-slate-500 dark:text-slate-500 font-mono mt-1">{role.Arn}</p>
								<p class="text-sm text-slate-600 dark:text-slate-400 mt-2">Created: {new Date(role.CreateDate).toLocaleDateString()}</p>
							</div>
							<span class="px-2 py-1 bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 rounded text-xs font-medium">Role</span>
						</div>
					</div>
				{/each}
			</div>
		{:else if activeTab === 'groups'}
			<div class="grid gap-4">
				{#each groups as group}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
						<div class="flex items-start justify-between">
							<div>
								<h3 class="font-semibold text-slate-900 dark:text-white">{group.GroupName}</h3>
								<p class="text-xs text-slate-500 dark:text-slate-500 font-mono mt-1">{group.Arn}</p>
								<p class="text-sm text-slate-600 dark:text-slate-400 mt-2">Created: {new Date(group.CreateDate).toLocaleDateString()}</p>
							</div>
							<span class="px-2 py-1 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 rounded text-xs font-medium">Group</span>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}
</div>
