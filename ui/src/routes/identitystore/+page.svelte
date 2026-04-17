<script lang="ts">
	import { onMount } from 'svelte';
	import { getIdentityStoreClient } from '$lib/aws-client';
	import { ListUsersCommand, ListGroupsCommand, CreateUserCommand, CreateGroupCommand } from '@aws-sdk/client-identitystore';
	import { toast } from 'svelte-sonner';

	const identityStore = getIdentityStoreClient();
	const storeId = 'd-0000000000';

	let activeTab = $state<'users' | 'groups'>('users');
	let users = $state<Array<{ UserName?: string; DisplayName?: string }>>([]);
	let groups = $state<Array<{ DisplayName?: string; Description?: string }>>([]);
	let showCreateUserModal = $state(false);
	let showCreateGroupModal = $state(false);
	let userName = $state('');
	let userDisplayName = $state('');
	let groupDisplayName = $state('');

	async function loadUsers() {
		const out = await identityStore.send(new ListUsersCommand({ IdentityStoreId: storeId }));
		users = out.Users ?? [];
	}

	async function loadGroups() {
		const out = await identityStore.send(new ListGroupsCommand({ IdentityStoreId: storeId }));
		groups = out.Groups ?? [];
	}

	async function refresh() {
		try {
			await Promise.all([loadUsers(), loadGroups()]);
		} catch (err: unknown) {
			toast.error(`Failed to load identity store: ${(err as Error).message}`);
		}
	}

	async function createUser() {
		try {
			await identityStore.send(new CreateUserCommand({ IdentityStoreId: storeId, UserName: userName, DisplayName: userDisplayName }));
			showCreateUserModal = false;
			userName = '';
			userDisplayName = '';
			await loadUsers();
		} catch (err: unknown) {
			toast.error(`Failed to create user: ${(err as Error).message}`);
		}
	}

	async function createGroup() {
		try {
			await identityStore.send(new CreateGroupCommand({ IdentityStoreId: storeId, DisplayName: groupDisplayName }));
			showCreateGroupModal = false;
			groupDisplayName = '';
			await loadGroups();
		} catch (err: unknown) {
			toast.error(`Failed to create group: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void refresh();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Identity Store</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Users and groups</p>
	</div>

	<div class="flex gap-2">
		<button type="button" onclick={() => (activeTab = 'users')} class="rounded-lg border px-4 py-2 text-sm">Users</button>
		<button type="button" onclick={() => (activeTab = 'groups')} class="rounded-lg border px-4 py-2 text-sm">Groups</button>
	</div>

	{#if activeTab === 'users'}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="mb-4 flex justify-end">
				<button type="button" onclick={() => (showCreateUserModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create User</button>
			</div>
			<table class="w-full text-left text-sm">
				<tbody>
					{#if users.length === 0}
						<tr><td class="py-3 text-slate-500 dark:text-slate-400">No users found</td></tr>
					{:else}
						{#each users as user}
							<tr class="border-b border-slate-100 dark:border-slate-800"><td class="py-3">{user.UserName}</td><td class="py-3">{user.DisplayName}</td></tr>
						{/each}
					{/if}
				</tbody>
			</table>
		</div>
	{:else}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="mb-4 flex justify-end">
				<button type="button" onclick={() => (showCreateGroupModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create Group</button>
			</div>
			<table class="w-full text-left text-sm">
				<tbody>
					{#if groups.length === 0}
						<tr><td class="py-3 text-slate-500 dark:text-slate-400">No groups found</td></tr>
					{:else}
						{#each groups as group}
							<tr class="border-b border-slate-100 dark:border-slate-800"><td class="py-3">{group.DisplayName}</td><td class="py-3">{group.Description}</td></tr>
						{/each}
					{/if}
				</tbody>
			</table>
		</div>
	{/if}
</div>

{#if showCreateUserModal}
	<div id="create-user-modal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<form onsubmit={(event) => { event.preventDefault(); createUser(); }} class="space-y-4">
				<input name="user_name" bind:value={userName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Username" />
				<input name="display_name" bind:value={userDisplayName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Display name" />
				<div class="flex justify-end gap-3"><button type="button" onclick={() => (showCreateUserModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
			</form>
		</div>
	</div>
{/if}

{#if showCreateGroupModal}
	<div id="create-group-modal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<form onsubmit={(event) => { event.preventDefault(); createGroup(); }} class="space-y-4">
				<input name="display_name" bind:value={groupDisplayName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Group name" />
				<div class="flex justify-end gap-3"><button type="button" onclick={() => (showCreateGroupModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
			</form>
		</div>
	</div>
{/if}
