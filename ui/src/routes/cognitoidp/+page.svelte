<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';

	type UserPool = {
		ID?: string;
		Name?: string;
	};

	let userPools = $state<UserPool[]>([]);
	let loading = $state(false);
	let showCreateModal = $state(false);
	let name = $state('');

	async function loadUserPools() {
		loading = true;
		try {
			const response = await fetch('/dashboard/api/cognitoidp/user-pools');
			userPools = ((await response.json()) as { userPools?: UserPool[] }).userPools ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load user pools: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createUserPool() {
		try {
			const response = await fetch('/dashboard/api/cognitoidp/user-pools', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ name })
			});
			if (!response.ok) {
				throw new Error(((await response.json()) as { error?: string }).error ?? 'request failed');
			}
			showCreateModal = false;
			name = '';
			await loadUserPools();
		} catch (err: unknown) {
			toast.error(`Failed to create user pool: ${(err as Error).message}`);
		}
	}

	async function deleteUserPool(poolID: string) {
		try {
			const response = await fetch(`/dashboard/api/cognitoidp/user-pools/${poolID}`, { method: 'DELETE' });
			if (!response.ok) {
				throw new Error(((await response.json()) as { error?: string }).error ?? 'request failed');
			}
			await loadUserPools();
		} catch (err: unknown) {
			toast.error(`Failed to delete user pool: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void loadUserPools();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito User Pools</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Manage user pool resources in the rewritten UI</p>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="mb-4 flex justify-end">
			<button type="button" onclick={() => (showCreateModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create User Pool</button>
		</div>
		{#if loading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading user pools...</p>
		{:else if userPools.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No Cognito user pools found</p>
		{:else}
			<table class="w-full text-left text-sm">
				<tbody>
					{#each userPools as pool}
						<tr class="border-b border-slate-100 dark:border-slate-800">
							<td class="py-3">{pool.Name}</td>
							<td class="py-3 text-xs text-slate-500 dark:text-slate-400">{pool.ID}</td>
							<td class="py-3 text-right">
								<button type="button" onclick={() => deleteUserPool(pool.ID ?? '')} class="rounded-lg border px-3 py-1 text-sm">Delete</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	</div>
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<form onsubmit={(event) => { event.preventDefault(); createUserPool(); }} class="space-y-4">
				<input name="name" bind:value={name} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="User pool name" />
				<div class="flex justify-end gap-3"><button type="button" onclick={() => (showCreateModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create User Pool</button></div>
			</form>
		</div>
	</div>
{/if}