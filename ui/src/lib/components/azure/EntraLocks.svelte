<script lang="ts">
	import { onMount } from 'svelte';
	import { LOCK_LEVELS, createLock, deleteLock, listLocks, validLockName, type Lock, type LockLevel } from '#lib/azureentra.ts';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { scope: string };
	let { scope }: Props = $props();

	let locks = $state<Lock[]>([]);
	let loading = $state(true);
	let error = $state('');
	let name = $state('');
	let level = $state<LockLevel>('CanNotDelete');
	let notes = $state('');

	async function load() {
		try {
			locks = await listLocks(scope);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function create(event: Event) {
		event.preventDefault();
		if (!validLockName(name)) return;
		const ok = await attempt(() => createLock(scope, name, level, notes), `Lock ${name} created`, `Failed to create ${name}`);
		if (ok) {
			name = notes = '';
			await load();
		}
	}

	async function remove(l: Lock) {
		const ok = await confirmedAttempt({ title: 'Delete lock', message: `Delete lock "${l.name}"? The scope can then be changed or deleted.`, confirmLabel: 'Delete' }, () => deleteLock(scope, l.name), `Lock ${l.name} deleted`, `Failed to delete ${l.name}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	onMount(load);
</script>

<div class="space-y-4" data-testid="entra-locks">
	<form class="flex flex-wrap items-end gap-2" onsubmit={create} data-testid="entra-lock-form">
		<label class="text-xs text-gray-500">Name<input class="{GCP_INPUT} mt-1" bind:value={name} data-testid="entra-lock-name" /></label>
		<label class="text-xs text-gray-500">Lock type
			<select class="{GCP_INPUT} mt-1" bind:value={level} data-testid="entra-lock-level">
				{#each LOCK_LEVELS as l (l)}<option value={l}>{l}</option>{/each}
			</select>
		</label>
		<label class="text-xs text-gray-500">Notes<input class="{GCP_INPUT} mt-1" bind:value={notes} data-testid="entra-lock-notes" /></label>
		<button class={GCP_PRIMARY} type="submit" disabled={!validLockName(name)} data-testid="entra-lock-create">Add lock</button>
	</form>

	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="entra-locks-error">{error}</p>
	{:else if loading}
		<p class="text-sm text-gray-500">Loading…</p>
	{:else if locks.length === 0}
		<p class="py-8 text-center text-gray-500" data-testid="entra-locks-empty">No locks at this scope.</p>
	{:else}
		<table class="w-full text-sm" data-testid="entra-locks-table">
			<thead class="text-left text-gray-500"><tr><th class={cell}>Name</th><th class={cell}>Lock type</th><th class={cell}>Notes</th><th class={cell}>Applies to</th><th class="{cell} text-right">Actions</th></tr></thead>
			<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
				{#each locks as l (l.id || l.name)}
					<tr data-testid="entra-lock">
						<td class="{cell} font-mono text-xs">{l.name}</td>
						<td class={cell} data-testid="entra-lock-type">{l.level}</td>
						<td class="{cell} text-xs">{l.notes || '—'}</td>
						<td class="{cell} font-mono text-xs break-all">{l.id.split('/providers/Microsoft.Authorization/')[0]}</td>
						<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => remove(l)} data-testid="entra-lock-delete">Delete</button></td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</div>
