<script lang="ts">
	import { onMount } from 'svelte';
	import {
		createRoleAssignment,
		deleteRoleAssignment,
		isGuid,
		listPrincipals,
		listRoleAssignments,
		listRoleDefinitions,
		resolvePrincipals,
		roleNameOf,
		type DirObject,
		type RoleAssignment,
		type RoleDefinition
	} from '#lib/azureentra.ts';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { scope: string; subscription: string };
	let { scope, subscription }: Props = $props();

	let assignments = $state<RoleAssignment[]>([]);
	let defs = $state<RoleDefinition[]>([]);
	let people = $state<DirObject[]>([]);
	let names = $state<Map<string, DirObject>>(new Map());
	let loading = $state(true);
	let error = $state('');
	let roleId = $state('');
	let principal = $state('');
	let rawPrincipal = $state('');

	const chosen = $derived(rawPrincipal.trim() || principal);
	const chosenType = $derived(people.find((p) => p.id === chosen)?.type ?? '');
	const canCreate = $derived(roleId !== '' && isGuid(chosen));

	async function load() {
		try {
			[defs, assignments] = await Promise.all([listRoleDefinitions(`/subscriptions/${subscription}`), listRoleAssignments(scope)]);
			error = '';
			[people, names] = await Promise.all([
				listPrincipals().catch(() => [] as DirObject[]),
				resolvePrincipals(assignments.map((a) => a.principalId)).catch(() => new Map<string, DirObject>())
			]);
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function create(event: Event) {
		event.preventDefault();
		if (!canCreate) return;
		const type = chosenType === 'User' || chosenType === 'Group' || chosenType === 'ServicePrincipal' ? chosenType : undefined;
		const ok = await attempt(() => createRoleAssignment(scope, { roleDefinitionId: roleId, principalId: chosen, principalType: type }), 'Role assignment created', 'Failed to create role assignment');
		if (ok) {
			principal = rawPrincipal = '';
			await load();
		}
	}

	async function remove(a: RoleAssignment) {
		const label = roleNameOf(defs, a.roleDefinitionId);
		const ok = await confirmedAttempt({ title: 'Delete role assignment', message: `Delete the "${label}" assignment for ${names.get(a.principalId)?.name || a.principalId}?`, confirmLabel: 'Delete' }, () => deleteRoleAssignment(scope, a.name), 'Role assignment deleted', 'Failed to delete role assignment');
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	onMount(load);
</script>

<div class="space-y-4" data-testid="entra-roles">
	<form class="flex flex-wrap items-end gap-2" onsubmit={create} data-testid="entra-role-form">
		<label class="text-xs text-gray-500">Role
			<select class="{GCP_INPUT} mt-1 !w-64" bind:value={roleId} data-testid="entra-role-pick">
				<option value="">Choose…</option>
				{#each defs as d (d.id)}<option value={d.id}>{d.roleName}</option>{/each}
			</select>
		</label>
		<label class="text-xs text-gray-500">Principal
			<select class="{GCP_INPUT} mt-1 !w-72" bind:value={principal} data-testid="entra-principal-pick">
				<option value="">Choose…</option>
				{#each people as p (p.id)}<option value={p.id}>{p.type}: {p.name}</option>{/each}
			</select>
		</label>
		<label class="text-xs text-gray-500">or object ID<input class="{GCP_INPUT} mt-1 !w-80 font-mono" bind:value={rawPrincipal} placeholder="00000000-0000-0000-0000-000000000000" data-testid="entra-principal-raw" /></label>
		<button class={GCP_PRIMARY} type="submit" disabled={!canCreate} data-testid="entra-role-create">Add role assignment</button>
	</form>

	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="entra-roles-error">{error}</p>
	{:else if loading}
		<p class="text-sm text-gray-500">Loading…</p>
	{:else if assignments.length === 0}
		<p class="py-8 text-center text-gray-500" data-testid="entra-roles-empty">No role assignments at this scope.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="entra-assignments">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Role</th><th class={cell}>Principal</th><th class={cell}>Type</th><th class={cell}>Scope</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each assignments as a (a.name)}
						{@const who = names.get(a.principalId)}
						<tr data-testid="entra-assignment">
							<td class={cell} data-testid="entra-assignment-role">{roleNameOf(defs, a.roleDefinitionId)}</td>
							<td class={cell} data-testid="entra-assignment-principal">{who?.name || a.principalId}{#if who}<span class="block font-mono text-xs text-gray-500">{a.principalId}</span>{/if}</td>
							<td class="{cell} text-xs">{who?.type || a.principalType || '—'}</td>
							<td class="{cell} font-mono text-xs break-all">{a.scope}</td>
							<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => remove(a)} data-testid="entra-assignment-delete">Delete</button></td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
