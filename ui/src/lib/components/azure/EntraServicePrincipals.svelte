<script lang="ts">
	import { onMount } from 'svelte';
	import {
		createServicePrincipal,
		isGuid,
		listAppRoleAssignments,
		listApplications,
		listServicePrincipals,
		type AppRoleAssignment,
		type Application,
		type ServicePrincipal
	} from '#lib/azureentra.ts';
	import { attempt, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let sps = $state<ServicePrincipal[]>([]);
	let apps = $state<Application[]>([]);
	let loading = $state(true);
	let error = $state('');
	let appId = $state('');
	let selected = $state('');
	let assignments = $state<AppRoleAssignment[]>([]);
	let assignError = $state('');

	const free = $derived(apps.filter((a) => !sps.some((s) => s.appId === a.appId)));

	async function load() {
		try {
			[sps, apps] = await Promise.all([listServicePrincipals(), listApplications()]);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function create(event: Event) {
		event.preventDefault();
		if (!isGuid(appId)) return;
		const ok = await attempt(() => createServicePrincipal(appId), 'Service principal created', 'Failed to create service principal');
		if (ok) {
			appId = '';
			await load();
		}
	}

	async function select(s: ServicePrincipal) {
		selected = selected === s.id ? '' : s.id;
		assignments = [];
		assignError = '';
		if (!selected) return;
		try {
			assignments = await listAppRoleAssignments(s.id);
		} catch (e) {
			assignError = describeGcpError(e);
		}
	}

	const cell = 'px-4 py-3';
	onMount(load);
</script>

<div class="space-y-4" data-testid="entra-sps">
	<form class="flex flex-wrap items-end gap-2" onsubmit={create} data-testid="entra-sp-form">
		<label class="text-xs text-gray-500">Application
			<select class="{GCP_INPUT} mt-1 !w-72" onchange={(e) => (appId = e.currentTarget.value)} data-testid="entra-sp-pick">
				<option value="">Choose…</option>
				{#each free as a (a.id)}<option value={a.appId}>{a.displayName} ({a.appId})</option>{/each}
			</select>
		</label>
		<label class="text-xs text-gray-500">or application ID<input class="{GCP_INPUT} mt-1 !w-80 font-mono" bind:value={appId} placeholder="00000000-0000-0000-0000-000000000000" data-testid="entra-sp-appid" /></label>
		<button class={GCP_PRIMARY} type="submit" disabled={!isGuid(appId)} data-testid="entra-sp-create">Create service principal</button>
	</form>

	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="entra-sps-error">{error}</p>
	{:else if loading}
		<p class="text-sm text-gray-500">Loading…</p>
	{:else if sps.length === 0}
		<p class="py-8 text-center text-gray-500" data-testid="entra-sps-empty">No service principals.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="entra-sps-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Name</th><th class={cell}>Application ID</th><th class={cell}>Type</th><th class={cell}>Enabled</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each sps as s (s.id)}
						<tr data-testid="entra-sp">
							<td class={cell}><button class="underline decoration-dotted" onclick={() => select(s)} aria-expanded={selected === s.id} data-testid="entra-sp-select">{s.displayName}</button></td>
							<td class="{cell} font-mono text-xs" data-testid="entra-sp-app">{s.appId}</td>
							<td class="{cell} text-xs">{s.type || '—'}</td>
							<td class="{cell} text-xs">{s.enabled ? 'Yes' : 'No'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	{#if selected}
		<div class="rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="entra-sp-detail">
			<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">App role assignments</h3>
			{#if assignError}<p class="text-sm text-red-600" role="alert">{assignError}</p>{/if}
			{#if assignments.length === 0}
				<p class="text-sm text-gray-500" data-testid="entra-sp-assign-empty">No app role assignments.</p>
			{:else}
				<table class="w-full text-sm" data-testid="entra-sp-assignments">
					<thead class="text-left text-gray-500"><tr><th class={cell}>Principal</th><th class={cell}>Type</th><th class={cell}>Resource</th><th class={cell}>Role ID</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each assignments as a (a.id)}
							<tr data-testid="entra-sp-assignment"><td class={cell}>{a.principal}</td><td class="{cell} text-xs">{a.principalType}</td><td class={cell}>{a.resource}</td><td class="{cell} font-mono text-xs">{a.appRoleId}</td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{/if}
</div>
